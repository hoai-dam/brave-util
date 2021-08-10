package brave.cache.annotation;

import brave.cache.Cache;
import brave.cache.CacheBackend;
import brave.cache.codec.Codec;
import brave.cache.local.LocalCache;
import brave.cache.redis.MultiLoader;
import brave.cache.redis.RedisCache;
import brave.cache.redis.SingleLoader;
import brave.cache.util.ConfigResolver;
import io.lettuce.core.RedisClient;
import lombok.extern.slf4j.Slf4j;
import org.cache2k.Cache2kBuilder;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.EmbeddedValueResolver;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.AbstractEnvironment;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.StringValueResolver;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static brave.cache.CacheBackend.REDIS;
import static brave.cache.CacheBackend.LOCAL;
import static brave.cache.util.ReflectionUtil.name;

@Slf4j
@SuppressWarnings("unchecked")
public class CacheableContext {

    private final Cache<?, ?> cache;

    CacheableContext(ConfigurableApplicationContext context, Object target) {
        Cacheable cacheable = target.getClass().getAnnotation(Cacheable.class);

        if (cacheable == null)
            throw new IllegalStateException("No " + Cacheable.class.getName() + " annotation found on target " + target);

        this.cache = buildCacheOnCacheableTarget(context, target);

        injectCacheToCacheableTarget(target);
    }

    private Cache<Object, Object> buildCacheOnCacheableTarget(ConfigurableApplicationContext context, Object target) {
        Cacheable cacheable = target.getClass().getAnnotation(Cacheable.class);
        ConfigResolver configResolver = getConfigResolver(context);

        SingleLoader<Object, Object> singleLoader = target instanceof SingleLoader
                ? (SingleLoader<Object, Object>) target
                : null;

        MultiLoader<Object, Object> multiLoader = target instanceof MultiLoader
                ? (MultiLoader<Object, Object>) target
                : null;

        Duration defaultTimeToLive = configResolver.getDuration(cacheable.timeToLive());
        CacheBackend backend = configResolver.getEnum(cacheable.backend(), CacheBackend.class);
        log.warn("Setting up {} with backend {}, default ttl {}", cacheable.name(), backend, defaultTimeToLive);

        if (backend == REDIS) {
            RedisClient redisClient = context.getBean(RedisClient.class);
            Codec<Object> keyCodec = (Codec<Object>) configResolver.getInstance(cacheable.redis().keyCodec());
            Codec<Object> valueCodec = (Codec<Object>) configResolver.getInstance(cacheable.redis().valueCodec());

            RedisCache.Builder<Object, Object> cacheBuilder = new RedisCache.Builder<>()
                    .redisClient(redisClient)
                    .defaultTimeToLive(defaultTimeToLive)
                    .keyCodec(keyCodec)
                    .valueCodec(valueCodec)
                    .singleLoader(singleLoader)
                    .multiLoader(multiLoader);

            return cacheBuilder.build();
        }

        if (backend == LOCAL) {
            Class<Object> keyClass = (Class<Object>) cacheable.local().keyClass();
            Class<Object> valueClass = (Class<Object>) cacheable.local().valueClass();
            Cache2kBuilder<Object, Object> cacheBuilder = Cache2kBuilder.of(keyClass, valueClass)
                    .name(cacheable.name())
                    .expireAfterWrite(defaultTimeToLive.toMillis(), TimeUnit.MILLISECONDS)
                    .entryCapacity(cacheable.local().entryCapacity())
                    .refreshAhead(cacheable.local().refreshAhead())
                    .permitNullValues(cacheable.local().permitNullValues());

            if (singleLoader != null) {
                //noinspection
                cacheBuilder.loader(singleLoader::load);
            }

            return new LocalCache<>(cacheBuilder.build());
        }

        return null;
    }

    private void injectCacheToCacheableTarget(Object target) {
        for (Field field : target.getClass().getDeclaredFields()) {
            if (field.getType() != Cache.class) continue;

            Cacheable.Inject inject = field.getAnnotation(Cacheable.Inject.class);
            if (inject == null) continue;

            boolean accessible = field.canAccess(target);
            try {
                field.setAccessible(true);
                field.set(target, cache);
            } catch (IllegalAccessException iaex) {
                throw new IllegalStateException("Cannot access field " + name(field), iaex);
            } finally {
                field.setAccessible(accessible);
            }
            break;
        }
    }

    public <K, V> Cache<K, V> getCache() {
        return (Cache<K, V>) cache;
    }

    private ConfigResolver getConfigResolver(ConfigurableApplicationContext context) {
        return new ConfigResolver(
                stringValueResolver(context),
                expressionParser(),
                context.getBean(AbstractEnvironment.class)
        );
    }

    private StringValueResolver stringValueResolver(ConfigurableApplicationContext context) {
        ConfigurableListableBeanFactory beanFactory = context.getBeanFactory();
        return new EmbeddedValueResolver(beanFactory);
    }

    private ExpressionParser expressionParser() {
        return new SpelExpressionParser();
    }


}
