package brave.cache.redis;

import brave.cache.ReactiveCache;
import brave.cache.codec.Codec;
import brave.cache.util.CollectionUtil;
import brave.cache.util.Tuple;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisException;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import static java.util.Collections.emptyMap;

@Slf4j
@RequiredArgsConstructor
public class ReactiveRedisCache<K, V> implements ReactiveCache<K, V>, AutoCloseable {

    private final StatefulRedisConnection<K, V> connection;
    private final ReactiveSingleLoader<K, V> singleLoader;
    private final ReactiveMultiLoader<K, V> multiLoader;
    private final Class<K> keyClass;
    private final long defaultTimeToLiveMillis;

    private RedisReactiveCommands<K, V> reactive() {
        return connection.reactive();
    }

    @Override
    public Mono<V> peek(K key) {
        return reactive().get(key)
                .onErrorResume(RedisException.class, reex -> {
                    log.error("Failed to execute redis 'get' {}", key, reex);
                    return Mono.empty();
                });
    }

    @Override
    public Mono<Map<K, V>> peekAll(Collection<K> keys) {
        K[] keysArray = CollectionUtil.toArray(keyClass, keys);
        return peekAll(keysArray);
    }

    @Override
    public Mono<Map<K, V>> peekAll(K[] keys) {
        return reactive()
                .mget(keys)
                .collectMap(KeyValue::getKey, kv -> kv.getValueOrElse(null))
                .onErrorResume(RedisException.class, reex -> {
                    log.error("Failed to execute redis 'mget' {} keys", keys.length, reex);
                    return Mono.just(emptyMap());
                });
    }

    @Override
    public Mono<V> get(K key) {
        return reactive()
                .get(key)
                .switchIfEmpty(Mono.defer(() -> loadAndCache(key)))
                .onErrorResume(RedisException.class, reex -> {
                    log.error("Failed to execute redis 'get' {}", key, reex);
                    return load(key);
                });
    }

    @Override
    public Mono<Map<K, V>> getAll(Collection<K> keys) {
        K[] keysArray = CollectionUtil.toArray(keyClass, keys);
        return getAll(keysArray);
    }

    @Override
    public Mono<Map<K, V>> getAll(K[] keys) {
        Flux<KeyValue<K, V>> keyValuesF = reactive().mget(keys).cache();

        Flux<Tuple<K, V>> hittingKeyValuesF = keyValuesF.filter(KeyValue::hasValue)
                .map(Tuple::tuple);

        Flux<Tuple<K, V>> missingKeyValuesF = keyValuesF.filter(KeyValue::isEmpty)
                .map(KeyValue::getKey)
                .collectList()
                .flatMapMany(this::loadAllAndCache);

        return hittingKeyValuesF.concatWith(missingKeyValuesF)
                .collectMap(Tuple::getKey, kv -> kv.getValueOrElse(null))
                .onErrorResume(RedisException.class, reex -> {
                    log.error("Failed to execute redis 'mget' {} keys", keys.length, reex);
                    return loadAll(keys);
                });
    }

    @Override
    public Mono<V> reloadIfExist(K key) {
        return reactive().get(key)
                .flatMap(oldValue -> loadAndCache(key))
                .onErrorResume(RedisException.class, reex -> {
                    log.error("Failed to execute redis 'get' {}", key, reex);
                    return Mono.empty();
                });
    }


    @Override
    public Mono<Boolean> put(K key, V value) {
        return putTimeToLiveMillis(key, value, defaultTimeToLiveMillis);
    }

    @Override
    public Mono<Boolean> put(K key, V value, Duration timeToLive) {
        return putTimeToLiveMillis(key, value, timeToLive.toMillis());
    }

    @Override
    public Mono<Boolean> put(K key, V value, long expireAtTimestamp) {
        long timeToLiveMillis = Math.max(expireAtTimestamp - System.currentTimeMillis(), 1);
        return putTimeToLiveMillis(key, value, timeToLiveMillis);
    }

    private Mono<Boolean>  putTimeToLiveMillis(K key, V value, long timeToLiveMillis) {
        return reactive()
                .psetex(key, timeToLiveMillis, value)
                .map("OK"::equals)
                .onErrorResume(RedisException.class, reex -> {
                    log.error("Failed to execute redis 'psetex' {}", key, reex);
                    return Mono.just(false);
                });
    }

    @Override
    public Mono<Long> remove(K[] keys) {
        return reactive()
                .del(keys)
                .onErrorResume(RedisException.class, reex -> {
                    log.error("Failed to execute redis 'del' {} keys", keys.length, reex);
                    return Mono.just(0L);
                });
    }

    @Override
    public Mono<Boolean> remove(K key) {
        //noinspection unchecked
        return reactive()
                .del(key)
                .map(deletedCount -> deletedCount == 1)
                .onErrorResume(RedisException.class, reex -> {
                    log.error("Failed to execute redis 'del' {}", key, reex);
                    return Mono.just(false);
                });
    }

    @Override
    public Mono<Boolean> expireAt(K key, long timestamp) {
        return reactive()
                .expireat(key, timestamp)
                .onErrorResume(RedisException.class, reex -> {
                    log.error("Failed to execute redis 'expireat' {}", key, reex);
                    return Mono.just(false);
                });
    }

    @Override
    public void close() {
        StatefulRedisConnection<K, V> capturedConnection = this.connection;
        if (capturedConnection.isOpen()) {
            capturedConnection.close();
        }
    }

    private Mono<V> load(K key) {
        if (singleLoader == null) return Mono.empty();
        return singleLoader.load(key);
    }

    private Mono<V> loadAndCache(K key) {
        if (singleLoader == null) return Mono.empty();
        return singleLoader
                .load(key)
                .doOnNext(value ->
                        put(key, value).subscribe());
    }

    private Mono<Map<K, V>> loadAll(K[] keys) {
        if (keys.length == 0 || multiLoader == null) return Mono.just(emptyMap());

        return multiLoader
                .loadAll(Arrays.asList(keys))
                .collectMap(Tuple::getKey, t -> t.getValueOrElse(null));
    }

    private Flux<Tuple<K, V>> loadAllAndCache(Collection<K> keys) {
        if (keys.isEmpty() || multiLoader == null) return Flux.empty();

        return multiLoader.loadAll(keys)
                .doOnNext(t -> {
                    if (t.hasValue()) {
                        put(t.getKey(), t.getValue()).subscribe();
                    }
                });
    }

    @Setter
    @Accessors(fluent = true, chain = true)
    public static class Builder<K, V> {

        private Duration defaultTimeToLive;
        private ReactiveSingleLoader<K, V> singleLoader;
        private ReactiveMultiLoader<K, V> multiLoader;
        private Codec<K> keyCodec;
        private Codec<V> valueCodec;
        private RedisClient redisClient;

        public ReactiveRedisCache<K, V> build() {
            long defaultTimeToLiveMillis = defaultTimeToLive != null
                    ? defaultTimeToLive.toMillis()
                    : Long.MAX_VALUE;

            RedisCodecImpl<K, V> redisCodec = new RedisCodecImpl<>(keyCodec, valueCodec);

            return new ReactiveRedisCache<>(
                    redisClient.connect(redisCodec),
                    singleLoader,
                    multiLoader,
                    keyCodec.getType(),
                    defaultTimeToLiveMillis
            );
        }
    }
}
