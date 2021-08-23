package brave.cache.redis;

import brave.cache.Cache;
import brave.cache.codec.Codec;
import brave.cache.util.CollectionUtil;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import reactor.util.annotation.NonNull;

import java.time.Duration;
import java.util.*;
import java.util.function.Supplier;

import static java.util.Collections.emptyMap;

@Slf4j
@RequiredArgsConstructor
public class RedisCache<K, V> implements Cache<K, V>, AutoCloseable{

    private final Supplier<StatefulRedisConnection<K, V>> redisConnectionSupplier;
    private final SingleLoader<K, V> singleLoader;
    private final MultiLoader<K, V> multiLoader;
    private final Class<K> keyClass;
    private final long defaultTimeToLiveMillis;

    private StatefulRedisConnection<K, V> connection;
    private RedisCommands<K, V> syncCommands;

    @NonNull
    @SneakyThrows
    private StatefulRedisConnection<K, V> connection() {
        if (connection == null) {
            connection = redisConnectionSupplier.get();
        }

        return connection;
    }

    @NonNull
    @SneakyThrows
    private RedisCommands<K, V> syncCommands() {
        if (syncCommands == null) {
            syncCommands = connection().sync();
        }

        return syncCommands;
    }

    @Override
    public V load(K key) {
        V value = syncCommands().get(key);
        if (value != null) return value;
        return loadAndCache(key);
    }

    @Override
    public Map<K, V> loadAll(Collection<K> keys) {
        K[] keysArray = CollectionUtil.toArray(keyClass, keys);
        return loadAll(keysArray);
    }

    @Override
    public Map<K, V> loadAll(K[] keys) {
        HashMap<K, V> keyValues = new HashMap<>();
        List<K> missingKeys = new ArrayList<>();

        for (KeyValue<K, V> kv : syncCommands().mget(keys)) {
            K k = kv.getKey();
            if (kv.hasValue()) {
                keyValues.put(k, kv.getValue());
            } else {
                missingKeys.add(k);
            }
        }

        if (missingKeys.size() > 0) {
            keyValues.putAll(loadAndCache(missingKeys));
        }

        return keyValues;
    }

    @Override
    public V reloadIfExist(K key) {
        V value = syncCommands().get(key);
        if (value != null) return loadAndCache(key);
        return null;
    }

    private V loadAndCache(K key) {
        if (singleLoader == null) return null;

        V value = singleLoader.load(key);
        if (value == null) return null;

        put(key, value);
        return value;
    }

    private Map<K, V> loadAndCache(List<K> missingKeys) {
        if (multiLoader == null) return emptyMap();
        Map<K, V> keyValues = multiLoader.loadAll(missingKeys);

        for (var kv : keyValues.entrySet()) {
            syncCommands().psetex(kv.getKey(), defaultTimeToLiveMillis, kv.getValue());
        }

        return keyValues;
    }

    @Override
    public boolean put(K key, V value) {
        return putTimeToLiveMillis(key, value, defaultTimeToLiveMillis);
    }

    @Override
    public boolean put(K key, V value, Duration timeToLive) {
        return putTimeToLiveMillis(key, value, timeToLive.toMillis());
    }

    @Override
    public boolean put(K key, V value, long expireAtTimestamp) {
        long timeToLiveMillis = Math.max(expireAtTimestamp - System.currentTimeMillis(), 1);
        return putTimeToLiveMillis(key, value, timeToLiveMillis);
    }

    private boolean putTimeToLiveMillis(K key, V value, long timeToLiveMillis) {
        String reply = syncCommands().psetex(key, timeToLiveMillis, value);
        if ("OK".equals(reply)) return true;
        log.warn("Failed to put key {}: {}", key, reply);
        return false;
    }

    @Override
    public boolean expireAt(K key, long timestamp) {
        return syncCommands().expireat(key, timestamp);
    }

    @Override
    public final long remove(K[] keys) {
        return syncCommands().del(keys);
    }

    @Override
    public boolean remove(K key) {
        //noinspection unchecked
        return syncCommands().del(key) == 1;
    }

    @Override
    public void close() {
        StatefulRedisConnection<K, V> capturedConnection = this.connection;
        this.connection = null;
        if (capturedConnection.isOpen()) {
            capturedConnection.close();
        }
    }

    /**
     * The only way to capture type info is to make this class pseudo ABSTRACT.
     * Every new implementation of this pseudo ABSTRACT class will hold the type info
     * of {@link K} and {@link V}.
     *
     * @param <K>
     * @param <V>
     */
    @Setter
    @Accessors(fluent = true, chain = true)
    public static class Builder<K, V> {

        private Duration defaultTimeToLive;
        private SingleLoader<K, V> singleLoader;
        private MultiLoader<K, V> multiLoader;
        private Codec<K> keyCodec;
        private Codec<V> valueCodec;
        private RedisClient redisClient;

        public RedisCache<K, V> build() {
            long defaultTimeToLiveMillis = defaultTimeToLive != null
                    ? defaultTimeToLive.toMillis()
                    : Long.MAX_VALUE;

            RedisCodecImpl<K, V> redisCodec = new RedisCodecImpl<>(keyCodec, valueCodec);

            return new RedisCache<>(
                    () -> redisClient.connect(redisCodec),
                    singleLoader,
                    multiLoader,
                    keyCodec.getType(),
                    defaultTimeToLiveMillis
            );
        }
    }

}
