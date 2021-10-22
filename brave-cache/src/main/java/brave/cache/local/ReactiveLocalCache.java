package brave.cache.local;

import brave.cache.ReactiveCache;
import lombok.extern.slf4j.Slf4j;
import org.cache2k.Cache;
import org.cache2k.CacheException;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

@Slf4j
public class ReactiveLocalCache<K, V> implements ReactiveCache<K, V> {

    private final org.cache2k.Cache<K, V> backStorage;

    public ReactiveLocalCache(Cache<K, V> backStorage) {
        this.backStorage = backStorage;
    }

    @Override
    public Mono<V> peek(K key) {
        return Mono.fromCallable(() -> backStorage.peek(key));
    }

    @Override
    public Mono<Map<K, V>> peekAll(Collection<K> keys) {
        return Mono.fromCallable(() -> backStorage.peekAll(keys));
    }

    @Override
    public Mono<Map<K, V>> peekAll(K[] keys) {
        return Mono.fromCallable(() -> backStorage.peekAll(Arrays.asList(keys)));
    }

    @Override
    public Mono<V> get(K key) {
        return Mono.fromCallable(() -> backStorage.get(key));
    }

    @Override
    public Mono<Map<K, V>> getAll(Collection<K> keys) {
        return Mono.fromCallable(() -> backStorage.getAll(keys));
    }

    @Override
    public Mono<Map<K, V>> getAll(K[] keys) {
        return Mono.fromCallable(() -> backStorage.getAll(Arrays.asList(keys)));
    }

    @Override
    public Mono<V> reloadIfExist(K key) {
        return Mono.fromCallable(() -> {
            if (backStorage.containsAndRemove(key)) {
                return backStorage.get(key);
            }
            return null;
        });
    }

    @Override
    public Mono<Boolean> put(K key, V value) {
        return Mono.fromCallable(() -> {
            backStorage.put(key, value);
            return true;
        }).onErrorResume(CacheException.class, caex -> {
            log.error("Failed to put {}", key, caex);
            return Mono.just(false);
        });
    }

    @Override
    public Mono<Boolean> put(K key, V value, Duration timeToLive) {
        return Mono.fromCallable(() -> {
            long expireAtTimestamp = System.currentTimeMillis() + timeToLive.toMillis();
            backStorage.put(key, value);
            backStorage.expireAt(key, expireAtTimestamp);
            return true;
        }).onErrorResume(CacheException.class, caex -> {
            log.error("Failed to put {}", key, caex);
            return Mono.just(false);
        });
    }

    @Override
    public Mono<Boolean> put(K key, V value, long expireAtTimestamp) {
        return Mono.fromCallable(() -> {
            backStorage.put(key, value);
            backStorage.expireAt(key, expireAtTimestamp);
            return true;
        }).onErrorResume(CacheException.class, caex -> {
            log.error("Failed to put {}", key, caex);
            return Mono.just(false);
        });
    }

    @Override
    public Mono<Long> remove(K[] keys) {
        return Mono.fromCallable(() -> {
            long removed = 0;
            for (K key : keys) {
                if (backStorage.containsAndRemove(key)) {
                    removed += 1;
                }
            }
            return removed;
        });
    }

    @Override
    public Mono<Boolean> remove(K key) {
        return Mono.fromCallable(() -> backStorage.containsAndRemove(key));
    }

    @Override
    public Mono<Boolean> expireAt(K key, long timestampMillis) {
        return Mono.fromCallable(() -> {
            backStorage.expireAt(key, timestampMillis);
            return true;
        });
    }
}
