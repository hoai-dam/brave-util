package brave.cache;

import brave.cache.annotation.CacheableContext;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;

public interface ReactiveCache<K, V> {

    Mono<V> peek(K key);

    Mono<Map<K, V>> peekAll(Collection<K> keys);

    Mono<Map<K, V>> peekAll(K[] keys);

    Mono<V> get(K key);

    Mono<Map<K, V>> getAll(Collection<K> keys);

    Mono<Map<K, V>> getAll(K[] keys);

    Mono<V> reloadIfExist(K key);

    Mono<Boolean> put(K key, V value);

    Mono<Boolean> put(K key, V value, Duration timeToLive);

    /**
     * Put the (key, value) pair in to the cache and set the expiry at a specific timestamp.
     * The timestamp is the difference in milliseconds between NOW and <code>Jan 1st, 1970UTC</code>.
     * <br>
     * @param key The key
     * @param value The value
     * @param expireAtTimestamp The expiry timestamp
     * @return Success or Failure
     */
    Mono<Boolean> put(K key, V value, long expireAtTimestamp);

    Mono<Long> remove(K[] key);

    Mono<Boolean> remove(K key);

    /**
     * Set the expiry of the specified {@param key} at a specific timestamp.
     * The timestamp is the difference in milliseconds between NOW and <code>Jan 1st, 1970UTC</code>.
     * <br>
     * @param key The key
     * @param timestamp The expiry timestamp
     * @return Success or Failure
     */
    Mono<Boolean> expireAt(K key, long timestamp);

    static <K, V> Cache<K, V> fromContext(CacheableContext cacheableContext) {
        return cacheableContext.getCache();
    }
}
