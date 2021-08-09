package brave.cache;

import brave.cache.annotation.CacheableContext;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;

/**
 * Partly extracted from the interface {@link org.cache2k.Cache}.
 * @param <K>
 * @param <V>
 */
public interface Cache<K, V> {

    V load(K key);

    Map<K, V> loadAll(Collection<K> keys);

    Map<K, V> loadAll(K[] keys);

    V reloadIfExist(K key);

    boolean put(K key, V value);

    boolean put(K key, V value, Duration timeToLive);

    /**
     * Put the (key, value) pair in to the cache and set the expiry at a specific timestamp.
     * The timestamp is the difference in milliseconds between NOW and <code>Jan 1st, 1970UTC</code>.
     * <br>
     * @param key The key
     * @param value The value
     * @param expireAtTimestamp The expiry timestamp
     * @return Success or Failure
     */
    boolean put(K key, V value, long expireAtTimestamp);

    long remove(K[] key);

    boolean remove(K key);

    boolean expireAt(K key, long timestamp);

    static <K, V> Cache<K, V> fromContext(CacheableContext cacheableContext) {
        return cacheableContext.getCache();
    }
}
