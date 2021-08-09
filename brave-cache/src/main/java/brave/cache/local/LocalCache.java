package brave.cache.local;

import brave.cache.Cache;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class LocalCache<K, V> implements Cache<K, V> {

    private final org.cache2k.Cache<K, V> backStorage;

    public LocalCache(org.cache2k.Cache<K, V> backStorage) {
        this.backStorage = backStorage;
    }

    @Override
    public V load(K key) {
        return backStorage.get(key);
    }

    @Override
    public Map<K, V> loadAll(Collection<K> keys) {
        return backStorage.getAll(keys);
    }

    @Override
    public Map<K, V> loadAll(K[] keys) {
        return backStorage.getAll(List.of(keys));
    }

    @Override
    public V reloadIfExist(K key) {
        if (backStorage.containsAndRemove(key)) {
            return backStorage.get(key);
        }
        return null;
    }

    @Override
    public boolean put(K key, V value) {
        backStorage.put(key, value);
        return true;
    }

    @Override
    public boolean put(K key, V value, Duration timeToLive) {
        long expireAtTimestamp = System.currentTimeMillis() + timeToLive.toMillis();
        backStorage.put(key, value);
        backStorage.expireAt(key, expireAtTimestamp);
        return true;
    }

    @Override
    public boolean put(K key, V value, long expireAtTimestamp) {
        backStorage.put(key, value);
        backStorage.expireAt(key, expireAtTimestamp);
        return true;
    }

    @Override
    public final long remove(K[] keys) {
        long removed = 0;
        for (K key : keys) {
            if (backStorage.containsAndRemove(key)) {
                removed += 1;
            }
        }
        return removed;
    }

    @Override
    public boolean remove(K key) {
        return backStorage.containsAndRemove(key);
    }

    @Override
    public boolean expireAt(K key, long timestamp) {
        backStorage.expireAt(key, timestamp);
        return true;
    }
}
