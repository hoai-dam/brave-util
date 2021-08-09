package brave.cache.redis;

@FunctionalInterface
public interface SingleLoader<K, V> {

    V load(K key);


}


