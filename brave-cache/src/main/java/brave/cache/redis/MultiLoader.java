package brave.cache.redis;

import java.util.Collection;
import java.util.Map;

@FunctionalInterface
public interface MultiLoader<K, V> {

    Map<K, V> loadAll(Collection<K> keys);

}
