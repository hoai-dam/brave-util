package brave.cache.redis;

import brave.cache.util.Tuple;
import reactor.core.publisher.Flux;

import java.util.Collection;

@FunctionalInterface
public interface ReactiveMultiLoader<K, V> {

    Flux<Tuple<K, V>> loadAll(Collection<K> keys);

}
