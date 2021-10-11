package brave.cache.redis;

import reactor.core.publisher.Mono;

@FunctionalInterface
public interface ReactiveSingleLoader<K, V> {

    Mono<V> load(K key);

}
