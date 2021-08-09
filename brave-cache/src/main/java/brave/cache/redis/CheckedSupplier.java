package brave.cache.redis;

@FunctionalInterface
public interface CheckedSupplier<T> {

    T get() throws Throwable;

}
