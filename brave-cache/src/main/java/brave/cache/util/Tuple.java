package brave.cache.util;

import io.lettuce.core.KeyValue;

import java.util.NoSuchElementException;

public class Tuple<K, V> {

    private final K key;
    private final V value;

    private Tuple(K key, V value) {
        if (key == null) {
            throw new IllegalArgumentException("Key must not be null");
        }
        this.key = key;
        this.value = value;
    }

    public static <K, V> Tuple<K, V> tuple(K key) {
        return new Tuple<>(key, null);
    }

    public static <K, V> Tuple<K, V> tuple(K key, V value) {
        return new Tuple<>(key, value);
    }

    public static <K, V> Tuple<K, V> tuple(KeyValue<K, V> keyValue) {
        return new Tuple<>(keyValue.getKey(), keyValue.getValueOrElse(null));
    }

    @Override
    public int hashCode() {
        int result = key.hashCode();
        result = 31 * result + (hasValue() ? getValue().hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return hasValue() ? String.format("Tuple(%s, %s)", key, getValue()) : String.format("KeyValue[%s].empty", key);
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        if (!hasValue()) {
            throw new NoSuchElementException("No value presents");
        }

        return value;
    }

    public V getValueOrElse(V other) {
        if (hasValue()) {
            return this.value;
        }

        return other;
    }

    public boolean hasValue() {
        return value != null;
    }

    public boolean noValue() {
        return value == null;
    }

}
