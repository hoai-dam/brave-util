package brave.cache.redis;

import brave.cache.codec.Codec;
import io.lettuce.core.codec.RedisCodec;

import java.nio.ByteBuffer;

public class RedisCodecImpl<K, V> implements RedisCodec<K, V> {

    private final Codec<K> keyCodec;
    private final Codec<V> valueCodec;

    public RedisCodecImpl(Codec<K> keyCodec, Codec<V> valueCodec) {
        this.keyCodec = keyCodec;
        this.valueCodec = valueCodec;
    }

    @Override
    public K decodeKey(ByteBuffer bytes) {
        return keyCodec.decode(bytes);
    }

    @Override
    public V decodeValue(ByteBuffer bytes) {
        return valueCodec.decode(bytes);
    }

    @Override
    public ByteBuffer encodeKey(K key) {
        return keyCodec.encode(key);
    }

    @Override
    public ByteBuffer encodeValue(V value) {
        return valueCodec.encode(value);
    }
}
