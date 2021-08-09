package brave.cache.codec;

import io.lettuce.core.codec.StringCodec;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public abstract class NamespaceCodec<T> implements Codec<T> {

    private final String namespace;
    private final StringCodec stringCodec;

    public NamespaceCodec(String namespace) {
        this(namespace, Charset.defaultCharset());
    }

    public NamespaceCodec(String namespace, Charset charset) {
        this.namespace = namespace;
        this.stringCodec = new StringCodec(charset);
    }

    @Override
    final public ByteBuffer encode(T obj) {
        String serializedObj = serialize(obj);
        String boxedSerializedObj = box(serializedObj);
        return stringCodec.encodeValue(boxedSerializedObj);
    }

    public abstract String serialize(T obj);

    public String box(String serializedObj) {
        return namespace + ":" + serializedObj;
    }

    @Override
    final public T decode(ByteBuffer rawObj) {
        String boxedSerializedObj = stringCodec.decodeValue(rawObj);
        String serializedObj = unbox(boxedSerializedObj);
        return deserialize(serializedObj);
    }

    public String unbox(String boxedSerializedObj) {
        return boxedSerializedObj.substring(namespace.length() + 1);
    }

    public abstract T deserialize(String serializedObj);

    public static NamespaceCodec<Long> forLong(String namespace) {
        return new NamespaceCodec<>(namespace) {
            @Override
            public Class<Long> getType() {
                return Long.class;
            }

            @Override
            public Long deserialize(String serializedObj) {
                return Long.parseLong(serializedObj);
            }

            @Override
            public String serialize(Long obj) {
                return String.valueOf(obj);
            }
        };
    }

    public static NamespaceCodec<String> forString(String namespace) {
        return new NamespaceCodec<>(namespace) {
            @Override
            public Class<String> getType() {
                return String.class;
            }

            @Override
            public String deserialize(String serializedObj) {
                return serializedObj;
            }

            @Override
            public String serialize(String obj) {
                return obj;
            }
        };
    }
}
