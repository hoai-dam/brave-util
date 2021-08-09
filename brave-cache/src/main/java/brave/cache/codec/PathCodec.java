package brave.cache.codec;

import org.apache.commons.lang3.StringUtils;

import java.nio.charset.Charset;

public abstract class PathCodec<K> extends NamespaceCodec<K> {

    private final String splitter;
    private final String prefix;
    private final String suffix;

    public PathCodec(String namespace, String prefix, String splitter, String suffix) {
        this(namespace, prefix, splitter, suffix, Charset.defaultCharset());
    }
    public PathCodec(String namespace, String prefix, String splitter, String suffix, Charset charset) {
        super(namespace, charset);
        this.splitter = splitter;
        this.prefix = prefix;
        this.suffix = suffix;
    }

    @Override
    public String box(String serializedObj) {
        String pathSerializedObj = serializedObjToPath(serializedObj);
        return super.box(pathSerializedObj);
    }

    private String serializedObjToPath(String serializedObj) {
        return StringUtils.isBlank(suffix)
                ? prefix + splitter + serializedObj
                : prefix + splitter + serializedObj + splitter + suffix;
    }

    @Override
    public String unbox(String boxedSerializedObj) {
        String pathObj = super.unbox(boxedSerializedObj);
        return pathToSerializedObj(pathObj);
    }

    private String pathToSerializedObj(String keyPath) {
        if (StringUtils.isBlank(suffix)) {
            return toSerializedObjWithoutSuffix(keyPath);
        } else
        return toSerializedObjWithSuffix(keyPath);
    }

    private String toSerializedObjWithoutSuffix(String keyPath) {
        String[] parts = keyPath.split(splitter);
        if (parts.length != 2)
            throw new IllegalStateException("Invalid caching key: " + keyPath);

        if (parts[0].equals(prefix))
            return parts[1];

        throw new IllegalStateException("Invalid caching key: " + keyPath);
    }

    private String toSerializedObjWithSuffix(String keyPath) {
        String[] parts = keyPath.split(splitter);
        if (parts.length != 3)
            throw new IllegalStateException("Invalid caching key: " + keyPath);

        if (parts[0].equals(prefix) && parts[2].equals(suffix)) {
            return parts[1];
        }

        throw new IllegalStateException("Invalid caching key: " + keyPath);
    }

    public static PathCodec<Long> forLong(String namespace, String prefix, String splitter, String suffix) {
        return new PathCodec<>(namespace, prefix, splitter, suffix) {
            @Override
            public Class<Long> getType() {
                return long.class;
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

    public static PathCodec<String> forString(String namespace, String prefix, String splitter, String suffix) {
        return new PathCodec<>(namespace, prefix, splitter, suffix) {
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
