package brave.cache.codec;

import java.nio.ByteBuffer;

public interface Codec<T> {

    Class<T> getType();

    T decode(ByteBuffer bytes);

    ByteBuffer encode(T value);

}
