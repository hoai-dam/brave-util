package brave.cache.codec;

import java.nio.ByteBuffer;

public class ByteArrayCodec implements Codec<byte[]>{
    @Override
    public Class<byte[]> getType() {
        return byte[].class;
    }

    @Override
    public byte[] decode(ByteBuffer data) {
        if (data == null) return null;

        data.rewind();

        if (data.hasArray()) {
            byte[] arr = data.array();
            if (data.arrayOffset() == 0 && arr.length == data.remaining()) {
                return arr;
            }
        }

        byte[] ret = new byte[data.remaining()];
        data.get(ret, 0, ret.length);
        data.rewind();
        return ret;
    }

    @Override
    public ByteBuffer encode(byte[] value) {
        if (value == null) return null;
        return ByteBuffer.wrap(value);
    }
}
