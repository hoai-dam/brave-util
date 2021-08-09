package brave.cache.codec;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

import java.nio.ByteBuffer;

@RequiredArgsConstructor
public class JacksonCodec<T> implements Codec<T> {

    private final ObjectMapper objectMapper;
    private final Class<T> type;

    @Override
    public Class<T> getType() {
        return type;
    }

    @Override
    @SneakyThrows
    public T decode(ByteBuffer bytes) {
        try (var byteBufferBackedInputStream = new ByteBufferBackedInputStream(bytes)) {
            return objectMapper.readValue(byteBufferBackedInputStream, type);
        }
    }

    @Override
    @SneakyThrows
    public ByteBuffer encode(T key) {
        return ByteBuffer.wrap(objectMapper.writeValueAsBytes(key));
    }

}
