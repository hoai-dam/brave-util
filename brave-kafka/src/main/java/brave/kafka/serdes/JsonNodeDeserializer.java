package brave.kafka.serdes;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class JsonNodeDeserializer implements Deserializer<JsonNode> {

    private final ObjectMapper objectMapper;

    public JsonNodeDeserializer() {
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public JsonNode deserialize(String topic, byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }

        try {
            return objectMapper.readTree(data);
        } catch (Throwable e) {
            var message = "Failed to deserialize '" + new String(data) + "'";
            throw new SerializationException(message, e);
        }
    }
}
