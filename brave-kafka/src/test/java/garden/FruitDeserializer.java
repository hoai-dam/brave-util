package garden;

import brave.kafka.serdes.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;

public class FruitDeserializer extends JsonDeserializer<Fruit> {
    public FruitDeserializer() {
        super(Fruit.class, new ObjectMapper());
    }
}
