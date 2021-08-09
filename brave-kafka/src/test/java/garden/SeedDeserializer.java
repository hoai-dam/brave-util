package garden;

import brave.kafka.serdes.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SeedDeserializer extends JsonDeserializer<Seed> {

    public SeedDeserializer() {
        super(Seed.class, new ObjectMapper());
    }
}
