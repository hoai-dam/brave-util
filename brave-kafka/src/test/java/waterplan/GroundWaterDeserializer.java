package waterplan;

import brave.kafka.serdes.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;

public class GroundWaterDeserializer extends JsonDeserializer<GroundWater> {
    public GroundWaterDeserializer() {
        super(GroundWater.class, new ObjectMapper());
    }
}
