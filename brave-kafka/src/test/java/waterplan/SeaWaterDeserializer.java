package waterplan;

import brave.kafka.serdes.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SeaWaterDeserializer extends JsonDeserializer<SeaWater> {
    public SeaWaterDeserializer() {
        super(SeaWater.class, new ObjectMapper());
    }

}
