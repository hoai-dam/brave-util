package waterplan;

import brave.kafka.serdes.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SurfaceWaterDeserializer extends JsonDeserializer<SurfaceWater> {
    public SurfaceWaterDeserializer() {
        super(SurfaceWater.class, new ObjectMapper());
    }

}
