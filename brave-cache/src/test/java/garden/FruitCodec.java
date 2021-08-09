package garden;

import brave.cache.codec.JacksonCodec;
import com.fasterxml.jackson.databind.ObjectMapper;

public class FruitCodec extends JacksonCodec<Fruit> {

    public FruitCodec() {
        super(new ObjectMapper(), Fruit.class);
    }
}
