package garden;

import brave.cache.codec.PathCodec;

public class SeedCodec extends PathCodec<Seed> {

    public SeedCodec() {
        super("experiment", "seed", "/", null);
    }

    @Override
    public Class<Seed> getType() {
        return Seed.class;
    }

    @Override
    public String serialize(Seed obj) {
        return obj.getName();
    }

    @Override
    public Seed deserialize(String serializedObj) {
        return new Seed(serializedObj);
    }
}
