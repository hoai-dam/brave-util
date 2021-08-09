package garden;

import brave.cache.Cache;
import brave.cache.annotation.Cacheable;
import brave.cache.redis.MultiLoader;
import brave.cache.redis.SingleLoader;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

@Cacheable(
        name = "gardenCache",
        timeToLive = "PT5M",
        backend = "REDIS",
        local = @Cacheable.Local(
                keyClass = Seed.class,
                valueClass = Fruit.class
        ),
        redis = @Cacheable.Redis(
                keyCodec = "garden.SeedCodec",
                valueCodec = "garden.FruitCodec"
        ))
@Component
public class GardenRepo implements SingleLoader<Seed, Fruit>, MultiLoader<Seed, Fruit> {

    @Cacheable.Inject
    private Cache<Seed, Fruit> cache;

    public Fruit plantSingle(Seed seed) {
        if (cache == null) return load(seed);
        return cache.load(seed);
    }

    public Map<Seed, Fruit> plantMulti(Collection<Seed> seeds) {
        if (cache == null) return loadAll(seeds);
        return cache.loadAll(seeds);
    }

    @Override
    public Fruit load(Seed seed) {
        System.err.printf(">>>>>>>> Planting %s\n", seed);

        return new Fruit(seed.getName());
    }

    @Override
    public Map<Seed, Fruit> loadAll(Collection<Seed> seeds) {
        System.err.printf(">>>>>>>> Planting %s\n", seeds);

        Map<Seed, Fruit> flowers = new LinkedHashMap<>();
        for (Seed s : seeds) {
            Fruit flower = new Fruit(s.getName());
            flowers.put(s, flower);
        }

        return flowers;
    }

}
