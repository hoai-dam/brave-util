package garden;

import brave.cache.Cache;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
public class GardenInvalidator {

    private final Cache<Seed, Fruit> cache;

    public GardenInvalidator(
            @Autowired(required = false) @Qualifier("gardenCache") Cache<Seed, Fruit> cache
    ) {
        this.cache = cache;
    }

    public void uproot(Seed flowerSeed) {
        if (cache != null) {
            if (cache.remove(flowerSeed)) {
                System.err.printf("\n>>>>>>>> Uprooted %s\n", flowerSeed);
            }
        }
    }

    public void refresh(Seed flowerSeed) {
        if (cache != null) {
            Fruit freshFruit = cache.reloadIfExist(flowerSeed);
            System.err.printf("\n>>>>>>>> Refreshed %s and got %s\n", flowerSeed, freshFruit);
        }
    }
}
