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
            System.err.printf("\n>>>>>>>> Uprooting %s\n", flowerSeed);
            cache.remove(flowerSeed);
        }
    }

    public void refresh(Seed flowerSeed) {
        if (cache != null) {
            System.err.printf("\n>>>>>>>> Refreshing %s\n", flowerSeed);
            cache.reloadIfExist(flowerSeed);
        }
    }
}
