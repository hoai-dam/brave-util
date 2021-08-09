package gardentest;

import garden.Fruit;
import garden.GardenInvalidator;
import garden.GardenRepo;
import garden.Seed;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.List;
import java.util.Map;

@SpringBootTest(classes = AppConfig.class)
@ExtendWith(SpringExtension.class)
public class GardenTest {

    @Autowired
    ApplicationContext context;

    @Test
    void test() {
        GardenRepo gardenRepo = context.getBean(GardenRepo.class);

        Fruit apple = gardenRepo.plantSingle(new Seed("apple"));
        System.err.printf("1. Got %s\n", apple);

        Fruit banana = gardenRepo.plantSingle(new Seed("banana"));
        System.err.printf("2. Got %s\n", banana);

        Map<Seed, Fruit> flowers = gardenRepo.plantMulti(List.of(new Seed("apple"), new Seed("orange")));
        System.err.printf("3. Got %s\n", Map.copyOf(flowers));

        apple = gardenRepo.plantSingle(new Seed("apple"));
        System.err.printf("4. Got %s\n", apple);

        Fruit orange = gardenRepo.plantSingle(new Seed("orange"));
        System.err.printf("5. Got %s\n", orange);

        GardenInvalidator gardenInvalidator = context.getBean(GardenInvalidator.class);
        gardenInvalidator.uproot(new Seed("apple"));
        apple = gardenRepo.plantSingle(new Seed("apple"));
        System.err.printf("6. Got %s\n", apple);

        gardenInvalidator.refresh(new Seed("orange"));
        orange = gardenRepo.plantSingle(new Seed("orange"));
        System.err.printf("7. Got %s\n", orange);

        gardenInvalidator.refresh(new Seed("kiwi"));
        orange = gardenRepo.plantSingle(new Seed("kiwi"));
        System.err.printf("8. Got %s\n", orange);
    }
}
