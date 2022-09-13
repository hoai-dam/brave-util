package gardentest;

import brave.extension.KafkaExtension;
import brave.extension.KafkaStub;
import garden.Fruit;
import garden.GardenRepo;
import garden.Seed;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = AppConfig.class)
@ExtendWith({
        SpringExtension.class,
        KafkaExtension.class
})
public class ProducersTest {


    @Autowired
    GardenRepo gardenRepo;

    @Test
    void producerOfGardenMetrics_shouldBeActive(KafkaStub kafkaStub) throws Exception {
        // Given
        kafkaStub.load("stubs/Garden/producerOfGardenMetrics_shouldBeActive/kafka");

        // When
        List<Fruit> expectedFruits = List.of(
                gardenRepo.plant(new Seed("Apple")),
                gardenRepo.plant(new Seed("Banana")),
                gardenRepo.plant(new Seed("Orange")),
                gardenRepo.plant(new Seed("Kiwi")),
                gardenRepo.plant(new Seed("Watermelon"))
        );

        List<Map.Entry<Seed, Fruit>> gardenEvents = kafkaStub.consumeMessages(
                gardenRepo.getGardenMetricTopic(), expectedFruits.size(),
                Seed.class, Fruit.class, 10000
        );

        List<Fruit> actualFruits = gardenEvents.stream()
                .map(Map.Entry::getValue)
                .collect(toList());

        // Then
        assertThat(actualFruits).containsExactlyInAnyOrderElementsOf(expectedFruits);

        // Cleanup
        kafkaStub.unload("stubs/Garden/producerOfGardenMetrics_shouldBeActive/kafka");
    }
}
