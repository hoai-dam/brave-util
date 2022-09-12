package gardentest;

import brave.extension.KafkaExtension;
import brave.extension.KafkaStub;
import com.fasterxml.jackson.databind.ObjectMapper;
import garden.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.time.Duration;
import java.util.List;
import java.util.Map.Entry;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = AppConfig.class)
@ExtendWith({
        SpringExtension.class,
        KafkaExtension.class
})
public class GardenTest {

    @Autowired
    ApplicationContext context;

    @Autowired
    GardenWatcher gardenWatcher;

    @Autowired
    GardenWatcherInBatch gardenWatcherInBatch;

    @Autowired
    GardenRepo gardenRepo;

    @Autowired
    ObjectMapper objectMapper;

    @Value("${kafka.bootstrap.servers}")
    String kafkaBootstrapServers;

    @Test
    void consumerOfGardenChanges_shouldBeActive(KafkaStub kafkaStub) throws Exception {
        // Given
        kafkaStub.load("stubs/Garden/consumerOfGardenChanges_shouldBeActive/kafka");

        // When
        boolean gardenIsFull = gardenWatcher.waitUntilGardenIsFull(Duration.ofSeconds(10));

        // Then
        assertThat(gardenIsFull).isTrue();

        // Cleanup
        kafkaStub.unload("stubs/Garden/consumerOfGardenChanges_shouldBeActive/kafka");
    }

    @Test
    void batchConsumerOfGardenChanges_shouldBeActive(KafkaStub kafkaStub) throws Exception {
        // Given
        kafkaStub.load("stubs/Garden/batchConsumerOfGardenChanges_shouldBeActive/kafka");

        // When
        boolean gardenIsFull = gardenWatcherInBatch.waitUntilGardenIsFull(Duration.ofSeconds(10));

        // Then
        assertThat(gardenIsFull).isTrue();

        // Cleanup
        kafkaStub.unload("stubs/Garden/batchConsumerOfGardenChanges_shouldBeActive/kafka");
    }

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

        List<Entry<Seed, Fruit>> gardenEvents = kafkaStub.consumeMessages(
                gardenRepo.getGardenMetricTopic(), expectedFruits.size(),
                Seed.class, Fruit.class, 10000
        );

        List<Fruit> actualFruits = gardenEvents.stream()
                .map(Entry::getValue)
                .collect(toList());

        // Then
        assertThat(actualFruits).containsExactlyInAnyOrderElementsOf(expectedFruits);

        // Cleanup
        kafkaStub.unload("stubs/Garden/producerOfGardenMetrics_shouldBeActive/kafka");
    }

}
