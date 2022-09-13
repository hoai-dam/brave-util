package gardentest;

import brave.extension.KafkaExtension;
import brave.extension.KafkaStub;
import garden.GardenWatcher;
import garden.GardenWatcherInBatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = AppConfig.class)
@ExtendWith({
        SpringExtension.class,
        KafkaExtension.class
})
public class ConsumersTest {

    @Autowired
    GardenWatcher gardenWatcher;

    @Autowired
    GardenWatcherInBatch gardenWatcherInBatch;

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

}
