package gardentest;

import brave.extension.KafkaExtension;
import brave.extension.KafkaStub;
import garden.*;
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
    GardenWatcherSingle gardenWatcherSingle;

    @Autowired
    GardenWatcherBatch gardenWatcherBatch;

    @Autowired
    GardenWatcherAutoCommit gardenWatcherAutoCommit;

    @Autowired
    GardenWatcherBatchAutoCommit gardenWatcherBatchAutoCommit;

    @Test
    void consumerOfGardenChanges_shouldBeActive(KafkaStub kafkaStub) throws Exception {
        // Given
        kafkaStub.load("stubs/Garden/singleRecordConsumer/kafka");

        // When
        boolean gardenIsFull = gardenWatcherSingle.waitUntilGardenIsFull(Duration.ofSeconds(10));

        // Then
        assertThat(gardenIsFull).isTrue();
        assertThat(gardenWatcherSingle.getHarvestedFruits()).contains(
                new Fruit("Apple 0"),
                new Fruit("Mango 9"),
                new Fruit("Apple 10"),
                new Fruit("Mango 19"),
                new Fruit("Apple 20"),
                new Fruit("Mango 29"),
                new Fruit("Apple 30"),
                new Fruit("Mango 39"),
                new Fruit("Apple 40"),
                new Fruit("Mango 44")
        );

        // Cleanup
        kafkaStub.unload("stubs/Garden/singleRecordConsumer/kafka");
    }

    @Test
    void batchConsumerOfGardenChanges_shouldBeActive(KafkaStub kafkaStub) throws Exception {
        // Given
        kafkaStub.load("stubs/Garden/batchedRecordsConsumer/kafka");

        // When
        boolean gardenIsFull = gardenWatcherBatch.waitUntilGardenIsFull(Duration.ofSeconds(10));

        // Then
        assertThat(gardenIsFull).isTrue();

        // Cleanup
        kafkaStub.unload("stubs/Garden/batchedRecordsConsumer/kafka");
    }

    @Test
    void autoCommitConsumer_shouldBeActive(KafkaStub kafkaStub) throws Exception {
        // Given
        kafkaStub.load("stubs/Garden/autoCommitConsumer/kafka");

        // When
        boolean gardenIsFull = gardenWatcherAutoCommit.waitUntilGardenIsFull(Duration.ofSeconds(10));

        // Then
        assertThat(gardenIsFull).isTrue();

        // Cleanup
        kafkaStub.unload("stubs/Garden/autoCommitConsumer/kafka");
    }

    @Test
    void batchedRecordsAutoCommitConsumer_shouldBeActive(KafkaStub kafkaStub) throws Exception {
        // Given
        kafkaStub.load("stubs/Garden/batchedRecordsAutoCommitConsumer/kafka");

        // When
        boolean gardenIsFull = gardenWatcherBatchAutoCommit.waitUntilGardenIsFull(Duration.ofSeconds(10));

        // Then
        assertThat(gardenIsFull).isTrue();

        // Cleanup
        kafkaStub.unload("stubs/Garden/batchedRecordsAutoCommitConsumer/kafka");
    }

}
