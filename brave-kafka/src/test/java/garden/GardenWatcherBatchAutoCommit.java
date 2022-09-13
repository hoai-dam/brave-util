package garden;


import brave.kafka.Consumers;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@Consumers(
        bootstrapServers = "${kafka.bootstrap.servers}",
        groupId = "garden-batch-auto-watcher",
        enableAutoCommit = true,
        autoCommitIntervalMillis = 2500,
        autoOffsetReset = "earliest",
        maxPollIntervalMillis = 60000,
        maxPollRecords = 50,
        sessionTimeoutMillis = 15000
)
@SuppressWarnings("DefaultAnnotationParam")
public class GardenWatcherBatchAutoCommit {

    private final CountDownLatch fullGardenLatch = new CountDownLatch(5);

    @Consumers.Handler(
            topics = "connect.garden.changes.batch.auto.commit",
            threadsCount = 2,
            pollingTimeoutMillis = 1500,
            ignoreException = true,
            reportHealthCheck = false,
            keyDeserializer = garden.SeedDeserializer.class,
            valueDeserializer = garden.FruitDeserializer.class
    )
    public void processBatch(ConsumerRecords<Seed, Fruit> consumerRecords) {

        for (TopicPartition partition : consumerRecords.partitions()) {
            for (ConsumerRecord<Seed, Fruit> cr : consumerRecords.records(partition)) {
                log.warn("Got seed {} flower {}", cr.key(), cr.value());
                fullGardenLatch.countDown();
                if (fullGardenLatch.getCount() == 0) {
                    log.info("Garden is now full");
                }
            }
        }
    }

    public boolean waitUntilGardenIsFull(Duration duration) throws InterruptedException {
        return fullGardenLatch.await(duration.toMillis(), TimeUnit.MILLISECONDS);
    }

}
