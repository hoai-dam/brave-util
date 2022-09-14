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
@Consumers(properties = "kafka.consumer", groupId = "garden-watcher-in-batch")
public class GardenWatcherBatch {

    private final CountDownLatch fullGardenLatch = new CountDownLatch(5);

    @Consumers.Handler(properties = "replicate.garden-watcher-in-batch")
    public void processBatch(ConsumerRecords<Seed, Fruit> consumerRecords, Consumer<Seed, Fruit> consumer) {

        for (TopicPartition partition : consumerRecords.partitions()) {
            for (ConsumerRecord<Seed, Fruit> cr : consumerRecords.records(partition)) {
                log.warn("Got seed {} flower {}", cr.key(), cr.value());
                fullGardenLatch.countDown();
                if (fullGardenLatch.getCount() == 0) {
                    log.info("Garden is now full");
                }
            }
        }

        consumer.commitSync();
    }

    public boolean waitUntilGardenIsFull(Duration duration) throws InterruptedException {
        return fullGardenLatch.await(duration.toMillis(), TimeUnit.MILLISECONDS);
    }

}
