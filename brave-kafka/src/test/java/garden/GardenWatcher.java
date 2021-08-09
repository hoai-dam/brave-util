package garden;


import brave.kafka.BraveConsumers;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@BraveConsumers(
        groupId = "garden-watcher",
        enableAutoCommit = "false",
        properties = "kafka.consumer"
)
@Slf4j
@Component
public class GardenWatcher {

    private final CountDownLatch fullGardenLatch = new CountDownLatch(5);

    @BraveConsumers.Handler(
            keyDeserializer = "garden.SeedDeserializer",
            valueDeserializer = "garden.FruitDeserializer",
            properties = "replicate.garden-watcher"
    )
    public void process(ConsumerRecord<Seed, Fruit> cr, Consumer<Seed, Fruit> consumer) {
        log.warn("Got seed {} flower {}", cr.key(), cr.value());
        fullGardenLatch.countDown();
        if (fullGardenLatch.getCount() == 0) {
            log.info("Garden is now full");
        }
        consumer.commitSync();
    }

    public boolean waitUntilGardenIsFull(Duration duration) throws InterruptedException {
        return fullGardenLatch.await(duration.toMillis(), TimeUnit.MILLISECONDS);
    }

}
