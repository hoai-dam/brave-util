package garden;


import brave.kafka.Consumers;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Consumers(
        properties = "kafka.consumer",
        groupId = "garden-watcher",
        enableAutoCommit = false
)
@Slf4j
@Component
public class GardenWatcher {

    private final CountDownLatch fullGardenLatch = new CountDownLatch(5);

    @Consumers.Handler(
            properties = "replicate.garden-watcher",
            keyDeserializer = garden.SeedDeserializer.class,
            valueDeserializer = garden.FruitDeserializer.class
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
