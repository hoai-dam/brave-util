package garden;


import brave.kafka.Consumers;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@Consumers(
        properties = "kafka.consumer",
        groupId = "garden-watcher",
        enableAutoCommit = false,
        maxPollRecords = 20
)
public class GardenWatcherSingle {

    private final CountDownLatch fullGardenLatch = new CountDownLatch(45);
    private final List<Fruit> harvestedFruits = new ArrayList<>();

    @Consumers.Handler(properties = "replicate.garden-watcher")
    public void handleGardenChangeSingle(ConsumerRecord<Seed, Fruit> cr, Consumer<Seed, Fruit> consumer) {
        log.warn("Got seed {} flower {}", cr.key(), cr.value());
        if (fullGardenLatch.getCount() == 0) {
            log.info("Garden is now full");
            return;
        }

        harvestedFruits.add(cr.value());
        fullGardenLatch.countDown();

        consumer.commitSync();
    }

    public boolean waitUntilGardenIsFull(Duration duration) throws InterruptedException {
        return fullGardenLatch.await(duration.toMillis(), TimeUnit.MILLISECONDS);
    }

    public List<Fruit> getHarvestedFruits() {
        return harvestedFruits;
    }
}
