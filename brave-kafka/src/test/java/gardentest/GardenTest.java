package gardentest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import extension.KafkaDataExtension;
import extension.KafkaServerExtension;
import extension.util.KafkaUtil;
import garden.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.*;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = AppConfig.class)
@ExtendWith({
        SpringExtension.class,
        KafkaServerExtension.class,
        KafkaDataExtension.class
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

    @Test
    void consumerOfGardenChanges_shouldBeActive() throws InterruptedException {
        assertThat(gardenWatcher).isNotNull();
        boolean gardenIsFull = gardenWatcher.waitUntilGardenIsFull(Duration.ofSeconds(10));
        assertThat(gardenIsFull).isTrue();
    }

    @Test
    void batchConsumerOfGardenChanges_shouldBeActive() throws InterruptedException {
        assertThat(gardenWatcherInBatch).isNotNull();
        boolean gardenIsFull = gardenWatcherInBatch.waitUntilGardenIsFull(Duration.ofSeconds(10));
        assertThat(gardenIsFull).isTrue();
    }

    @Test
    void producerOfGardenMetrics_shouldBeActive() throws ExecutionException, InterruptedException {
        assertThat(gardenRepo.getGardenProducer()).isNotNull();

        List<Fruit> expectedFruits = List.of(
                gardenRepo.plant(new Seed("Apple")),
                gardenRepo.plant(new Seed("Banana")),
                gardenRepo.plant(new Seed("Orange")),
                gardenRepo.plant(new Seed("Kiwi")),
                gardenRepo.plant(new Seed("Watermelon"))
        );

        List<Entry<Seed, Fruit>> gardenEvents = consume(
                gardenRepo.getGardenMetricTopic(),
                expectedFruits.size(),
                Seed.class,
                Fruit.class
        );

        List<Fruit> actualFruits = gardenEvents.stream()
                .map(Entry::getValue)
                .collect(toList());

        assertThat(actualFruits).containsExactlyInAnyOrderElementsOf(expectedFruits);
    }

    @SuppressWarnings("SameParameterValue")
    private <K,V> List<Entry<K, V>> consume(String topic, int eventCount, Class<K> keyClass, Class<V> valueClass) throws InterruptedException {
        Consumer<String, String> metricsConsumer = KafkaUtil.createConsumer(context, "garden-metrics-collector");
        metricsConsumer.subscribe(List.of(topic));

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        CountDownLatch fruitCounter = new CountDownLatch(eventCount);
        List<Entry<K, V>> events = new ArrayList<>();

        executorService.execute(() -> {
            while (fruitCounter.getCount() > 0) {
                ConsumerRecords<String, String> records = metricsConsumer.poll(Duration.ofMillis(1000));
                for (var record : records) {
                    try {
                        events.add(Map.entry(
                                objectMapper.readValue(record.key(), keyClass),
                                objectMapper.readValue(record.value(), valueClass)
                        ));
                        fruitCounter.countDown();
                    } catch (JsonProcessingException jpex) {
                        throw new IllegalStateException(jpex);
                    }
                }
            }
        });

        //noinspection ResultOfMethodCallIgnored
        fruitCounter.await(10, TimeUnit.SECONDS);
        executorService.shutdownNow();
        return events;
    }
}
