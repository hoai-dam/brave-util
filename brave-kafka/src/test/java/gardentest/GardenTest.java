package gardentest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import extension.KafkaDataExtension;
import extension.KafkaServerExtension;
import extension.util.KafkaUtil;
import garden.Fruit;
import garden.GardenRepo;
import garden.GardenWatcher;
import garden.Seed;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
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

    @Autowired(required = false)
    GardenWatcher gardenWatcher;

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
    void producerOfGardenMetrics_shouldBeActive() throws ExecutionException, InterruptedException {
        assertThat(gardenRepo.getGardenProducer()).isNotNull();

        List<Fruit> expectedFruits = List.of(
                gardenRepo.plant(new Seed("Apple")),
                gardenRepo.plant(new Seed("Banana")),
                gardenRepo.plant(new Seed("Orange")),
                gardenRepo.plant(new Seed("Kiwi")),
                gardenRepo.plant(new Seed("Watermelon"))
        );

        Consumer<String, String> metricsConsumer = KafkaUtil.createConsumer("localhost:9092", "garden-metrics-collector");
        metricsConsumer.subscribe(List.of(gardenRepo.getGardenMetricTopic()));

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        CountDownLatch fruitCounter = new CountDownLatch(expectedFruits.size());
        List<String> rawGardenEvents = new ArrayList<>();

        executorService.execute(() -> {
            while (fruitCounter.getCount() > 0) {
                ConsumerRecords<String, String> records = metricsConsumer.poll(Duration.ofMillis(1000));
                for (var record : records) {
                    rawGardenEvents.add(record.value());
                    fruitCounter.countDown();
                }
            }
        });

        boolean fruitCountFinished = fruitCounter.await(10, TimeUnit.SECONDS);
        executorService.shutdownNow();

        List<Fruit> actualFruits = rawGardenEvents.stream().map(rawEvent -> {
            try {
                return objectMapper.readValue(rawEvent, Fruit.class);
            } catch (JsonProcessingException e) {
                throw new IllegalStateException(e);
            }
        }).collect(toList());

        assertThat(fruitCountFinished).isTrue();
        assertThat(actualFruits).containsExactlyInAnyOrderElementsOf(expectedFruits);
    }
}
