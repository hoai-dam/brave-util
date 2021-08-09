package gardentest;

import extension.KafkaDataExtension;
import extension.KafkaServerExtension;
import garden.GardenWatcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.time.Duration;

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

    @Autowired(required = false)
    GardenWatcher gardenWatcher;

    @Test
    void consumerOfGardenChanges_shouldBeActive() throws InterruptedException {
        assertThat(gardenWatcher).isNotNull();
        boolean gardenIsFull = gardenWatcher.waitUntilFullGarden(Duration.ofSeconds(10));
        assertThat(gardenIsFull).isTrue();
    }

    @Test
    void producerOfGardenMetrics_shouldBeActive() {

    }
}
