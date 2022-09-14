package gardentest;

import brave.extension.KafkaExtension;
import brave.extension.KafkaStub;
import brave.extension.util.KafkaUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import waterplan.WaterPlan;

import java.time.Duration;
import java.util.List;
import java.util.Map;

@Slf4j
@SpringBootTest(classes = AppConfig.class)
@ExtendWith({
        SpringExtension.class,
        KafkaExtension.class
})
public class StreamsTest {

    @Autowired
    WaterPlan waterPlan;

    @Test
    void streamOfWater_shouldBeActive(KafkaStub kafkaStub) throws Exception {
        kafkaStub.loadForConsumerGroups("stubs/WaterPlan/generalKafkaStream", List.of("waterplan-general-test"));

        KafkaUtil.waitUntil(() -> waterPlan.getState() == KafkaStreams.State.RUNNING, Duration.ofMinutes(1));

        KafkaUtil.waitUntil(() -> {
            Map<String, Integer> waterVolumes = waterPlan.getWaterVolumes();

            int civil = waterVolumes.getOrDefault(WaterPlan.CIVIL_WATER_TANK, 0);
            int industry = waterVolumes.getOrDefault(WaterPlan.INDUSTRY_WATER_TANK, 0);
            int redistribute = waterVolumes.getOrDefault(WaterPlan.REDISTRIBUTE_TANK, 0);

            int currentVolumes = civil + industry + redistribute;
            System.err.println("Current vol = " + currentVolumes);

            return currentVolumes >= 36;
        }, Duration.ofSeconds(10));
    }

}
