package garden;

import brave.kafka.BraveProducers;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;


@Getter
@Slf4j
@Component
@BraveProducers(bootstrapServers = "${kafka.bootstrap.servers}")
public class GardenRepo {

    @BraveProducers.Inject(
            keySerializer = "garden.SeedSerializer",
            valueSerializer = "garden.FruitSerializer",
            acks = "all"
    )
    private Producer<Seed, Fruit> gardenProducer;
    private final String gardenMetricTopic = "connect.garden.metrics";

    public Fruit plant(Seed seed) throws ExecutionException, InterruptedException {
        Fruit fruit = new Fruit(seed.getName());

        ProducerRecord<Seed, Fruit> metricRecord = new ProducerRecord<>(gardenMetricTopic, seed, fruit);
        RecordMetadata recordMetadata = gardenProducer.send(metricRecord).get();

        log.info("Planted seed {} and push metric to {}", seed, recordMetadata);
        return fruit;
    }

}
