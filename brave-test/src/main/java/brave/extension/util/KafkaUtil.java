package brave.extension.util;

import io.github.embeddedkafka.EmbeddedKafka;
import io.github.embeddedkafka.EmbeddedKafkaConfigImpl;
import lombok.extern.slf4j.Slf4j;
//import net.manub.embeddedkafka.EmbeddedKafka;
//import net.manub.embeddedkafka.EmbeddedKafkaConfigImpl;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.Serdes;
import scala.collection.immutable.HashMap;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;

import static java.util.stream.Collectors.toList;
import static org.apache.kafka.common.serialization.Serdes.String;
import static scala.jdk.CollectionConverters.MapHasAsScala;

@Slf4j
public class KafkaUtil {

    /**
     * Start an instance of kafka server at localhost:port
     */
    public static void startKafkaServer(int bootstrapPort) {
        scala.collection.immutable.Map<String, String> brokerConfigs =
                scala.collection.immutable.Map.from(MapHasAsScala(Map.of(
                        "auto.create.topics.enable", "false",
                        "delete.topic.enable", "true"
                )).asScala());

        EmbeddedKafka.start(new EmbeddedKafkaConfigImpl(
                bootstrapPort,
                2181,
                brokerConfigs,
                new HashMap<>(),
                new HashMap<>()
        ));
    }

    /**
     * Create a kafka {@link Producer}.
     */
    public static Producer<String, String> createProducer(String bootstrapServers) {
        var properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");

        return new KafkaProducer<>(
                properties,
                Serdes.String().serializer(),
                Serdes.String().serializer());
    }

    public static KafkaConsumer<String, String> createConsumer(String bootstrapServers, String groupId) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new KafkaConsumer<>(properties, String().deserializer(), String().deserializer());
    }

    public static void assignTopicPartitions(KafkaConsumer<?,?> backOrderEventConsumer, String bootstrapServers, String topicName) throws InterruptedException, ExecutionException {
        try (var adminClient = KafkaUtil.createAdminClient(bootstrapServers)) {
            List<TopicPartition> topicPartitions = adminClient
                    .describeTopics(List.of(topicName)).values()
                    .get(topicName).get()
                    .partitions().stream()
                    .map(partitionInfo -> new TopicPartition(topicName, partitionInfo.partition()))
                    .collect(toList());
            backOrderEventConsumer.assign(topicPartitions);
        }
    }

    public static AdminClient createAdminClient(String bootstrapServers) {
        return AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers
        ));
    }

    /**
     * Ensure a kafka topic is available for use.
     * If not, create it.
     */
    public static void ensureTopicAvailability(AdminClient adminClient, String topicName) {
        if (topicExists(adminClient, topicName)) {
            log.warn("Topic {} already exists", topicName);
            return;
        }

        try {
            NewTopic topic = new NewTopic(topicName, 3, (short) 1);

            adminClient.createTopics(List.of(topic))
                    .values()
                    .get(topicName)
                    .get();

            log.warn("Created topic {}", topicName);
        } catch (Throwable t) {
            throw new IllegalStateException("Cannot create topic: " + topicName, t);
        }
    }

    /**
     * Delete topics and its data from Kafka.
     * This is intended for resetting topic for the next test.
     */
    public static void deleteTopics(AdminClient adminClient, Collection<String> topicNames) {
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(topicNames, new DeleteTopicsOptions().timeoutMs(10 * 1000));

        while (!deleteTopicsResult.all().isDone()) {
        }
    }

    private static boolean topicExists(AdminClient adminClient, String topicName) {
        try {
            adminClient.describeTopics(List.of(topicName)).values()
                    .get(topicName)
                    .get();
            return true;
        } catch (UnknownTopicOrPartitionException e) {
            return false;
        } catch (InterruptedException | ExecutionException e) {
            log.error(e.getMessage());
            return false;
        }
    }

    /**
     * Load all kafka events into plain text format.
     */
    public static List<Entry<String, String>> loadEvents(String resourceFile) throws IOException {
        var objectMapper = Config.getObjectMapper();
        var resourceStream = KafkaUtil.class.getClassLoader().getResourceAsStream(resourceFile);
        var objectTree = objectMapper.reader().readTree(resourceStream);

        var elements = objectTree.elements();
        var kafkaEvents = new ArrayList<Entry<String, String>>();
        while (elements.hasNext()) {
            var child = elements.next();
            kafkaEvents.add(
                    Map.entry(child.get("key").toString(), child.get("value").toString())
            );
        }

        return kafkaEvents;
    }

    /**
     * Delete consumer groups from Kafka.
     */
    public static void deleteConsumerGroups(AdminClient adminClient, Collection<String> topicNames) {
        DeleteConsumerGroupsResult deleteConsumerGroupsResult = adminClient.deleteConsumerGroups(topicNames
                , new DeleteConsumerGroupsOptions().timeoutMs(10 * 1000));

        while (!deleteConsumerGroupsResult.all().isDone()) {
        }
    }
}
