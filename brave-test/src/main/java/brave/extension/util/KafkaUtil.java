package brave.extension.util;

import io.github.embeddedkafka.EmbeddedKafka;
import io.github.embeddedkafka.EmbeddedKafkaConfigImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.Serdes;
import scala.collection.immutable.HashMap;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

import static org.apache.commons.lang3.exception.ExceptionUtils.throwableOfType;
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

        //noinspection resource
        return new KafkaProducer<>(properties, Serdes.String().serializer(), Serdes.String().serializer());
    }

    public static KafkaConsumer<String, String> createConsumer(String bootstrapServers, String groupId) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //noinspection resource
        return new KafkaConsumer<>(properties, String().deserializer(), String().deserializer());
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
            if (throwableOfType(t, TopicExistsException.class) != null) {
                log.warn("Topic {} already exist", topicName);
            } else {
                throw new IllegalStateException("Cannot create topic: " + topicName, t);
            }
        }
    }

    /**
     * Delete topics and its data from Kafka.
     * This is intended for resetting topic for the next test.
     */
    public static void deleteTopics(AdminClient adminClient, Collection<String> topicNames) {
        DeleteTopicsOptions options = new DeleteTopicsOptions().timeoutMs(10 * 1000);
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(topicNames, options);

        while (!deleteTopicsResult.all().isDone()) {
            LockSupport.parkNanos(10 * (long) 1e6);
        }
    }

    private static boolean topicExists(AdminClient adminClient, String topicName) {
        try {
            adminClient.describeTopics(List.of(topicName)).topicNameValues()
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
        DeleteConsumerGroupsOptions options = new DeleteConsumerGroupsOptions().timeoutMs(10 * 1000);
        DeleteConsumerGroupsResult deleteConsumerGroupsResult = adminClient.deleteConsumerGroups(topicNames, options);

        while (!deleteConsumerGroupsResult.all().isDone()) {
            LockSupport.parkNanos(10 * (long) 1e6);
        }
    }

    /**
     * Wait until when certain condition met
     * @param condition Condition to be checked every 10ms
     * @param timeout Maximum duration to wait for the condition to be met
     * @throws TimeoutException Throw when
     */
    public static void waitUntil(Supplier<Boolean> condition, Duration timeout) throws TimeoutException {
        long waitPeriodMillis = 10;
        long attempts = 0;

        while (!condition.get()) {
            LockSupport.parkNanos(waitPeriodMillis * (long) 1e6);
            attempts++;

            if (attempts * waitPeriodMillis > timeout.toMillis()) {
                throw new TimeoutException();
            }
        }
    }
}
