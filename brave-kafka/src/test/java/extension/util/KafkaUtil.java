package extension.util;

import brave.kafka.KafkaConfigResolver;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import net.manub.embeddedkafka.EmbeddedKafka;
import net.manub.embeddedkafka.EmbeddedKafkaConfigImpl;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import scala.collection.immutable.HashMap;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import static java.util.stream.Collectors.toList;
import static org.apache.kafka.common.serialization.Serdes.String;

@Slf4j
public class KafkaUtil {

    public static ObjectMapper objectMapper(ExtensionContext context) {
        ApplicationContext appContext = SpringExtension.getApplicationContext(context);
        return appContext.getBean(ObjectMapper.class);
    }

    public static String getBootstrapServers(ApplicationContext appContext) {
        KafkaConfigResolver configResolver = appContext.getBean(KafkaConfigResolver.class);
        return configResolver.getString("${kafka.bootstrap.servers}");
    }

    public static String getBootstrapServers(ExtensionContext context) {
        ApplicationContext appContext = SpringExtension.getApplicationContext(context);
        KafkaConfigResolver configResolver = appContext.getBean(KafkaConfigResolver.class);
        return configResolver.getString("${kafka.bootstrap.servers}");
    }

    public static int getBootstrapPort(ExtensionContext context) {
        String firstBootstrapServer = getBootstrapServers(context);
        String[] urlParts = firstBootstrapServer.split(":");

        if (urlParts.length == 2 || urlParts.length == 3)
            return Integer.parseInt(urlParts[urlParts.length - 1]);

        if (urlParts.length == 1)
            return 80;

        throw new IllegalArgumentException("Unrecognized bootstrap server " + firstBootstrapServer);
    }

    /**
     * Start an instance of kafka server at localhost:port,
     * with the port obtain from {@link #getBootstrapPort(ExtensionContext)}
     */
    public static void startKafkaServer(ExtensionContext context) {
        int bootstrapPort = getBootstrapPort(context);

        EmbeddedKafka.start(new EmbeddedKafkaConfigImpl(
                bootstrapPort,
                2181,
                new HashMap<>(),
                new HashMap<>(),
                new HashMap<>()
        ));
    }

    /**
     * Create a kafka {@link Producer}
     * with the bootstrap_servers obtain from {@link #getBootstrapServers(ExtensionContext)}
     */
    public static Producer<String, String> createProducer(ExtensionContext context) {
        String bootstrapServers = getBootstrapServers(context);
        var properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");

        return new KafkaProducer<>(
                properties,
                Serdes.String().serializer(),
                Serdes.String().serializer());
    }

    public static KafkaConsumer<String, String> createConsumer(ApplicationContext context, String groupId) {
        String bootstrapServers= getBootstrapServers(context);
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

    public static AdminClient createAdminClient(ExtensionContext extensionContext) {
        return createAdminClient(getBootstrapServers(extensionContext));
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
        adminClient.deleteTopics(topicNames)
                .values()
                .forEach((topicName, deleteTopicFuture) -> {
                    try {
                        deleteTopicFuture.get();
                        log.warn("Deleted topic " + topicName);
                    } catch (Throwable e) {
                        throw new IllegalStateException("Failed to delete topic: " + topicName, e);
                    }
                });
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
    public static List<Entry<String, String>> loadEvents(ExtensionContext context, String resourceFile) throws IOException {
        var objectMapper = objectMapper(context);
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
}
