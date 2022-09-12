package brave.extension;

import brave.extension.util.KafkaUtil;
import brave.extension.util.ResourcesPathUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

@Slf4j
public class KafkaStub implements AutoCloseable {

    private static final String JSON_SUFFIX = ".json";

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final AtomicBoolean isActive = new AtomicBoolean(false);
    private final String bootstrapServers;
    private Producer<String, String> producer;
    private AdminClient adminClient;
    private Integer consumerGroupCount = 0;

    public KafkaStub(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        this.producer = KafkaUtil.createProducer(bootstrapServers);
        this.adminClient = KafkaUtil.createAdminClient(bootstrapServers);
        this.isActive.set(true);
    }

    public void loadForConsumerGroups(String dataFolderClassPath, List<String> consumerGroupIds) throws IOException, InterruptedException {
        File[] topicDataFiles = getTopicDataFiles(dataFolderClassPath);
        if (topicDataFiles == null) return;

        Map<String, List<Map.Entry<String, String>>> mapTopicToEvents = extractTopicDataFiles(Arrays.asList(topicDataFiles));

        log.warn("Preparing topics: {}", mapTopicToEvents.keySet());
        loadTopics(mapTopicToEvents.keySet());

        log.warn("Waiting consumer group is assigned a topic");
        int attempts = 0;
        while (!areConsumerGroupsAssignedTopics(consumerGroupIds)) {
            LockSupport.parkNanos(10 * (long) 1e6);
            attempts++;
            if (attempts == 1000) {
                throw new InterruptedException("Time out for waiting consumer group is assigned a topic.");
            }
        }

        mapTopicToEvents.forEach(this::loadTopicEvents);
    }

    private Map<String, List<Map.Entry<String, String>>> extractTopicDataFiles(List<File> topicDataFiles) throws IOException {
        Map<String, List<Map.Entry<String, String>>> mapTopicToEvents = new HashMap<>();
        for (File topicDataFile : topicDataFiles) {
            log.warn("Extracting data to map of topic and events from file: {}", topicDataFile.getName());
            String topicName = ResourcesPathUtil.trimEnd(topicDataFile.getName(), JSON_SUFFIX);

            String topicDataClassPath = ResourcesPathUtil.filePathToClassPath(topicDataFile.getPath());
            var kafkaEvents = KafkaUtil.loadEvents(topicDataClassPath);
            mapTopicToEvents.put(topicName, kafkaEvents);
        }

        return mapTopicToEvents;
    }

    private void loadTopics(Set<String> topics) {
        topics.forEach((topic) -> KafkaUtil.ensureTopicAvailability(adminClient, topic));
    }

    private boolean areConsumerGroupsAssignedTopics(List<String> consumerGroupIds) {
        try {
            for (String consumerGroupId : consumerGroupIds) {
                DescribeConsumerGroupsResult describeResult = adminClient.describeConsumerGroups(
                        List.of(consumerGroupId),
                        (new DescribeConsumerGroupsOptions()).timeoutMs(10 * 1000));

                List<MemberDescription> members = new ArrayList<>(
                        describeResult.describedGroups().get(consumerGroupId).get().members());

                if (members.isEmpty()) {
                    return false;
                }

                for (MemberDescription member : members) {
                    Set<TopicPartition> assignedTopics = member.assignment().topicPartitions();
                    if (assignedTopics.isEmpty()) return false;
                }
            }

            return true;
        } catch (ExecutionException | InterruptedException e) {
            log.error("Failed to describe consumer group {}", e.getMessage());
        }

        return false;
    }

    private void loadTopicEvents(String topic, List<Map.Entry<String, String>> events) {
        log.warn("load events for topic: {}", topic);
        events.forEach((event) -> producer.send(new ProducerRecord<>(topic, event.getKey(), event.getValue())));
    }

    public void load(String dataFolderClassPath) throws Exception {
        File[] topicDataFiles = getTopicDataFiles(dataFolderClassPath);
        if (topicDataFiles == null) return;

        for (File topicDataFile : topicDataFiles) {
            String topicName = ResourcesPathUtil.trimEnd(topicDataFile.getName(), JSON_SUFFIX);
            log.warn("Preparing topic: {}", topicName);

            KafkaUtil.ensureTopicAvailability(adminClient, topicName);

            String topicDataClassPath = ResourcesPathUtil.filePathToClassPath(topicDataFile.getPath());
            var kafkaEvents = KafkaUtil.loadEvents(topicDataClassPath);

            for (var event : kafkaEvents) {
                producer.send(new ProducerRecord<>(topicName, event.getKey(), event.getValue()));
            }
        }
        
    }

    private File[] getTopicDataFiles(String dataFolderClassPath) {
        Path kafkaStubFilePath = ResourcesPathUtil.classPathToFilePath(dataFolderClassPath);
        File kafkaStubFolder = kafkaStubFilePath.toFile();

        if (!kafkaStubFolder.exists() || !kafkaStubFolder.isDirectory()) {
            log.warn("kafkaStubFolder {} does not exists or is not a directory", kafkaStubFilePath);
            return null;
        }

        File[] topicDataFiles = kafkaStubFolder.listFiles((file, name) -> name.endsWith(JSON_SUFFIX));

        if (topicDataFiles == null) {
            log.warn("Cannot list files in kafkaStubFolder {}", kafkaStubFilePath);
            return null;
        }
        return topicDataFiles;
    }

    public void unload(String dataFolderClassPath) {
        List<String> topicNames = getTopicNames(dataFolderClassPath);

        deleteTopics(topicNames);
    }

    public void deleteTopics(List<String> topics) {
        log.warn("Resetting topics: {}", String.join(", ", topics));
        KafkaUtil.deleteTopics(adminClient, topics);
        log.warn("Reset topics: {}", String.join(", ", topics));
    }

    private List<String> getTopicNames(String dataFolderClassPath) {
        Path kafkaStubFilePath = ResourcesPathUtil.classPathToFilePath(dataFolderClassPath);
        File kafkaStubFolder = kafkaStubFilePath.toFile();

        if (!kafkaStubFolder.exists() || !kafkaStubFolder.isDirectory()) {
            log.warn("kafkaStubFolder {} does not exists or is not a directory", kafkaStubFilePath);
            return Collections.emptyList();
        }

        File[] topicDataFiles = kafkaStubFolder.listFiles((file, name) -> name.endsWith(JSON_SUFFIX));
        if (topicDataFiles == null) {
            log.warn("Cannot list files in kafkaStubFolder {}", kafkaStubFilePath);
            return Collections.emptyList();
        }
        return Arrays.stream(topicDataFiles)
                .map(f -> f.toPath().getFileName().toString())
                .map(fileName -> ResourcesPathUtil.trimEnd(fileName, JSON_SUFFIX))
                .collect(Collectors.toList());
    }

    /*
     * Consume a number of events from topics
     * */
    public List<ConsumerRecord<String, String>> consumeEvents(
            List<String> topics, int eventCount, long timeoutMillis
    ) throws InterruptedException {
        final String tempConsumerGroupId = String.format("brave-temp-consumer-group-%d", consumerGroupCount);
        List<ConsumerRecord<String, String>> result = new ArrayList<>();

        try (Consumer<String, String> consumer = KafkaUtil.createConsumer(bootstrapServers, tempConsumerGroupId)) {
            consumerGroupCount += 1;
            consumer.subscribe(topics);

            ExecutorService executorService = Executors.newSingleThreadExecutor();
            CountDownLatch counter = new CountDownLatch(eventCount);
            executorService.execute(() -> {
                while (counter.getCount() > 0) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                    for (var record : records) {
                        result.add(record);
                        counter.countDown();
                    }
                }

            });

            if (!counter.await(timeoutMillis, TimeUnit.MILLISECONDS)) {
                String errorMessage = String.format("Timeout waiting for %d events from topics %s", eventCount, String.join(", ", topics));
                throw new IllegalStateException(errorMessage);
            }

            executorService.shutdownNow();
        }

        KafkaUtil.deleteConsumerGroups(adminClient, List.of(tempConsumerGroupId));
        return result;
    }

    public <K,V> List<Map.Entry<K, V>> consumeMessages(
            List<String> topics, int expectedCount,
            Class<K> keyClass, Class<V> valueClass,
            long timeoutMillis) throws InterruptedException {

        List<ConsumerRecord<String, String>> records = this.consumeEvents(topics, expectedCount, timeoutMillis);
        List<Map.Entry<K, V>> messages = new ArrayList<>();
        try {
            for (var record : records) {
                messages.add(Map.entry(
                        objectMapper.readValue(record.key(), keyClass),
                        objectMapper.readValue(record.value(), valueClass)
                ));
            }
        } catch (JsonProcessingException jpex) {
            throw new IllegalStateException(jpex);
        }

        return messages;
    }

    public <K,V> List<Map.Entry<K, V>> consumeMessages(
            String topic, int expectedCount,
            Class<K> keyClass, Class<V> valueClass,
            long timeoutMillis) throws InterruptedException {

        return consumeMessages(List.of(topic), expectedCount, keyClass, valueClass, timeoutMillis);
    }

    @Override
    public void close() {
        if (isActive.compareAndSet(true, false)) {
            AdminClient capturedClient = this.adminClient;
            this.adminClient = null;
            if (capturedClient != null) {
                capturedClient.close();
            }

            Producer<?, ?> capturedProducer = this.producer;
            this.producer = null;
            if (capturedProducer != null) {
                capturedProducer.close();
            }
        }
    }
}