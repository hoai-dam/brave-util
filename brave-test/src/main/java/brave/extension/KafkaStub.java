package brave.extension;

import brave.extension.util.KafkaUtil;
import brave.extension.util.ResourcesPathUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static java.lang.Thread.sleep;

@Slf4j
public class KafkaStub {

    public static final String JSON_SUFFIX = ".json";

    private final String bootstrapServers;
    private final Producer<String, String> producer;
    private final AdminClient adminClient;

    public KafkaStub(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        this.producer = KafkaUtil.createProducer(bootstrapServers);
        this.adminClient = KafkaUtil.createAdminClient(bootstrapServers);
    }

    public void loadWhenConsumerGroupsAreAssignedTopics(String dataFolderClassPath, List<String> consumerGroupIds) throws IOException, InterruptedException {
        File[] topicDataFiles = getTopicDataFiles(dataFolderClassPath);
        if (topicDataFiles == null) return;

        Map<String, List<Map.Entry<String, String>>> mapTopicToEvents = extractTopicDataFiles(Arrays.asList(topicDataFiles));

        log.warn("Preparing topics: {}", mapTopicToEvents.keySet());
        loadTopics(mapTopicToEvents.keySet());

        log.warn("Waiting consumer group is assigned a topic");
        int attempts = 0;
        while (!areConsumerGroupsAssignedTopics(consumerGroupIds)) {
            sleep(1000);
            attempts++;
            if (attempts == 10) {
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
}