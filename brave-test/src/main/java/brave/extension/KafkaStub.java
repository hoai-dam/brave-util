package brave.extension;

import brave.extension.util.KafkaUtil;
import brave.extension.util.ResourcesPathUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class KafkaStub {

    public static final String JSON_SUFFIX = ".json";

    private final String bootstrapServers;

    public KafkaStub(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public void load(String dataFolderClassPath) throws Exception {
        File[] topicDataFiles = getTopicDataFiles(dataFolderClassPath);
        if (topicDataFiles == null) return;

        try (Producer<String, String> producer = KafkaUtil.createProducer(bootstrapServers)) {
            try (AdminClient adminClient = KafkaUtil.createAdminClient(bootstrapServers)) {
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
        try (AdminClient adminClient = KafkaUtil.createAdminClient(bootstrapServers)) {
            List<String> topicNames = getTopicNames(dataFolderClassPath);

            log.warn("Resetting topics: {}", String.join(", ", topicNames));
            KafkaUtil.deleteTopics(adminClient, topicNames);
            log.warn("Reset topics: {}", String.join(", ", topicNames));
        }
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
