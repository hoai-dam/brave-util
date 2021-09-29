package brave.extension;

import brave.extension.util.KafkaUtil;
import brave.extension.util.ResourcesPathUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.nio.file.Path;

@Slf4j
public class KafkaDataExtension implements BeforeEachCallback, AfterEachCallback {

    public static final String JSON_SUFFIX = ".json";

    @Override
    public void beforeEach(ExtensionContext methodLevelContext) throws Exception {
        File[] topicDataFiles = getTopicDataFiles(methodLevelContext);
        if (topicDataFiles == null) return;

        try (Producer<String, String> producer = KafkaUtil.createProducer(methodLevelContext)) {
            try (AdminClient adminClient = KafkaUtil.createAdminClient(methodLevelContext)) {
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

    private File[] getTopicDataFiles(ExtensionContext methodLevelContext) {
        Path testMethodStubFilePath = ResourcesPathUtil.getStubFilePathOfTestMethod(methodLevelContext);
        Path kafkaStubFilePath = testMethodStubFilePath.resolve("kafka");
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

    @Override
    public void afterEach(ExtensionContext methodLevelContext) {

    }
}
