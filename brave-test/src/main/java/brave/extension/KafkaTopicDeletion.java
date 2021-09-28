package brave.extension;

import brave.extension.util.KafkaUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Arrays;
import java.util.List;

@Slf4j
public class KafkaTopicDeletion implements AfterAllCallback {


    private List<String> topicNames;

    /**
     * Register SQL scripts to be executed {@link BeforeAll} the test method in the test class.
     */
    public KafkaTopicDeletion deleteTopics(String ... topicNames) {
        this.topicNames = Arrays.asList(topicNames);
        return this;
    }

    @Override
    public void afterAll(ExtensionContext context) {
        try (AdminClient adminClient = KafkaUtil.createAdminClient(context)) {
            log.warn("Resetting topics: {}", String.join(", ", topicNames));
            KafkaUtil.deleteTopics(adminClient, topicNames);
            log.warn("Reset topics: {}", String.join(", ", topicNames));
        }

    }
}
