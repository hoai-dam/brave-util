package brave.kafka;

import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.*;

import static brave.kafka.ReflectionUtil.name;
import static brave.kafka.ReflectionUtil.signature;
import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.parseInt;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

@Component
@RequiredArgsConstructor
class ConsumerAnnotationProcessor {

    private final KafkaConfig config;
    private final KafkaConfigResolver resolver;
    private final Map<String, SimpleConsumerGroup<?, ?>> consumerGroups = new LinkedHashMap<>();

    @PostConstruct
    void initialize() {
        Map<String, Object> consumerBeans = config.getContext().getBeansWithAnnotation(BraveConsumers.class);

        for (String beanName : consumerBeans.keySet()) {
            Object consumerBean = consumerBeans.get(beanName);
            Map<String, SimpleConsumerGroup<?, ?>> consumerGroups = new ProcessingTask(consumerBean).process();
            this.consumerGroups.putAll(consumerGroups);
        }
    }

    @EventListener(ApplicationReadyEvent.class)
    public void applicationReady() {
        consumerGroups.values().forEach(SimpleConsumerGroup::start);
    }

    class ProcessingTask {

        private final Object target;
        private Properties properties;

        public ProcessingTask(Object target) {
            this.target = target;
        }

        Map<String, SimpleConsumerGroup<?, ?>> process() {
            BraveConsumers consumersAnnotation = target.getClass().getAnnotation(BraveConsumers.class);
            if (consumersAnnotation == null) return Collections.emptyMap();

            properties = getKafkaConsumerProperties(consumersAnnotation);
            Map<String, SimpleConsumerGroup<?, ?>> consumerGroups = new LinkedHashMap<>();
            for (Method method : target.getClass().getDeclaredMethods()) {
                BraveConsumers.Handler handlerAnnotation = method.getAnnotation(BraveConsumers.Handler.class);
                if (handlerAnnotation == null) continue;

                String consumerGroupName = signature(method);
                SimpleConsumerGroup<Object, Object> consumerGroup = getConsumerGroup(method, handlerAnnotation);
                consumerGroups.put(consumerGroupName, consumerGroup);

            }
            return consumerGroups;
        }

        private Properties getKafkaConsumerProperties(BraveConsumers braveConsumers) {
            Properties props;

            if (StringUtils.isNotBlank(braveConsumers.properties())) {
                props = resolver.getProperties(braveConsumers.properties());
            } else {
                props = new Properties();
            }
            if (StringUtils.isNotBlank(braveConsumers.bootstrapServers()))
                props.setProperty(BOOTSTRAP_SERVERS_CONFIG, resolver.getString(braveConsumers.bootstrapServers()));
            if (StringUtils.isNotBlank(braveConsumers.groupId()))
                props.setProperty(GROUP_ID_CONFIG, resolver.getString(braveConsumers.groupId()));
            if (StringUtils.isNotBlank(braveConsumers.enableAutoCommit()))
                props.setProperty(ENABLE_AUTO_COMMIT_CONFIG, resolver.getString(braveConsumers.enableAutoCommit()));
            if (StringUtils.isNotBlank(braveConsumers.autoCommitInterval()))
                props.setProperty(AUTO_COMMIT_INTERVAL_MS_CONFIG, resolver.getString(braveConsumers.autoCommitInterval()));
            if (StringUtils.isNotBlank(braveConsumers.autoOffsetReset()))
                props.setProperty(AUTO_OFFSET_RESET_CONFIG, resolver.getString(braveConsumers.autoOffsetReset()));
            if (StringUtils.isNotBlank(braveConsumers.maxPollInterval()))
                props.setProperty(MAX_POLL_INTERVAL_MS_CONFIG, resolver.getString(braveConsumers.maxPollInterval()));
            if (StringUtils.isNotBlank(braveConsumers.maxPollRecords()))
                props.setProperty(MAX_POLL_RECORDS_CONFIG, resolver.getString(braveConsumers.maxPollRecords()));
            if (StringUtils.isNotBlank(braveConsumers.sessionTimeout()))
                props.setProperty(SESSION_TIMEOUT_MS_CONFIG, resolver.getString(braveConsumers.sessionTimeout()));

            // TODO: Iterate through properties to convert it to exact type using ConsumerConfig.configDef()

            props.put(ENABLE_AUTO_COMMIT_CONFIG,
                    parseBoolean(props.getProperty(ENABLE_AUTO_COMMIT_CONFIG)));

            props.put(AUTO_COMMIT_INTERVAL_MS_CONFIG,
                    (int) Duration.parse(props.getProperty(AUTO_COMMIT_INTERVAL_MS_CONFIG)).toMillis());

            props.put(MAX_POLL_INTERVAL_MS_CONFIG,
                    (int) Duration.parse(props.getProperty(MAX_POLL_INTERVAL_MS_CONFIG)).toMillis());

            props.put(MAX_POLL_RECORDS_CONFIG,
                    parseInt(props.getProperty(MAX_POLL_RECORDS_CONFIG)));

            props.put(SESSION_TIMEOUT_MS_CONFIG,
                    (int) Duration.parse(props.getProperty(SESSION_TIMEOUT_MS_CONFIG)).toMillis());

            return props;
        }

        private SimpleConsumerGroup<Object, Object> getConsumerGroup(Method method, BraveConsumers.Handler recordConsumer) {
            BraveConsumers.Handler.Config cfg = getRecordConsumerConfig(recordConsumer);

            if (method.getParameterCount() == 0 || method.getParameterCount() > 2) {
                throw new IllegalStateException(
                        "Unrecognized record handler " + name(method) + " with " + method.getParameterCount() + "  parameters");
            }

            Class<?> firstParamType = method.getParameterTypes()[0];
            if (firstParamType != ConsumerRecords.class && firstParamType != ConsumerRecord.class) {
                throw new IllegalStateException(
                        "Unrecognized record handler " + name(method) + " with first param as " + firstParamType.getName());
            }

            boolean isBatchProcessing = firstParamType == ConsumerRecords.class;
            boolean injectConsumer = false;

            if (method.getParameterCount() == 2) {
                Class<?> secondParamType = method.getParameterTypes()[1];
                if (secondParamType != Consumer.class) {
                    throw new IllegalStateException(
                            "Unrecognized record handler " + name(method) + " with second param as " + secondParamType.getName());
                }
                injectConsumer = true;
            }

            SimpleConsumerGroup<Object, Object> consumerGroup = SimpleConsumerGroup.builder()
                    .consumerSupplier(() -> new KafkaConsumer<>(properties, cfg.getKeyDeserializer(), cfg.getValueDeserializer()))
                    .topics(cfg.getTopics())
                    .threadsCount(cfg.getThreadsCount())
                    .pollingTimeout(cfg.getPollingTimeout())
                    .recordProcessor(ConsumerRecordsDispatcher.builder()
                            .ignoreException(cfg.isIgnoreException())
                            .isBatchProcessing(isBatchProcessing)
                            .injectConsumer(injectConsumer)
                            .target(target)
                            .method(method)
                            .build()
                    )
                    .build();

            if (cfg.isReportHealthCheck()) {
                ConsumerAnnotationProcessor.this.config.registerBean("health(" + signature(method) + ")", (HealthIndicator) consumerGroup::health);
            }

            return consumerGroup;
        }
    }

    private BraveConsumers.Handler.Config getRecordConsumerConfig(BraveConsumers.Handler recordConsumer) {
        Properties props;
        if (StringUtils.isNotBlank(recordConsumer.properties())) {
            props = resolver.getProperties(recordConsumer.properties());
        } else {
            props = new Properties();
        }

        String[] topics = recordConsumer.topics().length > 0
                ? recordConsumer.topics()
                : props.getProperty("topics").split(",");

        int threadsCount = StringUtils.isNotBlank(recordConsumer.threadsCount())
                ? resolver.getInt(recordConsumer.threadsCount())
                : parseInt(props.getProperty("threads-count"));

        boolean ignoreException =  StringUtils.isNotBlank(recordConsumer.ignoreException())
                ? resolver.getBoolean(recordConsumer.ignoreException())
                : Boolean.parseBoolean(props.getProperty("ignore-exception")) ;

        Duration pollingTimeout = StringUtils.isNotBlank(recordConsumer.pollingTimeout())
                ? resolver.getDuration(recordConsumer.pollingTimeout())
                : Duration.parse(props.getProperty("polling-timeout"));
        boolean reportHealthCheck = StringUtils.isNotBlank(recordConsumer.reportHealthCheck())
                ? resolver.getBoolean(recordConsumer.reportHealthCheck())
                : Boolean.parseBoolean(props.getProperty("report-health-check"));
        //noinspection unchecked
        Deserializer<Object> keyDeserializer = StringUtils.isNotBlank(recordConsumer.keyDeserializer())
                ? resolver.getInstance(recordConsumer.keyDeserializer(), Deserializer.class)
                : resolver.getInstance(props.getProperty("key-deserializer"), Deserializer.class);
        //noinspection unchecked
        Deserializer<Object> valueDeserializer = StringUtils.isNotBlank(recordConsumer.valueDeserializer())
                ? resolver.getInstance(recordConsumer.valueDeserializer(), Deserializer.class)
                : resolver.getInstance(props.getProperty("value-deserializer"), Deserializer.class);

        return BraveConsumers.Handler.Config.builder()
                .topics(Arrays.asList(topics))
                .threadsCount(threadsCount)
                .ignoreException(ignoreException)
                .pollingTimeout(pollingTimeout)
                .reportHealthCheck(reportHealthCheck)
                .keyDeserializer(keyDeserializer)
                .valueDeserializer(valueDeserializer)
                .build();
    }
}
