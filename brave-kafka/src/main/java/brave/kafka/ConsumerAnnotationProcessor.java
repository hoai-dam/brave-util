package brave.kafka;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.ConfigKey;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.*;

import static brave.kafka.PropertyUtil.isEmpty;
import static brave.kafka.ReflectionUtil.name;
import static brave.kafka.ReflectionUtil.signature;
import static java.lang.Integer.parseInt;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

@Component
@RequiredArgsConstructor
class ConsumerAnnotationProcessor {

    private final KafkaConfig config;
    private final KafkaConfigResolver resolver;
    private final Map<String, SimpleConsumerGroup<?, ?>> consumerGroups = new LinkedHashMap<>();

    @PostConstruct
    void initialize() {
        Map<String, Object> consumerBeans = config.getContext().getBeansWithAnnotation(Consumers.class);

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
            Consumers consumersAnnotation = target.getClass().getAnnotation(Consumers.class);
            if (consumersAnnotation == null) return Collections.emptyMap();

            properties = getKafkaConsumerProperties(consumersAnnotation);
            System.err.printf("Building consumer with properties %s\n", properties);

            Map<String, SimpleConsumerGroup<?, ?>> consumerGroups = new LinkedHashMap<>();
            for (Method method : target.getClass().getDeclaredMethods()) {
                Consumers.Handler handlerAnnotation = method.getAnnotation(Consumers.Handler.class);
                if (handlerAnnotation == null) continue;

                String consumerGroupName = signature(method);
                SimpleConsumerGroup<Object, Object> consumerGroup = getConsumerGroup(method, handlerAnnotation);
                consumerGroups.put(consumerGroupName, consumerGroup);

            }
            return consumerGroups;
        }

        private Properties getKafkaConsumerProperties(Consumers braveConsumers) {
            Properties props;

            if (isNotBlank(braveConsumers.properties())) {
                props = resolver.getProperties(braveConsumers.properties());
            } else {
                props = new Properties();
            }

            ConfigDef configDef = ConsumerConfig.configDef();
            for (String propertyName : props.stringPropertyNames()) {
                String value = props.getProperty(propertyName);
                ConfigKey configKey = configDef.configKeys().get(propertyName);

                if (propertyName.endsWith(".ms")) {
                    if (configKey.type() == ConfigDef.Type.SHORT) {
                        props.put(propertyName, (short) Duration.parse(value).toMillis());
                    } else if (configKey.type() == ConfigDef.Type.INT) {
                        props.put(propertyName, (int) Duration.parse(value).toMillis());
                    } else if (configKey.type() == ConfigDef.Type.LONG) {
                        props.put(propertyName, Duration.parse(value).toMillis());
                    }
                }
            }

            fallbackConsumerProperties(braveConsumers, props);

            return props;
        }

        private void fallbackConsumerProperties(Consumers braveConsumers, Properties props) {
            if (isEmpty(props, BOOTSTRAP_SERVERS_CONFIG)) {
                if (braveConsumers.bootstrapServers().length == 0) {
                    throw new IllegalStateException("No bootstrap servers provided");
                }
                props.put(BOOTSTRAP_SERVERS_CONFIG, braveConsumers.bootstrapServers());
            }

            if (isEmpty(props, GROUP_ID_CONFIG)) {
                if (isBlank(braveConsumers.groupId())) {
                    throw new IllegalStateException("No group id provided");
                }
                props.setProperty(GROUP_ID_CONFIG, resolver.getString(braveConsumers.groupId()));
            }

            if (isEmpty(props, ENABLE_AUTO_COMMIT_CONFIG)) {
                props.put(ENABLE_AUTO_COMMIT_CONFIG, braveConsumers.enableAutoCommit());
            }

            if (isEmpty(props, AUTO_COMMIT_INTERVAL_MS_CONFIG)) {
                props.put(AUTO_COMMIT_INTERVAL_MS_CONFIG, braveConsumers.autoCommitIntervalMillis());
            }

            if (isEmpty(props, AUTO_OFFSET_RESET_CONFIG) && isNotBlank(braveConsumers.autoOffsetReset())) {
                props.setProperty(AUTO_OFFSET_RESET_CONFIG, resolver.getString(braveConsumers.autoOffsetReset()));
            }

            if (isEmpty(props, MAX_POLL_INTERVAL_MS_CONFIG)) {
                props.put(MAX_POLL_INTERVAL_MS_CONFIG, braveConsumers.maxPollIntervalMillis());
            }

            if (isEmpty(props, MAX_POLL_RECORDS_CONFIG)) {
                props.put(MAX_POLL_RECORDS_CONFIG, braveConsumers.maxPollRecords());
            }

            if (isEmpty(props, SESSION_TIMEOUT_MS_CONFIG)) {
                props.put(SESSION_TIMEOUT_MS_CONFIG, braveConsumers.sessionTimeoutMillis());
            }
        }

        private SimpleConsumerGroup<Object, Object> getConsumerGroup(Method method, Consumers.Handler recordConsumer) {
            Consumers.Handler.Config cfg = getRecordConsumerConfig(recordConsumer);

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

    private Consumers.Handler.Config getRecordConsumerConfig(Consumers.Handler recordConsumer) {
        Properties props;
        if (isNotBlank(recordConsumer.properties())) {
            props = resolver.getProperties(recordConsumer.properties());
        } else {
            props = new Properties();
        }

        String[] topics = recordConsumer.topics().length > 0
                ? recordConsumer.topics()
                : props.getProperty("topics").split(",");

        int threadsCount = isEmpty(props, "threads-count")
                ? recordConsumer.threadsCount()
                : parseInt(props.getProperty("threads-count"));

        boolean ignoreException =  isEmpty(props, "ignore-exception")
                ? recordConsumer.ignoreException()
                : Boolean.parseBoolean(props.getProperty("ignore-exception")) ;

        Duration pollingTimeout = isEmpty(props, "polling-timeout")
                ? Duration.ofMillis(recordConsumer.pollingTimeoutMillis())
                : Duration.parse(props.getProperty("polling-timeout"));

        boolean reportHealthCheck = isEmpty(props, "report-health-check")
                ? recordConsumer.reportHealthCheck()
                : Boolean.parseBoolean(props.getProperty("report-health-check"));

        //noinspection unchecked
        Deserializer<Object> keyDeserializer = isEmpty(props, "key-deserializer")
                ? resolver.getInstance(recordConsumer.keyDeserializer())
                : resolver.getInstance(props.getProperty("key-deserializer"), Deserializer.class);

        //noinspection unchecked
        Deserializer<Object> valueDeserializer = isEmpty(props, "value-deserializer")
                ? resolver.getInstance(recordConsumer.valueDeserializer())
                : resolver.getInstance(props.getProperty("value-deserializer"), Deserializer.class);

        return Consumers.Handler.Config.builder()
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
