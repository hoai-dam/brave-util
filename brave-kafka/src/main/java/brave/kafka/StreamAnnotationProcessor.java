package brave.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

import static brave.kafka.PropertyUtil.fallbackIfEmpty;
import static brave.kafka.PropertyUtil.sizeIsEmpty;
import static brave.kafka.ReflectionUtil.*;
import static java.lang.String.format;
import static org.apache.kafka.streams.KafkaStreams.State.ERROR;
import static org.apache.kafka.streams.KafkaStreams.State.PENDING_ERROR;
import static org.apache.kafka.streams.StreamsConfig.*;
import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;

@Slf4j
@Component
@RequiredArgsConstructor
public class StreamAnnotationProcessor {

    private final KafkaConfig config;
    private final KafkaConfigResolver resolver;
    private final Map<String, KafkaStreams> kafkaStreams = new LinkedHashMap<>();

    @PostConstruct
    void initialize() {
        Map<String, Object> streamBeans = config.getContext().getBeansWithAnnotation(Streams.class);

        for (String beanName : streamBeans.keySet()) {
            Object streamBean = streamBeans.get(beanName);

            this.kafkaStreams.put(beanName, new StreamsBuilder(beanName, streamBean).process());
        }
    }

    @EventListener(ApplicationReadyEvent.class)
    public void applicationReady() {
        for (String streamName : kafkaStreams.keySet()) {
            KafkaStreams streams = kafkaStreams.get(streamName);
            try {
                log.warn("Starting stream {}", streamName);
                streams.start();
                log.warn("Started stream {}", streamName);
            } catch (Exception ex) {
                log.error("Failed to start kafka stream {}", streamName, ex);
            }
        }
    }

    class StreamsBuilder {

        private final String streamName;
        private final Object target;
        private final Properties properties;
        private final Streams streams;

        StreamsBuilder(String streamName, Object target) {
            this.streamName = streamName;
            this.target = target;
            this.streams = target.getClass().getAnnotation(Streams.class);

            if (this.streams == null)
                throw new IllegalStateException("No @Streams annotation found on target");

            this.properties = getKafkaStreamsProperties(this.streams);
        }

        KafkaStreams process() {
            Topology topology = getTopology();
            KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);
            kafkaStreams.setStateListener((newState, oldState) ->
                    log.info("Stream state changed {} -> {}", oldState, newState));

            buildErrorHandling(kafkaStreams);
            injectKafkaStream(kafkaStreams);

            return kafkaStreams;
        }

        private Topology getTopology() {

            for (Method method : target.getClass().getDeclaredMethods()) {
                Streams.Topology topologyAnnotation = method.getAnnotation(Streams.Topology.class);
                if (topologyAnnotation != null) {
                    if (method.getParameterCount() == 0 && Topology.class.isAssignableFrom(method.getReturnType())) {
                        try {
                            return (Topology) method.invoke(target);
                        } catch (IllegalAccessException e) {
                            throw new IllegalStateException("Can not build topology", e);
                        } catch (InvocationTargetException e) {
                            throw new IllegalStateException("Failed to build topology", e);
                        }
                    } else {
                        throw new IllegalStateException("Invalid topology builder");
                    }
                }
            }


            throw new IllegalStateException("No topology builder found");
        }

        private void buildErrorHandling(KafkaStreams kafkaStreams) {
            if (streams.ignoreException()) {
                kafkaStreams.setUncaughtExceptionHandler(exception -> {
                    log.warn("Ignoring uncaught exception & replacing stream thread", exception);
                    return REPLACE_THREAD;
                });
            }

            if (streams.reportHealthCheck()) {
                String streamKey = "stream{" + streamName + "}";
                StreamAnnotationProcessor.this.config.registerBean(target.getClass().getName(), (HealthIndicator) () -> {
                    KafkaStreams.State streamState = kafkaStreams.state();
                    if (streamState == ERROR || streamState == PENDING_ERROR) {
                        log.warn("{} entered state {}", streamKey, streamState);
                    }
                    if (streamState == ERROR) {
                        return Health.down()
                                .withDetail(streamKey, streamState)
                                .build();
                    }
                    return Health.up()
                            .withDetail(streamKey, streamState)
                            .build();
                });
            }
        }

        private void injectKafkaStream(KafkaStreams kafkaStreams) {
            for (var field : target.getClass().getDeclaredFields()) {
                Streams.Inject injectAnnotation = field.getAnnotation(Streams.Inject.class);
                if (injectAnnotation == null) continue;
                if (canBeSet(field, KafkaStreams.class)) {
                    setField(target, field, kafkaStreams);
                    break;
                } else {
                    throw new IllegalStateException(format("%s.%s must be of type %s and must NOT be final",
                            target.getClass().getName(), field.getName(), KafkaStreams.class.getName()));
                }
            }

            for (var method : target.getClass().getDeclaredMethods()) {
                Streams.Inject injectAnnotation = method.getAnnotation(Streams.Inject.class);
                if (injectAnnotation == null) continue;
                if (isSetter(method, KafkaStreams.class)) {
                    try {
                        method.invoke(target, kafkaStreams);
                    } catch (IllegalAccessException e) {
                        throw new IllegalStateException("Can not build topology", e);
                    } catch (InvocationTargetException e) {
                        throw new IllegalStateException("Failed to build topology", e);
                    }
                    break;
                } else {
                    throw new IllegalStateException(format("%s.%s must accept exactly one parameter of type %s",
                            target.getClass().getName(), method.getName(), KafkaStreams.class.getName()));
                }
            }
        }

        private Properties getKafkaStreamsProperties(Streams streams) {
            Properties props = resolver.getProperties(streams.properties(), StreamsConfig.configDef());

            if (!sizeIsEmpty(streams.bootstrapServers())) {
                fallbackIfEmpty(props, BOOTSTRAP_SERVERS_CONFIG, resolver.getString(streams.bootstrapServers()));
            }

            if (!sizeIsEmpty(streams.applicationId())) {
                fallbackIfEmpty(props, APPLICATION_ID_CONFIG, resolver.getString(streams.applicationId()));
            }

            fallbackIfEmpty(props, POLL_MS_CONFIG, streams.pollMillis());
            fallbackIfEmpty(props, COMMIT_INTERVAL_MS_CONFIG, streams.commitIntervalMillis());
            fallbackIfEmpty(props, REQUEST_TIMEOUT_MS_CONFIG, streams.requestTimeoutMillis());
            fallbackIfEmpty(props, NUM_STREAM_THREADS_CONFIG, streams.numStreamThreads());

            props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, streams.defaultKeySerde());
            props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, streams.defaultValueSerde());

            return props;
        }

    }
}
