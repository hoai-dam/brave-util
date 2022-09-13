package brave.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.api.Processor;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import static brave.kafka.PropertyUtil.fallbackIfEmpty;
import static brave.kafka.ReflectionUtil.signature;
import static org.apache.kafka.streams.StreamsConfig.*;
import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;

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

            this.kafkaStreams.put(beanName, new StreamsBuilder(streamBean).process());
        }
    }

    @EventListener(ApplicationReadyEvent.class)
    public void applicationReady() {
        kafkaStreams.values().forEach(KafkaStreams::start);
    }

    class StreamsBuilder {

        private final Object target;
        private final Properties properties;
        private final Streams streams;

        StreamsBuilder(Object target) {
            this.target = target;
            this.streams = target.getClass().getAnnotation(Streams.class);

            if (this.streams == null)
                throw new IllegalStateException("No @Streams annotation found on target");

            this.properties = getKafkaStreamsProperties(this.streams);
        }

        KafkaStreams process() {
            Topology topology = new Topology();

            for (Method method : target.getClass().getDeclaredMethods()) {

                Streams.Source[] sources = method.getAnnotationsByType(Streams.Source.class);
                if (sources.length > 0) {
                    addSources(topology, sources);
                }

                Streams.Processor processor = method.getAnnotation(Streams.Processor.class);
                if (processor != null) {
                    addProcessor(topology, method, processor);
                }

                Streams.Sink[] sinks = method.getAnnotationsByType(Streams.Sink.class);
                if (sinks.length > 0) {
                    addSinks(topology, sinks);
                }
            }

            KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);
            kafkaStreams.setStateListener((newState, oldState) -> {

            });
            kafkaStreams.setUncaughtExceptionHandler(exception -> {
                if (streams.ignoreException()) {
                    log.warn("Ignoring uncaught exception & replacing stream thread", exception);
                    return REPLACE_THREAD;
                } else {
                    log.error("Getting uncaught exception & shutdown streams client", exception);
                    return SHUTDOWN_CLIENT;
                }
            });
            return kafkaStreams;
        }

        private Properties getKafkaStreamsProperties(Streams streams) {
            Properties props = resolver.getProperties(streams.properties(), StreamsConfig.configDef());

            fallbackIfEmpty(props, BOOTSTRAP_SERVERS_CONFIG, streams.bootstrapServers());
            fallbackIfEmpty(props, APPLICATION_ID_CONFIG, streams.applicationId());
            fallbackIfEmpty(props, POLL_MS_CONFIG, streams.pollMillis());
            fallbackIfEmpty(props, COMMIT_INTERVAL_MS_CONFIG, streams.commitIntervalMillis());
            fallbackIfEmpty(props, REQUEST_TIMEOUT_MS_CONFIG, streams.requestTimeoutMillis());
            fallbackIfEmpty(props, NUM_STREAM_THREADS_CONFIG, streams.numStreamThreads());

            props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, streams.defaultKeySerde());
            props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, streams.defaultValueSerde());

            return props;
        }

        private void addSources(Topology topology, Streams.Source[] sources) {
            for (Streams.Source source : sources) {
                //noinspection unchecked
                Deserializer<Object> keyDeserializer = resolver.getInstance(source.keyDeserializer());
                //noinspection unchecked
                Deserializer<Object> valueDeserializer = resolver.getInstance(source.valueDeserializer());
                topology.addSource(source.name(), keyDeserializer, valueDeserializer, source.topics());
            }
        }

        private void addProcessor(Topology topology, Method method, Streams.Processor processor) {

            if (method.getParameterCount() > 0) {
                throw new IllegalStateException("Processor supplier: " + signature(method) + " must have NO parameter");
            }

            if (!Processor.class.isAssignableFrom(method.getReturnType())) {
                throw new IllegalStateException("Processor supplier: " + signature(method) + " must return " + Processor.class.getName());
            }
            topology.addProcessor(processor.name(), () -> processorSupplier(method), processor.parentNames());
        }

        private void addSinks(Topology topology, Streams.Sink[] sinks) {
            for (Streams.Sink sink : sinks) {
                //noinspection unchecked
                Serializer<Object> keySerializer = resolver.getInstance(sink.keySerializer());
                //noinspection unchecked
                Serializer<Object> valueSerializer = resolver.getInstance(sink.valueSerializer());
                topology.addSink(sink.name(), sink.topic(), keySerializer, valueSerializer, sink.parentNames());
            }
        }

        private Processor<?, ?, ?, ?> processorSupplier(Method method) {
            try {
                return (Processor<?, ?, ?, ?>) method.invoke(target);
            } catch (IllegalAccessException iaex) {
                throw new IllegalStateException("Cannot access method: " + signature(method), iaex);
            } catch (InvocationTargetException itex) {
                throw new IllegalStateException("Cannot get a processor with: " + signature(method), itex);
            }
        }
    }
}
