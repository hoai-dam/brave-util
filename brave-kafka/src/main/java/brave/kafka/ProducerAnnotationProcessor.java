package brave.kafka;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import static brave.kafka.ReflectionUtil.*;
import static java.lang.String.format;

@Component
@RequiredArgsConstructor
public class ProducerAnnotationProcessor {

    private final KafkaConfig config;
    private final KafkaConfigResolver configResolver;
    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    private final Map<String, Producer<?, ?>> producers = new LinkedHashMap<>();

    @PostConstruct
    void initialize() {
        Map<String, Object> producerBeans = config.getContext().getBeansWithAnnotation(Producers.class);

        for (String beanName : producerBeans.keySet()) {
            Object target = producerBeans.get(beanName);

            Producers braveProducers = target.getClass().getAnnotation(Producers.class);
            if (braveProducers == null) {
                throw new IllegalStateException("No annotation " + Producers.class.getName() + " found on target");
            }

            for (Field field : target.getClass().getDeclaredFields()) {
                Producers.Inject injectProducer = field.getAnnotation(Producers.Inject.class);
                if (injectProducer == null) continue;
                if (canBeSet(field, KafkaProducer.class)) {
                    //noinspection unchecked
                    Serializer<Object> keySer = configResolver.getInstance(injectProducer.keySerializer());
                    //noinspection unchecked
                    Serializer<Object> valueSer = configResolver.getInstance(injectProducer.valueSerializer());

                    Properties properties = getKafkaConsumerProperties(braveProducers, injectProducer);
                    KafkaProducer<Object, Object> producer = new KafkaProducer<>(properties, keySer, valueSer);

                    setField(target, field, producer);
                    this.producers.put(name(field), producer);
                } else {
                    throw new IllegalStateException(format("%s.%s must be of type %s and must NOT be final",
                            target.getClass().getName(), field.getName(), KafkaProducer.class.getName()));
                }
            }
        }
    }

    private Properties getKafkaConsumerProperties(Producers braveProducers, Producers.Inject injectProducer) {
        var properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, configResolver.getString(braveProducers.bootstrapServers()));
        properties.put(ProducerConfig.ACKS_CONFIG, configResolver.getString(injectProducer.acks()));

        return properties;
    }
}
