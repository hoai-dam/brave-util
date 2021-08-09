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

import static brave.kafka.ReflectionUtil.name;

@Component
@RequiredArgsConstructor
public class ProducerAnnotationProcessor {

    private final KafkaConfig config;
    private final KafkaConfigResolver configResolver;
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
                if (field.getType() != Producer.class) {
                    throw new IllegalStateException("Only apply " + Producers.Inject.class.getName() + " on fields of type " + Producer.class.getName());
                }

                Properties properties = getKafkaConsumerProperties(braveProducers, injectProducer);
                //noinspection unchecked
                Serializer<Object> keySer = configResolver.getInstance(injectProducer.keySerializer());
                //noinspection unchecked
                Serializer<Object> valueSer = configResolver.getInstance(injectProducer.valueSerializer());
                KafkaProducer<Object, Object> producer = new KafkaProducer<>(properties, keySer, valueSer);

                boolean accessible = field.canAccess(target);
                try {
                    field.setAccessible(true);
                    field.set(target, producer);
                } catch (IllegalAccessException iaex) {
                    throw new IllegalStateException("Cannot access field " + name(field), iaex);
                } finally {
                    field.setAccessible(accessible);
                }

                this.producers.put(name(field), producer);
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
