package brave.kafka;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KafkaStreams;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.LinkedHashMap;
import java.util.Map;

@Component
@RequiredArgsConstructor
public class StreamAnnotationProcessor {

    private final KafkaConfig config;
    private final KafkaConfigResolver resolver;
    private final Map<String, KafkaStreams> kafkaStreams = new LinkedHashMap<>();

    @PostConstruct
    void initialize() {
        Map<String, Object> streamBeans = config.getContext().getBeansWithAnnotation(Streams.class);

        for (String beanName: streamBeans.keySet()) {
            Object streamBean = streamBeans.get(beanName);

            this.kafkaStreams.put(beanName, new StreamsBuilder(streamBean).process());
        }

    }

    class StreamsBuilder {

        private final Object target;

        StreamsBuilder(Object target) {
            this.target = target;
        }

        KafkaStreams process() {

            return  null;
        }
    }
}
