package brave.kafka;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.common.serialization.Deserializer;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.time.Duration;
import java.util.List;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface BraveConsumers {

    String bootstrapServers() default "";
    String groupId() default "";
    String enableAutoCommit() default "";
    String autoCommitInterval() default "";
    String autoOffsetReset() default "";
    String maxPollInterval() default "";
    String maxPollRecords() default "";
    String sessionTimeout() default "";
    String properties() default "";

    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.RUNTIME)
    @interface Handler {

        String[] topics() default {};
        String pollingTimeout() default "";
        String threadsCount() default "";
        String ignoreException() default "";
        String reportHealthCheck() default "";
        String keyDeserializer() default "";
        String valueDeserializer() default "";
        String properties() default "";

        @Setter
        @Getter
        @Builder
        class Config {

            private List<String> topics;
            private Duration pollingTimeout;
            private int threadsCount;
            private boolean ignoreException;
            private boolean reportHealthCheck;
            private Deserializer<Object> keyDeserializer;
            private Deserializer<Object> valueDeserializer;

        }
    }
}
