package brave.kafka;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.Deserializer;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.time.Duration;
import java.util.List;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Consumers {

    String properties() default "";
    String[] bootstrapServers() default {};
    String groupId() default "";
    boolean enableAutoCommit() default true;
    int autoCommitIntervalMillis() default 5000;
    String autoOffsetReset() default "latest";
    int maxPollIntervalMillis() default 300000;
    int maxPollRecords() default 512;
    int sessionTimeoutMillis() default 10000;

    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.RUNTIME)
    @interface Handler {

        String properties() default "";
        String[] topics() default {};
        int pollingTimeoutMillis() default 1000;
        int threadsCount() default 1;
        boolean ignoreException() default false;
        boolean reportHealthCheck() default true;
        Class<? extends Deserializer> keyDeserializer() default BytesDeserializer.class;
        Class<? extends Deserializer> valueDeserializer() default BytesDeserializer.class;

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
