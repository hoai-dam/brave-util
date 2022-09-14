package brave.kafka;

import org.apache.kafka.common.serialization.*;

import java.lang.annotation.*;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Streams {

    String properties() default "";

    // Kafka stream configs
    String bootstrapServers() default "";
    String applicationId() default "";
    int pollMillis() default 100;
    int commitIntervalMillis() default 5000;
    int requestTimeoutMillis() default 60000;
    int numStreamThreads() default 1;
    Class<? extends Serde> defaultKeySerde() default Serdes.ByteArraySerde.class;
    Class<? extends Serde> defaultValueSerde() default Serdes.ByteArraySerde.class;

    // Error handling configs
    boolean ignoreException() default false;
    boolean reportHealthCheck() default true;

    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.RUNTIME)
    @interface Topology {

    }

    @Target({ElementType.FIELD, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    @interface Inject {

    }
}
