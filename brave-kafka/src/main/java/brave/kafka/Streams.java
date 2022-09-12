package brave.kafka;

import org.apache.kafka.common.serialization.*;

import java.lang.annotation.*;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Streams {

    String properties() default "";

    // Kafka stream configs
    String[] bootstrapServers() default {};
    String applicationId() default "";
    int pollMillis() default 300000;
    int commitIntervalMillis() default 5000;
    int requestTimeoutMillis() default 60000;
    int numStreamThreads() default 1;
    Class<? extends Serde> defaultKeySerde() default Serdes.ByteArraySerde.class;
    Class<? extends Serde> defaultValueSerde() default Serdes.ByteArraySerde.class;

    // Error handling configs
    boolean ignoreException() default false;
    boolean reportHealthCheck() default true;

    @Repeatable(Sources.class)
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.RUNTIME)
    @interface Source {

        String name();
        String[] topics() default {};
        Class<? extends Deserializer> keyDeserializer() default ByteArrayDeserializer.class;
        Class<? extends Deserializer> valueDeserializer() default ByteArrayDeserializer.class;

    }

    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.RUNTIME)
    @interface Sources {
        Source[] value();
    }

    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.RUNTIME)
    @interface Processor {
        String name();
        String[] parentNames();
    }

    @Repeatable(Sinks.class)
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.RUNTIME)
    @interface Sink {
        String name();
        String[] parentNames();
        Class<? extends Serializer> keySerializer() default ByteArraySerializer .class;
        Class<? extends Serializer> valueSerializer() default ByteArraySerializer.class;
    }

    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.RUNTIME)
    @interface Sinks {
        Sink[] value();
    }
}
