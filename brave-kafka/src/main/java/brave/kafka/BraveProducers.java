package brave.kafka;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface BraveProducers {

    String bootstrapServers();

    @Target(ElementType.FIELD)
    @Retention(RetentionPolicy.RUNTIME)
    @interface Inject {

        String acks() default "1";
        String keySerializer() default "org.apache.kafka.common.serialization.ByteArraySerializer";
        String valueSerializer() default "org.apache.kafka.common.serialization.ByteArraySerializer";

    }
}
