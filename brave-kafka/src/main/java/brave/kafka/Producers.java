package brave.kafka;

import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Producers {

    String bootstrapServers();

    @Target(ElementType.FIELD)
    @Retention(RetentionPolicy.RUNTIME)
    @interface Inject {

        String acks() default "1";
        Class<? extends Serializer> keySerializer() default ByteArraySerializer.class;
        Class<? extends Serializer> valueSerializer() default ByteArraySerializer.class;

    }
}
