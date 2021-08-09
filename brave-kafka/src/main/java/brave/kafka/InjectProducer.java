package brave.kafka;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface InjectProducer {

    String acks() default "1";
    String keySerializer() default "org.apache.kafka.common.serialization.ByteArraySerializer";
    String valueSerializer() default "org.apache.kafka.common.serialization.ByteArraySerializer";

}
