package brave.kafka;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

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

}
