package brave.cache.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Cacheable
{

    String name();
    String backend() default "REDIS";
    String timeToLive();
    Local local() default @Local(
            keyClass = void.class,
            valueClass = void.class
    );
    Redis redis() default @Redis(
            keyCodec = "",
            valueCodec = ""
    );

    @interface Local {
        long entryCapacity() default Long.MAX_VALUE;
        boolean refreshAhead() default true;
        boolean permitNullValues() default true;
        Class<?> keyClass();
        Class<?> valueClass();
    }

    @interface Redis {
        String keyCodec();
        String valueCodec();
    }

    @Retention(RetentionPolicy.RUNTIME)
    @interface Inject {}
}
