package brave.cache.annotation;

import brave.cache.codec.ByteArrayCodec;
import brave.cache.codec.Codec;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Cacheable
{

    String name();
    String backend() default "LOCAL";
    String timeToLive();
    Local local() default @Local(
            keyClass = void.class,
            valueClass = void.class
    );
    Redis redis() default @Redis(
            keyCodec = ByteArrayCodec.class,
            valueCodec = ByteArrayCodec.class
    );

    @interface Local {
        long entryCapacity() default Long.MAX_VALUE;
        boolean refreshAhead() default true;
        boolean permitNullValues() default true;
        Class<?> keyClass();
        Class<?> valueClass();
    }

    @interface Redis {
        Class<? extends Codec<?>> keyCodec();
        Class<? extends Codec<?>> valueCodec();
    }

    @Retention(RetentionPolicy.RUNTIME)
    @interface Inject {}
}
