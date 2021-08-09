package brave.kafka;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.stream.Collectors;

public class ReflectionUtil {

    public static String name(Method method) {
        return method.getDeclaringClass().getName() + "." + method.getName();
    }

    public static String name(Field field) {
        return field.getDeclaringClass().getName() + "." + field.getName();
    }

    public static String signature(Method method) {
        String parameterTypes = Arrays.stream(method.getParameterTypes())
                .map(Class::getName)
                .collect(Collectors.joining(","));

        return method.getDeclaringClass().getName() + "." + method.getName() + "(" + parameterTypes + ")";
    }
}
