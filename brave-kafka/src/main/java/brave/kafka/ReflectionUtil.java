package brave.kafka;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
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

    public static boolean isNotFinal(Field field) {
        return (field.getModifiers() & Modifier.FINAL) == 0;
    }

    public static boolean canBeSet(Field field, Class<?> clazz) {
        return isNotFinal(field) && field.getType().isAssignableFrom(clazz);
    }

    public static boolean isSetter(Method method, Class<?> clazz) {
        return method.getParameterCount() == 1 &&
                method.getParameters()[0].getType().isAssignableFrom(clazz) &&
                method.getReturnType().equals(Void.TYPE);
    }

    public static void setField(Object target, Field field, Object value) {
        boolean accessible = field.canAccess(target);
        try {
            field.setAccessible(true);
            field.set(target, value);
        } catch (IllegalAccessException iaex) {
            throw new IllegalStateException("Cannot access field " + name(field), iaex);
        } finally {
            field.setAccessible(accessible);
        }
    }
}
