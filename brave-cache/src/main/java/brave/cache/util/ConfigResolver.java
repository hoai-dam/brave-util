package brave.cache.util;

import lombok.RequiredArgsConstructor;
import org.springframework.core.env.AbstractEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.expression.ExpressionParser;
import org.springframework.util.StringValueResolver;

import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.StreamSupport;

@RequiredArgsConstructor
public class ConfigResolver {

    private final StringValueResolver valueResolver;
    private final ExpressionParser expressionParser;
    private final AbstractEnvironment environment;

    public Properties getProperties(String path) {
        Properties properties = new Properties();
        MutablePropertySources propertySources = environment.getPropertySources();
        String pathPrefix = path + ".";

        //noinspection rawtypes
        StreamSupport.stream(propertySources.spliterator(), false)
                .filter(propSource -> propSource instanceof EnumerablePropertySource)
                .map(propSource -> ((EnumerablePropertySource) propSource).getPropertyNames())
                .flatMap(Arrays::stream)
                .filter(propName -> propName.startsWith(pathPrefix))
                .forEach(propName -> {
                    String key = propName.substring(pathPrefix.length());
                    String value = environment.getProperty(propName);
                    properties.put(key, value);
                });

        return properties;
    }

    public String getString(String expression) {
        String resolvedValue = valueResolver.resolveStringValue(expression);
        if (resolvedValue == null) {
            throw new IllegalStateException("Cannot resolve expression '" + expression + "'");
        }
        return resolvedValue;
    }

    public <T extends Enum<T>> T getEnum(String expression, Class<T> enumClass) {
        String resolvedValue = valueResolver.resolveStringValue(expression);
        if (resolvedValue == null) {
            throw new IllegalStateException("Cannot resolve expression '" + expression + "'");
        }
        return Enum.valueOf(enumClass, resolvedValue);
    }

    public Duration getDuration(String expression) {
        String resolvedValue = valueResolver.resolveStringValue(expression);
        if (resolvedValue == null) {
            throw new IllegalStateException("Cannot resolve expression '" + expression + "'");
        }
        return Duration.parse(resolvedValue);
    }

    public int getInt(String expression) {
        String resolvedValue = valueResolver.resolveStringValue(expression);
        if (resolvedValue == null) {
            throw new IllegalStateException("Cannot resolve expression '" + expression + "'");
        }
        return Integer.parseInt(resolvedValue);
    }

    public boolean getBoolean(String expression) {
        String resolvedValue = valueResolver.resolveStringValue(expression);
        if (resolvedValue == null) {
            throw new IllegalStateException("Cannot resolve expression '" + expression + "'");
        }
        return Boolean.parseBoolean(resolvedValue);
    }

    public <T> T getInstance(String expression, Class<T> clazz) {
        if (expression.startsWith("#{") && expression.endsWith("}")) {
            String spelExpression = expression.substring(2, expression.length() - 1);
            return expressionParser.parseExpression(spelExpression).getValue(clazz);
        }
        try {
            String className = valueResolver.resolveStringValue(expression);
            Class<?> resolvedClass = Class.forName(className);
            if (clazz.isAssignableFrom(resolvedClass)) {
                //noinspection unchecked
                Class<T> desClass = (Class<T>) resolvedClass;
                try {
                    return desClass.getConstructor().newInstance();
                } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
                    throw new IllegalStateException("Failed to construct " + desClass.getName(), e);
                }
            }
            throw new IllegalStateException(resolvedClass.getName() + " is not " + clazz.getName());
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Cannot resolve expression '" + expression + "'");
        }
    }
}
