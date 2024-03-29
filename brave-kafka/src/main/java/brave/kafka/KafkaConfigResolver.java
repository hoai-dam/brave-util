package brave.kafka;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.config.ConfigDef;
import org.springframework.core.env.AbstractEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.expression.ExpressionParser;
import org.springframework.stereotype.Component;
import org.springframework.util.StringValueResolver;

import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.StreamSupport;

import static org.apache.commons.lang3.StringUtils.isBlank;

@Component
@RequiredArgsConstructor
public class KafkaConfigResolver {

    private final StringValueResolver valueResolver;
    private final ExpressionParser expressionParser;
    private final AbstractEnvironment environment;

    public Properties getProperties(String path) {
        if (isBlank(path))
            return new Properties();

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

    public Properties getProperties(String path, ConfigDef configDef) {
        Properties props = this.getProperties(path);

        for (String propertyName : props.stringPropertyNames()) {
            String value = props.getProperty(propertyName);
            ConfigDef.ConfigKey configKey = configDef.configKeys().get(propertyName);

            if (propertyName.endsWith(".ms")) {
                if (configKey.type() == ConfigDef.Type.SHORT) {
                    props.put(propertyName, (short) Duration.parse(value).toMillis());
                } else if (configKey.type() == ConfigDef.Type.INT) {
                    props.put(propertyName, (int) Duration.parse(value).toMillis());
                } else if (configKey.type() == ConfigDef.Type.LONG) {
                    props.put(propertyName, Duration.parse(value).toMillis());
                }
            }
        }

        return props;
    }

    public String getString(String expression) {
        String resolvedValue = valueResolver.resolveStringValue(expression);
        if (resolvedValue == null) {
            throw new IllegalStateException("Cannot resolve expression '" + expression + "'");
        }
        return resolvedValue;
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

    public <T> T getInstance(Class<T> clazz) {
        try {
            return clazz.getConstructor().newInstance();
        } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new IllegalStateException("Failed to construct " + clazz.getName(), e);
        }
    }
}
