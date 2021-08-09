package brave.kafka;

import java.util.List;
import java.util.Properties;

import static org.apache.commons.lang3.StringUtils.isBlank;

public class PropertyUtil {

    public static boolean isEmpty(Properties properties, String propertyName) {
        Object value = properties.get(propertyName);
        if (value instanceof String) {
            return isBlank((String)value);
        }
        if (value instanceof List) {
            //noinspection rawtypes
            return ((List) value).isEmpty();
        }
        return value == null;
    }
}
