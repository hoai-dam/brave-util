package brave.kafka;

import org.apache.commons.collections4.IterableUtils;

import java.lang.reflect.Array;
import java.util.*;

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

    public static boolean fallbackIfEmpty(Properties properties, String propertyName, Object fallbackValue) {
        if (sizeIsEmpty(properties.get(propertyName))) {
            if (sizeIsEmpty(fallbackValue)) {
                throw new IllegalStateException("No " + propertyName + " provided");
            }

            properties.put(propertyName, fallbackValue);
            return true;
        }

        return false;
    }

    public static boolean sizeIsEmpty(final Object obj) {
        if (obj == null) {
            return true;

        } else if (obj instanceof String) {
            return ((String) obj).isBlank();

        } else if (obj instanceof Collection<?>) {
            return ((Collection<?>) obj).isEmpty();

        } else if (obj instanceof Iterable<?>) {
            return IterableUtils.isEmpty((Iterable<?>) obj);

        } else if (obj instanceof Map<?, ?>) {
            return ((Map<?, ?>) obj).isEmpty();

        } else if (obj instanceof Iterator<?>) {
            return !((Iterator<?>) obj).hasNext();

        } else if (obj instanceof Enumeration<?>) {
            return !((Enumeration<?>) obj).hasMoreElements();

        } else if (obj.getClass().isArray()) {
            return Array.getLength(obj) == 0;
        }
        return false;
    }
}
