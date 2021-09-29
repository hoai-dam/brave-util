package brave.extension.util;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.util.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class EnvironmentUtil {

    public static String getString(ExtensionContext context, String key, String ... defaultValue) {
        List<String> alternativeKeys = new ArrayList<>();
        alternativeKeys.add(key);

        if (key.contains(".")) {
            alternativeKeys.add(key.replace('.', '_'));
            alternativeKeys.add(key.replace('.', '-'));
        } else if (key.contains("_")) {
            alternativeKeys.add(key.replace('_', '.'));
            alternativeKeys.add(key.replace('_', '-'));
        } else if (key.contains("-")) {
            alternativeKeys.add(key.replace('-', '.'));
            alternativeKeys.add(key.replace('-', '_'));
        }

        for (String aKey : alternativeKeys) {
            String raw = context != null
                    ? context.getConfigurationParameter(aKey).orElse("")
                    : System.getProperty(aKey);
            if (StringUtils.isNotBlank(raw)) return raw;

            raw = System.getenv(aKey);
            if (StringUtils.isNotBlank(raw)) return raw;

            raw = System.getenv(aKey.toUpperCase());
            if (StringUtils.isNotBlank(raw)) return raw;
        }

        if (defaultValue.length == 0) {
            throw new IllegalStateException("No default value provided for '" + key + "'");
        }

        return defaultValue[0];
    }

    public static int getInt(ExtensionContext context, String key, int defaultValue) {
        try {
            String raw = getString(context, key, String.valueOf(defaultValue));
            return Integer.parseInt(raw);
        } catch (NumberFormatException nfex) {
            throw new IllegalArgumentException("Provided '" + key + "' value is not valid integer");
        }
    }

    public static int getInt(String key, int defaultValue) {
        return getInt(null, key, defaultValue);
    }

}
