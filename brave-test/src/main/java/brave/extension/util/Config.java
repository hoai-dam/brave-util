package brave.extension.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariConfig;
import org.junit.jupiter.api.extension.ExtensionContext;

public class Config {

    public static final String PROJECT_RESOURCE_PATH = "src/test/resources/";
    public static final String TEST_SUFFIX = "Test";

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static HikariConfig getDatasource(ExtensionContext context, String dataSourceName) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(EnvironmentUtil.getString(context, Key.DATA_SOURCES + "." + dataSourceName + ".url"));
        config.setUsername(EnvironmentUtil.getString(context, Key.DATA_SOURCES + "." + dataSourceName + ".username"));
        config.setPassword(EnvironmentUtil.getString(context, Key.DATA_SOURCES + "." + dataSourceName + ".password"));
        config.setDriverClassName(EnvironmentUtil.getString(context, Key.DATA_SOURCES + "." + dataSourceName + ".driver"));

        return config;
    }

    public static String getScript(ExtensionContext context, String dataSourceName) {
        return EnvironmentUtil.getString(context, Key.DATA_SOURCES + "." + dataSourceName + ".script");
    }

    public static String[] getScripts(ExtensionContext context, String dataSourceName) {
        String scripts = EnvironmentUtil.getString(context, Key.DATA_SOURCES + "." + dataSourceName + ".scripts");
        return scripts.split(":");
    }

    public static ObjectMapper getObjectMapper() {
        return objectMapper;
    }

    public static String getKafkaBootstrapServers(ExtensionContext context) {
        return "localhost:" + getKafkaBootstrapPort(context);
    }

    public static int getKafkaBootstrapPort(ExtensionContext context) {
        return EnvironmentUtil.getInt(context, Key.KAFKA_BOOTSTRAP_PORT, 9092);
    }

    public static int getWireMockPort(ExtensionContext context) {
        return EnvironmentUtil.getInt(context, Key.WIREMOCK_PORT, 8080);
    }

    public static class Key {
        public static final String KAFKA_BOOTSTRAP_PORT = "brave.test.kafka.bootstrap.port";
        public static final String WIREMOCK_PORT = "brave.test.wiremock.port";
        public static final String DATA_SOURCES = "brave.test.datasources";
    }
}
