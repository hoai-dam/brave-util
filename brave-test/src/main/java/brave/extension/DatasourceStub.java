package brave.extension;

import brave.extension.util.ResourcesPathUtil;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.ExtensionContext;

import javax.sql.DataSource;
import java.io.File;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

import static java.lang.String.format;

/**
 * Prepare database data before the execution of each test method.
 */
@Slf4j
public class DatasourceStub {

    private static final String CSV_SUFFIX = ".csv";
    private final Map<String, DataSource> datasources;

    public DatasourceStub(Map<String, DataSource> datasources) {
        this.datasources = datasources;
    }

    public void load(String datasourceName, String dataClassPath) throws SQLException {
        log.warn("Preparing data for {} from folder {}", datasourceName, dataClassPath);

        Path dataFolderPath = ResourcesPathUtil.classPathToFilePath(dataClassPath);
        File dataFolder = dataFolderPath.toFile();
        if (!dataFolder.exists() || !dataFolder.isDirectory()) {
            log.warn("dataFolder {} for {} is not valid", dataClassPath, datasourceName);
            return;
        }

        DataSource dataSource = datasources.get(datasourceName);
        if (dataSource == null) {
            throw new IllegalArgumentException("Datasource '" + datasourceName + "' not found");
        }

        try (Connection connection = dataSource.getConnection()) {
            prepareDatabaseForTest(dataFolder, connection);
        }
    }

    static void prepareDatabaseForTest(File schemaDataFolder, Connection connection) throws SQLException {
        if (schemaDataFolder == null) {
            log.warn("Schema data folder should not be null");
            return;
        }

        if (!schemaDataFolder.exists() || !schemaDataFolder.isDirectory()) {
            log.warn("Provided schemaDataFolder should be a folder");
            return;
        }

        var tableFiles = schemaDataFolder.listFiles((folder, name) -> name.endsWith(CSV_SUFFIX));
        if (tableFiles == null) {
            log.warn("Provided schemaDataFolder should be a accessible");
            return;
        }

        String schemaName = schemaDataFolder.getName();
        Arrays.sort(tableFiles, Comparator.comparing(File::getName));

        for (var tableDataFile : tableFiles) {
            var tableName = tableDataFileNameToTableName(tableDataFile.getName(), CSV_SUFFIX);
            var tableFullName = schemaName + "." + tableName;

            clearTable(tableFullName, connection);

            String csvDataClassPath = ResourcesPathUtil.filePathToClassPath(tableDataFile.getPath());
            loadCsv(csvDataClassPath, tableFullName, connection);
        }
    }

    @SuppressWarnings("SameParameterValue")
    static String tableDataFileNameToTableName(String original, String ending) {
        final int firstDot = original.indexOf(".");
        if (firstDot >= 0) {
            try {
                Integer.parseInt(original.substring(0, firstDot));
                return original.substring(firstDot + 1, original.length() - ending.length());
            } catch (Throwable ignored) {

            }
        }
        return original.substring(0, original.length() - ending.length());
    }

    static void clearTable(String tableFullName, Connection connection) throws SQLException {
        var deleteAllSql = format("DELETE FROM %s", tableFullName);

        try (var statement = connection.createStatement()) {
            statement.execute(deleteAllSql);
        }
    }

    static void loadCsv(String csvClassPath, String tableFullName, Connection connection) throws SQLException {
        var insertFromCsvSql = format("INSERT INTO %s SELECT * FROM CSVREAD('classpath:/%s', null, 'null=null charset=UTF-8')", tableFullName, csvClassPath);

        try (var statement = connection.createStatement()) {
            statement.execute(insertFromCsvSql);
        }
    }

}
