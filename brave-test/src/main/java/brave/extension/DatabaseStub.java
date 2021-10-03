package brave.extension;

import brave.extension.util.ResourcesPathUtil;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.io.File;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;

import static java.lang.String.format;

/**
 * Prepare database data before the execution of each test method.
 */
@Slf4j
public class DatabaseStub {

    private static final String CSV_SUFFIX = ".csv";
    private final Map<String, DataSource> datasources;

    public DatabaseStub(Map<String, DataSource> datasources) {
        this.datasources = datasources;
    }

    public void load(String datasourceName, String dataFolderClassPath) throws SQLException {
        log.warn("Preparing data for {} from folder {}", datasourceName, dataFolderClassPath);

        DataSource dataSource = datasources.get(datasourceName);
        if (dataSource == null) {
            throw new IllegalArgumentException("Datasource '" + datasourceName + "' not found");
        }

        File dataFolder = getDataFolder(dataFolderClassPath);
        try (Connection connection = dataSource.getConnection()) {
            prepareDatabaseForTest(dataFolder, connection);
        }
    }

    public void unload(String datasourceName, String dataFolderClassPath) throws SQLException {
        log.warn("Unloading data for {} from folder {}", datasourceName, dataFolderClassPath);

        DataSource dataSource = datasources.get(datasourceName);
        if (dataSource == null) {
            throw new IllegalArgumentException("Datasource '" + datasourceName + "' not found");
        }

        File dataFolder = getDataFolder(dataFolderClassPath);
        try (Connection connection = dataSource.getConnection()) {
            clearDatabaseForTest(dataFolder, connection);
        }
    }

    private File getDataFolder(String dataFolderClassPath) {
        Path dataFolderPath = ResourcesPathUtil.classPathToFilePath(dataFolderClassPath);
        File dataFolder = dataFolderPath.toFile();
        if (!dataFolder.exists() || !dataFolder.isDirectory()) {
            throw new IllegalArgumentException("dataFolder '" + dataFolderClassPath + "' is not valid");
        }
        return dataFolder;
    }

    static void prepareDatabaseForTest(File dataFolder, Connection connection) throws SQLException {
        File[] tableFiles = getTableFiles(dataFolder);
        String schemaName = dataFolder.getName();
        Arrays.sort(tableFiles, Comparator.comparing(File::getName));

        for (var tableDataFile : tableFiles) {
            var tableName = fileNameToTableName(tableDataFile.getName(), CSV_SUFFIX);
            var tableFullName = schemaName + "." + tableName;

            String csvDataClassPath = ResourcesPathUtil.filePathToClassPath(tableDataFile.getPath());
            loadCsv(csvDataClassPath, tableFullName, connection);
        }
    }

    static void clearDatabaseForTest(File dataFolder, Connection connection) throws SQLException {
        File[] tableFiles = getTableFiles(dataFolder);
        String schemaName = dataFolder.getName();
        Arrays.sort(tableFiles, Comparator.comparing(File::getName));

        for (var tableDataFile : tableFiles) {
            var tableName = fileNameToTableName(tableDataFile.getName(), CSV_SUFFIX);
            var tableFullName = schemaName + "." + tableName;

            clearTable(tableFullName, connection);
        }
    }

    private static File[] getTableFiles(File dataFolder) {
        if (dataFolder == null) {
            throw new IllegalArgumentException("Schema data folder should not be null");
        }

        if (!dataFolder.exists() || !dataFolder.isDirectory()) {
            throw new IllegalArgumentException("'" + dataFolder + "' should be a folder");
        }

        var tableFiles = dataFolder.listFiles((folder, name) -> name.endsWith(CSV_SUFFIX));
        if (tableFiles == null) {
            throw new IllegalArgumentException("'" + dataFolder + "' should be accessible");
        }
        return tableFiles;
    }

    @SuppressWarnings("SameParameterValue")
    static String fileNameToTableName(String original, String ending) {
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
