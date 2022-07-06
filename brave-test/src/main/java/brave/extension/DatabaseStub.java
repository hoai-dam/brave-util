package brave.extension;

import brave.extension.util.ResourcesPathUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.jdbc.ScriptRunner;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;

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

    public void run(String dataSourceName, String scriptClassPath) throws SQLException, IOException {
        log.warn("Running script {} for data source {}", scriptClassPath, dataSourceName);
        DataSource dataSource = datasources.get(dataSourceName);
        if (dataSource != null) {
            run(dataSource, scriptClassPath);
        } else {
            throw new IllegalArgumentException("Data source '" + dataSourceName + "' not found");
        }
    }

    private void run(DataSource dataSource, String scriptFile) throws SQLException, IOException {
        SqlSessionFactory factory = getSqlSessionFactory(dataSource);

        try (var session = factory.openSession()) {
            try (var connection = session.getConnection()) {
                var sqlRunner = new ScriptRunner(connection);

                try (Reader scriptReader = Resources.getResourceAsReader(scriptFile)) {
                    sqlRunner.runScript(scriptReader);
                }
            }
        }
    }

    private SqlSessionFactory getSqlSessionFactory(DataSource dataSource) {
        Configuration mybatisConfig = new Configuration();
        Environment mybatisEnvironment = new Environment("test", new JdbcTransactionFactory(), dataSource);
        mybatisConfig.setEnvironment(mybatisEnvironment);
        return new SqlSessionFactoryBuilder().build(mybatisConfig);
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
        return dataFolderPath.toFile();
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

        if (!dataFolder.exists()) {
            log.warn("'" + dataFolder + "' should be a folder");
            return new File[0];
        }

        if (!dataFolder.isDirectory()) {
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

    public List<Map<String, Object>> queryToList(String dataSourceName, String sqlQuery) throws SQLException {
        DataSource dataSource = datasources.get(dataSourceName);
        if (dataSource != null) {
            return query(dataSource, sqlQuery);
        } else {
            throw new IllegalArgumentException("Data source '" + dataSourceName + "' not found");
        }
    }

    public Map<String, Object> findFirstById(String dataSourceName, String table, String id) throws SQLException {
        DataSource dataSource = datasources.get(dataSourceName);
        String sqlQuery = format("SELECT * FROM %s WHERE id = '%s'", table, id);
        if (dataSource != null) {
            List<Map<String, Object>> resultList = query(dataSource, sqlQuery);
            if (!resultList.isEmpty()) {
                return resultList.get(0);
            }
        } else {
            throw new IllegalArgumentException("Data source '" + dataSourceName + "' not found");
        }

        return null;
    }

    private List<Map<String, Object>> query(DataSource dataSource, String sqlQuery) throws SQLException {
        SqlSessionFactory factory = getSqlSessionFactory(dataSource);

        try (var session = factory.openSession()) {
            try (var connection= session.getConnection()) {
                try (PreparedStatement statement = connection.prepareStatement(sqlQuery)) {
                    ResultSet resultSet = statement.executeQuery();
                    if (resultSet == null) {
                        return new ArrayList<>();
                    }
                    return rsToList(resultSet);
                }
            }
        }
    }

    private static List<Map<String, Object>> rsToList(ResultSet rs) throws SQLException {
        ResultSetMetaData md = rs.getMetaData();
        int columns = md.getColumnCount();
        List<Map<String, Object>> results = new ArrayList<>();

        while (rs.next()) {
            HashMap<String, Object> row = new HashMap<>();
            for (int i = 1; i <= columns; i++) {
                String property = md.getColumnLabel(i);
                Object value = rs.getObject(i);
                row.put(property, value);
            }
            results.add(row);
        }

        return results;
    }
}
