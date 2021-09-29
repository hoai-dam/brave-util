package brave.extension;

import brave.extension.util.Config;
import brave.extension.util.EnvironmentUtil;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.jdbc.ScriptRunner;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;
import org.junit.jupiter.api.extension.*;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.Reader;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Prepare database schemas before the execution of an extended test class.
 * All the test methods in that test class will SHARE THE SAME DATABASE SCHEMAS.
 */
@Slf4j
public class DatabaseExtension implements BeforeAllCallback, ParameterResolver {

    private final static AtomicBoolean schemaInitialized = new AtomicBoolean(false);
    private DatabaseStub databaseDataExtension;

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        if (schemaInitialized.compareAndSet(false, true)) {
            Map<String, DataSource> datasources = new HashMap<>();

            String[] datasourceNames =  EnvironmentUtil.getString(context, Config.Key.DATASOURCES).split(",");
            for (String datasourceName : datasourceNames) {
                HikariConfig config = Config.getDatasource(context, datasourceName);
                DataSource dataSource = new HikariDataSource(config);
                String scriptClassPath = Config.getScript(context, datasourceName);

                initiateSchemaForDataSource(scriptClassPath, dataSource);
                datasources.put(datasourceName, dataSource);
            }

            databaseDataExtension = new DatabaseStub(datasources);
        }
    }

    private void initiateSchemaForDataSource(String scriptFile, DataSource dataSource) throws SQLException, IOException {
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

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return parameterContext.getParameter().getType() == DatabaseStub.class;
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return databaseDataExtension;
    }
}
