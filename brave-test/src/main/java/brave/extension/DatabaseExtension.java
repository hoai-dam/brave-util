package brave.extension;

import brave.extension.util.Config;
import brave.extension.util.EnvironmentUtil;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.*;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Prepare database schemas before the execution of an extended test class.
 * All the test methods in that test class will SHARE THE SAME DATABASE SCHEMAS.
 */
@Slf4j
public class DatabaseExtension implements BeforeAllCallback, ParameterResolver, ExtensionContext.Store.CloseableResource {

    private final static AtomicBoolean schemaInitialized = new AtomicBoolean(false);
    private static DatabaseStub databaseStub;

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        if (schemaInitialized.compareAndSet(false, true)) {

            context.getRoot()
                    // This ExtensionContext.Store instance will be disposed at the end of root context life-cycle
                    .getStore(ExtensionContext.Namespace.GLOBAL)
                    // And any values in this store that implements ExtensionContext.Store.CloseableResource
                    // will be disposed too
                    .put(DatabaseExtension.class.getName(), this);

            Map<String, DataSource> datasources = new HashMap<>();
            String[] datasourceNames =  EnvironmentUtil.getString(context, Config.Key.DATASOURCES).split(",");
            for (String datasourceName : datasourceNames) {
                HikariConfig config = Config.getDatasource(context, datasourceName);
                datasources.put(datasourceName, new HikariDataSource(config));
            }

            databaseStub = new DatabaseStub(datasources);

            for (String datasourceName : datasourceNames) {
                for (String scriptClassPath : Config.getScripts(context, datasourceName)) {
                    databaseStub.run(datasourceName, scriptClassPath);
                }
            }

            BraveTestContext.setDatabaseStub(databaseStub);
        }
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return parameterContext.getParameter().getType() == DatabaseStub.class;
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return databaseStub;
    }

    @Override
    public void close() throws Throwable {
        if (schemaInitialized.compareAndSet(true, false)) {
            log.warn("Removing database stub");
            databaseStub = null;
            BraveTestContext.setDatabaseStub(null);
        }
    }
}
