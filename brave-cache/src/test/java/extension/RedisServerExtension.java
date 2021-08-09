package extension;

import gardentest.AppConfig;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import redis.embedded.RedisServer;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Start the {@link RedisServer} only once.
 * <br><br>
 * This means even when you extend multiple test classes with this extension,
 * the {@link RedisServer} instance will start only ONCE, and before the execution of
 * all those test classes.
 * <br><br>
 * Then stop the {@link RedisServer} instance when the <b>root</b> {@link ExtensionContext}
 * of all above test classes ends.
 */
public class RedisServerExtension implements BeforeAllCallback, ExtensionContext.Store.CloseableResource {

    private final static AtomicBoolean redisStarted = new AtomicBoolean(false);

    private RedisServer redisServer;

    @Override
    public void beforeAll(ExtensionContext classLevelContext) {
        if (redisStarted.compareAndSet(false, true)) {
            classLevelContext.getRoot()
                    // This ExtensionContext.Store instance will be disposed at the end of root context life-cycle
                    .getStore(ExtensionContext.Namespace.GLOBAL)
                    // And any values in this store that implements ExtensionContext.Store.CloseableResource
                    // will be disposed too
                    .put(RedisServer.class.getName(), this);

            int port = getConfiguredRedisPort(classLevelContext);

            redisServer = RedisServer.builder()
                    .port(port)
                    .setting("daemonize no")
                    .setting("appendonly no")
                    .setting("maxmemory 128M")
                    .build();

            System.err.printf("Starting %s at %d\n", RedisServer.class.getName(), port);
            redisServer.start();
        }
    }

    private int getConfiguredRedisPort(ExtensionContext classLevelContext) {
        ApplicationContext appContext = SpringExtension.getApplicationContext(classLevelContext);
        AppConfig appConfig = appContext.getBean(AppConfig.class);
        return appConfig.redisPort();
    }

    @Override
    public void close() throws Throwable {
        if (redisStarted.compareAndSet(true, false)) {
            // When this is disposed (trigger by the disposing of ExtensionContext.Store)
            // we stop the EmbeddedKafka
            System.err.printf("Stopping %s\n", RedisServer.class.getName());
            redisServer.stop();
        }
    }
}
