package brave.extension;

import brave.extension.util.EnvironmentUtil;
import com.github.tomakehurst.wiremock.WireMockServer;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.*;

import java.util.concurrent.atomic.AtomicBoolean;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;


/**
 * Start the {@link WireMockServer} only once. {@link WireMockServer} provides the
 * external http service mocks.
 * <br><br>
 * This means even when you extend multiple test classes with this extension,
 * the {@link WireMockServer} instance will start only ONCE, and before the execution of
 * all those test classes.
 * <br><br>
 * You can provide the listening port in <b>junit-platform.properties</b>:
 * <br><br>
 * {@code brave.test.wiremock.port=8080}
 * <br><br>
 * Default port is {@code 6636}
 * <br><br>
 * Then stop the {@link WireMockServer} instance when the <b>root</b> {@link ExtensionContext}
 * of all above test classes ends.
 */
@Slf4j
public class WireMockExtension implements BeforeAllCallback, ParameterResolver, ExtensionContext.Store.CloseableResource  {

    public static final String WIREMOCK_PORT_KEY = "brave.test.wiremock.port";
    private final static AtomicBoolean wiremockStarted = new AtomicBoolean(false);
    private WireMockServer wireMockServer;
    private WiremockStub wiremockStub;

    @Override
    public void beforeAll(ExtensionContext classLevelContext) {
        if (wiremockStarted.compareAndSet(false, true)) {
            log.warn("Starting {}", WireMockServer.class.getName());

            classLevelContext.getRoot()
                    // This ExtensionContext.Store instance will be disposed at the end of root context life-cycle
                    .getStore(ExtensionContext.Namespace.GLOBAL)
                    // And any values in this store that implements ExtensionContext.Store.CloseableResource
                    // will be disposed too
                    .put(WireMockExtension.class.getName(), this);

            int port = EnvironmentUtil.getInt(classLevelContext, WIREMOCK_PORT_KEY, 6636);
            String wiremockBaseUrl = "http://localhost:" + port;

            wireMockServer = new WireMockServer(options().port(port));
            wireMockServer.start();
            wiremockStub = new WiremockStub(wireMockServer, wiremockBaseUrl);
        }
    }

    @Override
    public void close() {
        if (wiremockStarted.compareAndSet(true, false)) {
            log.warn("Stopping {}", WireMockServer.class.getName());
            wireMockServer.stop();
            wireMockServer = null;
        }
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return parameterContext.getParameter().getType() == WiremockStub.class;
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return wiremockStub;
    }

}
