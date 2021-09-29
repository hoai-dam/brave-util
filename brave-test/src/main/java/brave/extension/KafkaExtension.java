package brave.extension;

import brave.extension.util.Config;
import brave.extension.util.KafkaUtil;
import lombok.extern.slf4j.Slf4j;
import net.manub.embeddedkafka.EmbeddedKafka;
import org.junit.jupiter.api.extension.*;

import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class KafkaExtension implements BeforeAllCallback, ParameterResolver, ExtensionContext.Store.CloseableResource {

    private final static AtomicBoolean kafkaStarted = new AtomicBoolean(false);

    private KafkaStub kafkaStub;

    @Override
    public void beforeAll(ExtensionContext classLevelContext) {
        if (kafkaStarted.compareAndSet(false, true)) {
            log.warn("Starting {}", EmbeddedKafka.class.getName());
            ExtensionContext rootContext = classLevelContext.getRoot();

            rootContext
                    // This ExtensionContext.Store instance will be disposed at the end of root context life-cycle
                    .getStore(ExtensionContext.Namespace.GLOBAL)
                    // And any values in this store that implements ExtensionContext.Store.CloseableResource
                    // will be disposed too
                    .put(KafkaExtension.class.getName(), this);

            int bootstrapPort = Config.getKafkaBootstrapPort(classLevelContext);
            KafkaUtil.startKafkaServer(bootstrapPort);

            String bootstrapServer = Config.getKafkaBootstrapServers(classLevelContext);
            kafkaStub = new KafkaStub(bootstrapServer);
        }
    }

    @Override
    public void close() {
        if (kafkaStarted.compareAndSet(true, false)) {
            // When this is disposed (trigger by the disposing of ExtensionContext.Store)
            // we stop the EmbeddedKafka
            log.warn("Stopping {}", EmbeddedKafka.class.getName());
            EmbeddedKafka.stop();
        }
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return parameterContext.getParameter().getType() == KafkaStub.class;
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return kafkaStub;
    }
}
