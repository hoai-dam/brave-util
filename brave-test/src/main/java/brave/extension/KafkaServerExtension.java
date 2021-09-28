package brave.extension;

import brave.extension.util.KafkaUtil;
import lombok.extern.slf4j.Slf4j;
import net.manub.embeddedkafka.EmbeddedKafka;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class KafkaServerExtension implements BeforeAllCallback, ExtensionContext.Store.CloseableResource {

    private final static AtomicBoolean kafkaStarted = new AtomicBoolean(false);

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
                    .put(KafkaServerExtension.class.getName(), this);

            KafkaUtil.startKafkaServer(classLevelContext);
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
}
