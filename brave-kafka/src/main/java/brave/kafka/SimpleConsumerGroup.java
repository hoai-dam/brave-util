package brave.kafka;

import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.boot.actuate.health.Health;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

@Slf4j
@Builder
class SimpleConsumerGroup<K, V> {

    private final List<SimpleConsumer> consumers = new ArrayList<>();
    private final Supplier<Consumer<K, V>> consumerSupplier;
    private final List<String> topics;
    private final Integer threadsCount;
    private final Duration pollingTimeout;
    private final ConsumerRecordsDispatcher<K, V> recordProcessor;

    public void start() {
        for (int i = 0; i < threadsCount; i++) {
            consumers.add(new SimpleConsumer(consumerSupplier.get()));
        }

        log.warn("Starting {} consumer threads", consumers.size());
        consumers.forEach(SimpleConsumer::start);
        log.warn("Started {} consumer threads", consumers.size());
    }

    public void stop() {
        log.warn("Stopping {} consumer threads", consumers.size());
        consumers.forEach(SimpleConsumer::stop);
        log.warn("Stopped {} consumer threads", consumers.size());
    }

    public Health health() {
        String healthKey = "consumers{" + String.join(",", topics) + "}";
        long aliveInvalidatorsCount = consumers.stream().filter(SimpleConsumer::isAlive).count();
        if (aliveInvalidatorsCount > 0) {
            String details = String.format("%d / %d consumer threads ALIVE", aliveInvalidatorsCount, consumers.size());
            return Health
                    .up()
                    .withDetail(healthKey, details)
                    .build();
        }

        return Health
                .down()
                .withDetail(healthKey, "All consumer threads STOPPED")
                .build();
    }

    @RequiredArgsConstructor
    private class SimpleConsumer {

        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final Consumer<K, V> consumer;
        private final Thread consumerThread = new Thread(this::consumeMessages);

        public void start() {
            consumerThread.start();
        }

        public boolean isAlive() {
            return consumerThread.isAlive() && !closed.get();
        }

        private void consumeMessages() {
            try {
                consumer.subscribe(topics);

                while (!closed.get()) {
                    ConsumerRecords<K, V> consumerRecords = consumer.poll(pollingTimeout);
                    recordProcessor.process(consumerRecords, consumer);
                }
            } catch (WakeupException waex) {
                if (!closed.get()) throw waex;
            } finally {
                consumer.close();
            }
        }

        public void stop() {
            closed.set(true);
            consumer.wakeup();
        }
    }

}
