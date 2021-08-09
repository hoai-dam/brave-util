package brave.kafka;

import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import static brave.kafka.ReflectionUtil.name;

@Slf4j
@Builder
class ConsumerRecordsDispatcher<K, V> {

    private final boolean ignoreException;
    private final boolean isBatchProcessing;
    private final boolean injectConsumer;
    private final Object target;
    private final Method method;

    @SneakyThrows
    void process(ConsumerRecords<K, V> consumerRecords, Consumer<K, V> consumer) {
        if (isBatchProcessing) {
            Object[] params = injectConsumer
                    ? new Object[]{consumerRecords, consumer}
                    : new Object[]{consumerRecords};

            try {
                method.invoke(target, params);
            } catch (InvocationTargetException itex) {
                if (ignoreException) {
                    String errorMessage = String.format("Failed to process %s records", consumerRecords.count());
                    log.warn(errorMessage, itex.getTargetException());
                } else {
                    throw itex.getTargetException();
                }
            } catch (IllegalAccessException iaex) {
                throw new IllegalStateException("Cannot access method " + target.getClass().getName() + "." + method.getName(), iaex);
            }
        } else {
            for (TopicPartition partition : consumerRecords.partitions()) {
                List<ConsumerRecord<K, V>> partitionRecords = consumerRecords.records(partition);
                if (partitionRecords.size() == 0) continue;

                for (ConsumerRecord<K, V> pr : partitionRecords) {
                    Object[] params = injectConsumer
                            ? new Object[]{pr, consumer}
                            : new Object[]{pr};

                    try {
                        method.invoke(target, params);
                    } catch (InvocationTargetException itex) {
                        if (ignoreException) {
                            String errorMessage = String.format("Failed to process record %s-%d:%d", pr.topic(), pr.partition(), pr.offset());
                            log.warn(errorMessage, itex.getTargetException());
                        } else {
                            throw itex.getTargetException();
                        }
                    } catch (IllegalAccessException iaex) {
                        throw new IllegalStateException("Cannot access method " + name(method), iaex);
                    }
                }
            }
        }
    }

}
