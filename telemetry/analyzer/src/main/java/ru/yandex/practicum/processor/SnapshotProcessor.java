package ru.yandex.practicum.processor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.handler.SnapshotHandler;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Slf4j
@Component
public class SnapshotProcessor {
    private final List<String> TOPICS;
    private static final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
    private final Duration CONSUME_ATTEMPT_TIMEOUT;

    private final KafkaConsumer<String, SensorsSnapshotAvro> consumer;
    private final SnapshotHandler handler;

    public SnapshotProcessor(SnapshotHandler handler,
                             @Value("${spring.kafka.consumer.snapshot:properties.group.id}") String groupId,
                             @Value("${spring.kafka.consumer.snapshot:properties.bootstrap.servers}") String bootstrapServers,
                             @Value("${spring.kafka.consumer.snapshot:properties.key.deserializer}") Class<?> keyDeserializer,
                             @Value("${spring.kafka.consumer.snapshot:properties.value.deserializer}") Class<?> valueDeserializer,
                             @Value("${spring.kafka.consumer.snapshot:properties.consume-timeout}") long consumeTimeout) {
        this.TOPICS = List.of("telemetry.snapshots.v1");
        this.CONSUME_ATTEMPT_TIMEOUT = Duration.ofMillis(consumeTimeout);
        this.consumer = new KafkaConsumer<>(getConsumerProperties(groupId, bootstrapServers, keyDeserializer, valueDeserializer));
        this.handler = handler;
    }

    public void start() {
        try {
            Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));
            consumer.subscribe(TOPICS);

            while (true) {
                ConsumerRecords<String, SensorsSnapshotAvro> records = consumer.poll(CONSUME_ATTEMPT_TIMEOUT);
                for (ConsumerRecord<String, SensorsSnapshotAvro> record : records) {
                    SensorsSnapshotAvro sensorsSnapshotAvro = record.value();
                    log.info("Received snapshot from hub ID = {}", sensorsSnapshotAvro.getHubId());
                    handler.handle(sensorsSnapshotAvro);
                    manageOffsets(record, consumer);
                }
            }
        } catch (WakeupException ignored) {
        } catch (Exception e) {
            log.error("Error:", e);
        } finally {
            try {
                consumer.commitAsync(currentOffsets, (offsets, exception) -> {
                    if (exception != null) {
                        log.error("Failed to commit offsets", exception);
                    }
                });
            } finally {
                consumer.close();
                log.info("Consumer closed");
            }
        }
    }

    public void stop() {
        consumer.wakeup();
    }

    private Properties getConsumerProperties(String groupId, String bootstrapServers, Class<?> keyDeserializer, Class<?> valueDeserializer) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "snapshotConsumer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
        return properties;
    }

    private static void manageOffsets(ConsumerRecord<String, SensorsSnapshotAvro> record,
                                      KafkaConsumer<String, SensorsSnapshotAvro> consumer) {
        currentOffsets.put(
                new TopicPartition(record.topic(), record.partition()),
                new OffsetAndMetadata(record.offset() + 1)
        );
    }
}