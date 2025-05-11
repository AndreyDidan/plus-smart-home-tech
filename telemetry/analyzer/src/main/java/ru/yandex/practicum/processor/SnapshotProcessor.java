package ru.yandex.practicum.processor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.handler.SnapshotHandler;
import ru.yandex.practicum.kafka.deserializer.SensorSnapshotDeserializer;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Component
@Slf4j
public class SnapshotProcessor {
    private static final List<String> TOPICS = List.of("telemetry.snapshots.v1");
    private static final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
    private static final Duration CONSUME_ATTEMPT_TIMEOUT = Duration.ofMillis(1000);

    private final KafkaConsumer<String, SensorsSnapshotAvro> consumer;
    private final SnapshotHandler handler;

    public SnapshotProcessor(SnapshotHandler handler) {
        consumer = new KafkaConsumer<>(getConsumerProperties());
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
                consumer.commitSync(currentOffsets);
            } finally {
                consumer.close();
                log.info("Consumer close");
            }
        }
    }

    public void stop() {
        consumer.wakeup();
    }

    private static Properties getConsumerProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "snapshotConsumer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "snapshot.analyzing");
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SensorSnapshotDeserializer.class);
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