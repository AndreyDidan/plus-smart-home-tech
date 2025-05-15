package ru.yandex.practicum.processor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.handler.HubEventHandler;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Slf4j
@Component
public class HubEventProcessor implements Runnable {
    private final List<String> TOPICS;
    private static final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
    private final Duration CONSUME_ATTEMPT_TIMEOUT;

    private final HubEventHandler handler;
    private final KafkaConsumer<String, HubEventAvro> consumer;

    public HubEventProcessor(HubEventHandler handler,
                             @Value("${spring.kafka.consumer.hub:properties.group.id}") String groupId,
                             @Value("${spring.kafka.consumer.hub:properties.bootstrap.servers}") String bootstrapServers,
                             @Value("${spring.kafka.consumer.hub:properties.key.deserializer}") Class<?> keyDeserializer,
                             @Value("${spring.kafka.consumer.hub:properties.value.deserializer}") Class<?> valueDeserializer,
                             @Value("${spring.kafka.consumer.hub:properties.consume-timeout}") long consumeTimeout) {
        this.TOPICS = List.of("telemetry.hubs.v1");
        this.CONSUME_ATTEMPT_TIMEOUT = Duration.ofMillis(consumeTimeout);
        this.consumer = new KafkaConsumer<>(getConsumerProperties(groupId, bootstrapServers, keyDeserializer, valueDeserializer));
        this.handler = handler;
    }

    @Override
    public void run() {
        boolean hasRecordsProcessed = false;

        try {
            consumer.subscribe(TOPICS);

            while (true) {
                ConsumerRecords<String, HubEventAvro> records = consumer.poll(CONSUME_ATTEMPT_TIMEOUT);
                if (!records.isEmpty()) {
                    for (ConsumerRecord<String, HubEventAvro> record : records) {
                        HubEventAvro hubEventAvro = record.value();
                        log.info("Received hubEvent from hub ID = {}", hubEventAvro.getHubId());
                        handler.handle(hubEventAvro);
                        manageOffsets(record, consumer);
                    }
                    hasRecordsProcessed = true;
                }
            }
        } catch (WakeupException ignored) {
        } catch (Exception e) {
            log.error("Error:", e);
        } finally {
            try {
                if (hasRecordsProcessed) {
                    consumer.commitSync(currentOffsets);
                }
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
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "hubConsumer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
        return properties;
    }

    private void manageOffsets(ConsumerRecord<String, HubEventAvro> record,
                               KafkaConsumer<String, HubEventAvro> consumer) {
        currentOffsets.put(
                new TopicPartition(record.topic(), record.partition()),
                new OffsetAndMetadata(record.offset() + 1)
        );
    }
}