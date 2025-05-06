package ru.yandex.practicum;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

@Slf4j
@Component
@RequiredArgsConstructor
public class AggregationStarter {
    private final KafkaConsumer<String, SensorEventAvro> consumer;
    private final KafkaProducer<String, SensorsSnapshotAvro> producer;
    private final SnapshotAggregator aggregator;

    private static final String SNAPSHOT_TOPIC = "telemetry.snapshots.v1";
    /**
     * Метод для начала процесса агрегации данных.
     * Подписывается на топики для получения событий от датчиков,
     * формирует снимок их состояния и записывает в кафку.
     */
    public void start() {
        consumer.subscribe(List.of("telemetry.sensors.v1"));
        try {
            while (true) {
                ConsumerRecords<String, SensorEventAvro> records = consumer.poll(Duration.ofMillis(500));

                if (records.isEmpty()) {
                    log.info("Нет новых сообщений для обработки");
                }

                for (ConsumerRecord<String, SensorEventAvro> record : records) {
                    SensorEventAvro event = record.value();
                    log.info("Обрабатываю событие: {}", event);
                    Optional<SensorsSnapshotAvro> snapshotOpt = aggregator.updateState(event);
                    snapshotOpt.ifPresent(snapshot -> {
                        log.info("Обновлен снимок состояния: {}", snapshot);
                        producer.send(new ProducerRecord<>(SNAPSHOT_TOPIC, snapshot.getHubId(), snapshot), (metadata, exception) -> {
                            if (exception != null) {
                                log.error("Ошибка отправки в Kafka: {}", exception.getMessage());
                            } else {
                                log.info("Сообщение успешно отправлено в Kafka в топик {}", SNAPSHOT_TOPIC);
                            }
                        });
                    });
                }
                consumer.commitSync();
            }
        } catch (WakeupException ignored) {
            // игнорируем - закрываем консьюмер и продюсер в блоке finally
        } catch (Exception e) {
            log.error("Ошибка во время обработки событий от датчиков", e);
        } finally {

            try {
                producer.flush();
                consumer.commitSync();

            } finally {
                log.info("Закрываем консьюмер");
                consumer.close();
                log.info("Закрываем продюсер");
                producer.close();
            }
        }
    }
}
