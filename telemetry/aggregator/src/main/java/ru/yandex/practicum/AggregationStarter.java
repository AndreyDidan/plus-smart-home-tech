package ru.yandex.practicum;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.VoidDeserializer;
import org.apache.kafka.common.serialization.VoidSerializer;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.deserializer.SensorEventDeserializer;
import ru.yandex.practicum.kafka.serializer.GeneralAvroSerializer;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Duration;
import java.util.*;

@Slf4j
@Component
@RequiredArgsConstructor
public class AggregationStarter {

    // ... объявление полей и конструктора ...
    private static final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
    private static final List<String> TOPICS = List.of("telemetry.sensors.v1");
    private static  final String SNAPSHOT_TOPIC = "telemetry.snapshots.v1";

    private final СhangeStates changeStates;

    private final KafkaConsumer<String, SensorEventAvro> consumer = new KafkaConsumer<>(getConsumerProperties());
    private final KafkaProducer<String, SpecificRecordBase> producer = new KafkaProducer<>(getProducerProperties());

    /**
     * Метод для начала процесса агрегации данных.
     * Подписывается на топики для получения событий от датчиков,
     * формирует снимок их состояния и записывает в кафку.
     */
    public void start() {
        try {
            Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));
            consumer.subscribe(TOPICS);

            // ... подготовка к обработке данных ...
            // ... например, подписка на топик ...

            // Цикл обработки событий
            while (true) {
                // ... реализация цикла опроса ...
                // ... и обработка полученных данных ...
                ConsumerRecords<String, SensorEventAvro> consumerRecords = consumer.poll(Duration.ofMillis(100));

                int count = 0;
                for (ConsumerRecord<String, SensorEventAvro> consumerRecord : consumerRecords) {
                    Optional<SensorsSnapshotAvro> sensorsSnapshotAvroOpt= changeStates.updateState(consumerRecord.value());

                    if (sensorsSnapshotAvroOpt.isPresent()) {
                        SensorsSnapshotAvro snapshotAvro = sensorsSnapshotAvroOpt.get();
                        ProducerRecord<String, SpecificRecordBase> producerRecord =
                                new ProducerRecord<>(SNAPSHOT_TOPIC,
                                        null,
                                        snapshotAvro.getTimestamp().getEpochSecond(),
                                        null,
                                        snapshotAvro);
                        producer.send(producerRecord);
                        log.info("Snapshot из hub ID = {} направляем в topic: {}", snapshotAvro.getHubId(),
                                SNAPSHOT_TOPIC);
                    }
                    manageOffsets(consumerRecord,count,consumer);
                    count++;
                }
                consumer.commitAsync();
            }

        } catch (WakeupException ignored) {
            // игнорируем - закрываем консьюмер и продюсер в блоке finally
        } catch (Exception e) {
            log.error("Ошибка во время обработки событий от датчиков", e);
        } finally {

            try {
                // Перед тем, как закрыть продюсер и консьюмер, нужно убедится,
                // что все сообщения, лежащие в буффере, отправлены и
                // все оффсеты обработанных сообщений зафиксированы

                // здесь нужно вызвать метод продюсера для сброса данных в буффере
                producer.flush();
                // здесь нужно вызвать метод консьюмера для фиксиции смещений
                consumer.commitSync();

            } finally {
                log.info("Закрываем консьюмер");
                consumer.close();
                log.info("Закрываем продюсер");
                producer.close();
            }
        }
    }

    private static Properties getConsumerProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "SomeConsumer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "some.group.id");
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, VoidDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SensorEventDeserializer.class);
        return properties;
    }

    private static Properties getProducerProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, VoidSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GeneralAvroSerializer.class);
        return properties;
    }

    private static void manageOffsets(ConsumerRecord<String, SensorEventAvro> record, int count,
                                      KafkaConsumer<String, SensorEventAvro> consumer) {
        currentOffsets.put(
                new TopicPartition(record.topic(), record.partition()),
                new OffsetAndMetadata(record.offset() + 1)
        );

        if(count % 10 == 0) {
            consumer.commitAsync(currentOffsets, (offsets, exception) -> {
                if(exception != null) {
                    log.warn("Ошибка во время фиксации оффсетов: {}", offsets, exception);
                }
            });
        }
    }
}
