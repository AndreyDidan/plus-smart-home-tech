package ru.yandex.practicum.collector.service;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.collector.kafka.KafkaClient;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaProducerService {
    private final KafkaClient kafkaClient;

    private Producer<String, SpecificRecordBase> producer;

    @PostConstruct
    public void init() {
        this.producer = kafkaClient.getProducer(); // теперь создаётся один раз после внедрения зависимостей
    }

    public void sendEvent(String key, long timestamp, SpecificRecordBase value, String topic, Class<?> eventClass) {
        ProducerRecord<String, SpecificRecordBase> record = new ProducerRecord<>(
                topic, null, timestamp, key, value
        );

        try {
            Future<RecordMetadata> futureResult = producer.send(record);
            producer.flush();

            RecordMetadata metadata = futureResult.get();
            log.info("Событие {} было успешно сохранено в топик {} в партицию {} со смещением {}",
                    eventClass.getSimpleName(), metadata.topic(), metadata.partition(), metadata.offset());
        } catch (InterruptedException | ExecutionException e) {
            log.warn("Ошибка при отправке события {}: {}", eventClass.getSimpleName(), e.getMessage(), e);
            Thread.currentThread().interrupt(); // корректно сбрасываем флаг прерывания
        }
    }
}
