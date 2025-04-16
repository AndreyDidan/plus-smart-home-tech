package ru.yandex.practicum.collector.service;

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

    public void sendEvent(ProducerRecord<String, SpecificRecordBase> record, Class<?> eventClass) {
        Producer<String, SpecificRecordBase> producer = kafkaClient.getProducer();
        Future<RecordMetadata> futureResult = producer.send(record);
        producer.flush();

        try {
            RecordMetadata metadata = futureResult.get();
            log.info("Событие {} было успешно сохранено в топик {} в партицию {} со смещением {}",
                    eventClass.getSimpleName(), metadata.topic(), metadata.partition(), metadata.offset());
        } catch (InterruptedException | ExecutionException e) {
            log.warn("Ошибка при отправке события {}: {}", eventClass.getSimpleName(), e.getMessage(), e);
        }
    }
}
