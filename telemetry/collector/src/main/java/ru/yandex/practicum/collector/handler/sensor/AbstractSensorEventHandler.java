package ru.yandex.practicum.collector.handler.sensor;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import ru.yandex.practicum.collector.handler.TimestampMapper;
import ru.yandex.practicum.collector.service.KafkaProducerService;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

@Slf4j
public abstract class AbstractSensorEventHandler<T extends SpecificRecordBase> implements SensorEventHandler {

    @Value("${sensors}")
    protected String topic;

    protected final KafkaProducerService producerService;

    protected AbstractSensorEventHandler(KafkaProducerService producerService) {
        this.producerService = producerService;
    }

    protected abstract T mapPayload(SensorEventProto proto);

    protected abstract Class<T> getEventClass();

    @Override
    public void handle(SensorEventProto eventProto) {
        T payload = mapPayload(eventProto);
        SensorEventAvro eventAvro = SensorEventAvro.newBuilder()
                .setId(eventProto.getId())
                .setHubId(eventProto.getHubId())
                .setTimestamp(TimestampMapper.mapToInstant(eventProto.getTimestamp()))
                .setPayload(payload)
                .build();

        String hubId = eventProto.getHubId();
        long timestamp = eventAvro.getTimestamp().getEpochSecond();

        producerService.sendEvent(hubId, timestamp, eventAvro, topic, getEventClass());
        log.info("Событие из sensor ID = {} отправлено в топик: {}", eventAvro.getId(), topic);
    }
}