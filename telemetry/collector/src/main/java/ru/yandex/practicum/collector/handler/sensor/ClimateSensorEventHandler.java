package ru.yandex.practicum.collector.handler.sensor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.handler.TimestampMapper;
import ru.yandex.practicum.collector.kafka.KafkaClient;
import ru.yandex.practicum.grpc.telemetry.event.ClimateSensorProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

@Slf4j
@Component
@RequiredArgsConstructor
public class ClimateSensorEventHandler implements SensorEventHandler {

    @Value(value = "${sensors}")
    private String topic;
    private final KafkaClient kafkaClient;

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.CLIMATE_SENSOR_EVENT;
    }

    @Override
    public void handle(SensorEventProto eventProto) {
        SensorEventAvro eventAvro = map(eventProto);
        log.info("Готовимся отправить событие в Kafka: ID = {}, HubID = {}, Payload: {}",
                eventAvro.getId(), eventAvro.getHubId(), eventAvro.getPayload());
        ProducerRecord<String, SpecificRecordBase> producerRecord = new ProducerRecord<>(topic, null,
                eventAvro.getTimestamp().getEpochSecond(), null, eventAvro);
        kafkaClient.getProducer().send(producerRecord);
        log.info("Событие из sensor ID = {} отправлено в топик: {}", eventAvro.getId(), topic);
    }

    private SensorEventAvro map(SensorEventProto eventProto) {
        ClimateSensorProto climateSensorProto = eventProto.getClimateSensorEvent();
        ClimateSensorAvro climateSensorAvro = ClimateSensorAvro.newBuilder()
                .setCo2Level(climateSensorProto.getCo2Level())
                .setHumidity(climateSensorProto.getHumidity())
                .setTemperatureC(climateSensorProto.getTemperatureC())
                .build();
        return SensorEventAvro.newBuilder()
                .setId(eventProto.getId())
                .setHubId(eventProto.getHubId())
                .setTimestamp(TimestampMapper.mapToInstant(eventProto.getTimestamp()))
                .setPayload(climateSensorAvro)
                .build();
    }
}