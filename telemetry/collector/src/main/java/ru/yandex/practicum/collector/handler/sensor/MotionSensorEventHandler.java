package ru.yandex.practicum.collector.handler.sensor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.handler.TimestampMapper;
import ru.yandex.practicum.collector.kafka.KafkaClient;
import ru.yandex.practicum.grpc.telemetry.event.MotionSensorProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;

import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

@Component
@RequiredArgsConstructor
@Slf4j
public class MotionSensorEventHandler implements SensorEventHandler {
    @Value(value = "${sensors}")
    private String topic;
    private final KafkaClient kafkaClient;

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.MOTION_SENSOR_EVENT;
    }

    @Override
    public void handle(SensorEventProto eventProto) {
        SensorEventAvro eventAvro = map(eventProto);
        ProducerRecord<String, SpecificRecordBase> producerRecord = new ProducerRecord<>(topic, null,
                eventAvro.getTimestamp().getEpochSecond(), null, eventAvro);
        kafkaClient.getProducer().send(producerRecord);
        log.info("Событие из sensor ID = {} отправлено в топик: {}", eventAvro.getId(), topic);
    }

    private SensorEventAvro map(SensorEventProto eventProto) {
        MotionSensorProto motionSensorEvent = eventProto.getMotionSensorEvent();
        MotionSensorAvro motionSensorAvro = MotionSensorAvro.newBuilder()
                .setMotion(motionSensorEvent.getMotion())
                .setVoltage(motionSensorEvent.getVoltage())
                .setLinkQuality(motionSensorEvent.getLinkQuality())
                .build();
        return SensorEventAvro.newBuilder()
                .setId(eventProto.getId())
                .setHubId(eventProto.getHubId())
                .setTimestamp(TimestampMapper.mapToInstant(eventProto.getTimestamp()))
                .setPayload(motionSensorAvro)
                .build();
    }
}