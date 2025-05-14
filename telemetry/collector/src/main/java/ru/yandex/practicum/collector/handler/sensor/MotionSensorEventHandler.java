package ru.yandex.practicum.collector.handler.sensor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.handler.TimestampMapper;
import ru.yandex.practicum.collector.service.KafkaProducerService;
import ru.yandex.practicum.grpc.telemetry.event.MotionSensorProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;

import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

@Slf4j
@Component
@RequiredArgsConstructor
public class MotionSensorEventHandler implements SensorEventHandler {

    @Value("${sensors}")
    private String topic;
    private final KafkaProducerService producerService;

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.MOTION_SENSOR_EVENT;
    }

    @Override
    public void handle(SensorEventProto eventProto) {
        SensorEventAvro eventAvro = map(eventProto);
        ProducerRecord<String, SpecificRecordBase> record = new ProducerRecord<>(
                topic, null, eventAvro.getTimestamp().getEpochSecond(), null, eventAvro
        );
        producerService.sendEvent(record, MotionSensorAvro.class);
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