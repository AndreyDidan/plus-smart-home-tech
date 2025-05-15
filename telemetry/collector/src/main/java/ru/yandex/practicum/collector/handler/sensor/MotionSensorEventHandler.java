package ru.yandex.practicum.collector.handler.sensor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.service.KafkaProducerService;
import ru.yandex.practicum.grpc.telemetry.event.MotionSensorProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;

import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;

@Slf4j
@Component
public class MotionSensorEventHandler extends AbstractSensorEventHandler<MotionSensorAvro> {

    public MotionSensorEventHandler(KafkaProducerService producerService) {
        super(producerService);
    }

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.MOTION_SENSOR_EVENT;
    }

    @Override
    protected MotionSensorAvro mapPayload(SensorEventProto proto) {
        MotionSensorProto source = proto.getMotionSensorEvent();
        return MotionSensorAvro.newBuilder()
                .setMotion(source.getMotion())
                .setVoltage(source.getVoltage())
                .setLinkQuality(source.getLinkQuality())
                .build();
    }

    @Override
    protected Class<MotionSensorAvro> getEventClass() {
        return MotionSensorAvro.class;
    }
}
