package ru.yandex.practicum.collector.handler.sensor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.service.KafkaProducerService;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SwitchSensorProto;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;

@Slf4j
@Component
public class SwitchSensorEventHandler extends AbstractSensorEventHandler<SwitchSensorAvro> {

    public SwitchSensorEventHandler(KafkaProducerService producerService) {
        super(producerService);
    }

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.SWITCH_SENSOR_EVENT;
    }

    @Override
    protected SwitchSensorAvro mapPayload(SensorEventProto proto) {
        SwitchSensorProto source = proto.getSwitchSensorEvent();
        return SwitchSensorAvro.newBuilder()
                .setState(source.getState())
                .build();
    }

    @Override
    protected Class<SwitchSensorAvro> getEventClass() {
        return SwitchSensorAvro.class;
    }
}