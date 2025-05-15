package ru.yandex.practicum.collector.handler.sensor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.service.KafkaProducerService;
import ru.yandex.practicum.grpc.telemetry.event.LightSensorProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;

@Slf4j
@Component
public class LightSensorEventHandler extends AbstractSensorEventHandler<LightSensorAvro> {

    public LightSensorEventHandler(KafkaProducerService producerService) {
        super(producerService);
    }

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.LIGHT_SENSOR_EVENT;
    }

    @Override
    protected LightSensorAvro mapPayload(SensorEventProto proto) {
        LightSensorProto source = proto.getLightSensorEvent();
        return LightSensorAvro.newBuilder()
                .setLuminosity(source.getLuminosity())
                .setLinkQuality(source.getLinkQuality())
                .build();
    }

    @Override
    protected Class<LightSensorAvro> getEventClass() {
        return LightSensorAvro.class;
    }
}