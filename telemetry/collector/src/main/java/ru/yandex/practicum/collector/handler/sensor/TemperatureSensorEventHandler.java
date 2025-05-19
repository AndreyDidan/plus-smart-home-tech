package ru.yandex.practicum.collector.handler.sensor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.service.KafkaProducerService;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.grpc.telemetry.event.TemperatureSensorProto;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;

@Slf4j
@Component
public class TemperatureSensorEventHandler extends AbstractSensorEventHandler<TemperatureSensorAvro> {

    public TemperatureSensorEventHandler(KafkaProducerService producerService) {
        super(producerService);
    }

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.TEMPERATURE_SENSOR_EVENT;
    }

    @Override
    protected TemperatureSensorAvro mapPayload(SensorEventProto proto) {
        TemperatureSensorProto temperatureSensorProto = proto.getTemperatureSensorEvent();
        return TemperatureSensorAvro.newBuilder()
                .setTemperatureC(temperatureSensorProto.getTemperatureC())
                .setTemperatureF(temperatureSensorProto.getTemperatureF())
                .build();
    }

    @Override
    protected Class<TemperatureSensorAvro> getEventClass() {
        return TemperatureSensorAvro.class;
    }
}