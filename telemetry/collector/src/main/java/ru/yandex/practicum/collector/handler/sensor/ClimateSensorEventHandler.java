package ru.yandex.practicum.collector.handler.sensor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.service.KafkaProducerService;
import ru.yandex.practicum.grpc.telemetry.event.ClimateSensorProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;

@Slf4j
@Component
public class ClimateSensorEventHandler extends AbstractSensorEventHandler<ClimateSensorAvro> {

    public ClimateSensorEventHandler(KafkaProducerService producerService) {
        super(producerService);
    }

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.CLIMATE_SENSOR_EVENT;
    }

    @Override
    protected ClimateSensorAvro mapPayload(SensorEventProto proto) {
        ClimateSensorProto source = proto.getClimateSensorEvent();
        return ClimateSensorAvro.newBuilder()
                .setCo2Level(source.getCo2Level())
                .setHumidity(source.getHumidity())
                .setTemperatureC(source.getTemperatureC())
                .build();
    }

    @Override
    protected Class<ClimateSensorAvro> getEventClass() {
        return ClimateSensorAvro.class;
    }
}