package ru.yandex.practicum.collector.service.sensor;

import lombok.RequiredArgsConstructor;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.collector.kafka.KafkaClient;
import ru.yandex.practicum.collector.model.sensor.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

@Service
@RequiredArgsConstructor
public class SensorServiceImpi implements SensorService {

    private final KafkaClient kafkaClient;

    @Value(value = "${sensors}")
    private String topicSensors;

    @Override
    public void sendSensorService(SensorEvent sensorEvent) {
        SensorEventAvro sensorEventAvro = mapToAvro(sensorEvent);
        kafkaClient.getProducer().send(new ProducerRecord<>(topicSensors, sensorEventAvro));
    }

    private SensorEventAvro mapToAvro(SensorEvent sensorEvent) {
        Object payload;
        switch (sensorEvent) {
            case ClimateSensorEvent climateSensorEvent -> payload = ClimateSensorAvro.newBuilder()
                    .setCo2Level(climateSensorEvent.getCo2Level())
                    .setHumidity(climateSensorEvent.getHumidity())
                    .setTemperatureC(climateSensorEvent.getTemperatureC())
                    .build();

            case LightSensorEvent lightSensorEvent -> payload = LightSensorAvro.newBuilder()
                    .setLinkQuality(lightSensorEvent.getLinkQuality())
                    .setLuminosity(lightSensorEvent.getLuminosity())
                    .build();

            case MotionSensorEvent motionSensorEvent -> payload = MotionSensorAvro.newBuilder()
                    .setMotion(motionSensorEvent.isMotion())
                    .setLinkQuality(motionSensorEvent.getLinkQuality())
                    .setVoltage(motionSensorEvent.getVoltage())
                    .build();

            case SwitchSensorEvent switchSensorEvent -> payload = SwitchSensorAvro.newBuilder()
                    .setState(switchSensorEvent.isState())
                    .build();

            case null, default -> throw new IllegalStateException("Unexpected value: " + sensorEvent.getType());
        }
        return SensorEventAvro.newBuilder()
                .setHubId(sensorEvent.getHubId())
                .setId(sensorEvent.getId())
                .setTimestamp(sensorEvent.getTimestamp())
                .setPayload(payload)
                .build();
    }
}
