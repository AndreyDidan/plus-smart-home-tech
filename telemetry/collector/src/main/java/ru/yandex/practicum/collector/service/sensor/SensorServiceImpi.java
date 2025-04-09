package ru.yandex.practicum.collector.service.sensor;

import lombok.RequiredArgsConstructor;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.collector.kafka.KafkaClient;
import ru.yandex.practicum.collector.model.sensor.*;
import ru.yandex.practicum.collector.service.hub.HubService;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.util.Objects;

@Service
@RequiredArgsConstructor
public class SensorServiceImpi implements SensorService {

    private final KafkaClient kafkaClient;

    @Value(value = "${sensors}")
    private String topicSensors;

    @Override
    public void sendSensorService(SensorEvent sensorEvent) {
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
        SensorEventAvro sensorEventAvro = new SensorEventAvro(
                sensorEvent.getId(),
                sensorEvent.getHubId(),
                sensorEvent.getTimestamp(),
                payload
        );
        ProducerRecord<String, SpecificRecordBase> producerRecord = new ProducerRecord<>(
                topicSensors,
                sensorEvent.getHubId(),
                sensorEventAvro
        );
        kafkaClient.getProducer().send(producerRecord);
    }
}
