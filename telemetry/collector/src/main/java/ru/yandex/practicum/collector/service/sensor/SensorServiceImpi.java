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
        ProducerRecord<String, SpecificRecordBase> record = new ProducerRecord<>(
                topicSensors,
                null,
                sensorEvent.getTimestamp().toEpochMilli(),
                sensorEvent.getHubId(), // ключ — hubId
                sensorEventAvro
        );

        try {
            kafkaClient.getProducer().send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("Ошибка при отправке сенсора: " + exception.getMessage());
                    exception.printStackTrace();
                } else {
                    System.out.println("Сенсор отправлен, offset: " + metadata.offset());
                }
            });
            kafkaClient.getProducer().flush();
        } catch (Exception e) {
            System.err.println("Исключение при отправке сенсорного события: " + e.getMessage());
            e.printStackTrace();
        }
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
