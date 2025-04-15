package ru.yandex.practicum.collector.service.hub;

import lombok.RequiredArgsConstructor;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.collector.kafka.KafkaClient;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.collector.model.hub.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.util.List;

@Service
@RequiredArgsConstructor
public class HubServiceImpi implements HubService {

    private final KafkaClient kafkaClient;

    @Value(value = "${hubs}")
    private String topicHubs;

    @Override
    public void sendHubEvent(HubEvent hubEvent) {
        HubEventAvro hubEventAvro = mapToAvro(hubEvent);
        ProducerRecord<String, SpecificRecordBase> record = new ProducerRecord<>(
                topicHubs,
                null, // partition (можно null — Kafka определит по ключу)
                hubEvent.getTimestamp().toEpochMilli(), // timestamp
                hubEvent.getHubId(), // key
                hubEventAvro // value
        );

        try {
            kafkaClient.getProducer().send(record, (metadata, exception) -> {
                if (exception != null) {
                    // логируем ошибку
                    System.err.println("Ошибка при отправке сообщения в Kafka: " + exception.getMessage());
                    exception.printStackTrace();
                } else {
                    // логируем успех, если нужно
                    System.out.println("Сообщение отправлено в Kafka, offset: " + metadata.offset());
                }
            });
            kafkaClient.getProducer().flush(); // гарантируем доставку
        } catch (Exception e) {
            // логируем возможные ошибки
            System.err.println("Исключение при отправке события: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public HubEventAvro mapToAvro(HubEvent hubEvent) {
        Object payload;
        switch (hubEvent) {
            case DeviceAddedEvent deviceAddedEvent -> payload = DeviceAddedEventAvro.newBuilder()
                    .setId(deviceAddedEvent.getId())
                    .setType(DeviceTypeAvro.valueOf(deviceAddedEvent.getDeviceType().name()))
                    .build();

            case DeviceRemovedEvent deviceRemovedEvent -> payload = DeviceRemovedEventAvro.newBuilder()
                    .setId(deviceRemovedEvent.getId())
                    .build();

            case ScenarioAddedEvent scenarioAddedEvent -> {
                List<DeviceActionAvro> deviceActionAvroList = scenarioAddedEvent.getActions().stream()
                        .map(this::map)
                        .toList();
                List<ScenarioConditionAvro> scenarioConditionAvroList = scenarioAddedEvent.getConditions().stream()
                        .map(this::map)
                        .toList();
                payload = ScenarioAddedEventAvro.newBuilder()
                        .setName(scenarioAddedEvent.getName())
                        .setActions(deviceActionAvroList)
                        .setConditions(scenarioConditionAvroList)
                        .build();
            }
            case null, default -> throw new IllegalStateException("Unexpected value: " + hubEvent.getType());
        }
        return HubEventAvro.newBuilder()
                .setHubId(hubEvent.getHubId())
                .setTimestamp(hubEvent.getTimestamp())
                .setPayload(payload)
                .build();
    }

    private DeviceActionAvro map(DeviceAction action) {
        return DeviceActionAvro.newBuilder()
                .setType(ActionTypeAvro.valueOf(action.getType().name()))
                .setSensorId(action.getSensorId())
                .setValue(action.getValue())
                .build();
    }

    private ScenarioConditionAvro map(ScenarioCondition condition) {
        return ScenarioConditionAvro.newBuilder()
                .setSensorId(condition.getSensorId())
                .setType(ConditionTypeAvro.valueOf(condition.getType().name()))
                .setValue(condition.getValue())
                .setOperation(ConditionOperationAvro.valueOf(condition.getOperation().name()))
                .build();
    }
}