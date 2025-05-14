package ru.yandex.practicum.collector.handler.hub;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.handler.TimestampMapper;
import ru.yandex.practicum.collector.service.KafkaProducerService;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioAddedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioConditionProto;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class ScenarioAddedEventHandler implements HubEventHandler {

    @Value("${hubs}")
    private String topic;
    private final KafkaProducerService producerService;

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.SCENARIO_ADDED;
    }

    @Override
    public void handle(HubEventProto eventProto) {
        HubEventAvro eventAvro = map(eventProto);
        ProducerRecord<String, SpecificRecordBase> record = new ProducerRecord<>(
                topic, null, eventAvro.getTimestamp().getEpochSecond(), null, eventAvro
        );
        producerService.sendEvent(record, ScenarioAddedEventAvro.class);
        log.info("Событие из hub ID = {} отправлено в топик: {}", eventAvro.getHubId(), topic);
    }

    private HubEventAvro map(HubEventProto eventProto) {
        ScenarioAddedEventProto scenarioAddedEventProto = eventProto.getScenarioAdded();
        List<DeviceActionAvro> deviceActionAvroList = scenarioAddedEventProto.getActionList().stream()
                .map(this::map)
                .toList();
        List<ScenarioConditionAvro> scenarioConditionAvroList = scenarioAddedEventProto.getConditionList().stream()
                .map(this::map)
                .toList();
        ScenarioAddedEventAvro scenarioAddedEventAvro = ScenarioAddedEventAvro.newBuilder()
                .setName(scenarioAddedEventProto.getName())
                .setActions(deviceActionAvroList)
                .setConditions(scenarioConditionAvroList)
                .build();
        return HubEventAvro.newBuilder()
                .setHubId(eventProto.getHubId())
                .setTimestamp(TimestampMapper.mapToInstant(eventProto.getTimestamp()))
                .setPayload(scenarioAddedEventAvro)
                .build();
    }

    private ScenarioConditionAvro map(ScenarioConditionProto conditionProto) {
        Object value = null;
        if (conditionProto.getValueCase() == ScenarioConditionProto.ValueCase.INT_VALUE) {
            value = conditionProto.getIntValue();
        } else if (conditionProto.getValueCase() == ScenarioConditionProto.ValueCase.BOOL_VALUE) {
            value = conditionProto.getBoolValue();
        }
        return ScenarioConditionAvro.newBuilder()
                .setSensorId(conditionProto.getSensorId())
                .setType(ConditionTypeAvro.valueOf(conditionProto.getType().name()))
                .setOperation(ConditionOperationAvro.valueOf(conditionProto.getOperation().name()))
                .setValue(value)
                .build();
    }

    private DeviceActionAvro map(DeviceActionProto actionProto) {
        return DeviceActionAvro.newBuilder()
                .setSensorId(actionProto.getSensorId())
                .setValue(actionProto.getValue())
                .setType(ActionTypeAvro.valueOf(actionProto.getType().name()))
                .build();
    }
}