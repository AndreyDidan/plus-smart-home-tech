package ru.yandex.practicum.collector.handler.hub;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.service.KafkaProducerService;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioAddedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioConditionProto;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.util.List;

@Slf4j
@Component
public class ScenarioAddedEventHandler extends AbstractHubEventHandler<ScenarioAddedEventAvro> {

    public ScenarioAddedEventHandler(KafkaProducerService producerService) {
        super(producerService);
    }

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.SCENARIO_ADDED;
    }

    @Override
    protected ScenarioAddedEventAvro mapPayload(HubEventProto eventProto) {
        ScenarioAddedEventProto proto = eventProto.getScenarioAdded();
        List<DeviceActionAvro> actions = proto.getActionList().stream()
                .map(this::mapAction)
                .toList();

        List<ScenarioConditionAvro> conditions = proto.getConditionList().stream()
                .map(this::mapCondition)
                .toList();

        return ScenarioAddedEventAvro.newBuilder()
                .setName(proto.getName())
                .setActions(actions)
                .setConditions(conditions)
                .build();
    }

    @Override
    protected Class<ScenarioAddedEventAvro> getEventClass() {
        return ScenarioAddedEventAvro.class;
    }

    private DeviceActionAvro mapAction(DeviceActionProto actionProto) {
        return DeviceActionAvro.newBuilder()
                .setSensorId(actionProto.getSensorId())
                .setValue(actionProto.getValue())
                .setType(ActionTypeAvro.valueOf(actionProto.getType().name()))
                .build();
    }

    private ScenarioConditionAvro mapCondition(ScenarioConditionProto conditionProto) {
        ScenarioConditionAvro.Builder builder = ScenarioConditionAvro.newBuilder()
                .setSensorId(conditionProto.getSensorId())
                .setType(ConditionTypeAvro.valueOf(conditionProto.getType().name()))
                .setOperation(ConditionOperationAvro.valueOf(conditionProto.getOperation().name()));

        switch (conditionProto.getValueCase()) {
            case INT_VALUE -> builder.setValue(conditionProto.getIntValue());
            case BOOL_VALUE -> builder.setValue(conditionProto.getBoolValue());
            default -> builder.setValue(null);
        }

        return builder.build();
    }
}