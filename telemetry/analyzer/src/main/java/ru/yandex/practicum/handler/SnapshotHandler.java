package ru.yandex.practicum.handler;

import com.google.protobuf.Timestamp;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.service.HubRouterClient;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.model.*;
import ru.yandex.practicum.repository.ActionRepository;
import ru.yandex.practicum.repository.ConditionRepository;

import java.time.Instant;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Component
@RequiredArgsConstructor
public class SnapshotHandler {
    private final HubRouterClient grpcClient;
    private final ConditionRepository conditionRepository;
    private final ActionRepository actionRepository;

    public void handle(SensorsSnapshotAvro snapshot) {
        String hubId = snapshot.getHubId();
        log.info("Обработка снимка hub ID: \"{}\"", hubId);
        List<Scenario> successfulScenarios = checkScenario(hubId, snapshot);
        if (successfulScenarios.isEmpty()) {
            log.info("Завершение обработки снимка hub ID: \"{}\". Нет успешных сценариев", hubId);
            return;
        }
        log.info("Отработанные сценарии: {} из hub ID: \"{}\"", successfulScenarios, hubId);
        List<Action> actions = actionRepository.findAllByScenarioIn(successfulScenarios);
        for (Action action : actions) {
            grpcClient.sendData(mapToActionRequest(action));
        }
        log.info("Действвие: {} отправить в hub ID: \"{}\"", actions, hubId);
    }

    private SensorEvent mapToWrapper(String sensorId, SensorStateAvro sensorStateAvro) {
        SensorEvent wrapper = new SensorEvent();
        wrapper.setId(sensorId);
        wrapper.setData(sensorStateAvro.getData());
        return wrapper;
    }

    private DeviceActionRequest mapToActionRequest(Action action) {
        Scenario scenario = action.getScenario();
        Instant ts = Instant.now();
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(ts.getEpochSecond())
                .setNanos(ts.getNano())
                .build();
        DeviceActionProto deviceActionProto = DeviceActionProto.newBuilder()
                .setSensorId(action.getSensor().getId())
                .setType(ActionTypeProto.valueOf(action.getType().name()))
                .setValue(action.getValue())
                .build();
        return DeviceActionRequest.newBuilder()
                .setHubId(scenario.getHubId())
                .setScenarioName(scenario.getName())
                .setAction(deviceActionProto)
                .setTimestamp(timestamp)
                .build();
    }

    private List<Scenario> checkScenario(String hubId, SensorsSnapshotAvro snapshot) {
        List<Condition> conditions = conditionRepository.findAllByScenarioHubId(hubId);
        Supplier<Stream<SensorEvent>> streamSupplier = () -> snapshot.getSensorsState().entrySet().stream()
                .map(e -> mapToWrapper(e.getKey(), e.getValue()));

        Map<Condition, Boolean> result = new HashMap<>();
        for (Condition condition : conditions) {
            boolean isConditionDone = checkCondition(streamSupplier.get(), condition);
            result.put(condition, isConditionDone);
        }

        Map<Scenario, List<Boolean>> scenarios = result.entrySet().stream()
                .collect(Collectors.groupingBy(e -> e.getKey().getScenario(),
                        Collectors.mapping(Map.Entry::getValue, Collectors.toList())));

        return scenarios.entrySet().stream()
                .filter(e -> !e.getValue().contains(false))
                .map(Map.Entry::getKey)
                .toList();
    }

    private boolean checkCondition(Stream<SensorEvent> stream, Condition condition) {
        return stream.filter(o -> o.getId().equals(condition.getSensor().getId()))
                .map(getFunction(condition))
                .anyMatch(getPredicate(condition));
    }

    private Predicate<Integer> getPredicate(Condition condition) {
        ConditionOperation operation = condition.getOperation();
        int value = condition.getValue();
        Predicate<Integer> predicate;

        switch (operation) {
            case EQUALS -> predicate = x -> x == value;
            case GREATER_THAN -> predicate = x -> x > value;
            case LOWER_THAN -> predicate = x -> x < value;
            default -> predicate = null;
        }

        return predicate;
    }

    Function<SensorEvent, Integer> getFunction(Condition condition) {
        ConditionType type = condition.getType();
        Function<SensorEvent, Integer> func;

        switch (type) {
            case MOTION -> func = x -> {
                MotionSensorAvro data = (MotionSensorAvro) x.getData();
                boolean motion = data.getMotion();
                if (motion) {
                    return 1;
                } else {
                    return 0;
                }
            };

            case LUMINOSITY -> func = x -> {
                LightSensorAvro data = (LightSensorAvro) x.getData();
                return data.getLuminosity();
            };

            case SWITCH -> func = x -> {
                SwitchSensorAvro data = (SwitchSensorAvro) x.getData();
                boolean state = data.getState();
                if (state) {
                    return 1;
                } else {
                    return 0;
                }
            };

            case TEMPERATURE -> func = x -> {
                Object object = x.getData();
                if (object instanceof ClimateSensorAvro) {
                    ClimateSensorAvro data = (ClimateSensorAvro) object;
                    return data.getTemperatureC();
                } else {
                    TemperatureSensorAvro data = (TemperatureSensorAvro) object;
                    return data.getTemperatureC();
                }
            };

            case CO2LEVEL -> func = x -> {
                ClimateSensorAvro data = (ClimateSensorAvro) x.getData();
                return data.getCo2Level();
            };

            case HUMIDITY ->
                    func = x -> {
                        ClimateSensorAvro data = (ClimateSensorAvro) x.getData();
                        return data.getHumidity();
                    };
            default -> {
                return null;
            }
        }
        return func;
    }
}