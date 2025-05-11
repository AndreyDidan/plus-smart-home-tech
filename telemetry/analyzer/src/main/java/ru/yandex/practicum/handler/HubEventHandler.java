package ru.yandex.practicum.handler;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.model.*;
import ru.yandex.practicum.repository.ActionRepository;
import ru.yandex.practicum.repository.ConditionRepository;
import ru.yandex.practicum.repository.ScenarioRepository;
import ru.yandex.practicum.repository.SensorRepository;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Component
@Transactional
@RequiredArgsConstructor
public class HubEventHandler {
    private final SensorRepository sensorRepository;
    private final ScenarioRepository scenarioRepository;
    private final ConditionRepository conditionRepository;
    private final ActionRepository actionRepository;

    public void handle(HubEventAvro event) {
        Object payload = event.getPayload();
        String hubId = event.getHubId();

        if (payload.getClass() == DeviceAddedEventAvro.class) {
            handleDeviceAdded(hubId, (DeviceAddedEventAvro) payload);

        } else if (payload.getClass() == DeviceRemovedEventAvro.class) {
            handleDeviceRemoved(hubId, (DeviceRemovedEventAvro) payload);

        } else if (payload.getClass() == ScenarioAddedEventAvro.class) {
            handleScenarioAdded(hubId, (ScenarioAddedEventAvro) payload);

        } else {
            handleScenarioRemoved(hubId, (ScenarioRemovedEventAvro) payload);
        }
    }

    private void handleDeviceAdded(String hubId, DeviceAddedEventAvro event) {
        log.info("Добавление устройства из hub ID: \"{}\"", hubId);
        String id = event.getId();
        if (!sensorRepository.existsByIdInAndHubId(List.of(id), hubId)) {
            Sensor sensor = new Sensor();
            sensor.setId(id);
            sensor.setHubId(hubId);
            sensorRepository.save(sensor);
            log.info("Sensor added: {}", sensor);
        } else {
            log.info("Устройство ID: \"{}\" добавлено", id);
        }
    }

    private void handleDeviceRemoved(String hubId, DeviceRemovedEventAvro event) {
        log.info("Удаление устройства из hub ID: \"{}\"", hubId);
        String id = event.getId();
        sensorRepository.deleteById(id);
        log.info("Устройство ID: \"{}\" удалено в hub ID: \"{}\"", id, hubId);
    }

    private void handleScenarioAdded(String hubId, ScenarioAddedEventAvro event) {
        log.info("Добавление сценария в hub ID: \"{}\"", hubId);
        Optional<Scenario> scenarioOpt = scenarioRepository.findByHubIdAndName(hubId, event.getName());
        try {
            if (scenarioOpt.isPresent()) {
                updateScenario(scenarioOpt.get(), event);
            } else {
                addScenario(hubId, event);
            }
        } catch (RuntimeException e) {
            log.error("Сценарий: \"{}\" в hub ID: \"{}\" некорректен", event.getName(), hubId);
        }
    }

    private void handleScenarioRemoved(String hubId, ScenarioRemovedEventAvro event) {
        log.info("Удаление сценария из hub ID: \"{}\"", hubId);
        String name = event.getName();
        Optional<Scenario> scenarioOpt = scenarioRepository.findByHubIdAndName(hubId, name);
        if (scenarioOpt.isPresent()) {
            Scenario scenario = scenarioOpt.get();
            conditionRepository.deleteByScenario(scenario);
            actionRepository.deleteByScenario(scenario);
            scenarioRepository.delete(scenario);
        }
        log.info("Сценарий: \"{}\" удалён в hub ID: \"{}\"", name, hubId);
    }

    private void addScenario(String hubId, ScenarioAddedEventAvro event) throws RuntimeException {
        Scenario scenario = new Scenario();
        scenario.setHubId(hubId);
        scenario.setName(event.getName());
        scenarioRepository.save(scenario);
        addCondition(scenario, event);
        addAction(scenario, event);
        log.info("Сценарий: \"{}\" добавлен в hub ID: \"{}\"", scenario.getName(), hubId);
    }

    private void addAction(Scenario scenario, ScenarioAddedEventAvro event) throws RuntimeException {
        List<String> sensorIdlist = event.getActions().stream()
                .map(DeviceActionAvro::getSensorId)
                .toList();
        List<Sensor> sensorList = sensorRepository.findAllById(sensorIdlist);
        checkHubId(scenario, sensorList);
        Map<String, Sensor> sensorMap = sensorList.stream()
                .collect(Collectors.toMap(Sensor::getId, Function.identity()));
        List<Action> actions = event.getActions().stream()
                .map(actionAvro ->
                        map(scenario, actionAvro, sensorMap.get(actionAvro.getSensorId())))
                .toList();
        actionRepository.saveAll(actions);
    }

    private void addCondition(Scenario scenario, ScenarioAddedEventAvro event) throws RuntimeException {
        List<String> sensorIdlist = event.getConditions().stream()
                .map(ScenarioConditionAvro::getSensorId)
                .toList();
        List<Sensor> sensorList = sensorRepository.findAllById(sensorIdlist);
        checkHubId(scenario, sensorList);
        Map<String, Sensor> sensorMap = sensorList.stream()
                .collect(Collectors.toMap(Sensor::getId, Function.identity()));
        List<Condition> conditions = event.getConditions().stream()
                .map(conditionAvro ->
                        map(scenario, conditionAvro, sensorMap.get(conditionAvro.getSensorId())))
                .toList();
        conditionRepository.saveAll(conditions);
    }

    private Condition map(Scenario scenario, ScenarioConditionAvro conditionAvro, Sensor sensor) {
        Condition condition = new Condition();
        condition.setType(ConditionType.valueOf(conditionAvro.getType().name()));
        condition.setOperation(ConditionOperation.valueOf(conditionAvro.getOperation().name()));
        Object value = conditionAvro.getValue();
        if (value.getClass() == Integer.class) {
            condition.setValue((int) value);
        } else {
            if (Boolean.TRUE.equals(value)) {
                condition.setValue(1);
            } else {
                condition.setValue(0);
            }
        }
        condition.setScenario(scenario);
        condition.setSensor(sensor);
        return condition;
    }

    private Action map(Scenario scenario, DeviceActionAvro deviceActionAvro, Sensor sensor) {
        Action action = new Action();
        action.setSensor(sensor);
        action.setScenario(scenario);
        action.setType(ActionType.valueOf(deviceActionAvro.getType().name()));
        action.setValue(deviceActionAvro.getValue());
        return action;
    }

    private void checkHubId(Scenario scenario, List<Sensor> sensors) throws RuntimeException {
        boolean isNotValidHubId = sensors.stream()
                .map(Sensor::getHubId)
                .anyMatch(hubId -> !hubId.equals(scenario.getHubId()));
        if (isNotValidHubId) {
            throw new RuntimeException("Неверный hub id");
        }
    }

    private void updateScenario(Scenario scenario, ScenarioAddedEventAvro event) throws RuntimeException {
        actionRepository.deleteByScenario(scenario);
        conditionRepository.deleteByScenario(scenario);
        addAction(scenario, event);
        addCondition(scenario, event);
        log.info("Сценарий: \"{}\" обновлен в hub ID: \"{}\"", scenario.getName(), scenario.getHubId());
    }
}