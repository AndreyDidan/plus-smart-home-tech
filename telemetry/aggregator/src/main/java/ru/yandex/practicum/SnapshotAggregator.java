package ru.yandex.practicum;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class SnapshotAggregator {
    private final Map<String, SensorsSnapshotAvro> snapshots = new ConcurrentHashMap<>();

    public Optional<SensorsSnapshotAvro> updateState(SensorEventAvro event) {
        String hubId = event.getHubId();
        String sensorId = event.getId();
        Instant eventTimestamp = event.getTimestamp();

        SensorsSnapshotAvro snapshot = snapshots.computeIfAbsent(hubId, h -> {
            SensorsSnapshotAvro snap = new SensorsSnapshotAvro();
            snap.setHubId(h);
            snap.setTimestamp(eventTimestamp);
            snap.setSensorsState(new HashMap<>());
            return snap;
        });

        Map<String, SensorStateAvro> stateMap = snapshot.getSensorsState();
        SensorStateAvro oldState = stateMap.get(sensorId);

        if (oldState == null || !oldState.getData().equals(event.getPayload())) {
            SensorStateAvro newState = new SensorStateAvro();
            newState.setTimestamp(eventTimestamp);
            newState.setData(event.getPayload());

            stateMap.put(sensorId, newState);
            snapshot.setTimestamp(eventTimestamp);

            return Optional.of(snapshot); // возвращаем обновленный снимок
        }

        return Optional.empty(); // нет изменений
    }
}
