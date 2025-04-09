package ru.yandex.practicum.collector.service.sensor;

import ru.yandex.practicum.collector.model.sensor.SensorEvent;

public interface SensorService {

    void sendSensorService(SensorEvent sensorEvent);
}
