package ru.yandex.practicum.collector.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import ru.yandex.practicum.collector.model.hub.HubEvent;
import ru.yandex.practicum.collector.model.sensor.SensorEvent;
import ru.yandex.practicum.collector.service.hub.HubService;
import ru.yandex.practicum.collector.service.sensor.SensorService;

@Validated
@RestController
@RequestMapping(path = "/events")
@RequiredArgsConstructor
public class EventController {

    private final SensorService sensorService;
    private final HubService hubService;

    @PostMapping("/sensors")
    public void collectSensorEvent(@RequestBody @Valid SensorEvent sensorEvent) {
        sensorService.sendSensorService(sensorEvent);
    }

    @PostMapping("/hubs")
    public void collectHubEvent(@RequestBody @Valid HubEvent hubEvent) {
        hubService.sendHubEvent(hubEvent);
    }
}
