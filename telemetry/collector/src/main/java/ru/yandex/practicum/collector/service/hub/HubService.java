package ru.yandex.practicum.collector.service.hub;

import ru.yandex.practicum.collector.model.hub.HubEvent;

public interface HubService {
    void sendHubEvent(HubEvent hubEvent);
}
