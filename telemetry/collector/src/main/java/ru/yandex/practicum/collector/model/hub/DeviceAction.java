package ru.yandex.practicum.collector.model.hub;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString(callSuper = true)
public class DeviceAction {

    private String sensorId;

    private DeviceActionType type;

    private int value;
}
