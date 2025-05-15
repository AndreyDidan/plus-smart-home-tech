package ru.yandex.practicum.collector.handler.hub;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.service.KafkaProducerService;
import ru.yandex.practicum.grpc.telemetry.event.DeviceAddedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceTypeAvro;

@Slf4j
@Component
public class DeviceAddedEventHandler extends AbstractHubEventHandler<DeviceAddedEventAvro> {

    public DeviceAddedEventHandler(KafkaProducerService producerService) {
        super(producerService);
    }

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.DEVICE_ADDED;
    }

    @Override
    protected DeviceAddedEventAvro mapPayload(HubEventProto eventProto) {
        DeviceAddedEventProto proto = eventProto.getDeviceAdded();
        return DeviceAddedEventAvro.newBuilder()
                .setId(proto.getId())
                .setType(DeviceTypeAvro.valueOf(proto.getType().name()))
                .build();
    }

    @Override
    protected Class<DeviceAddedEventAvro> getEventClass() {
        return DeviceAddedEventAvro.class;
    }
}