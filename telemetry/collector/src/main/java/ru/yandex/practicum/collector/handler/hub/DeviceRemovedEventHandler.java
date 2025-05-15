package ru.yandex.practicum.collector.handler.hub;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.service.KafkaProducerService;
import ru.yandex.practicum.grpc.telemetry.event.DeviceRemovedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;

@Slf4j
@Component
public class DeviceRemovedEventHandler extends AbstractHubEventHandler<DeviceRemovedEventAvro> {

    public DeviceRemovedEventHandler(KafkaProducerService producerService) {
        super(producerService);
    }

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.DEVICE_REMOVED;
    }

    @Override
    protected DeviceRemovedEventAvro mapPayload(HubEventProto eventProto) {
        DeviceRemovedEventProto proto = eventProto.getDeviceRemoved();
        return DeviceRemovedEventAvro.newBuilder()
                .setId(proto.getId())
                .build();
    }

    @Override
    protected Class<DeviceRemovedEventAvro> getEventClass() {
        return DeviceRemovedEventAvro.class;
    }
}
