package ru.yandex.practicum.collector.handler.hub;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.handler.TimestampMapper;
import ru.yandex.practicum.collector.service.KafkaProducerService;
import ru.yandex.practicum.grpc.telemetry.event.DeviceAddedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

@Slf4j
@Component
@RequiredArgsConstructor
public class DeviceAddedEventHandler implements HubEventHandler {

    @Value("${hubs}")
    private String topic;
    private final KafkaProducerService producerService;

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.DEVICE_ADDED;
    }

    @Override
    public void handle(HubEventProto eventProto) {
        HubEventAvro eventAvro = map(eventProto);
        ProducerRecord<String, SpecificRecordBase> record = new ProducerRecord<>(
                topic, null, eventAvro.getTimestamp().getEpochSecond(), null, eventAvro
        );
        producerService.sendEvent(record, DeviceAddedEventAvro.class);
        log.info("Событие из hub ID = {} отправлено в топик: {}", eventAvro.getHubId(), topic);
    }

    private HubEventAvro map(HubEventProto eventProto) {
        DeviceAddedEventProto deviceAddedEventProto = eventProto.getDeviceAdded();
        DeviceAddedEventAvro deviceAddedEventAvro = DeviceAddedEventAvro.newBuilder()
                .setId(deviceAddedEventProto.getId())
                .setType(DeviceTypeAvro.valueOf(deviceAddedEventProto.getType().name()))
                .build();
        return HubEventAvro.newBuilder()
                .setHubId(eventProto.getHubId())
                .setTimestamp(TimestampMapper.mapToInstant(eventProto.getTimestamp()))
                .setPayload(deviceAddedEventAvro)
                .build();
    }
}