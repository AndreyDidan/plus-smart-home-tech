package ru.yandex.practicum.collector.handler.hub;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.beans.factory.annotation.Value;
import ru.yandex.practicum.collector.handler.TimestampMapper;
import ru.yandex.practicum.collector.service.KafkaProducerService;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

@Slf4j
public abstract class AbstractHubEventHandler<T extends SpecificRecordBase> implements HubEventHandler {

    @Value("${hubs}")
    protected String topic;

    protected final KafkaProducerService producerService;

    protected AbstractHubEventHandler(KafkaProducerService producerService) {
        this.producerService = producerService;
    }

    protected abstract T mapPayload(HubEventProto eventProto);

    protected abstract Class<T> getEventClass();

    @Override
    public void handle(HubEventProto eventProto) {
        T payload = mapPayload(eventProto);
        HubEventAvro eventAvro = HubEventAvro.newBuilder()
                .setHubId(eventProto.getHubId())
                .setTimestamp(TimestampMapper.mapToInstant(eventProto.getTimestamp()))
                .setPayload(payload)
                .build();

        String hubId = eventProto.getHubId();
        long timestamp = eventAvro.getTimestamp().getEpochSecond();

        producerService.sendEvent(hubId, timestamp, eventAvro, topic, getEventClass());
        log.info("Событие из hub ID = {} отправлено в топик: {}", hubId, topic);
    }
}