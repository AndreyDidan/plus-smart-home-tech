package ru.yandex.practicum.collector.handler.hub;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.handler.TimestampMapper;
import ru.yandex.practicum.collector.service.KafkaProducerService;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioRemovedEventProto;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;

@Slf4j
@Component
@RequiredArgsConstructor
public class ScenarioRemovedEventHandler implements HubEventHandler {

    @Value("${hubs}")
    private String topic;
    private final KafkaProducerService producerService;

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.SCENARIO_REMOVED;
    }

    @Override
    public void handle(HubEventProto eventProto) {
        HubEventAvro eventAvro = map(eventProto);
        ProducerRecord<String, SpecificRecordBase> record = new ProducerRecord<>(
                topic, null, eventAvro.getTimestamp().getEpochSecond(), null, eventAvro
        );
        producerService.sendEvent(record, ScenarioRemovedEventAvro.class);
        log.info("Событие из hub ID = {} отправлено в топик: {}", eventAvro.getHubId(), topic);
    }

    private HubEventAvro map(HubEventProto eventProto) {
        ScenarioRemovedEventProto scenarioRemovedEventProto = eventProto.getScenarioRemoved();
        ScenarioRemovedEventAvro scenarioRemovedEventAvro = ScenarioRemovedEventAvro.newBuilder()
                .setName(scenarioRemovedEventProto.getName())
                .build();
        return HubEventAvro.newBuilder()
                .setHubId(eventProto.getHubId())
                .setTimestamp(TimestampMapper.mapToInstant(eventProto.getTimestamp()))
                .setPayload(scenarioRemovedEventAvro)
                .build();
    }
}