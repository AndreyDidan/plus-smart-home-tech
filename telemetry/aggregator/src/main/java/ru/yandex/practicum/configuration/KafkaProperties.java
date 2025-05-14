package ru.yandex.practicum.configuration;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "spring.kafka")
@Data
public class KafkaProperties {
    private String bootstrapServers;
    private KafkaConsumerProperties consumer;
    private KafkaProducerProperties producer;
    private TopicsProperties topics;

    @Data
    public static class KafkaConsumerProperties {
        private String groupId;
        private String keyDeserializer;
        private String valueDeserializer;
        private String autoOffsetReset;
    }

    @Data
    public static class KafkaProducerProperties {
        private String keySerializer;
        private String valueSerializer;
    }

    @Data
    public static class TopicsProperties {
        private String sensors;
        private String snapshots;
    }
}