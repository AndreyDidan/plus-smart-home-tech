package ru.yandex.practicum.configuration;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "kafka")
public class KafkaTopicsProperties {
    private Topics topics = new Topics();

    @Data
    public static class Topics {
        private String sensors;
        private String snapshots;
    }
}