package ru.yandex.practicum.collector.kafka;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
@ConfigurationProperties
public class KafkaConfiguration {

    @Value("${kafka_server}")
    String bootstrap;

    @Value("${key_serializer}")
    String keySerializer;

    @Value("${value_serializer}")
    String valueSerializer;

    private Producer<String, SpecificRecordBase> producer;

    @Bean
    public KafkaClient getClient() {
        return new KafkaClient() {

            @Override
            public Producer<String, SpecificRecordBase> getProducer() {
                if (producer == null) {
                    Properties properties = new Properties();
                    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
                    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
                    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
                    producer = new KafkaProducer<>(properties);
                }
                return producer;
            }

            @Override
            public void stop() {
                if (producer != null) {
                    producer.flush();
                    producer.close();
                }
            }
        };
    }
}