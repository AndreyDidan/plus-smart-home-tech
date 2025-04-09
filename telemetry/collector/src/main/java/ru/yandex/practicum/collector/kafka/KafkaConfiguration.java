package ru.yandex.practicum.collector.kafka;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.VoidSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yandex.practicum.kafka.serializer.GeneralAvroSerializer;

import java.util.Properties;

@Configuration
public class KafkaConfiguration {

    private Producer<String, SpecificRecordBase> producer;

    @Bean
    public KafkaClient getClient() {
        return new KafkaClient() {

            @Override
            public Producer<String, SpecificRecordBase> getProducer() {
                if (producer == null) {
                    Properties properties = new Properties();
                    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
                    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, VoidSerializer.class);
                    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GeneralAvroSerializer.class);
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