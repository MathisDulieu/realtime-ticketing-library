package com.mathisdulieu.ticketing.library.test.kafka.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mathisdulieu.ticketing.library.test.kafka.client.DefaultKafkaIntegrationTestClient;
import com.mathisdulieu.ticketing.library.test.kafka.client.KafkaIntegrationTestClient;
import com.mathisdulieu.ticketing.library.test.kafka.consumer.KafkaTestConsumer;
import com.mathisdulieu.ticketing.library.test.kafka.consumer.KafkaTopicBufferRegistry;
import com.mathisdulieu.ticketing.library.test.kafka.utils.KafkaJsonMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

@Configuration
public class KafkaIntegrationTestConfiguration {

    @Bean("kafkaIntegrationTestTemplate")
    public KafkaTemplate<String, String> kafkaIntegrationTestTemplate(Environment environment) {
        return new KafkaTemplate<>(
            new DefaultKafkaProducerFactory<>(
                Map.of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    environment.getProperty("spring.embedded.kafka.brokers"),
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    StringSerializer.class,
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    StringSerializer.class
                )
            )
        );
    }

    @Bean
    public KafkaConsumer<String, String> kafkaConsumer(Environment environment) {
        return new KafkaConsumer<>(
            Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                Objects.requireNonNull(environment.getProperty("spring.embedded.kafka.brokers")),

                ConsumerConfig.GROUP_ID_CONFIG,
                "test-consumer-" + UUID.randomUUID(),

                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest",

                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class,

                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class
            )
        );
    }

    @Bean
    public KafkaTopicBufferRegistry kafkaTopicBufferRegistry() {
        return new KafkaTopicBufferRegistry();
    }

    @Bean
    public KafkaJsonMapper kafkaJsonMapper(ObjectMapper objectMapper) {
        return new KafkaJsonMapper(objectMapper);
    }

    @Bean
    public KafkaIntegrationTestClient kafkaIntegrationTestClient(
        @Qualifier("kafkaIntegrationTestTemplate") KafkaTemplate<String, String> kafkaTemplate,
        KafkaTopicBufferRegistry registry,
        KafkaJsonMapper mapper
    ) {
        return new DefaultKafkaIntegrationTestClient(kafkaTemplate, registry, mapper);
    }

    @Bean
    public KafkaTestConsumer kafkaTestConsumer(KafkaConsumer<String, String> consumer, KafkaTopicBufferRegistry registry, Environment environment) {
        Binder binder = Binder.get(environment);

        List<String> topics = binder.bind(
            "library.test.kafka.topics",
            Bindable.listOf(String.class)
        ).orElse(List.of());

        if (topics.isEmpty()) {
            throw new IllegalStateException("No Kafka test topics found. Configure library.test.kafka.topics.");
        }

        return new KafkaTestConsumer(
            consumer,
            registry,
            topics.stream()
                .map(String::trim)
                .filter(topic -> !topic.isBlank())
                .toList()
        );
    }
}
