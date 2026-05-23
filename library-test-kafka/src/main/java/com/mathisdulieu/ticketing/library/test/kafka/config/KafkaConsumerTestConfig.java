package com.mathisdulieu.ticketing.library.test.kafka.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.Map;

public class KafkaConsumerTestConfig {

    public static <T> ConsumerFactory<String, T> consumerFactory(final String bootstrapServers, final Class<T> targetClass, final String autoOffsetReset) {
        JsonDeserializer<T> deserializer = new JsonDeserializer<>(targetClass);
        deserializer.setRemoveTypeHeaders(false);
        deserializer.addTrustedPackages("*");
        deserializer.setUseTypeMapperForKey(true);

        return new DefaultKafkaConsumerFactory<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false
        ), new StringDeserializer(), deserializer);
    }

    public static <T> ConsumerFactory<String, T> consumerFactory(final String bootstrapServers, final Class<T> targetClass) {
        return consumerFactory(bootstrapServers, targetClass, "earliest");
    }

    public static <T> ConcurrentKafkaListenerContainerFactory<String, T> listenerContainerFactory(final ConsumerFactory<String, T> consumerFactory, final boolean manualAck) {
        ConcurrentKafkaListenerContainerFactory<String, T> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);

        if (manualAck) {
            factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        }

        return factory;
    }

    public static <T> ConcurrentKafkaListenerContainerFactory<String, T> listenerContainerFactory(final ConsumerFactory<String, T> consumerFactory) {
        return listenerContainerFactory(consumerFactory, false);
    }

}