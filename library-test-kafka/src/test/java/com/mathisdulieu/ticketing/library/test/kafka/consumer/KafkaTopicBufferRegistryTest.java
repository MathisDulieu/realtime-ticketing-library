package com.mathisdulieu.ticketing.library.test.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaTopicBufferRegistryTest {

    private final KafkaTopicBufferRegistry registry = new KafkaTopicBufferRegistry();

    @Test
    void shouldReturnBuffer() {
        // Arrange

        // Act
        KafkaTopicBuffer buffer = registry.getBuffer("topic-1");

        // Assert
        assertThat(buffer).isNotNull();
    }

    @Test
    void shouldClearTopicRecords() {
        // Arrange
        KafkaTopicBuffer buffer = registry.getBuffer("topic-1");
        buffer.getRecords().add(new ConsumerRecord<>("topic-1", 0, 0, "key", "value"));

        // Act
        registry.clearTopic("topic-1");

        // Assert
        assertThat(buffer.getRecords()).isEmpty();
    }
}
