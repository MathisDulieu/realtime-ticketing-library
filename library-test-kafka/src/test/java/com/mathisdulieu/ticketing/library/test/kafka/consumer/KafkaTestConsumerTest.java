package com.mathisdulieu.ticketing.library.test.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaTestConsumerTest {

    private final KafkaConsumer<String, String> consumer = mock(KafkaConsumer.class);

    private final KafkaTopicBufferRegistry registry = new KafkaTopicBufferRegistry();

    @Test
    void shouldStartConsumerAndDispatchPolledRecordsToTopicBuffers() throws Exception {
        // Arrange
        TopicPartition topicPartition = new TopicPartition("topic-1", 0);
        ConsumerRecord<String, String> record = new ConsumerRecord<>("topic-1", 0, 0, "key-1", "value-1");
        ConsumerRecords<String, String> records = new ConsumerRecords<>(Map.of(topicPartition, List.of(record)));

        when(consumer.poll(any())).thenReturn(records, ConsumerRecords.empty());

        KafkaTestConsumer testConsumer = new KafkaTestConsumer(consumer, registry, List.of("topic-1"));

        // Act
        testConsumer.start();

        // Assert
        ConsumerRecord<String, String> receivedRecord = registry.getBuffer("topic-1")
            .getRecords()
            .poll(2, TimeUnit.SECONDS);

        testConsumer.stop();

        verify(consumer).subscribe(List.of("topic-1"));

        assertThat(receivedRecord).isNotNull();
        assertThat(receivedRecord.topic()).isEqualTo("topic-1");
        assertThat(receivedRecord.key()).isEqualTo("key-1");
        assertThat(receivedRecord.value()).isEqualTo("value-1");
    }

    @Test
    void shouldCloseConsumerOnStop() {
        // Arrange
        KafkaTestConsumer testConsumer = new KafkaTestConsumer(consumer, registry, List.of("topic-1"));
        testConsumer.start();

        // Act
        testConsumer.stop();

        // Assert
        verify(consumer).wakeup();
        verify(consumer).close();
    }

}
