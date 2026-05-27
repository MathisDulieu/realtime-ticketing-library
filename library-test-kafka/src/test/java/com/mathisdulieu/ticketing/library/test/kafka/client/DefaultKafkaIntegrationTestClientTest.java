package com.mathisdulieu.ticketing.library.test.kafka.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mathisdulieu.ticketing.library.test.kafka.consumer.KafkaTopicBuffer;
import com.mathisdulieu.ticketing.library.test.kafka.consumer.KafkaTopicBufferRegistry;
import com.mathisdulieu.ticketing.library.test.kafka.exception.KafkaTestTimeoutException;
import com.mathisdulieu.ticketing.library.test.kafka.utils.KafkaJsonMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DefaultKafkaIntegrationTestClientTest {

    private final KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);;

    private final KafkaTopicBufferRegistry registry = new KafkaTopicBufferRegistry();
    private final KafkaIntegrationTestClient client = new DefaultKafkaIntegrationTestClient(kafkaTemplate, registry, new KafkaJsonMapper(new ObjectMapper()));

    record DummyEvent(String id, String name) {
    }

    @Test
    void shouldSendPayloadWithoutKey() {
        // Arrange
        DummyEvent dummyEvent = new DummyEvent("id-1", "name-1");

        // Act
        client.send("topic-1", dummyEvent);

        // Assert
        verify(kafkaTemplate).send("topic-1", "{\"id\":\"id-1\",\"name\":\"name-1\"}");
    }

    @Test
    void shouldSendPayloadWithKey() {
        // Arrange
        DummyEvent dummyEvent = new DummyEvent("id-1", "name-1");

        // Act
        client.send("topic-1", "key-1", dummyEvent);

        // Assert
        verify(kafkaTemplate).send("topic-1", "key-1", "{\"id\":\"id-1\",\"name\":\"name-1\"}");
    }

    @Test
    void shouldReceivePayload() {
        // Arrange
        registry.getBuffer("topic-1").getRecords()
            .add(new ConsumerRecord<>("topic-1", 0, 0, "key-1", "{\"id\":\"id-1\",\"name\":\"name-1\"}"));

        // Act
        DummyEvent dummyEvent = client.receive("topic-1", DummyEvent.class, 1);

        // Assert
        assertThat(dummyEvent).isEqualTo(new DummyEvent("id-1", "name-1"));
    }

    @Test
    void shouldReceiveRecord() {
        // Arrange
        registry.getBuffer("topic-1")
            .getRecords()
            .add(new ConsumerRecord<>("topic-1", 0, 12, "key-1", "{\"id\":\"id-1\",\"name\":\"name-1\"}"));

        // Act
        ConsumerRecord<String, DummyEvent> record = client.receiveRecord("topic-1", DummyEvent.class, 1);

        // Assert
        assertThat(record.topic()).isEqualTo("topic-1");
        assertThat(record.partition()).isZero();
        assertThat(record.offset()).isEqualTo(12);
        assertThat(record.key()).isEqualTo("key-1");
        assertThat(record.value()).isEqualTo(new DummyEvent("id-1", "name-1"));
    }

    @Test
    void shouldThrowRuntimeException_whenReceiveRecordIsInterrupted() throws InterruptedException {
        // Arrange
        BlockingQueue<ConsumerRecord<String, String>> records = mock(BlockingQueue.class);

        KafkaTopicBuffer buffer = mock(KafkaTopicBuffer.class);
        when(buffer.getRecords()).thenReturn(records);

        KafkaTopicBufferRegistry registry = mock(KafkaTopicBufferRegistry.class);
        when(registry.getBuffer("topic-1")).thenReturn(buffer);

        KafkaIntegrationTestClient client = new DefaultKafkaIntegrationTestClient(kafkaTemplate, registry, new KafkaJsonMapper(new ObjectMapper()));

        when(records.poll(1, TimeUnit.SECONDS)).thenThrow(new InterruptedException("interrupted"));

        // Act & Assert
        assertThatThrownBy(() -> client.receiveRecord("topic-1", DummyEvent.class, 1))
            .isInstanceOf(RuntimeException.class)
            .hasCauseInstanceOf(InterruptedException.class);

        assertThat(Thread.currentThread().isInterrupted()).isTrue();
    }

    @Test
    void shouldReceiveManyPayloads() {
        // Arrange
        registry.getBuffer("topic-1")
            .getRecords()
            .add(new ConsumerRecord<>("topic-1", 0, 0, "key-1", "{\"id\":\"id-1\",\"name\":\"name-1\"}"));

        registry.getBuffer("topic-1")
            .getRecords()
            .add(new ConsumerRecord<>("topic-1", 0, 1, "key-2", "{\"id\":\"id-2\",\"name\":\"name-2\"}"));

        // Act
        List<DummyEvent> dummyEvents = client.receiveMany("topic-1", DummyEvent.class, 2, 1);

        // Assert
        assertThat(dummyEvents).containsExactly(
            new DummyEvent("id-1", "name-1"),
            new DummyEvent("id-2", "name-2")
        );
    }

    @Test
    void shouldReceiveManyRecords() {
        // Arrange
        registry.getBuffer("topic-1")
            .getRecords()
            .add(new ConsumerRecord<>("topic-1", 0, 0, "key-1", "{\"id\":\"id-1\",\"name\":\"name-1\"}"));

        registry.getBuffer("topic-1")
            .getRecords()
            .add(new ConsumerRecord<>("topic-1", 0, 1, "key-2", "{\"id\":\"id-2\",\"name\":\"name-2\"}"));

        // Act
        List<ConsumerRecord<String, DummyEvent>> records = client.receiveManyRecords("topic-1", DummyEvent.class, 2, 1);

        // Assert
        assertThat(records).hasSize(2);
        assertThat(records.getFirst().key()).isEqualTo("key-1");
        assertThat(records.getFirst().value()).isEqualTo(new DummyEvent("id-1", "name-1"));
        assertThat(records.getLast().key()).isEqualTo("key-2");
        assertThat(records.getLast().value()).isEqualTo(new DummyEvent("id-2", "name-2"));
    }

    @Test
    void shouldThrowTimeoutException_whenNoRecordIsReceived() {
        // Arrange

        // Act & Assert
        assertThatThrownBy(() -> client.receive("topic-1", DummyEvent.class, 1))
            .isInstanceOf(KafkaTestTimeoutException.class)
            .hasMessage("No record received on topic 'topic-1' within 1 seconds");
    }

    @Test
    void shouldAssertNoMessageReceivedOnTopic() {
        // Arrange

        // Act & Assert
        client.assertNoMessage("topic-1", 1);
    }

    @Test
    void shouldThrowAssertionError_whenMessageIsReceived() {
        // Arrange
        registry.getBuffer("topic-1")
            .getRecords()
            .add(new ConsumerRecord<>("topic-1", 0, 0, "key-1", "{\"id\":\"id-1\",\"name\":\"name-1\"}"));

        // Act & Assert
        assertThatThrownBy(() -> client.assertNoMessage("topic-1", 1))
            .isInstanceOf(AssertionError.class)
            .hasMessage("Expected no record on topic 'topic-1' but received one");
    }

    @Test
    void shouldThrowRuntimeException_whenAssertNoMessageIsInterrupted() throws InterruptedException {
        // Arrange
        BlockingQueue<ConsumerRecord<String, String>> records = mock(BlockingQueue.class);

        KafkaTopicBuffer buffer = mock(KafkaTopicBuffer.class);
        when(buffer.getRecords()).thenReturn(records);

        KafkaTopicBufferRegistry registry = mock(KafkaTopicBufferRegistry.class);
        when(registry.getBuffer("topic-1")).thenReturn(buffer);

        KafkaIntegrationTestClient client = new DefaultKafkaIntegrationTestClient(kafkaTemplate, registry, new KafkaJsonMapper(new ObjectMapper()));

        when(records.poll(1, TimeUnit.SECONDS)).thenThrow(new InterruptedException("interrupted"));

        // Act & Assert
        assertThatThrownBy(() -> client.assertNoMessage("topic-1", 1))
            .isInstanceOf(RuntimeException.class)
            .hasCauseInstanceOf(InterruptedException.class);

        assertThat(Thread.currentThread().isInterrupted()).isTrue();
    }

    @Test
    void shouldClearTopic() {
        // Arrange
        registry.getBuffer("topic-1")
            .getRecords()
            .add(new ConsumerRecord<>("topic-1", 0, 0, "key-1", "{\"id\":\"id-1\",\"name\":\"name-1\"}"));

        // Act
        client.clearTopic("topic-1");

        // Assert
        assertThat(registry.getBuffer("topic-1").getRecords()).isEmpty();
    }

}
