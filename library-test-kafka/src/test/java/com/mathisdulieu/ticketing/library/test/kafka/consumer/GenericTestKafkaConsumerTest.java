package com.mathisdulieu.ticketing.library.test.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GenericTestKafkaConsumerTest {

    private final GenericTestKafkaConsumer<DummyEntity> consumer = new GenericTestKafkaConsumer<>();

    record DummyEntity(String id, String name) {}

    @Test
    void shouldPollRecord() throws InterruptedException {
        // Arrange
        DummyEntity dummyEntity1 = new DummyEntity("id1", "name1");
        DummyEntity dummyEntity2 = new DummyEntity("id2", "name2");

        consumer.records.add(new ConsumerRecord<>("test-topic-2", 0, 0, "test-key-2", dummyEntity2));
        consumer.records.add(new ConsumerRecord<>("test-topic-1", 0, 1, "test-key-1", dummyEntity1));

        // Act
        ConsumerRecord<String, DummyEntity> polledRecord = consumer.pollRecord("test-topic-1", 5);

        // Assert
        assertThat(polledRecord).isNotNull();
        assertThat(polledRecord.topic()).isEqualTo("test-topic-1");
        assertThat(polledRecord.key()).isEqualTo("test-key-1");
        assertThat(polledRecord.value()).isEqualTo(dummyEntity1);
    }

    @Test
    void shouldPollRecords() throws InterruptedException {
        // Arrange
        DummyEntity dummyEntity1 = new DummyEntity("id1", "name1");
        DummyEntity dummyEntity2 = new DummyEntity("id2", "name2");
        DummyEntity dummyEntity3 = new DummyEntity("id3", "name3");

        consumer.records.add(new ConsumerRecord<>("test-topic-2", 0, 0, "test-key-3", dummyEntity3));

        consumer.records.add(new ConsumerRecord<>("test-topic-1", 0, 1, "test-key-1", dummyEntity1));
        consumer.records.add(new ConsumerRecord<>("test-topic-1", 0, 2, "test-key-2", dummyEntity2));

        // Act
        List<ConsumerRecord<String, DummyEntity>> polledRecords = consumer.pollRecords("test-topic-1", 2, 5);

        // Assert
        assertThat(polledRecords).hasSize(2);
        assertThat(polledRecords.getFirst().topic()).isEqualTo("test-topic-1");
        assertThat(polledRecords.getFirst().key()).isEqualTo("test-key-1");
        assertThat(polledRecords.getFirst().value()).isEqualTo(dummyEntity1);

        assertThat(polledRecords.getLast().topic()).isEqualTo("test-topic-1");
        assertThat(polledRecords.getLast().key()).isEqualTo("test-key-2");
        assertThat(polledRecords.getLast().value()).isEqualTo(dummyEntity2);
    }

    @Test
    void shouldClearRecordsFromTopic() throws InterruptedException {
        // Arrange
        DummyEntity dummyEntity1 = new DummyEntity("id1", "name1");
        DummyEntity dummyEntity2 = new DummyEntity("id2", "name2");
        DummyEntity dummyEntity3 = new DummyEntity("id3", "name3");

        consumer.records.add(new ConsumerRecord<>("test-topic-1", 0, 0, "test-key-1", dummyEntity1));
        consumer.records.add(new ConsumerRecord<>("test-topic-1", 0, 1, "test-key-2", dummyEntity2));

        consumer.records.add(new ConsumerRecord<>("test-topic-2", 0, 2, "test-key-3", dummyEntity3));

        // Act
        consumer.clearRecordsFromTopic("test-topic-1");

        // Assert
        boolean hasNoRecords = consumer.hasNoRecords("test-topic-1");
        assertThat(hasNoRecords).isTrue();

        ConsumerRecord<String, DummyEntity> polledRecord = consumer.pollRecord("test-topic-2", 5);
        assertThat(polledRecord.topic()).isEqualTo("test-topic-2");
        assertThat(polledRecord.key()).isEqualTo("test-key-3");
        assertThat(polledRecord.value()).isEqualTo(dummyEntity3);
    }

    @Test
    void shouldReturnTrue_whenTopicHasNoRecord() {
        // Arrange
        DummyEntity dummyEntity = new DummyEntity("id", "name");
        consumer.records.add(new ConsumerRecord<>("other-topic", 0, 0, "test-key", dummyEntity));

        // Act
        boolean hasNoRecords = consumer.hasNoRecords("test-topic");

        // Assert
        assertThat(hasNoRecords).isTrue();
    }

    @Test
    void shouldReturnFalse_whenTopicHasRecord() {
        // Arrange
        DummyEntity dummyEntity = new DummyEntity("id", "name");
        consumer.records.add(new ConsumerRecord<>("test-topic", 0, 0, "test-key", dummyEntity));

        // Act
        boolean hasNoRecords = consumer.hasNoRecords("test-topic");

        // Assert
        assertThat(hasNoRecords).isFalse();
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 3})
    void shouldReturnRecordCountFromTopic(int expectedCount) {
        // Arrange
        for (int i = 0; i < expectedCount; i++) {
            DummyEntity dummyEntity = new DummyEntity("id" + i, "name" + i);
            consumer.records.add(new ConsumerRecord<>("test-topic-1", 0, i, "test-key-" + i, dummyEntity));
        }

        DummyEntity dummyEntity = new DummyEntity("other-id", "other-name");
        consumer.records.add(new ConsumerRecord<>("test-topic-2", 0, 0, "other-key", dummyEntity));

        // Act
        int recordCount = consumer.recordCount("test-topic-1");

        // Assert
        assertThat(recordCount).isEqualTo(expectedCount);
    }

    @Test
    void shouldThrowAssertionError_whenNoRecordReceivedWithinTimeout_whenPollRecord() {
        // Arrange

        // Act & Assert
        assertThatThrownBy(() -> consumer.pollRecord("test-topic", 1))
                .isInstanceOf(AssertionError.class)
                .hasMessage("No record received on topic 'test-topic' within 1 seconds");
    }

    @Test
    void shouldThrowAssertionError_whenRecordReceivedOnDifferentTopic_whenPollRecord() {
        // Arrange
        DummyEntity dummyEntity = new DummyEntity("id", "name");
        consumer.records.add(new ConsumerRecord<>("other-topic", 0, 0, "test-key", dummyEntity));

        // Act & Assert
        assertThatThrownBy(() -> consumer.pollRecord("test-topic", 1))
                .isInstanceOf(AssertionError.class)
                .hasMessage("No record received on topic 'test-topic' within 1 seconds");
    }

    @Test
    void shouldThrowAssertionError_whenNotEnoughRecordsReceivedWithinTimeout_whenPollRecords() {
        // Arrange
        DummyEntity dummyEntity = new DummyEntity("id", "name");
        consumer.records.add(new ConsumerRecord<>("test-topic", 0, 0, "test-key-1", dummyEntity));

        // Act & Assert
        assertThatThrownBy(() -> consumer.pollRecords("test-topic", 3, 1))
                .isInstanceOf(AssertionError.class)
                .hasMessage("Expected 3 record(s) on topic 'test-topic' within 1 seconds but got 1");
    }

}