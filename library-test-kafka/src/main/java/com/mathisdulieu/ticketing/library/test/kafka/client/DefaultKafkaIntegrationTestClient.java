package com.mathisdulieu.ticketing.library.test.kafka.client;

import com.mathisdulieu.ticketing.library.test.kafka.consumer.KafkaTopicBuffer;
import com.mathisdulieu.ticketing.library.test.kafka.consumer.KafkaTopicBufferRegistry;
import com.mathisdulieu.ticketing.library.test.kafka.exception.KafkaTestTimeoutException;
import com.mathisdulieu.ticketing.library.test.kafka.utils.KafkaJsonMapper;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@RequiredArgsConstructor
public class DefaultKafkaIntegrationTestClient implements KafkaIntegrationTestClient {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaTopicBufferRegistry registry;
    private final KafkaJsonMapper mapper;

    @Override
    public <T> void send(String topic, T payload) {
        kafkaTemplate.send(topic, mapper.write(payload));
    }

    @Override
    public <T> void send(String topic, String key, T payload) {
        kafkaTemplate.send(topic, key, mapper.write(payload));
    }

    @Override
    public <T> T receive(String topic, Class<T> clazz, long timeoutSeconds) {
        return receiveRecord(topic, clazz, timeoutSeconds).value();
    }

    @Override
    public <T> ConsumerRecord<String, T> receiveRecord(String topic, Class<T> clazz, long timeoutSeconds) {
        KafkaTopicBuffer buffer = registry.getBuffer(topic);

        try {
            ConsumerRecord<String, String> rawRecord = buffer.getRecords().poll(timeoutSeconds, TimeUnit.SECONDS);

            if (rawRecord == null) {
                throw new KafkaTestTimeoutException("No record received on topic '%s' within %s seconds".formatted(topic, timeoutSeconds));
            }

            T value = mapper.read(rawRecord.value(), clazz);

            return new ConsumerRecord<>(rawRecord.topic(), rawRecord.partition(), rawRecord.offset(), rawRecord.key(), value);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(exception);
        }
    }

    @Override
    public <T> List<T> receiveMany(String topic, Class<T> clazz, int expectedCount, long timeoutSeconds) {
        return receiveManyRecords(topic, clazz, expectedCount, timeoutSeconds)
            .stream()
            .map(ConsumerRecord::value)
            .toList();
    }

    @Override
    public <T> List<ConsumerRecord<String, T>> receiveManyRecords(String topic, Class<T> clazz, int expectedCount, long timeoutSeconds) {
        List<ConsumerRecord<String, T>> records = new ArrayList<>();

        for (int i = 0; i < expectedCount; i++) {
            records.add(receiveRecord(topic, clazz, timeoutSeconds));
        }

        return records;
    }

    @Override
    public void assertNoMessage(String topic, long timeoutSeconds) {
        KafkaTopicBuffer buffer = registry.getBuffer(topic);

        try {
            ConsumerRecord<String, String> record = buffer.getRecords().poll(timeoutSeconds, TimeUnit.SECONDS);

            if (record != null) {
                throw new AssertionError("Expected no record on topic '%s' but received one".formatted(topic));
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(exception);
        }
    }

    @Override
    public void clearTopic(String topic) {
        registry.clearTopic(topic);
    }
}
