package com.mathisdulieu.ticketing.library.test.kafka.client;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;

public interface KafkaIntegrationTestClient {

    <T> void send(String topic, T payload);

    <T> void send(String topic, String key, T payload);

    <T> T receive(String topic, Class<T> clazz, long timeoutSeconds);

    <T> ConsumerRecord<String, T> receiveRecord(String topic, Class<T> clazz, long timeoutSeconds);

    <T> List<T> receiveMany(String topic, Class<T> clazz, int expectedCount, long timeoutSeconds);

    <T> List<ConsumerRecord<String, T>> receiveManyRecords(String topic, Class<T> clazz, int expectedCount, long timeoutSeconds);

    void assertNoMessage(String topic, long timeoutSeconds);

    void clearTopic(String topic);
}
