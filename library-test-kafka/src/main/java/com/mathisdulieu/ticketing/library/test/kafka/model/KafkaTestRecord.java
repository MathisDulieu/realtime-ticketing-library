package com.mathisdulieu.ticketing.library.test.kafka.model;

public record KafkaTestRecord<T>(
    String topic,
    String key,
    T value,
    int partition,
    long offset
) {
}
