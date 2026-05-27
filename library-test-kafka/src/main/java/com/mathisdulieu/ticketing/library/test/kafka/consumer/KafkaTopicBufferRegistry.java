package com.mathisdulieu.ticketing.library.test.kafka.consumer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KafkaTopicBufferRegistry {

    private final Map<String, KafkaTopicBuffer> buffers = new ConcurrentHashMap<>();

    public KafkaTopicBuffer getBuffer(String topic) {
        return buffers.computeIfAbsent(topic, ignored -> new KafkaTopicBuffer());
    }

    public void clearTopic(String topic) {
        getBuffer(topic).getRecords().clear();
    }
}
