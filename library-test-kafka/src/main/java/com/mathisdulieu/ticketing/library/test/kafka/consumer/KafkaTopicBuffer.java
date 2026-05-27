package com.mathisdulieu.ticketing.library.test.kafka.consumer;

import lombok.Getter;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Getter
public class KafkaTopicBuffer {

    private final BlockingQueue<ConsumerRecord<String, String>> records = new LinkedBlockingQueue<>();

}
