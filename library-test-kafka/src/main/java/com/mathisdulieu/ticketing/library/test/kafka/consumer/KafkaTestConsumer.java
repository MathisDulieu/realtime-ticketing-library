package com.mathisdulieu.ticketing.library.test.kafka.consumer;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KafkaTestConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaTestConsumer.class);

    private final KafkaConsumer<String, String> consumer;
    private final KafkaTopicBufferRegistry registry;
    private final Collection<String> topics;

    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    private volatile boolean running = true;

    public KafkaTestConsumer(
        KafkaConsumer<String, String> consumer,
        KafkaTopicBufferRegistry registry,
        Collection<String> topics
    ) {
        this.consumer = consumer;
        this.registry = registry;
        this.topics = topics;
    }

    @PostConstruct
    public void start() {
        consumer.subscribe(topics);

        executorService.submit(() -> {
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> record : records) {
                    registry.getBuffer(record.topic()).getRecords().add(record);

                    log.debug("Record received topic={} key={} partition={} offset={}", record.topic(), record.key(), record.partition(), record.offset());
                }
            }
        });
    }

    @PreDestroy
    public void stop() {
        running = false;
        consumer.wakeup();
        executorService.shutdownNow();
        consumer.close();
    }
}
