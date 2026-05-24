package com.mathisdulieu.ticketing.library.test.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class GenericTestKafkaConsumer<T> {

    private static final Logger log = LoggerFactory.getLogger(GenericTestKafkaConsumer.class);

    public final BlockingQueue<ConsumerRecord<String, T>> records = new LinkedBlockingQueue<>();

    public ConsumerRecord<String, T> pollRecord(final String topic, final long timeoutSeconds) throws InterruptedException {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(timeoutSeconds);

        while (true) {
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) throw new AssertionError("No record received on topic '" + topic + "' within " + timeoutSeconds + " seconds");
            ConsumerRecord<String, T> record = records.poll(remaining, TimeUnit.MILLISECONDS);
            if (record == null) throw new AssertionError("No record received on topic '" + topic + "' within " + timeoutSeconds + " seconds");
            if (topic.equals(record.topic())) {
                log.debug("Record received on topic '{}' with key '{}'", record.topic(), record.key());
                return record;
            }
            records.put(record);
        }
    }

    public List<ConsumerRecord<String, T>> pollRecords(final String topic, final int count, final long timeoutSeconds) throws InterruptedException {
        List<ConsumerRecord<String, T>> result = new ArrayList<>();
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(timeoutSeconds);

        while (result.size() < count) {
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) break;
            ConsumerRecord<String, T> record = records.poll(remaining, TimeUnit.MILLISECONDS);
            if (record == null) break;
            if (topic.equals(record.topic())) {
                log.debug("Record {}/{} received on topic '{}' with key '{}'", result.size() + 1, count, record.topic(), record.key());
                result.add(record);
            } else {
                records.put(record);
            }
        }

        if (result.size() < count) throw new AssertionError("Expected " + count + " record(s) on topic '" + topic + "' within " + timeoutSeconds + " seconds but got " + result.size());

        return result;
    }

    public void clearRecordsFromTopic(final String topic) {
        records.removeIf(record -> topic.equals(record.topic()));
    }

    public void assertNoRecordReceived(final String topic, final long timeoutSeconds) throws InterruptedException {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(timeoutSeconds);

        while (true) {
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                log.debug("No record received on topic '{}' within {} seconds (as expected)", topic, timeoutSeconds);
                return;
            }
            ConsumerRecord<String, T> record = records.poll(remaining, TimeUnit.MILLISECONDS);
            if (record == null) return;
            if (topic.equals(record.topic())) {
                throw new AssertionError("Expected no record on topic '" + topic + "' but got one with key '" + record.key() + "'");
            }
            records.put(record);
        }
    }

    public int recordCount(final String topic) {
        return (int) records.stream().filter(record -> topic.equals(record.topic())).count();
    }

}
