package com.mathisdulieu.ticketing.library.test.kafka.exception;

public class KafkaTestTimeoutException extends RuntimeException {

    public KafkaTestTimeoutException(String message) {
        super(message);
    }
}
