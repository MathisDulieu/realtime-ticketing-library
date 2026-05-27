package com.mathisdulieu.ticketing.library.test.kafka.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class KafkaJsonMapper {

    private final ObjectMapper objectMapper;

    public String write(Object payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException exception) {
            throw new RuntimeException(exception);
        }
    }

    public <T> T read(String payload, Class<T> clazz) {
        try {
            return objectMapper.readValue(payload, clazz);
        } catch (JsonProcessingException exception) {
            throw new RuntimeException(exception);
        }
    }
}
