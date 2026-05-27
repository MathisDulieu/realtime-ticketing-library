package com.mathisdulieu.ticketing.library.test.kafka.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class KafkaJsonMapperTest {

    private final KafkaJsonMapper kafkaJsonMapper = new KafkaJsonMapper(new ObjectMapper());

    record DummyEvent(String id, String name) {
    }

    @Test
    void shouldWritePayloadAsJson() {
        // Arrange
        DummyEvent dummyEvent = new DummyEvent("id-1", "name-1");

        // Act
        String eventJson = kafkaJsonMapper.write(dummyEvent);

        // Assert
        assertThat(eventJson).isEqualTo("{\"id\":\"id-1\",\"name\":\"name-1\"}");
    }

    @Test
    void shouldThrowRuntimeException_whenWriteFails() throws JsonProcessingException {
        // Arrange
        ObjectMapper objectMapper = mock(ObjectMapper.class);
        KafkaJsonMapper mapper = new KafkaJsonMapper(objectMapper);
        DummyEvent event = new DummyEvent("id-1", "name-1");

        when(objectMapper.writeValueAsString(event)).thenThrow(new JsonProcessingException("write error") {});

        // Act & Assert
        assertThatThrownBy(() -> mapper.write(event))
            .isInstanceOf(RuntimeException.class)
            .hasCauseInstanceOf(JsonProcessingException.class);
    }

    @Test
    void shouldReadJsonAsPayload() {
        // Arrange
        String eventJson = "{\"id\":\"id-1\",\"name\":\"name-1\"}";

        // Act
        DummyEvent dummyEvent = kafkaJsonMapper.read(eventJson, DummyEvent.class);

        // Assert
        assertThat(dummyEvent).isEqualTo(new DummyEvent("id-1", "name-1"));
    }

    @Test
    void shouldThrowRuntimeException_whenReadFails() throws JsonProcessingException {
        // Arrange
        ObjectMapper objectMapper = mock(ObjectMapper.class);
        KafkaJsonMapper mapper = new KafkaJsonMapper(objectMapper);

        when(objectMapper.readValue("invalid-json", DummyEvent.class)).thenThrow(new JsonProcessingException("read error") {});

        // Act & Assert
        assertThatThrownBy(() -> mapper.read("invalid-json", DummyEvent.class))
            .isInstanceOf(RuntimeException.class)
            .hasCauseInstanceOf(JsonProcessingException.class);
    }

}
