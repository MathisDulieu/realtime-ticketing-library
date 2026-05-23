package com.mathisdulieu.ticketing.library.api.event;

import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClient;

import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.delete;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static org.assertj.core.api.Assertions.assertThat;

@WireMockTest(httpPort = 8090)
class EventApiClientTest {

    private final EventApiClient eventApiClient = new EventApiClient(RestClient.builder().build(), "http://localhost:8090");

    @Test
    void shouldCallEventApiWithGetMethod() {
        // Arrange
        stubFor(get("/api/v1/events/event-id").willReturn(ok()
                .withHeader("Content-Type", "application/json")
                .withBody("""
                        {
                            "code": "anyCode",
                            "errors": [
                                "anyError1",
                                "anyError2"
                            ]
                        }
                        """)));

        // Act
        ResponseEntity<EventApiResponse> response = eventApiClient.get("/api/v1/events/event-id");

        // Assert
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().code()).isEqualTo("anyCode");
        assertThat(response.getBody().errors()).isEqualTo(List.of("anyError1", "anyError2"));
    }

    @Test
    void shouldCallEventApiWithPostMethod() {
        // Arrange
        stubFor(post("/api/v1/events").willReturn(ok()
                .withHeader("Content-Type", "application/json")
                .withBody("""
                        {
                            "code": "anyCode",
                            "errors": [
                                "anyError1",
                                "anyError2"
                            ]
                        }
                        """)));

        // Act
        ResponseEntity<EventApiResponse> response = eventApiClient.post("/api/v1/events", "{}");

        // Assert
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().code()).isEqualTo("anyCode");
        assertThat(response.getBody().errors()).isEqualTo(List.of("anyError1", "anyError2"));
    }

    @Test
    void shouldCallEventApiWithDeleteMethod() {
        // Arrange
        stubFor(delete("/api/v1/events/event-id").willReturn(ok()
                .withHeader("Content-Type", "application/json")
                .withBody("""
                        {
                            "code": "anyCode",
                            "errors": [
                                "anyError1",
                                "anyError2"
                            ]
                        }
                        """)));

        // Act
        ResponseEntity<EventApiResponse> response = eventApiClient.delete("/api/v1/events/event-id");

        // Assert
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().code()).isEqualTo("anyCode");
        assertThat(response.getBody().errors()).isEqualTo(List.of("anyError1", "anyError2"));
    }
}