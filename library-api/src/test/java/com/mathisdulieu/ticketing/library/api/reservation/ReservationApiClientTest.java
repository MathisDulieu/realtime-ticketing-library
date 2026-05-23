package com.mathisdulieu.ticketing.library.api.reservation;

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

@WireMockTest(httpPort = 8092)
class ReservationApiClientTest {

    private final ReservationApiClient reservationApiClient = new ReservationApiClient(RestClient.builder().build(), "http://localhost:8092");

    @Test
    void shouldCallReservationApiWithGetMethod() {
        // Arrange
        stubFor(get("/api/v1/reservations/reservation-id").willReturn(ok()
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
        ResponseEntity<ReservationApiResponse> response = reservationApiClient.get("/api/v1/reservations/reservation-id");

        // Assert
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().code()).isEqualTo("anyCode");
        assertThat(response.getBody().errors()).isEqualTo(List.of("anyError1", "anyError2"));
    }

    @Test
    void shouldCallReservationApiWithPostMethod() {
        // Arrange
        stubFor(post("/api/v1/reservations").willReturn(ok()
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
        ResponseEntity<ReservationApiResponse> response = reservationApiClient.post("/api/v1/reservations", "{}");

        // Assert
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().code()).isEqualTo("anyCode");
        assertThat(response.getBody().errors()).isEqualTo(List.of("anyError1", "anyError2"));
    }

    @Test
    void shouldCallReservationApiWithDeleteMethod() {
        // Arrange
        stubFor(delete("/api/v1/reservations/reservation-id").willReturn(ok()
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
        ResponseEntity<ReservationApiResponse> response = reservationApiClient.delete("/api/v1/reservations/reservation-id");

        // Assert
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().code()).isEqualTo("anyCode");
        assertThat(response.getBody().errors()).isEqualTo(List.of("anyError1", "anyError2"));
    }
}