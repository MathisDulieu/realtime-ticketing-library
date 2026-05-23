package com.mathisdulieu.ticketing.library.api.inventory;

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

@WireMockTest(httpPort = 8091)
class InventoryApiClientTest {

    private final InventoryApiClient inventoryApiClient = new InventoryApiClient(RestClient.builder().build(), "http://localhost:8091");

    @Test
    void shouldCallInventoryApiWithGetMethod() {
        // Arrange
        stubFor(get("/api/v1/inventories/inventory-id").willReturn(ok()
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
        ResponseEntity<InventoryApiResponse> response = inventoryApiClient.get("/api/v1/inventories/inventory-id");

        // Assert
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().code()).isEqualTo("anyCode");
        assertThat(response.getBody().errors()).isEqualTo(List.of("anyError1", "anyError2"));
    }

    @Test
    void shouldCallInventoryApiWithPostMethod() {
        // Arrange
        stubFor(post("/api/v1/inventories").willReturn(ok()
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
        ResponseEntity<InventoryApiResponse> response = inventoryApiClient.post("/api/v1/inventories", "{}");

        // Assert
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().code()).isEqualTo("anyCode");
        assertThat(response.getBody().errors()).isEqualTo(List.of("anyError1", "anyError2"));
    }

    @Test
    void shouldCallInventoryApiWithDeleteMethod() {
        // Arrange
        stubFor(delete("/api/v1/inventories/inventory-id").willReturn(ok()
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
        ResponseEntity<InventoryApiResponse> response = inventoryApiClient.delete("/api/v1/inventories/inventory-id");

        // Assert
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().code()).isEqualTo("anyCode");
        assertThat(response.getBody().errors()).isEqualTo(List.of("anyError1", "anyError2"));
    }
}