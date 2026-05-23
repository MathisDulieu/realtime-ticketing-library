package com.mathisdulieu.ticketing.library.api.inventory;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClient;

@Slf4j
@RequiredArgsConstructor
public class InventoryApiClient {

    private final RestClient restClient;
    private final String baseUrl;

    public ResponseEntity<InventoryApiResponse> get(final String path) {
        log.debug("Calling inventory-service: GET {}{}", baseUrl, path);
        return restClient.get()
                .uri(baseUrl + path)
                .retrieve()
                .toEntity(InventoryApiResponse.class);
    }

    public ResponseEntity<InventoryApiResponse> post(final String path, final Object body) {
        log.debug("Calling inventory-service: POST {}{}", baseUrl, path);
        return restClient.post()
                .uri(baseUrl + path)
                .body(body)
                .retrieve()
                .toEntity(InventoryApiResponse.class);
    }

    public ResponseEntity<InventoryApiResponse> delete(final String path) {
        log.debug("Calling inventory-service: DELETE {}{}", baseUrl, path);
        return restClient.delete()
                .uri(baseUrl + path)
                .retrieve()
                .toEntity(InventoryApiResponse.class);
    }
}