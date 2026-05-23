package com.mathisdulieu.ticketing.library.api.event;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClient;

@Slf4j
@RequiredArgsConstructor
public class EventApiClient {

    private final RestClient restClient;
    private final String baseUrl;

    public ResponseEntity<EventApiResponse> get(final String path) {
        log.debug("Calling event-service: GET {}{}", baseUrl, path);
        return restClient.get()
                .uri(baseUrl + path)
                .retrieve()
                .toEntity(EventApiResponse.class);
    }

    public ResponseEntity<EventApiResponse> post(final String path, final Object body) {
        log.debug("Calling event-service: POST {}{}", baseUrl, path);
        return restClient.post()
                .uri(baseUrl + path)
                .body(body)
                .retrieve()
                .toEntity(EventApiResponse.class);
    }

    public ResponseEntity<EventApiResponse> delete(final String path) {
        log.debug("Calling event-service: DELETE {}{}", baseUrl, path);
        return restClient.delete()
                .uri(baseUrl + path)
                .retrieve()
                .toEntity(EventApiResponse.class);
    }
}