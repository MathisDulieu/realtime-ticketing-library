package com.mathisdulieu.ticketing.library.api.reservation;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClient;

@Slf4j
@RequiredArgsConstructor
public class ReservationApiClient {

    private final RestClient restClient;
    private final String baseUrl;

    public ResponseEntity<ReservationApiResponse> get(final String path) {
        log.debug("Calling reservation-service: GET {}{}", baseUrl, path);
        return restClient.get()
                .uri(baseUrl + path)
                .retrieve()
                .toEntity(ReservationApiResponse.class);
    }

    public ResponseEntity<ReservationApiResponse> post(final String path, final Object body) {
        log.debug("Calling reservation-service: POST {}{}", baseUrl, path);
        return restClient.post()
                .uri(baseUrl + path)
                .body(body)
                .retrieve()
                .toEntity(ReservationApiResponse.class);
    }

    public ResponseEntity<ReservationApiResponse> delete(final String path) {
        log.debug("Calling reservation-service: DELETE {}{}", baseUrl, path);
        return restClient.delete()
                .uri(baseUrl + path)
                .retrieve()
                .toEntity(ReservationApiResponse.class);
    }
}