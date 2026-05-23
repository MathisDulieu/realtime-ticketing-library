package com.mathisdulieu.ticketing.library.api.reservation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Builder;

import java.util.List;

@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public record ReservationApiResponse(
        String code,
        List<String> errors
) {}