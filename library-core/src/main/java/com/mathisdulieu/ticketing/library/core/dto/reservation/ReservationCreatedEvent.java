package com.mathisdulieu.ticketing.library.core.dto.reservation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Builder;

@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public record ReservationCreatedEvent(
        String eventId
) {}
