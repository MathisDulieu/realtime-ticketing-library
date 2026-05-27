package com.mathisdulieu.ticketing.library.core.dto.notification;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Builder;

@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public record NotificationEvent(
    String eventId
) {
}
