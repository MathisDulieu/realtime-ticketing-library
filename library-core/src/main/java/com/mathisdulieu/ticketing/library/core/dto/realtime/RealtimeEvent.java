package com.mathisdulieu.ticketing.library.core.dto.realtime;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Builder;

@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public record RealtimeEvent(
    String eventId
) {
}
