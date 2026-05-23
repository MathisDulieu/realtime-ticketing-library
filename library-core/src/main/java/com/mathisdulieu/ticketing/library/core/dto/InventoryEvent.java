package com.mathisdulieu.ticketing.library.core.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Builder;

@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public record InventoryEvent(
        String eventId
) {}