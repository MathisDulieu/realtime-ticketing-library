package com.mathisdulieu.ticketing.library.api.inventory;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Builder;

import java.util.List;

@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public record InventoryApiResponse(
        String code,
        List<String> errors
) {}