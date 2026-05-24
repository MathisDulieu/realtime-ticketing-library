package com.mathisdulieu.ticketing.library.api.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Builder;

import java.util.List;

@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public record EventApiResponse(
        String code,
        List<String> errors
) {}
