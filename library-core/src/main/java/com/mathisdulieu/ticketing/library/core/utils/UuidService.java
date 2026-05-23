package com.mathisdulieu.ticketing.library.core.utils;

import java.util.UUID;

public class UuidService {

    public String generateUuid() {
        return UUID.randomUUID().toString();
    }

}