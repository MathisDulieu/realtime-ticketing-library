package com.mathisdulieu.ticketing.library.test.mongo.config;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;

import static org.springframework.util.Assert.hasText;

@ConfigurationProperties(prefix = "library.test.mongo")
public record MongoTestProperties(
        String databaseName
) implements InitializingBean {
    @Override
    public void afterPropertiesSet() {
        hasText(databaseName, "library.test.mongo.databaseName must be given");
    }
}