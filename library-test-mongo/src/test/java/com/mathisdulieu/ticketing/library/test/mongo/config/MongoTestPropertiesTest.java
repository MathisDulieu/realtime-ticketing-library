package com.mathisdulieu.ticketing.library.test.mongo.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MongoTestPropertiesTest {

    @Test
    void shouldNotThrow_whenPropertiesAreComplete() {
        // Arrange
        MongoTestProperties mongoTestProperties = new MongoTestProperties("databaseName");

        // Act
        mongoTestProperties.afterPropertiesSet();

        // Assert
        assertThat(mongoTestProperties.databaseName()).isEqualTo("databaseName");
    }

    @ParameterizedTest
    @NullAndEmptySource
    void shouldThrow_whenDatabaseNameIsBlankOrNull(String databaseName) {
        // Arrange
        MongoTestProperties mongoTestProperties = new MongoTestProperties(databaseName);

        // Act & Assert
        assertThatThrownBy(mongoTestProperties::afterPropertiesSet)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("library.test.mongo.databaseName must be given");
    }
  
}