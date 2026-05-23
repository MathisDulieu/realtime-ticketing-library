package com.mathisdulieu.ticketing.library.test.mongo.config;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.backend.memory.MemoryBackend;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.mongodb.core.MongoTemplate;

@Configuration
public class MongoTestConfig {

    @Bean(destroyMethod = "shutdown")
    public MongoServer mongoServer() {
        MongoServer mongoServer = new MongoServer(new MemoryBackend());
        mongoServer.bind();
        return mongoServer;
    }

    @Primary
    @Bean(destroyMethod = "close")
    public MongoClient mongoClient(MongoServer mongoServer) {
        return MongoClients.create("mongodb://" + mongoServer.getLocalAddress().getHostName() + ":" + mongoServer.getLocalAddress().getPort());
    }

    @Primary
    @Bean
    public MongoTemplate mongoTemplate(MongoClient mongoClient, MongoTestProperties mongoTestProperties) {
        return new MongoTemplate(mongoClient, mongoTestProperties.databaseName());
    }
}