package com.ing.kallitech.kafka.batch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;

// FIX: Added @EnableKafka and @ConfigurationPropertiesScan
@SpringBootApplication
@ConfigurationPropertiesScan
public class SpringBootKafkaBatchApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringBootKafkaBatchApplication.class, args);
    }
}
