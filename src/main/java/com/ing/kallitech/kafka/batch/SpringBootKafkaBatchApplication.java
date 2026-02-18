package com.ing.kallitech.kafka.batch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.kafka.annotation.EnableKafka;

// FIX: Added @EnableKafka and @ConfigurationPropertiesScan
@SpringBootApplication
@EnableKafka
@ConfigurationPropertiesScan
public class SpringBootKafkaBatchApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringBootKafkaBatchApplication.class, args);
    }
}
