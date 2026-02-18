package com.ing.kallitech.kafka.batch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;

// FIX: Disable Kafka auto-configuration to allow startup without Kafka broker
@SpringBootApplication(exclude = KafkaAutoConfiguration.class)
@ConfigurationPropertiesScan
public class SpringBootKafkaBatchApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringBootKafkaBatchApplication.class, args);
    }
}
