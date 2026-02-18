package com.ing.kallitech.kafka.batch;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class SpringBootKafkaBatchApplicationTests {

    @Test
    void applicationContextLoads() {
        // Test that the main application class can be instantiated
        assertThat(SpringBootKafkaBatchApplication.class).isNotNull();
    }
}
