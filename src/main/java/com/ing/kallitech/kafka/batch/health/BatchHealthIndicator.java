package com.ing.kallitech.kafka.batch.health;

import com.ing.kallitech.kafka.batch.utils.BatchUtils;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.stereotype.Component;

@Component("batch")
public class BatchHealthIndicator extends AbstractHealthIndicator {

    private final BatchUtils batchUtils;

    public BatchHealthIndicator(BatchUtils batchUtils) {
        this.batchUtils = batchUtils;
    }

    @Override
    protected void doHealthCheck(Health.Builder builder) {
        JobExecution lastExecution = batchUtils.getLastJobExecution("csvImportJob");

        if (lastExecution == null) {
            builder.up().withDetail("message", "No jobs run yet");
            return;
        }

        BatchStatus status = lastExecution.getStatus();
        builder.status(status == BatchStatus.FAILED ? "DOWN" : "UP")
                .withDetail("lastJobId", lastExecution.getId())
                .withDetail("lastJobStatus", status);
    }
}
