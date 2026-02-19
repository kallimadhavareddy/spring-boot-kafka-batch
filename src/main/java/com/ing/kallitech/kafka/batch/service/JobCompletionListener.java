package com.ing.kallitech.kafka.batch.service;

import com.ing.kallitech.kafka.batch.idempotency.IdempotencyService;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Optional;

/**
 * NEW: Job lifecycle listener.
 * - Updates idempotency table (PROCESSING → COMPLETED/FAILED)
 * - Releases KafkaMessageListener's concurrency semaphore
 * - Records job-level Micrometer metrics
 */
@Component
public class JobCompletionListener implements JobExecutionListener {

    private static final Logger log = LoggerFactory.getLogger(JobCompletionListener.class);

    private final IdempotencyService idempotencyService;
    private final MeterRegistry meterRegistry;
    private final ObjectProvider<KafkaMessageListener> kafkaMessageListener;

    public JobCompletionListener(IdempotencyService idempotencyService, MeterRegistry meterRegistry, ObjectProvider<KafkaMessageListener> kafkaMessageListener) {
        this.idempotencyService = idempotencyService;
        this.meterRegistry = meterRegistry;
        this.kafkaMessageListener = kafkaMessageListener;
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        String fileId = jobExecution.getJobParameters().getString("fileId");
        BatchStatus status = jobExecution.getStatus();

        long written = jobExecution.getStepExecutions().stream()
            .mapToLong(s -> s.getWriteCount()).sum();
        long skipped = jobExecution.getStepExecutions().stream()
            .mapToLong(s -> s.getSkipCount()).sum();

        Duration duration = (jobExecution.getStartTime() != null && jobExecution.getEndTime() != null)
            ? Duration.between(jobExecution.getStartTime(), jobExecution.getEndTime())
            : Duration.ZERO;

        if (status == BatchStatus.COMPLETED) {
            idempotencyService.markCompleted(fileId, written);
            meterRegistry.counter("batch.job.completed").increment();
            log.info("Job COMPLETED: fileId={} durationMs={} written={} skipped={}", fileId, duration.toMillis(), written, skipped);
        } else {
            String desc = jobExecution.getExitStatus().getExitDescription();
            idempotencyService.markFailed(fileId, desc != null && desc.length() > 2000 ? desc.substring(0, 2000) : desc);
            meterRegistry.counter("batch.job.failed").increment();
            log.error("Job FAILED: fileId={} durationMs={} written={} skipped={} desc={}", fileId, duration.toMillis(), written, skipped, desc);
        }

        // Always release the concurrency slot
        kafkaMessageListener.ifAvailable(KafkaMessageListener::releaseConcurrencySlot);
    }
}
