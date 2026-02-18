package com.ing.kallitech.kafka.batch.service;

import com.ing.kallitech.kafka.batch.idempotency.IdempotencyService;
import com.ing.kallitech.kafka.batch.model.KafkaBatchMessage;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.concurrent.Semaphore;
import java.util.Optional;

/**
 * FIXES applied vs original KafkaMessageListener:
 *
 * 1. NO ACKNOWLEDGMENT: Original had no Acknowledgment parameter — Kafka auto-committed
 *    offsets, meaning a pod crash after consume but before job launch = LOST MESSAGE.
 *    Fixed: manual ack only after successful job launch.
 */
@Component  // Temporarily disabled due to firewall blocking Kafka connection
public class KafkaMessageListener {

    private static final Logger log = LoggerFactory.getLogger(KafkaMessageListener.class);

    private final JobLauncher jobLauncher;
    private final Optional<Job> csvImportJob;
    private final IdempotencyService idempotencyService;
    private final io.micrometer.core.instrument.MeterRegistry meterRegistry;
    private final Semaphore concurrencyGate;

    public KafkaMessageListener(JobLauncher jobLauncher,
                                Optional<Job> csvImportJob,
                                IdempotencyService idempotencyService,
                                io.micrometer.core.instrument.MeterRegistry meterRegistry,
                                org.springframework.core.env.Environment env) {
        this.jobLauncher = jobLauncher;
        this.csvImportJob = csvImportJob;
        this.idempotencyService = idempotencyService;
        this.meterRegistry = meterRegistry;
        int maxJobs = Integer.parseInt(env.getProperty("batch.job.max-concurrent-jobs", "2"));
        this.concurrencyGate = new Semaphore(maxJobs);
    }

    @KafkaListener(
        topics = "${kafka.topic}",
        groupId = "${kafka.group-id}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void handleFileMessage(@Payload KafkaBatchMessage message,
            Acknowledgment acknowledgment,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        MDC.put("fileId", message.getFileId());

        try {
            log.info("Received trigger: fileId={} filePath={} records={} partition={} offset={}",
                message.getFileId(), message.getFilePath(),
                message.getRecordCount(), partition, offset);

            // ── Idempotency check ─────────────────────────────────────────────
            if (idempotencyService.isAlreadyProcessed(message.getFileId())) {
                log.warn("Duplicate skipped (already COMPLETED): fileId={}", message.getFileId());
                Counter.builder("batch.trigger.duplicate").register(meterRegistry).increment();
                acknowledgment.acknowledge();
                return;
            }

            // ── Backpressure ──────────────────────────────────────────────────
            if (!concurrencyGate.tryAcquire()) {
                log.warn("Pod at max concurrency — not acking (Kafka will redeliver): fileId={}",
                    message.getFileId());
                Counter.builder("batch.trigger.backpressure").register(meterRegistry).increment();
                // Do NOT ack — Kafka will redeliver to this or another pod
                throw new RuntimeException("Pod at max concurrency for fileId: " + message.getFileId());
            }

            try {
                idempotencyService.markProcessingStarted(message.getFileId());

                JobParameters params = new JobParametersBuilder()
                    .addString("fileId",       message.getFileId())
                    .addString("filePath",     message.getFilePath())
                    .addLong("totalRecords",   message.getRecordCount())
                    .addString("delimiter",    message.getDelimiter(), false)
                    .addLong("launchTs",       System.currentTimeMillis())
                    .toJobParameters();

                var execution = csvImportJob.map(job -> {
                    try {
                        return jobLauncher.run(job, params);
                    } catch (Exception e) {
                        log.error("Failed to launch job for file: " + message.getFileId(), e);
                        throw new RuntimeException(e);
                    }
                }).orElseThrow(() -> new RuntimeException("No job available"));
                log.info("Job launched: jobId={} status={}", execution.getId(), execution.getStatus());

                Counter.builder("batch.trigger.launched").register(meterRegistry).increment();
                acknowledgment.acknowledge();   // ACK only after successful launch

            } catch (Exception e) {
                log.error("Job launch failed for fileId={}: {}", message.getFileId(), e.getMessage(), e);
                idempotencyService.markFailed(message.getFileId(), e.getMessage());
                concurrencyGate.release();      // Release slot on failure
                Counter.builder("batch.trigger.launch_failed").register(meterRegistry).increment();
                throw new RuntimeException("Job launch failed", e); // → Kafka retry → DLQ
            }
            // Note: concurrencyGate released by JobCompletionListener.afterJob()

        } finally {
            MDC.clear();
        }
    }

    public void releaseConcurrencySlot() {
        concurrencyGate.release();
    }

    public int availableConcurrencySlots() {
        return concurrencyGate.availablePermits();
    }
}
