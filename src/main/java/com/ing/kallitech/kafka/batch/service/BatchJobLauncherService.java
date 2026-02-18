package com.ing.kallitech.kafka.batch.service;

/**
 * FIX: This class is no longer needed as a separate service.
 *
 * The original BatchJobLauncherService had two problems:
 *  1. It defined buildJobParameters(filePath) but KafkaMessageListener called
 *     parseMessage(String message) — a method that never existed.
 *  2. launchJob() silently swallowed exceptions with e.printStackTrace()
 *     — failures were invisible to Kafka's retry/DLQ mechanism.
 *
 * The job launch logic now lives directly in KafkaMessageListener where
 * it has access to the Acknowledgment and can properly control when to
 * ACK, retry, or route to DLQ.
 *
 * This file is kept as a stub to preserve the original class path.
 * It can be safely deleted.
 */
public class BatchJobLauncherService {
    // Intentionally empty — see KafkaMessageListener for launch logic
}
