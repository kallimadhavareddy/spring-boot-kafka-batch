package com.ing.kallitech.kafka.batch.idempotency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * NEW: File-level idempotency tracking via job_file_log table.
 *
 * Prevents duplicate processing when:
 *  - Kafka redelivers a message after pod restart
 *  - Consumer group rebalances during long-running batch
 *  - Manual replay from DLQ
 *
 * State machine: (absent) → PROCESSING → COMPLETED
 *                                      ↘ FAILED (eligible for retry)
 */
@Service
public class IdempotencyService {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyService.class);

    private final JdbcTemplate jdbcTemplate;

    public IdempotencyService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public boolean isAlreadyProcessed(String fileId) {
        Integer count = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM job_file_log WHERE file_id = ? AND status = 'COMPLETED'",
            Integer.class, fileId);
        return count != null && count > 0;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markProcessingStarted(String fileId) {
        try {
            jdbcTemplate.update(
                "INSERT INTO job_file_log (file_id, status, started_at) " +
                "VALUES (?, 'PROCESSING', CURRENT_TIMESTAMP)", fileId);
        } catch (DuplicateKeyException e) {
            // Previous FAILED run — reset to PROCESSING
            jdbcTemplate.update(
                "UPDATE job_file_log SET status='PROCESSING', started_at=CURRENT_TIMESTAMP, " +
                "completed_at=NULL, error_message=NULL WHERE file_id=?", fileId);
        }
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markCompleted(String fileId, long recordCount) {
        jdbcTemplate.update(
            "UPDATE job_file_log SET status='COMPLETED', completed_at=CURRENT_TIMESTAMP, " +
            "record_count=? WHERE file_id=?", recordCount, fileId);
        log.info("Marked COMPLETED: fileId={} records={}", fileId, recordCount);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markFailed(String fileId, String error) {
        jdbcTemplate.update(
            "UPDATE job_file_log SET status='FAILED', completed_at=CURRENT_TIMESTAMP, " +
            "error_message=? WHERE file_id=?", error, fileId);
        log.warn("Marked FAILED: fileId={}", fileId);
    }
}
