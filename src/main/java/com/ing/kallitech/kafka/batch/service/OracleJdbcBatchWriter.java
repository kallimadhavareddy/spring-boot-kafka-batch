package com.ing.kallitech.kafka.batch.service;

import com.ing.kallitech.kafka.batch.model.RecordDTO;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * FIXES applied vs original OracleJdbcBatchWriter:
 *
 * 1. WRONG IMPORT: Was org.springframework.batch.infrastructure.item.ItemWriter
 *    (does not exist). Correct: org.springframework.batch.item.ItemWriter
 *
 * 2. WRONG METHOD SIGNATURE: write(List<? extends T>) was removed in Spring Batch 5.
 *    New signature: write(Chunk<? extends T> chunk)
 *
 * 3. WRONG HINT: INSERT /*+ APPEND *\/ is for INSERT...SELECT only.
 *    For row-by-row: use /*+ APPEND_VALUES *\/
 *
 * 4. MISSING IDEMPOTENCY FALLBACK: On retry, some records already exist in DB.
 *    A plain INSERT would throw ORA-00001 and fail the whole chunk.
 *    Fixed: catch DuplicateKeyException → fall back to individual upserts.
 *
 * 5. MISSING FIELDS: Original inserted only id/name/value. Fixed to insert all
 *    enriched fields: record_hash, job_id, partition_idx, event_ts, status.
 *
 * 6. MISSING METRICS: No timing or counters. Fixed with Micrometer Timer.
 */
@Component
public class OracleJdbcBatchWriter implements ItemWriter<RecordDTO> {

    private static final Logger log = LoggerFactory.getLogger(OracleJdbcBatchWriter.class);

    private final JdbcTemplate jdbcTemplate;
    private final MeterRegistry meterRegistry;

    public OracleJdbcBatchWriter(JdbcTemplate jdbcTemplate, MeterRegistry meterRegistry) {
        this.jdbcTemplate = jdbcTemplate;
        this.meterRegistry = meterRegistry;
    }

    // FIX: APPEND_VALUES (not APPEND) for single-row inserts
    // H2 silently ignores the Oracle hint — works in dev without changes
    private static final String INSERT_SQL = """
        INSERT /*+ APPEND_VALUES */ INTO batch_records
            (external_id, name, value, category, event_ts,
             record_hash, job_id, partition_idx, created_at, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
        """;

    @Override
    @Transactional
    public void write(Chunk<? extends RecordDTO> chunk) {
        List<? extends RecordDTO> items = chunk.getItems();
        if (items.isEmpty()) return;

        log.info("OracleJdbcBatchWriter.write() called with {} items", items.size());
        
        Timer.Sample sample = Timer.start(meterRegistry);

        try {
            executeBatch(items);
            meterRegistry.counter("batch.records.written").increment(items.size());
            log.debug("Wrote {} records", items.size());

        } catch (DuplicateKeyException e) {
            // Partial retry: some records already inserted — fall back row-by-row
            log.warn("Duplicate key in chunk of {} — switching to upsert fallback", items.size());
            executeUpsertFallback(items);
            meterRegistry.counter("batch.records.upsert_fallback").increment(items.size());

        } finally {
            sample.stop(Timer.builder("batch.write.duration")
                .description("Oracle JDBC batch write time per chunk")
                .register(meterRegistry));
        }
    }

    private void executeBatch(List<? extends RecordDTO> items) {
        jdbcTemplate.batchUpdate(INSERT_SQL, items, items.size(), (ps, r) -> {
            ps.setString(1, r.getExternalId());
            ps.setString(2, r.getName());
            ps.setBigDecimal(3, r.getValue());
            ps.setString(4, r.getCategory());
            ps.setTimestamp(5, r.getEventTs() != null ? Timestamp.from(r.getEventTs()) : null);
            ps.setString(6, r.getRecordHash());
            ps.setString(7, r.getJobId());
            ps.setInt(8, r.getPartitionIndex());
            ps.setString(9, r.getStatus());
        });
    }

    private void executeUpsertFallback(List<? extends RecordDTO> items) {
        List<RecordDTO> failed = new ArrayList<>();
        for (RecordDTO r : items) {
            try {
                jdbcTemplate.update(INSERT_SQL,
                    r.getExternalId(), r.getName(), r.getValue(), r.getCategory(),
                    r.getEventTs() != null ? Timestamp.from(r.getEventTs()) : null,
                    r.getRecordHash(), r.getJobId(), r.getPartitionIndex(), r.getStatus());
            } catch (DuplicateKeyException dup) {
                log.debug("Skipping duplicate: record_hash={}", r.getRecordHash());
                meterRegistry.counter("batch.records.skipped.duplicate").increment();
            } catch (Exception ex) {
                log.error("Failed to upsert record: hash={}", r.getRecordHash(), ex);
                failed.add(r);
            }
        }
        if (!failed.isEmpty()) {
            throw new RuntimeException("Upsert failed for " + failed.size() + " records");
        }
    }
}
