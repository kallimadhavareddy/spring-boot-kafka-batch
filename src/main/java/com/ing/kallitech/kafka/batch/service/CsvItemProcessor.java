package com.ing.kallitech.kafka.batch.service;

import com.ing.kallitech.kafka.batch.model.RecordDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.math.RoundingMode;

/**
 * FIXES applied vs original CsvItemProcessor:
 *
 * 1. WRONG IMPORT: Was org.springframework.batch.infrastructure.item.ItemProcessor
 *    (does not exist). Correct: org.springframework.batch.item.ItemProcessor
 *
 * 2. SILENT NULL RETURN: Returning null silently drops the record — no visibility.
 *    Fixed: throw RecordValidationException (counted against skipLimit, logged).
 *
 * 3. NO ENRICHMENT: Missing recordHash (idempotency key), jobId, partitionIndex.
 *    Fixed: SHA-256 hash of business key computed here; job metadata injected.
 *
 * 4. NO VALUE VALIDATION: BigDecimal scale not checked — could cause ORA-01438
 *    (value larger than specified precision) on Oracle insert.
 */
@Component
public class CsvItemProcessor implements ItemProcessor<RecordDTO, RecordDTO> {

    private static final Logger log = LoggerFactory.getLogger(CsvItemProcessor.class);

    private String jobId;
    private int    partitionIndex;

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        this.jobId          = String.valueOf(stepExecution.getJobExecution().getId());
        this.partitionIndex = stepExecution.getExecutionContext().getInt("partitionIndex", 0);
    }

    @Override
    public RecordDTO process(RecordDTO item) throws RecordValidationException {

        // ── Validation ───────────────────────────────────────────────────────
        if (!StringUtils.hasText(item.getName())) {
            throw new RecordValidationException("name is blank", item);
        }
        if (item.getValue() == null) {
            throw new RecordValidationException("value is null", item);
        }
        if (item.getValue().scale() > 4) {
            item.setValue(item.getValue().setScale(4, RoundingMode.HALF_UP));
        }
        if (!StringUtils.hasText(item.getCategory())) {
            item.setCategory("UNKNOWN");
        }

        // ── Enrichment ───────────────────────────────────────────────────────
        item.setRecordHash(computeHash(item));
        item.setJobId(jobId);
        item.setPartitionIndex(partitionIndex);

        return item;
    }

    /**
     * Deterministic SHA-256 of business key fields.
     * Same input always produces same hash → DB UNIQUE constraint = idempotency.
     */
    private String computeHash(RecordDTO rec) {
        String key = String.join("|",
            nullSafe(rec.getExternalId()),
            nullSafe(rec.getCategory()),
            rec.getEventTs() != null ? rec.getEventTs().toString() : ""
        );
        return DigestUtils.sha256Hex(key);
    }

    private String nullSafe(String s) { return s != null ? s : ""; }
}
