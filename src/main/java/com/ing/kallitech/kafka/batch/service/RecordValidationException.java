package com.ing.kallitech.kafka.batch.service;

import com.ing.kallitech.kafka.batch.model.RecordDTO;

/**
 * NEW: Thrown by CsvItemProcessor for records that fail validation.
 * Configured as skippable in BatchConfig (up to skipLimit per partition).
 * ChunkMetricsListener logs every skipped record for audit.
 */
public class RecordValidationException extends Exception {

    private final RecordDTO record;

    public RecordValidationException(String message, RecordDTO record) {
        super(message);
        this.record = record;
    }

    public RecordDTO getRecord() {
        return record;
    }
}
