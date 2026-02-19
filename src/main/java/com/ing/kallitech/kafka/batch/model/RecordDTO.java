package com.ing.kallitech.kafka.batch.model;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;

/**
 * FIX: Added Lombok, externalId (business key), category, eventTs, recordHash,
 *      jobId, partitionIndex — all needed for idempotency and DB insert.
 *      Original had only id/name/value with manual getters/setters.
 */
public class RecordDTO {

    // CSV source fields
    private String     externalId;   // Business key from CSV
    private String     name;
    private BigDecimal value;
    private String     category;
    private Timestamp eventTs;

    // Enriched by processor
    private String  recordHash;      // SHA-256(externalId|category|eventTs) — idempotency key
    private String  jobId;
    private int     partitionIndex;

    private String status = "LOADED";

    // Default constructor
    public RecordDTO() {}

    // All args constructor
    public RecordDTO(String externalId, String name, BigDecimal value, String category, Timestamp eventTs, String recordHash, String jobId, int partitionIndex, String status) {
        this.externalId = externalId;
        this.name = name;
        this.value = value;
        this.category = category;
        this.eventTs = eventTs;
        this.recordHash = recordHash;
        this.jobId = jobId;
        this.partitionIndex = partitionIndex;
        this.status = status;
    }

    // Getters and Setters
    public String getExternalId() {
        return externalId;
    }

    public void setExternalId(String externalId) {
        this.externalId = externalId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public BigDecimal getValue() {
        return value;
    }

    public void setValue(BigDecimal value) {
        this.value = value;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public Timestamp getEventTs() {
        return eventTs;
    }

    public void setEventTs(Timestamp eventTs) {
        this.eventTs = eventTs;
    }

    public String getRecordHash() {
        return recordHash;
    }

    public void setRecordHash(String recordHash) {
        this.recordHash = recordHash;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public int getPartitionIndex() {
        return partitionIndex;
    }

    public void setPartitionIndex(int partitionIndex) {
        this.partitionIndex = partitionIndex;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "RecordDTO{" +
                "externalId='" + externalId + '\'' +
                ", name='" + name + '\'' +
                ", value=" + value +
                ", category='" + category + '\'' +
                ", eventTs=" + eventTs +
                ", recordHash='" + recordHash + '\'' +
                ", jobId='" + jobId + '\'' +
                ", partitionIndex=" + partitionIndex +
                ", status='" + status + '\'' +
                '}';
    }
}
