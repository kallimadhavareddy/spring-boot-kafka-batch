package com.ing.kallitech.kafka.batch.entity;

import jakarta.persistence.*;

import java.math.BigDecimal;
import java.time.Instant;

@Entity
@Table(name="batch_records")
public class Record {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String     externalId;   // Business key from CSV
    private String     name;
    private BigDecimal value;
    private String     category;
    private Instant eventTs;

    // Enriched by processor
    private String  recordHash;      // SHA-256(externalId|category|eventTs) — idempotency key
    private String  jobId;
    private int     partitionIndex;
    private String status = "LOADED";

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Record(Long id, String externalId, String name, BigDecimal value, String category, Instant eventTs, String recordHash, String jobId, int partitionIndex, String status) {
        this.id = id;
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

    public Instant getEventTs() {
        return eventTs;
    }

    public void setEventTs(Instant eventTs) {
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

}
