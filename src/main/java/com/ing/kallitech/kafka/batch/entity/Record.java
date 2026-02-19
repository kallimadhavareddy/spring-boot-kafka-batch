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

    @Column(name = "external_id")
    private String externalId;   // Business key from CSV
    
    @Column(name = "name", nullable = false)
    private String name;
    
    @Column(name = "\"value\"")  // Quote to handle SQL reserved keyword
    private BigDecimal valueRec;
    
    @Column(name = "category")
    private String category;
    
    @Column(name = "event_ts")
    private Instant eventTs;

    // Enriched by processor
    @Column(name = "record_hash")
    private String recordHash;      // SHA-256(externalId|category|eventTs) — idempotency key
    
    @Column(name = "job_id")
    private String jobId;
    
    @Column(name = "partition_idx")
    private int partitionIndex;
    
    @Column(name = "status")
    private String status = "LOADED";

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Record(Long id, String externalId, String name, BigDecimal valueRec, String category, Instant eventTs, String recordHash, String jobId, int partitionIndex, String status) {
        this.id = id;
        this.externalId = externalId;
        this.name = name;
        this.valueRec = valueRec;
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

    public BigDecimal getValueRec() {
        return valueRec;
    }

    public void setValueRec(BigDecimal valueRec) {
        this.valueRec = valueRec;
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
