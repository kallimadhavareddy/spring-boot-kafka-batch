package com.ing.kallitech.kafka.batch.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;

/**
 * Kafka message payload for batch-trigger-topic.
 *
 * FIX: Original KafkaMessageListener consumed raw String and had no structured
 *      message contract. This DTO enforces a typed, validated contract.
 *
 * Producers must send JSON matching this schema:
 * {
 *   "fileId":      "unique-file-id-001",
 *   "filePath":    "/data/csv/records.csv",
 *   "recordCount": 1000000,
 *   "delimiter":   ","
 * }
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class KafkaBatchMessage {

    @NotBlank
    private String fileId;        // Idempotency key — must be globally unique per file

    @NotBlank
    private String filePath;      // Absolute path on shared PVC/NFS

    @Positive
    private long   recordCount;   // Total data rows (excluding header) — used for partitioning

    private String delimiter = ",";

    private String sourceSystem;

    // Default constructor
    public KafkaBatchMessage() {}

    // All args constructor
    public KafkaBatchMessage(String fileId, String filePath, long recordCount, String delimiter, String sourceSystem) {
        this.fileId = fileId;
        this.filePath = filePath;
        this.recordCount = recordCount;
        this.delimiter = delimiter;
        this.sourceSystem = sourceSystem;
    }

    // Getters and Setters
    public String getFileId() {
        return fileId;
    }

    public void setFileId(String fileId) {
        this.fileId = fileId;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public long getRecordCount() {
        return recordCount;
    }

    public void setRecordCount(long recordCount) {
        this.recordCount = recordCount;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public String getSourceSystem() {
        return sourceSystem;
    }

    public void setSourceSystem(String sourceSystem) {
        this.sourceSystem = sourceSystem;
    }

    @Override
    public String toString() {
        return "KafkaBatchMessage{" +
                "fileId='" + fileId + '\'' +
                ", filePath='" + filePath + '\'' +
                ", recordCount=" + recordCount +
                ", delimiter='" + delimiter + '\'' +
                ", sourceSystem='" + sourceSystem + '\'' +
                '}';
    }
}
