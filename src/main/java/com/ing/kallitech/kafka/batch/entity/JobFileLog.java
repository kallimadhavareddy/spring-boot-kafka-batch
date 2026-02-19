package com.ing.kallitech.kafka.batch.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "job_file_log")
public class JobFileLog {
    
    @Id
    @Column(name = "file_id")
    private String fileId;
    
    @Column(name = "status", nullable = false)
    private String status = "PROCESSING";
    
    @Column(name = "job_execution_id")
    private Long jobExecutionId;
    
    @Column(name = "started_at")
    private LocalDateTime startedAt;
    
    @Column(name = "completed_at")
    private LocalDateTime completedAt;
    
    @Column(name = "record_count")
    private Long recordCount;
    
    @Column(name = "error_message", length = 2000)
    private String errorMessage;
    
    // Constructors
    public JobFileLog() {}
    
    public JobFileLog(String fileId) {
        this.fileId = fileId;
        this.startedAt = LocalDateTime.now();
    }
    
    // Getters and Setters
    public String getFileId() { return fileId; }
    public void setFileId(String fileId) { this.fileId = fileId; }
    
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    
    public Long getJobExecutionId() { return jobExecutionId; }
    public void setJobExecutionId(Long jobExecutionId) { this.jobExecutionId = jobExecutionId; }
    
    public LocalDateTime getStartedAt() { return startedAt; }
    public void setStartedAt(LocalDateTime startedAt) { this.startedAt = startedAt; }
    
    public LocalDateTime getCompletedAt() { return completedAt; }
    public void setCompletedAt(LocalDateTime completedAt) { this.completedAt = completedAt; }
    
    public Long getRecordCount() { return recordCount; }
    public void setRecordCount(Long recordCount) { this.recordCount = recordCount; }
    
    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
}
