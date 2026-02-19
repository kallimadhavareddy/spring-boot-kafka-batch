package com.ing.kallitech.kafka.batch.controller;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DebugController {

    @Autowired
    private JobRepository jobRepository;

    @GetMapping("/debug-beans")
    public ResponseEntity<String> debugBeans() {
        StringBuilder sb = new StringBuilder();
        
        try {
            sb.append("JobRepository: ").append(jobRepository != null ? "OK" : "NULL").append("\n");
            sb.append("JobRepository type: ").append(jobRepository.getClass().getName()).append("\n");
        } catch (Exception e) {
            sb.append("JobRepository error: ").append(e.getMessage()).append("\n");
        }
        
        return ResponseEntity.ok(sb.toString());
    }
}
