package com.ing.kallitech.kafka.batch.controller;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Optional;

@RestController
public class JobStatusController {

    @Autowired
    private Optional<Job> csvImportJob;

    @GetMapping("/job-status")
    public ResponseEntity<String> checkJobStatus() {
        if (csvImportJob.isPresent()) {
            return ResponseEntity.ok("Job available: " + csvImportJob.get().getName());
        } else {
            return ResponseEntity.status(500).body("No job available");
        }
    }
}
