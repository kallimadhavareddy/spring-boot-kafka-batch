package com.ing.kallitech.kafka.batch.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class BatchDebugController {

    @Autowired
    private ApplicationContext applicationContext;

    @GetMapping("/batch-debug")
    public ResponseEntity<String> debugBatch() {
        StringBuilder sb = new StringBuilder();
        
        // Check for Spring Batch specific beans
        String[] jobRepository = applicationContext.getBeanNamesForType(org.springframework.batch.core.repository.JobRepository.class);
        sb.append("JobRepository beans: ").append(jobRepository.length).append("\n");
        
        String[] jobLauncher = applicationContext.getBeanNamesForType(org.springframework.batch.core.launch.JobLauncher.class);
        sb.append("JobLauncher beans: ").append(jobLauncher.length).append("\n");
        
        String[] transactionManager = applicationContext.getBeanNamesForType(org.springframework.transaction.PlatformTransactionManager.class);
        sb.append("PlatformTransactionManager beans: ").append(transactionManager.length).append("\n");
        
        // Check if BatchConfig is loaded
        boolean batchConfigExists = applicationContext.containsBean("batchConfig");
        sb.append("BatchConfig loaded: ").append(batchConfigExists).append("\n");
        
        return ResponseEntity.ok(sb.toString());
    }
}
