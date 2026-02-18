package com.ing.kallitech.kafka.batch.utils;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class BatchUtils {

    private final JobOperator jobOperator;
    private final JobExplorer jobExplorer;

    public BatchUtils(JobOperator jobOperator, JobExplorer jobExplorer) {
        this.jobOperator = jobOperator;
        this.jobExplorer = jobExplorer;
    }

    /**
     * Returns the latest JobExecution for the given job name, or null if none exist.
     */
    public JobExecution getLastJobExecution(String jobName) {

        // 1️⃣ Get the latest job instance IDs (0 = start, 1 = count)
        try {
            List<Long> jobInstanceIds = jobOperator.getJobInstances(jobName, 0, 1);
            if (jobInstanceIds.isEmpty()) return null;
            long latestInstanceId = jobInstanceIds.get(0);
            // 2️⃣ Get the JobInstance
            JobInstance jobInstance = jobExplorer.getJobInstance(latestInstanceId);
            if (jobInstance == null) return null;

            // 3️⃣ Get the last JobExecution for this instance
            return jobExplorer.getLastJobExecution(jobInstance);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    }


