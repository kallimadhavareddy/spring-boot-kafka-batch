package com.ing.kallitech.kafka.batch.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Splits the CSV file into N non-overlapping line ranges.
 *
 * This partitioner gets job parameters from the StepExecution that's passed
 * by Spring Batch's SimpleStepExecutionSplitter. The StepExecution is available
 * through a thread-local context when the partition() method is called.
 *
 * Each partition's ExecutionContext carries:
 *   filePath, startLine, maxItemCount, partitionIndex, delimiter
 *
 * Line 1 is always the header — partition 0 starts at line 2.
 */
@Component
public class CsvPartitioner implements Partitioner {

    private static final Logger log = LoggerFactory.getLogger(CsvPartitioner.class);

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        // Get the current StepExecution from the thread-local context
        // Spring Batch sets this when calling the partitioner
        StepExecution stepExecution = getCurrentStepExecution();
        
        if (stepExecution == null) {
            throw new IllegalStateException("StepExecution not available in thread context. " +
                "This partitioner must be called from within a Spring Batch step execution.");
        }

        var params = stepExecution.getJobParameters();
        String filePath = params.getString("filePath");
        Long totalObj = params.getLong("totalRecords");
        String delimiter = params.getString("delimiter", ",");

        if (filePath == null || totalObj == null) {
            throw new IllegalStateException("Required job parameters missing: filePath=" + filePath + ", totalRecords=" + totalObj);
        }

        long total = totalObj;

        log.info("Partitioning: filePath={} totalRecords={} gridSize={}", filePath, total, gridSize);

        long partitionSize = Math.max(1, total / gridSize);
        Map<String, ExecutionContext> result = new LinkedHashMap<>();

        for (int i = 0; i < gridSize; i++) {
            long startLine = (long) i * partitionSize + 2;   // +2: 1-based, skip header
            long endLine = (i == gridSize - 1) ? total + 1 : startLine + partitionSize - 1;

            if (startLine > total + 1) break;  // Skip empty trailing partitions

            var ctx = new ExecutionContext();
            ctx.putString("filePath", filePath);
            ctx.putLong("startLine", startLine);
            ctx.putLong("maxItemCount", endLine - startLine + 1);
            ctx.putInt("partitionIndex", i);
            ctx.putString("delimiter", delimiter);

            result.put("partition-" + i, ctx);
            log.debug("  partition-{}: lines {}-{}", i, startLine, endLine);
        }

        log.info("Created {} partitions", result.size());
        return result;
    }

    /**
     * Get the current StepExecution from Spring Batch's thread-local context.
     * This is the standard way partitioners can access StepExecution.
     */
    private StepExecution getCurrentStepExecution() {
        try {
            // Spring Batch stores StepExecution in a thread-local during step execution
            return org.springframework.batch.core.scope.context.StepContext.getStepExecution();
        } catch (Exception e) {
            log.error("Failed to get StepExecution from thread context", e);
            return null;
        }
    }
}
