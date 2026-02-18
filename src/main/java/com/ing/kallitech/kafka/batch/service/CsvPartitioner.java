package com.ing.kallitech.kafka.batch.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * NEW: Splits the CSV file into N non-overlapping line ranges.
 *
 * Each partition's ExecutionContext carries:
 *   filePath, startLine, maxItemCount, partitionIndex, delimiter
 *
 * Line 1 is always the header — partition 0 starts at line 2.
 */
@Component
public class CsvPartitioner implements Partitioner {

    private static final Logger log = LoggerFactory.getLogger(CsvPartitioner.class);

    private StepExecution stepExecution;

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        this.stepExecution = stepExecution;
    }

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        var params       = stepExecution.getJobParameters();
        String filePath  = params.getString("filePath");
        long total       = params.getLong("totalRecords");
        String delimiter = params.getString("delimiter", ",");

        log.info("Partitioning: filePath={} totalRecords={} gridSize={}", filePath, total, gridSize);

        long partitionSize = Math.max(1, total / gridSize);
        Map<String, ExecutionContext> result = new LinkedHashMap<>();

        for (int i = 0; i < gridSize; i++) {
            long startLine = (long) i * partitionSize + 2;   // +2: 1-based, skip header
            long endLine   = (i == gridSize - 1) ? total + 1 : startLine + partitionSize - 1;

            if (startLine > total + 1) break;  // Skip empty trailing partitions

            var ctx = new ExecutionContext();
            ctx.putString("filePath",       filePath);
            ctx.putLong("startLine",         startLine);
            ctx.putLong("maxItemCount",      endLine - startLine + 1);
            ctx.putInt("partitionIndex",     i);
            ctx.putString("delimiter",       delimiter);

            result.put("partition-" + i, ctx);
            log.debug("  partition-{}: lines {}-{}", i, startLine, endLine);
        }

        log.info("Created {} partitions", result.size());
        return result;
    }
}
