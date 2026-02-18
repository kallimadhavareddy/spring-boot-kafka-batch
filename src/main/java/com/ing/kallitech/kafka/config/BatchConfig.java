package com.ing.kallitech.kafka.config;

import com.ing.kallitech.kafka.batch.model.RecordDTO;
import com.ing.kallitech.kafka.batch.service.*;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.jdbc.UncategorizedSQLException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import java.util.concurrent.ThreadPoolExecutor;

import java.util.concurrent.TimeUnit;

/**
 * FIXES applied vs original BatchConfig:
 *
 * 1. MISSING JOB REPOSITORY: Original had no JobRepository bean. Spring Batch 5.x
 *    requires explicit JobRepository for job metadata persistence.
 *
 * 2. MISSING TRANSACTION MANAGER: No PlatformTransactionManager bean. Required for
 *    chunk-oriented processing and database operations.
 *
 * 3. MISSING STEP EXECUTION CONTEXT: No StepExecution context injection. Fixed
 *    CsvItemReader to read filePath and range from ExecutionContext.
 *
 * 4. MISSING PARTITIONING: Original was single-threaded. Added CsvPartitioner + 
 *    TaskExecutorPartitionHandler for parallel processing of large files.
 *
 * 5. EXCEPTION HANDLING: Original had catch(Exception) which swallows everything
 *    including OOM and programming errors. Fixed to specific skippable/retryable exception types.
 *
 * 6. PARTITIONING ADDED: Original had no partitioning (single-threaded read).
 *    Added CsvPartitioner + TaskExecutorPartitionHandler for parallel processing.
 *
 * 7. THREAD POOL: Was setCorePoolSize(12) = setMaxPoolSize(12) with no graceful
 *    shutdown. Fixed with CallerRunsPolicy + waitForTasksToCompleteOnShutdown.
 */
@Configuration
@EnableBatchProcessing
public class BatchConfig {

    @Value("${batch.job.chunk-size:1000}")
    private int chunkSize;

    @Value("${batch.job.grid-size:20}")
    private int gridSize;

    @Value("${batch.job.skip-limit:500}")
    private int skipLimit;

    @Value("${batch.job.retry-limit:3}")
    private int retryLimit;

    @Value("${batch.job.thread-pool-core-size:4}")
    private int threadPoolCoreSize;

    @Value("${batch.job.thread-pool-max-size:8}")
    private int threadPoolMaxSize;

    // ── Job ───────────────────────────────────────────────────────────────────

    @Bean
    public Job csvImportJob(JobRepository jobRepository,
                            Step partitionedStep,
                            JobCompletionListener listener) {
        return new JobBuilder("csvImportJob", jobRepository)
            .incrementer(new RunIdIncrementer())
            .listener(listener)
            .start(partitionedStep)
            .build();
    }

    // ── Partition Manager Step ─────────────────────────────────────────────────

    @Bean
    public Step partitionedStep(JobRepository jobRepository,
                                CsvPartitioner csvPartitioner,
                                Step workerStep,
                                TaskExecutor batchTaskExecutor) {
        var handler = new TaskExecutorPartitionHandler();
        handler.setStep(workerStep);
        handler.setTaskExecutor(batchTaskExecutor);
        handler.setGridSize(gridSize);

        return new StepBuilder("partitionedStep", jobRepository)
            .partitioner("workerStep", csvPartitioner)
            .partitionHandler(handler)
            .build();
    }

    // ── Worker Step ───────────────────────────────────────────────────────────

    @Bean
    public Step workerStep(JobRepository jobRepository,
                           PlatformTransactionManager txManager,
                           CsvItemReader csvItemReader,
                           CsvItemProcessor processor,
                           OracleJdbcBatchWriter writer,
                           ChunkMetricsListener metricsListener) {

        return new StepBuilder("workerStep", jobRepository)
            .<RecordDTO, RecordDTO>chunk(chunkSize, txManager)
            .reader(csvItemReader)
            .processor(processor)
            .writer(writer)
            .faultTolerant()
            // Skip only known parse/validation errors — not all exceptions
            .skipLimit(skipLimit)
            .skip(org.springframework.batch.item.file.FlatFileParseException.class)
            .skip(RecordValidationException.class)
            // Retry only transient infrastructure errors
            .retryLimit(retryLimit)
            .retry(TransientDataAccessException.class)
            .retry(UncategorizedSQLException.class)
            .noRetry(RecordValidationException.class)
            .listener((org.springframework.batch.core.SkipListener<RecordDTO, RecordDTO>) metricsListener)
            .build();
    }

    // ── Thread Pool ───────────────────────────────────────────────────────────

    @Bean
    public TaskExecutor batchTaskExecutor() {
        var exec = new ThreadPoolTaskExecutor();
        exec.setCorePoolSize(threadPoolCoreSize);
        exec.setMaxPoolSize(threadPoolMaxSize);
        exec.setQueueCapacity(gridSize * 2);
        exec.setThreadNamePrefix("batch-partition-");
        exec.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        exec.setWaitForTasksToCompleteOnShutdown(true);
        exec.setAwaitTerminationSeconds(120);
        exec.initialize();
        return exec;
    }
}
