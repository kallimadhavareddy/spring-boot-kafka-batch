package com.ing.kallitech.kafka.batch.service;

import com.ing.kallitech.kafka.batch.model.RecordDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.*;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.core.io.FileSystemResource;
import org.springframework.stereotype.Component;

/**
 * FIX: Original BatchConfig.csvItemReader(null) passed null as filePath.
 * This would cause a NullPointerException at runtime when Spring tried to
 * open the FileSystemResource.
 *
 * Fix: Reader is now @Component + uses @BeforeStep to read filePath and
 * line range from the StepExecution's ExecutionContext (injected by
 * CsvPartitioner). saveState=true enables restart from last committed chunk.
 */
@Component
public class CsvItemReader implements ItemStreamReader<RecordDTO> {

    private static final Logger log = LoggerFactory.getLogger(CsvItemReader.class);

    private static final String[] FIELD_NAMES =
        {"externalId", "name", "value", "category", "eventTs"}; // Skip 'email' field

    private FlatFileItemReader<RecordDTO> delegate;
    private boolean opened = false;

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        var ctx          = stepExecution.getExecutionContext();
        String filePath  = ctx.getString("filePath");
        long startLine   = ctx.getLong("startLine");
        long maxItems    = ctx.getLong("maxItemCount");
        String delimiter = ctx.containsKey("delimiter") ? ctx.getString("delimiter") : ",";
        int partIdx      = ctx.getInt("partitionIndex", 0);

        log.info("CsvItemReader init: partition={} file={} startLine={} maxItems={}",
            partIdx, filePath, startLine, maxItems);

        var tokenizer = new DelimitedLineTokenizer(delimiter);
        tokenizer.setNames(FIELD_NAMES);
        tokenizer.setStrict(false); // Allow missing columns and different field counts
        
        log.info("Tokenizer configured with fields: {}", String.join(",", FIELD_NAMES));

        var fieldMapper = new BeanWrapperFieldSetMapper<RecordDTO>();
        fieldMapper.setTargetType(RecordDTO.class);

        // Create a custom line mapper for better error handling
        var lineMapper = new DefaultLineMapper<RecordDTO>();
        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(fieldMapper);

        // Create a new resource for each partition to avoid stream conflicts
        var resource = new FileSystemResource(filePath);
        
        delegate = new FlatFileItemReaderBuilder<RecordDTO>()
            .name("csvReader-" + partIdx)
            .resource(resource)
            .linesToSkip((int) startLine - 1)  // Skip header + all lines before this partition
            .maxItemCount((int) maxItems)      // Read exactly this partition's items
            .saveState(false)  // Disable save state for partitioned readers
            .lineMapper(lineMapper)  // Use custom line mapper
            .build();
            
        log.info("Configured reader partition{}: skip={}, maxItems={}", 
            partIdx, (int) startLine - 1, (int) maxItems);
            
        opened = false;
    }

    @Override 
    public RecordDTO read() throws Exception { 
        if (!opened) {
            log.info("Opening CSV reader for partition");
            delegate.open(new ExecutionContext());
            opened = true;
        }
        
        RecordDTO record = delegate.read();
        if (record != null) {
            log.info("Successfully read record: externalId={}, name={}, value={}", 
                record.getExternalId(), record.getName(), record.getValue());
        } else {
            log.info("No more records to read");
        }
        return record;
    }
    
    @Override 
    public void open(ExecutionContext ctx) { 
        // Don't open here - open on first read instead
    }
    
    @Override 
    public void update(ExecutionContext ctx) { 
        if (opened) {
            delegate.update(ctx); 
        }
    }
    
    @Override 
    public void close() { 
        if (opened) {
            log.info("Closing CSV reader");
            delegate.close(); 
            opened = false;
        }
    }
}
