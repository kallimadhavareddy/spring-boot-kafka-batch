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
        {"externalId", "name", "email", "value", "category", "eventTs"};

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

        var fieldMapper = new BeanWrapperFieldSetMapper<RecordDTO>();
        fieldMapper.setTargetType(RecordDTO.class);

        // Create a new resource for each partition to avoid stream conflicts
        var resource = new FileSystemResource(filePath);
        
        delegate = new FlatFileItemReaderBuilder<RecordDTO>()
            .name("csvReader-" + partIdx)
            .resource(resource)
            .linesToSkip((int) startLine - 1)
            .maxItemCount((int) maxItems)
            .saveState(false)  // Disable save state for partitioned readers
            .lineTokenizer(tokenizer)
            .fieldSetMapper(fieldMapper)
            .build();
            
        opened = false;
    }

    @Override 
    public RecordDTO read() throws Exception { 
        if (!opened) {
            log.info("Opening CSV reader for partition");
            delegate.open(new ExecutionContext());
            opened = true;
        }
        return delegate.read(); 
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
