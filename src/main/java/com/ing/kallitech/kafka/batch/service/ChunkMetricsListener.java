package com.ing.kallitech.kafka.batch.service;

import com.ing.kallitech.kafka.batch.model.RecordDTO;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.SkipListener;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.stereotype.Component;

/** NEW: Per-chunk and skip-event metrics + logging. */
@Component
public class ChunkMetricsListener
        implements ChunkListener, SkipListener<RecordDTO, RecordDTO> {

    private static final Logger log = LoggerFactory.getLogger(ChunkMetricsListener.class);

    private final MeterRegistry meterRegistry;

    public ChunkMetricsListener(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    @Override public void beforeChunk(ChunkContext ctx) {}

    @Override public void afterChunk(ChunkContext ctx) {
        meterRegistry.counter("batch.chunk.completed").increment();
    }

    @Override public void afterChunkError(ChunkContext ctx) {
        meterRegistry.counter("batch.chunk.error").increment();
        log.warn("Chunk error in step={}", ctx.getStepContext().getStepName());
    }

    @Override public void onSkipInRead(Throwable t) {
        meterRegistry.counter("batch.skip.read").increment();
        if (t instanceof FlatFileParseException ex) {
            log.warn("Skipped unparseable line {}: {}", ex.getLineNumber(), ex.getInput());
        } else {
            log.warn("Skip on read: {}", t.getMessage());
        }
    }

    @Override public void onSkipInProcess(RecordDTO item, Throwable t) {
        meterRegistry.counter("batch.skip.process").increment();
        log.warn("Skipped record in process: name={} reason={}", item.getName(), t.getMessage());
    }

    @Override public void onSkipInWrite(RecordDTO item, Throwable t) {
        meterRegistry.counter("batch.skip.write").increment();
        log.warn("Skipped record in write: hash={} reason={}", item.getRecordHash(), t.getMessage());
    }
}
