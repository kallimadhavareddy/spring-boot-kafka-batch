package com.ing.kallitech.kafka.batch.service;

import com.ing.kallitech.kafka.batch.model.RecordDTO;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.SkipListener;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class ChunkMetricsListener
        implements ChunkListener, SkipListener<RecordDTO, RecordDTO> {

    private static final Logger log =
            LoggerFactory.getLogger(ChunkMetricsListener.class);

    private final MeterRegistry meterRegistry;

    public ChunkMetricsListener(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    // ========================
    // Chunk Lifecycle
    // ========================

    @Override
    public void beforeChunk(ChunkContext context) {
        // no-op (optional: track chunk start time here)
    }

    @Override
    public void afterChunk(ChunkContext context) {
        String stepName = context.getStepContext().getStepName();

        meterRegistry.counter(
                "batch.chunk.completed",
                List.of(Tag.of("step", stepName))
        ).increment();
    }

    @Override
    public void afterChunkError(ChunkContext context) {
        String stepName = context.getStepContext().getStepName();

        meterRegistry.counter(
                "batch.chunk.error",
                List.of(Tag.of("step", stepName))
        ).increment();

        log.warn("Chunk error in step={}", stepName);
    }

    // ========================
    // Skip Handling
    // ========================

    @Override
    public void onSkipInRead(Throwable t) {
        meterRegistry.counter("batch.skip.read").increment();

        if (t instanceof FlatFileParseException ex) {
            log.warn("Skipped unparseable line {}: {}",
                    ex.getLineNumber(),
                    ex.getInput());
        } else {
            log.warn("Skip on read: {}", t.getMessage(), t);
        }
    }

    @Override
    public void onSkipInProcess(RecordDTO item, Throwable t) {
        meterRegistry.counter("batch.skip.process").increment();

        String name = item != null ? item.getName() : "unknown";

        log.warn("Skipped record in process: name={} reason={}",
                name,
                t.getMessage(),
                t);
    }

    @Override
    public void onSkipInWrite(RecordDTO item, Throwable t) {
        meterRegistry.counter("batch.skip.write").increment();

        String hash = item != null ? item.getRecordHash() : "unknown";

        log.warn("Skipped record in write: hash={} reason={}",
                hash,
                t.getMessage(),
                t);
    }
}
