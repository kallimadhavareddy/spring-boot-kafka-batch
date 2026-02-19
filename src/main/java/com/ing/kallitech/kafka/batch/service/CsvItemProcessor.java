package com.ing.kallitech.kafka.batch.service;

import com.ing.kallitech.kafka.batch.model.RecordDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.math.RoundingMode;

@Component
public class CsvItemProcessor implements ItemProcessor<RecordDTO, RecordDTO> {

    private static final Logger log = LoggerFactory.getLogger(CsvItemProcessor.class);

    private String jobId;
    private int    partitionIndex;

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        this.jobId          = String.valueOf(stepExecution.getJobExecution().getId());
        this.partitionIndex = stepExecution.getExecutionContext().getInt("partitionIndex", 0);
    }

    @Override
    public RecordDTO process(RecordDTO item) throws RecordValidationException {

        if (!StringUtils.hasText(item.getName())) {
            throw new RecordValidationException("name is blank", item);
        }
        if (item.getValueRec() == null) {
            throw new RecordValidationException("value is null", item);
        }
        if (item.getValueRec().scale() > 4) {
            item.setValueRec(item.getValueRec().setScale(4, RoundingMode.HALF_UP));
        }
        if (!StringUtils.hasText(item.getCategory())) {
        }

        item.setRecordHash(computeHash(item));
        item.setJobId(jobId);
        item.setPartitionIndex(partitionIndex);

        return item;
    }


    private String computeHash(RecordDTO rec) {
        String key = String.join("|",
            nullSafe(rec.getExternalId()),
            nullSafe(rec.getCategory()),
            rec.getEventTs() != null ? rec.getEventTs().toString() : ""
        );
        return DigestUtils.sha256Hex(key);
    }

    private String nullSafe(String s) { return s != null ? s : ""; }
}
