package com.ing.kallitech.kafka;

import com.ing.kallitech.kafka.batch.model.KafkaBatchMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api")
public class Controller {

    private final KafkaTemplate<String, KafkaBatchMessage> kafkaTemplate;

    @Autowired
    public Controller(KafkaTemplate<String, KafkaBatchMessage> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }



    @GetMapping("/send")
    ResponseEntity<String> createMessage() {
        KafkaBatchMessage kafkaBatchMessage = new KafkaBatchMessage();
        kafkaBatchMessage.setFileId("file-20260218-001");
        kafkaBatchMessage.setFilePath("/home/kalli/sample.csv");
        kafkaBatchMessage.setRecordCount(5);
        kafkaBatchMessage.setDelimiter(",");
        kafkaBatchMessage.setSourceSystem("TEST-System");
        kafkaTemplate.send("test-topic",kafkaBatchMessage);
        return ResponseEntity.ok("sent");
    }

}
