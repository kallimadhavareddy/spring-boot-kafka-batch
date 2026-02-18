package com.ing.kallitech.kafka;

import com.ing.kallitech.kafka.batch.model.KafkaBatchMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.Optional;

@RestController
@RequestMapping("/api")
public class ControllerApi {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    public ControllerApi(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }



    @GetMapping("/send")
    ResponseEntity<String> createMessage() {

        System.out.println("Calling message send");
        KafkaBatchMessage kafkaBatchMessage = new KafkaBatchMessage();
        kafkaBatchMessage.setFileId("file-20260218-001");
        kafkaBatchMessage.setFilePath("/home/kalli/sample.csv");
        kafkaBatchMessage.setRecordCount(5);
        kafkaBatchMessage.setDelimiter(",");
        kafkaBatchMessage.setSourceSystem("TEST-System");
        System.out.println("Message created" + kafkaBatchMessage);

        // Send as JSON string for now
        String messageJson = "{\"fileId\":\"" + kafkaBatchMessage.getFileId() +
                "\",\"filePath\":\"" + kafkaBatchMessage.getFilePath() +
                "\",\"recordCount\":" + kafkaBatchMessage.getRecordCount() +
                ",\"delimiter\":\"" + kafkaBatchMessage.getDelimiter() +
                "\",\"sourceSystem\":\"" + kafkaBatchMessage.getSourceSystem() + "\"}";

        try {
            kafkaTemplate.send("test-topic", messageJson);
        } catch (Exception e) {
            System.err.println("Failed to send message to Kafka: " + e.getMessage());
            return ResponseEntity.status(500).body("Failed to send message: " + e.getMessage());
        }
        return ResponseEntity.status(200).body("Message sent successfully");
    }

}
