package com.ing.kallitech.kafka.batch.controller;


import com.ing.kallitech.kafka.batch.model.KafkaBatchMessage;
import com.ing.kallitech.kafka.batch.entity.Record;
import com.ing.kallitech.kafka.batch.repository.RecordRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api")
public class DashBoardController {
    private final RecordRepository recordRepository;
    public DashBoardController(RecordRepository recordRepository) {
        this.recordRepository = recordRepository;
    }

    @GetMapping("/records")
    ResponseEntity<List<Record>> createMessage() {
        return ResponseEntity.status(200).body(recordRepository.findAll());
    }
}
