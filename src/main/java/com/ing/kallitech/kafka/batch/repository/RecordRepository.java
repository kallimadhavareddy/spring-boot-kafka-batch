package com.ing.kallitech.kafka.batch.repository;

import com.ing.kallitech.kafka.batch.entity.Record;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface RecordRepository extends JpaRepository<Record, Long> {
}
