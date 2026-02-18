package com.ing.kallitech.kafka.config;

import com.ing.kallitech.kafka.batch.model.KafkaBatchMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * FIXES applied vs original KafkaConfig:
 *
 * 1. WRONG PACKAGE: was com.example.csvbatch.config — fixed to com.ing.kallitech.kafka.config
 * 2. TYPED MESSAGES: Consumer now deserializes KafkaBatchMessage (not raw String)
 * 3. MANUAL ACK: Added AckMode.MANUAL_IMMEDIATE (original had no ACK mode — messages
 *    could be lost on pod crash since Kafka would auto-commit unconsumed offsets)
 * 4. EXPONENTIAL BACKOFF: Was FixedBackOff(5000, 3) — replaced with ExponentialBackOff
 *    (2s → 4s → 8s) to avoid thundering herd on mass failures
 * 5. PARTITION-AWARE DLQ: DLQ routes to same partition as source for message ordering
 * 6. CONCURRENCY: Was 6 threads (too high for long batch jobs) — set to 3
 * 7. POLL TIMEOUTS: Added session.timeout.ms and max.poll.interval.ms to survive
 *    long-running batch jobs without Kafka kicking the consumer
 * 8. IDEMPOTENT PRODUCER: Added enable.idempotence=true and acks=all
 */
@Configuration
public class KafkaConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.group-id}")
    private String groupId;

    @Value("${kafka.dlq-topic}")
    private String dlqTopic;

    // ── Consumer ──────────────────────────────────────────────────────────────

    @Bean
    public ConsumerFactory<String, KafkaBatchMessage> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);      // Manual ACK
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5);            // Backpressure
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 300_000);    // 5 min
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 600_000);  // 10 min
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10_000);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(JsonDeserializer.TRUSTED_PACKAGES,
                  "com.ing.kallitech.kafka.batch.model");
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);

        return new DefaultKafkaConsumerFactory<>(
            props,
            new StringDeserializer(),
            new JsonDeserializer<>(KafkaBatchMessage.class, false)
        );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, KafkaBatchMessage>
           kafkaListenerContainerFactory() {

        var factory = new ConcurrentKafkaListenerContainerFactory<String, KafkaBatchMessage>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(3);
        // CRITICAL: Manual ACK — commit offset only after job launches successfully
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setCommonErrorHandler(buildErrorHandler());
        return factory;
    }

    private DefaultErrorHandler buildErrorHandler() {
        // Exponential: 2s → 4s → 8s, then DLQ
        var backOff = new ExponentialBackOff(2_000L, 2.0);
        backOff.setMaxAttempts(3);
        backOff.setMaxInterval(30_000L);

        var recoverer = new DeadLetterPublishingRecoverer(
            dlqKafkaTemplate(),
            (record, ex) -> {
                log.error("Publishing to DLQ after retries exhausted: topic={} partition={} offset={}",
                    record.topic(), record.partition(), record.offset());
                return new TopicPartition(dlqTopic, record.partition());
            }
        );

        var handler = new DefaultErrorHandler(recoverer, backOff);
        // Never retry permanent failures
        handler.addNotRetryableExceptions(IllegalArgumentException.class);
        handler.setRetryListeners((record, ex, attempt) ->
            log.warn("Kafka retry attempt {}/3: {}", attempt, ex.getMessage()));

        return handler;
    }

    // ── Producer (for DLQ publishing) ─────────────────────────────────────────

    @Bean
    public ProducerFactory<String, Object> dlqProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, Object> dlqKafkaTemplate() {
        return new KafkaTemplate<>(dlqProducerFactory());
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(
            ProducerFactory<String, String> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}
