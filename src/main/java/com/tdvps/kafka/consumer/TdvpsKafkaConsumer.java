package com.tdvps.kafka.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.tdvps.kafka.model.ConsumerOptions;
import com.tdvps.kafka.model.TdvpsRecord;
import com.tdvps.kafka.util.RecordBodyDecoder;
import com.tdvps.kafka.util.RecordPrinter;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

/**
 * TDVPS Kafka Consumer
 *
 * Reads records from a TDVPS topic, decodes record_body (bytes → UTF-8 → JSON),
 * and displays the result.
 *
 * Key behaviours (per the document spec):
 *  - Deserializes records using the TDVPS schema
 *  - Decodes record_body: bytes → UTF-8 string → JSON parse
 *  - Supports seek by: offset, partition, atTime (timestamp-based seek)
 *  - Graceful shutdown after 10 consecutive empty polls
 *  - SIGINT (Ctrl-C) also triggers graceful shutdown
 */
public class TdvpsKafkaConsumer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TdvpsKafkaConsumer.class);

    /** Graceful shutdown: consecutive empty polls before stopping */
    private static final int MAX_EMPTY_POLLS = 10;

    /** Poll timeout */
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(5);

    private final ConsumerOptions options;
    private final RecordBodyDecoder decoder;
    private final RecordPrinter printer;
    private KafkaConsumer<String, byte[]> kafkaConsumer;
    private volatile boolean running = true;

    public TdvpsKafkaConsumer(ConsumerOptions options) {
        this.options = options;
        this.decoder = new RecordBodyDecoder();
        this.printer = new RecordPrinter(decoder, options.isPretty());
    }

    /**
     * Main consume loop.
     */
    public void consume() {
        kafkaConsumer = buildKafkaConsumer();

        // Register shutdown hook for Ctrl-C
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received — stopping consumer...");
            running = false;
            kafkaConsumer.wakeup();
        }));

        try {
            assignAndSeek(kafkaConsumer);

            int emptyPollCount = 0;
            int totalRecords = 0;

            log.info("Starting poll loop on topic '{}' (max empty polls: {})",
                options.getTopic(), MAX_EMPTY_POLLS);

            while (running) {
                ConsumerRecords<String, byte[]> records = kafkaConsumer.poll(POLL_TIMEOUT);

                if (records.isEmpty()) {
                    emptyPollCount++;
                    log.debug("Empty poll #{}/{}", emptyPollCount, MAX_EMPTY_POLLS);

                    if (emptyPollCount >= MAX_EMPTY_POLLS) {
                        log.info("Received {} consecutive empty polls — shutting down gracefully.",
                            MAX_EMPTY_POLLS);
                        break;
                    }
                    continue;
                }

                // Non-empty batch — reset counter
                emptyPollCount = 0;

                for (ConsumerRecord<String, byte[]> raw : records) {
                    if (options.hasKeyFilter()) {
                        if (raw.key() == null || !raw.key().contains(options.getFilterKey())) {
                            continue;
                        }
                    }

                    TdvpsRecord record = deserialize(raw);
                    printer.print(record);
                    totalRecords++;

                    if (options.isLimited() && totalRecords >= options.getMaxRecords()) {
                        log.info("Reached max-records limit ({}) — stopping.", options.getMaxRecords());
                        running = false;
                        break;
                    }
                }
            }

        } catch (org.apache.kafka.common.errors.WakeupException e) {
            // Normal shutdown via wakeup() — not an error
            log.info("Consumer wakeup called — shutting down.");
        } finally {
            printer.printSummary();
        }
    }

    // -------------------------------------------------------------------------
    // Assign partitions and apply seek strategy
    // -------------------------------------------------------------------------

    private void assignAndSeek(KafkaConsumer<String, byte[]> consumer) {
        String topic = options.getTopic();

        if (options.hasSpecificPartition()) {
            // Manual assignment to a specific partition
            TopicPartition tp = new TopicPartition(topic, options.getPartition());
            consumer.assign(Collections.singletonList(tp));
            applySeek(consumer, Collections.singletonList(tp));
        } else {
            // Subscribe to all partitions; seek is applied on first assignment
            consumer.subscribe(Collections.singletonList(topic), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    log.debug("Partitions revoked: {}", partitions);
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    log.info("Assigned partitions: {}", partitions);
                    applySeek(consumer, partitions);
                }
            });
        }
    }

    /**
     * Apply the seek strategy based on options:
     *   atTime   → offsetsForTimes() then seek
     *   offset=0 → seekToBeginning (EARLIEST)
     *   offset>0 → seek to exact offset
     *   default  → seekToEnd (LATEST) — only poll new messages
     */
    private void applySeek(KafkaConsumer<String, byte[]> consumer,
                           Collection<TopicPartition> partitions) {
        if (options.hasAtTime()) {
            // Timestamp-based seek
            Map<TopicPartition, Long> timestampMap = new HashMap<>();
            for (TopicPartition tp : partitions) {
                timestampMap.put(tp, options.getAtTimeMs());
            }
            Map<TopicPartition, OffsetAndTimestamp> offsets = consumer.offsetsForTimes(timestampMap);
            for (Map.Entry<TopicPartition, OffsetAndTimestamp> e : offsets.entrySet()) {
                if (e.getValue() != null) {
                    log.info("Seeking {} to offset {} (atTime={})",
                        e.getKey(), e.getValue().offset(), options.getAtTimeMs());
                    consumer.seek(e.getKey(), e.getValue().offset());
                } else {
                    log.warn("No offset found for {} at time {} — seeking to end",
                        e.getKey(), options.getAtTimeMs());
                    consumer.seekToEnd(Collections.singletonList(e.getKey()));
                }
            }
        } else if (options.hasSpecificOffset()) {
            if (options.getOffset() == 0) {
                log.info("Seeking to beginning (earliest) for {}", partitions);
                consumer.seekToBeginning(partitions);
            } else {
                for (TopicPartition tp : partitions) {
                    log.info("Seeking {} to offset {}", tp, options.getOffset());
                    consumer.seek(tp, options.getOffset());
                }
            }
        } else {
            // Default: start from the latest (only consume new messages)
            log.info("Seeking to end (latest) for {}", partitions);
            consumer.seekToEnd(partitions);
        }
    }

    // -------------------------------------------------------------------------
    // Deserialize a raw Kafka record → TdvpsRecord
    // -------------------------------------------------------------------------

    /**
     * Deserialize a raw Kafka record into a TdvpsRecord.
     *
     * The Kafka value is the raw record_body bytes (UTF-8 encoded JSON).
     * We also extract the other schema fields from headers if present,
     * or parse the full envelope from the value if it's the full schema JSON.
     *
     * Strategy:
     *   1. Try to parse value as full TDVPS envelope JSON.
     *   2. If that fails, treat the value directly as the record_body.
     */
    private TdvpsRecord deserialize(ConsumerRecord<String, byte[]> raw) {
        TdvpsRecord record = new TdvpsRecord();

        // Populate Kafka metadata
        record.setKafkaTopic(raw.topic());
        record.setKafkaPartition(raw.partition());
        record.setKafkaOffset(raw.offset());
        record.setKafkaTimestampMs(raw.timestamp());

        byte[] value = raw.value();
        if (value == null) {
            log.warn("Null value at offset {}", raw.offset());
            return record;
        }

        // Attempt to parse as full TDVPS envelope JSON
        try {
            JsonNode envelope = new com.fasterxml.jackson.databind.ObjectMapper().readTree(value);

            if (envelope.has("record_uuid")) {
                // It's the full envelope — parse all fields
                record.setRecordUuid(envelope.path("record_uuid").asText(null));
                record.setDatasetStorageUuid(envelope.path("dataset_storage_uuid").asText(null));
                record.setRecordBodyCompression(
                    envelope.path("record_body_compression").asText("NONE"));

                JsonNode ts = envelope.path("record_timestamp");
                if (!ts.isMissingNode()) {
                    TdvpsRecord.RecordTimestamp rts = new TdvpsRecord.RecordTimestamp();
                    rts.setSeconds(ts.path("seconds").asLong(0));
                    rts.setNanos(ts.path("nanos").asInt(0));
                    record.setRecordTimestamp(rts);
                }

                // record_body is base64-encoded bytes in JSON, or a nested string
                JsonNode bodyNode = envelope.path("record_body");
                if (bodyNode.isBinary()) {
                    record.setRecordBodyBytes(bodyNode.binaryValue());
                } else if (bodyNode.isTextual()) {
                    record.setRecordBodyBytes(bodyNode.asText().getBytes(StandardCharsets.UTF_8));
                } else if (!bodyNode.isMissingNode()) {
                    // Already a JSON object embedded
                    record.setRecordBodyJson(bodyNode);
                    return record;
                }
            } else {
                // No envelope — the entire value IS the record_body
                record.setRecordBodyBytes(value);
                record.setRecordBodyCompression("NONE");
            }

        } catch (Exception e) {
            // Not valid JSON at all — treat as raw bytes
            record.setRecordBodyBytes(value);
            record.setRecordBodyCompression("NONE");
        }

        // Decode the body bytes
        if (record.getRecordBodyBytes() != null && record.getRecordBodyJson() == null) {
            JsonNode decoded = decoder.decode(
                record.getRecordBodyBytes(),
                record.getRecordBodyCompression());
            record.setRecordBodyJson(decoded);
        }

        return record;
    }

    // -------------------------------------------------------------------------
    // Kafka consumer factory
    // -------------------------------------------------------------------------

    private KafkaConsumer<String, byte[]> buildKafkaConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, options.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, options.getGroupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            ByteArrayDeserializer.class.getName());
        // Don't commit offsets automatically — this is a read-only support tool
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // Don't reset to earliest on unknown group (only when explicitly requested)
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        // Performance tuning for a support/read tool
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "500");

        return new KafkaConsumer<>(props);
    }

    @Override
    public void close() {
        if (kafkaConsumer != null) {
            try {
                kafkaConsumer.close(Duration.ofSeconds(5));
                log.info("Kafka consumer closed.");
            } catch (Exception e) {
                log.warn("Error closing consumer: {}", e.getMessage());
            }
        }
    }
}
