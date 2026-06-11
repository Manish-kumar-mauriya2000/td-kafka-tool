package com.tdvps.kafka.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Represents a TDVPS Kafka message deserialized from the topic.
 *
 * Schema (from TDVPS Admin Tooling doc):
 *   record_uuid              STRING
 *   record_timestamp         ROW(seconds BIGINT, nanos INT)
 *   dataset_storage_uuid     STRING
 *   record_body              BYTES  — UTF-8 encoded JSON payload
 *   record_body_compression  STRING — e.g. "NONE", "GZIP"
 *
 * record_body is the primary field of interest:
 *   bytes → UTF-8 decode → JSON parse → display
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TdvpsRecord {

    @JsonProperty("record_uuid")
    private String recordUuid;

    @JsonProperty("record_timestamp")
    private RecordTimestamp recordTimestamp;

    @JsonProperty("dataset_storage_uuid")
    private String datasetStorageUuid;

    /**
     * Raw bytes of the record body (UTF-8 encoded JSON).
     * Stored as byte[] when deserialized from Kafka.
     */
    private byte[] recordBodyBytes;

    /**
     * Decoded JSON payload (parsed from recordBodyBytes).
     * Populated after decodeBody() is called.
     */
    private JsonNode recordBodyJson;

    @JsonProperty("record_body_compression")
    private String recordBodyCompression;

    // Kafka metadata — populated by the consumer, not from the message itself
    private String kafkaTopic;
    private int kafkaPartition;
    private long kafkaOffset;
    private long kafkaTimestampMs;

    // -------------------------------------------------------------------------
    // Inner class: RecordTimestamp
    // -------------------------------------------------------------------------

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class RecordTimestamp {
        private long seconds;
        private int nanos;

        public long getSeconds() { return seconds; }
        public void setSeconds(long seconds) { this.seconds = seconds; }

        public int getNanos() { return nanos; }
        public void setNanos(int nanos) { this.nanos = nanos; }

        @Override
        public String toString() {
            return seconds + "." + String.format("%09d", nanos) + "s (epoch)";
        }
    }

    // -------------------------------------------------------------------------
    // Getters / Setters
    // -------------------------------------------------------------------------

    public String getRecordUuid() { return recordUuid; }
    public void setRecordUuid(String recordUuid) { this.recordUuid = recordUuid; }

    public RecordTimestamp getRecordTimestamp() { return recordTimestamp; }
    public void setRecordTimestamp(RecordTimestamp recordTimestamp) {
        this.recordTimestamp = recordTimestamp;
    }

    public String getDatasetStorageUuid() { return datasetStorageUuid; }
    public void setDatasetStorageUuid(String uuid) { this.datasetStorageUuid = uuid; }

    public byte[] getRecordBodyBytes() { return recordBodyBytes; }
    public void setRecordBodyBytes(byte[] bytes) { this.recordBodyBytes = bytes; }

    public JsonNode getRecordBodyJson() { return recordBodyJson; }
    public void setRecordBodyJson(JsonNode json) { this.recordBodyJson = json; }

    public String getRecordBodyCompression() { return recordBodyCompression; }
    public void setRecordBodyCompression(String c) { this.recordBodyCompression = c; }

    public String getKafkaTopic() { return kafkaTopic; }
    public void setKafkaTopic(String kafkaTopic) { this.kafkaTopic = kafkaTopic; }

    public int getKafkaPartition() { return kafkaPartition; }
    public void setKafkaPartition(int p) { this.kafkaPartition = p; }

    public long getKafkaOffset() { return kafkaOffset; }
    public void setKafkaOffset(long kafkaOffset) { this.kafkaOffset = kafkaOffset; }

    public long getKafkaTimestampMs() { return kafkaTimestampMs; }
    public void setKafkaTimestampMs(long ts) { this.kafkaTimestampMs = ts; }
}
