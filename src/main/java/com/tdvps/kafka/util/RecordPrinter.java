package com.tdvps.kafka.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.tdvps.kafka.model.TdvpsRecord;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Formats and prints a TdvpsRecord to stdout.
 *
 * Extracts and highlights key support-relevant fields from the record_body JSON:
 *   - event type / allotment / trigger
 *   - status / workflowStatus
 *   - settlement info
 *   - amounts / currency
 *   - timestamps
 */
public class RecordPrinter {

    private static final String DIVIDER =
        "─────────────────────────────────────────────────────────────────────────────";
    private static final DateTimeFormatter TS_FMT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS 'UTC'").withZone(ZoneOffset.UTC);

    private final RecordBodyDecoder decoder;
    private final boolean pretty;
    private long recordCount = 0;

    public RecordPrinter(RecordBodyDecoder decoder, boolean pretty) {
        this.decoder = decoder;
        this.pretty = pretty;
    }

    /**
     * Print a fully decoded TdvpsRecord.
     */
    public void print(TdvpsRecord record,String filterKey) {
        recordCount++;

        System.out.println(DIVIDER);
        System.out.printf("  Record #%-5d  Topic: %-50s  Partition: %d  Offset: %d%n",
            recordCount,
            record.getKafkaTopic(),
            record.getKafkaPartition(),
            record.getKafkaOffset());
        System.out.println(DIVIDER);

        // ── Envelope metadata ────────────────────────────────────────────────
        System.out.println("  [Envelope]");
        System.out.printf("    record_uuid           : %s%n", nvl(record.getRecordUuid()));
        System.out.printf("    dataset_storage_uuid  : %s%n", nvl(record.getDatasetStorageUuid()));
        System.out.printf("    record_body_compression: %s%n", nvl(record.getRecordBodyCompression()));

        if (record.getRecordTimestamp() != null) {
            long epochSec = record.getRecordTimestamp().getSeconds();
            String human = epochSec > 0
                ? TS_FMT.format(Instant.ofEpochSecond(epochSec))
                : "N/A";
            System.out.printf("    record_timestamp      : %s  [%s]%n",
                record.getRecordTimestamp(), human);
        }

        // ── Kafka metadata ───────────────────────────────────────────────────
        System.out.println("  [Kafka Metadata]");
        if (record.getKafkaTimestampMs() > 0) {
            System.out.printf("    kafka_timestamp       : %s%n",
                TS_FMT.format(Instant.ofEpochMilli(record.getKafkaTimestampMs())));
        }

        // ── Decoded payload ──────────────────────────────────────────────────
        JsonNode body = record.getRecordBodyJson();
        if (body != null) {

            boolean isFiltered = filterKey != null && !filterKey.isBlank();
            if(isFiltered){
                //Filter mode - skip summary, show complete JSON with all nested objects
                System.out.println(" [record_body — Full Payload  filter: " + filterKey + "]");
            } else {
                // Normal mode - show summary highlights first
                //System.out.println(" [record_body — Decoded Payload]");
                printSummaryFields(body);
            }
            System.out.println("  [record_body — Decoded Payload]");
            printSummaryFields(body);
            System.out.println("  [Full JSON]");
            String json = pretty ? decoder.toPrettyString(body) : decoder.toCompactString(body);
            // Indent each line for readability
            for (String line : json.split("\n")) {
                System.out.println("    " + line);
            }
        } else {
            System.out.println("  [record_body] — could not decode (null or parse error)");
        }

        System.out.println();
    }

    /**
     * Extract and print the most support-relevant fields from the payload.
     * Based on example payloads in the document.
     */
    private void printSummaryFields(JsonNode body) {
        System.out.println("  ┌─ Key fields:");
        printIfPresent(body, "event",           "event");
        printIfPresent(body, "allotment",       "allotment");
        printIfPresent(body, "status",          "status");
        printIfPresent(body, "workflowStatus",  "workflowStatus");
        printIfPresent(body, "triggerEventId",  "triggerEventId");
        printIfPresent(body, "eventId",         "eventId");
        printIfPresent(body, "eventTime",       "eventTime");
        printIfPresent(body, "atTime",          "atTime");
        printIfPresent(body, "asOfTime",        "asOfTime");
        printIfPresent(body, "instrumentId",    "instrumentId");
        printIfPresent(body, "inventoryId",     "inventoryId");
        printIfPresent(body, "numberOfUnits",   "numberOfUnits");
        printIfPresent(body, "price",           "price");
        printIfPresent(body, "quantity",        "quantity");
        printIfPresent(body, "settlementDate",  "settlementDate");
        printIfPresent(body, "settlementCurrency", "settlementCurrency");
        printIfPresent(body, "kafkaTopic",      "kafkaTopic");
        printIfPresent(body, "kafkaOffset",     "kafkaOffset");
        printIfPresent(body, "kafkaPartition",  "kafkaPartition");
        printIfPresent(body, "kafkaTimestamp",  "kafkaTimestamp");
        printIfPresent(body, "dataType",        "dataType");
        printIfPresent(body, "traderID",        "traderID");
        printIfPresent(body, "userID",          "userID");
        printIfPresent(body, "source",          "source");

        // settledCash array
        if (body.has("settledCash") && body.get("settledCash").isArray()) {
            System.out.println("  │  settledCash:");
            for (JsonNode sc : body.get("settledCash")) {
                System.out.printf("  │    amount=%s  currency=%s%n",
                    sc.path("amount").asText("N/A"),
                    sc.path("currency").asText("N/A"));
            }
        }

        System.out.println("  └─");
    }

    private void printIfPresent(JsonNode node, String field, String label) {
        if (node.has(field) && !node.get(field).isNull()) {
            System.out.printf("  │  %-25s: %s%n", label, node.get(field).asText());
        }
    }

    public void printSummary() {
        System.out.println(DIVIDER);
        System.out.printf("  Total records displayed: %d%n", recordCount);
        System.out.println(DIVIDER);
    }

    private String nvl(String s) {
        return s != null ? s : "N/A";
    }
}
