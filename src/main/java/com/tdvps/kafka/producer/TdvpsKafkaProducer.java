package com.tdvps.kafka.producer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * TDVPS Test Producer
 *
 * Writes sample messages to a Kafka topic using the TDVPS schema:
 *   record_uuid, record_timestamp, dataset_storage_uuid,
 *   record_body (UTF-8 JSON bytes), record_body_compression
 *
 * For local testing only — not for production use.
 */
public class TdvpsKafkaProducer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TdvpsKafkaProducer.class);

    private final String bootstrapServers;
    private final ObjectMapper mapper = new ObjectMapper();
    private KafkaProducer<String, byte[]> kafkaProducer;

    public TdvpsKafkaProducer(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public void init() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        kafkaProducer = new KafkaProducer<>(props);
        log.info("Kafka producer initialized — bootstrap: {}", bootstrapServers);
    }

    /**
     * Send N sample messages to the given topic.
     */
    public void sendSamples(String topic, int count, String messageType) throws Exception {
        log.info("Sending {} sample '{}' messages to topic '{}'", count, messageType, topic);

        for (int i = 0; i < count; i++) {
            String recordUuid = UUID.randomUUID().toString();
            byte[] envelope = buildEnvelope(recordUuid, messageType, i);

            ProducerRecord<String, byte[]> record =
                new ProducerRecord<>(topic, recordUuid, envelope);

            RecordMetadata meta = kafkaProducer.send(record).get(); // sync for testing
            log.info("Sent record #{} → partition={} offset={} uuid={}",
                i + 1, meta.partition(), meta.offset(), recordUuid);
        }

        kafkaProducer.flush();
        log.info("Done sending {} messages.", count);
    }

    // -------------------------------------------------------------------------
    // Build a full TDVPS envelope JSON (value bytes)
    // -------------------------------------------------------------------------

    private byte[] buildEnvelope(String recordUuid, String messageType, int idx) throws Exception {
        Instant now = Instant.now();
        ObjectNode envelope = mapper.createObjectNode();

        envelope.put("record_uuid", recordUuid);
        envelope.put("dataset_storage_uuid", UUID.randomUUID().toString());
        envelope.put("record_body_compression", "NONE");

        ObjectNode ts = envelope.putObject("record_timestamp");
        ts.put("seconds", now.getEpochSecond());
        ts.put("nanos", now.getNano());

        // Build the record_body JSON then store as UTF-8 string
        String bodyJson = buildRecordBody(messageType, idx, now);
        envelope.put("record_body", bodyJson);

        return mapper.writeValueAsBytes(envelope);
    }

    /**
     * Build the record_body JSON payload.
     * Matches the example payloads from the TDVPS Admin Tooling document.
     */
    private String buildRecordBody(String messageType, int idx, Instant now) throws Exception {
        ObjectNode body = mapper.createObjectNode();
        String nowStr = now.toString();

        switch (messageType.toLowerCase()) {
            case "position": {
                body.put("asOfCounter", 100 + idx);
                body.put("asOfTime", nowStr);
                body.put("atTime", nowStr);
                body.put("averagePrice", 0.0);
                body.put("averagePricePurchases", 0.0);
                body.put("averagePriceSOD", 0.0);
                body.put("averagePriceSales", 0.0);
                body.put("dayPurchaseAvg", 0.0);
                body.put("dayPurchaseUnits", 0.0);
                body.put("daySaleAvg", 0.0);
                body.put("daySaleUnits", 0.0);
                body.put("instrumentId", 697827417L + idx);
                body.put("instrumentSource", "SOPHIS");
                body.put("inventoryId", 2101 + idx);
                body.put("inventorySource", "SOPHIS");
                body.put("kafkaOffset", 148251599L + idx);
                body.put("kafkaPartition", idx % 8);
                body.put("kafkaTimestamp", nowStr);
                body.put("kafkaTopic", "cpa02_app_tds_tdvps_positions_egress_ged_positions");
                body.put("maxTime", nowStr);
                body.put("numberOfUnits", 0.0);
                body.put("numberOfUnitsPurchases", 1.0);
                body.put("numberOfUnitsSOD", 0.0);
                body.put("numberOfUnitsSales", 0.0);
                ArrayNode settledCash = body.putArray("settledCash");
                ObjectNode cash = settledCash.addObject();
                cash.put("amount", -9656932.397100002 + idx);
                cash.put("currency", "USD");
                body.putArray("unsettledCash");
                body.put("status", "Settled");
                body.put("triggerEventId", 298974559L + idx);
                break;
            }

            case "trade": {
                body.put("allotment", "Equity Future");
                body.put("amount", 124.55 + idx);
                body.put("asOfCounter", (JsonNode) null);
                body.put("auditTime", nowStr);
                ArrayNode counterparty = body.putArray("counterparty");
                ObjectNode cp1 = counterparty.addObject();
                cp1.put("counterpartyId", 10062699);
                cp1.put("counterpartyRole", "Broker");
                ObjectNode cp2 = counterparty.addObject();
                cp2.put("counterpartyId", 10064056);
                cp2.put("counterpartyRole", "Depositary");
                body.put("counterpartySource", "SOPHIS");
                body.put("creationSource", "SOPHIS");
                body.put("creationType", "manual");
                body.put("dataType", "NON_ELECTRONIC_TRADES");
                body.put("event", "Payment");
                body.put("eventId", 548339611L + idx);
                body.put("eventReference", 0);
                body.put("eventTime", nowStr);
                body.put("eventVersion", 1);
                body.put("feeType", "");
                body.put("fxRate", 1.6057545975);
                body.put("instrumentId", 976906816L + idx);
                body.put("instrumentSource", "SOPHIS");
                body.put("inventoryId", 1961 + idx);
                body.put("inventorySource", "SOPHIS");
                body.put("pnlDate", now.toString().substring(0, 10));
                body.put("price", 1.0);
                body.put("quantity", 1.0);
                body.put("settlementCurrency", "EUR");
                body.put("settlementDate", now.toString().substring(0, 10));
                body.put("shortSale", false);
                body.put("source", "SettlementBatchPipeline");
                body.put("sourceTableName", "NON_ELECTRONIC_TRADES");
                body.put("status", "Settled");
                ObjectNode timeCtx = body.putObject("timeContext");
                timeCtx.put("validFrom", nowStr);
                timeCtx.put("validTo", "2099-12-31T23:59:59");
                body.put("tradeDate", now.toString().substring(0, 10));
                body.put("traderID", "SUPPORT" + (idx + 1));
                body.put("userID", "SUPPORT" + (idx + 1));
                body.put("workflowStatus", "Committed");
                break;
            }

            default: {
                // Generic fallback
                body.put("event", "GENERIC_EVENT");
                body.put("eventId", 100000L + idx);
                body.put("status", "Active");
                body.put("workflowStatus", "Pending");
                body.put("instrumentId", 111111L + idx);
                body.put("amount", 1000.0 * (idx + 1));
                body.put("currency", "USD");
                body.put("timestamp", now.toString());
                body.put("source", "TEST_PRODUCER");
                body.put("traderID", "TEST_USER");
            }
        }

        return mapper.writeValueAsString(body);
    }

    @Override
    public void close() {
        if (kafkaProducer != null) {
            kafkaProducer.close();
            log.info("Kafka producer closed.");
        }
    }
}
