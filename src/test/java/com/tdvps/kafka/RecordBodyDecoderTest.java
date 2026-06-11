package com.tdvps.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.tdvps.kafka.util.RecordBodyDecoder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.*;

class RecordBodyDecoderTest {

    private RecordBodyDecoder decoder;

    @BeforeEach
    void setUp() {
        decoder = new RecordBodyDecoder();
    }

    @Test
    void decodeSimplePositionPayload() {
        String json = """
            {
              "status": "Settled",
              "instrumentId": 697827417,
              "numberOfUnits": 5.0,
              "kafkaTopic": "cpa02_app_tds_tdvps_positions_egress_ged_positions",
              "triggerEventId": 298974559
            }
            """;
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);

        JsonNode result = decoder.decode(bytes, "NONE");

        assertNotNull(result);
        assertEquals("Settled", result.path("status").asText());
        assertEquals(697827417, result.path("instrumentId").asInt());
        assertEquals(5.0, result.path("numberOfUnits").asDouble());
    }

    @Test
    void decodeTradePayload() {
        String json = """
            {
              "event": "Payment",
              "workflowStatus": "Committed",
              "traderID": "SUPPORT1",
              "settlementDate": "2026-06-04",
              "settlementCurrency": "EUR",
              "amount": 124.55
            }
            """;
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);

        JsonNode result = decoder.decode(bytes, null);

        assertNotNull(result);
        assertEquals("Payment", result.path("event").asText());
        assertEquals("Committed", result.path("workflowStatus").asText());
        assertEquals("SUPPORT1", result.path("traderID").asText());
    }

    @Test
    void decodeGzipCompressedPayload() throws Exception {
        String json = "{\"status\":\"Active\",\"amount\":9999.0}";
        byte[] compressed = gzip(json.getBytes(StandardCharsets.UTF_8));

        JsonNode result = decoder.decode(compressed, "GZIP");

        assertNotNull(result);
        assertEquals("Active", result.path("status").asText());
        assertEquals(9999.0, result.path("amount").asDouble());
    }

    @Test
    void decodeNullBytesReturnsNull() {
        JsonNode result = decoder.decode(null, "NONE");
        assertNull(result);
    }

    @Test
    void decodeEmptyBytesReturnsNull() {
        JsonNode result = decoder.decode(new byte[0], "NONE");
        assertNull(result);
    }

    @Test
    void decodeInvalidJsonReturnsNull() {
        byte[] notJson = "this is not json !!!".getBytes(StandardCharsets.UTF_8);
        JsonNode result = decoder.decode(notJson, "NONE");
        assertNull(result);
    }

    @Test
    void toPrettyStringFormatsCorrectly() throws Exception {
        String json = "{\"a\":1,\"b\":\"hello\"}";
        JsonNode node = new com.fasterxml.jackson.databind.ObjectMapper().readTree(json);
        String pretty = decoder.toPrettyString(node);
        assertTrue(pretty.contains("\n"), "Pretty output should contain newlines");
        assertTrue(pretty.contains("\"a\" : 1") || pretty.contains("\"a\":1"));
    }

    @Test
    void unknownCompressionTreatedAsUncompressed() {
        String json = "{\"status\":\"OK\"}";
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        JsonNode result = decoder.decode(bytes, "SNAPPY"); // unsupported → falls through
        assertNotNull(result);
        assertEquals("OK", result.path("status").asText());
    }

    // -------------------------------------------------------------------------
    // Helper
    // -------------------------------------------------------------------------

    private byte[] gzip(byte[] data) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (GZIPOutputStream gos = new GZIPOutputStream(bos)) {
            gos.write(data);
        }
        return bos.toByteArray();
    }
}
