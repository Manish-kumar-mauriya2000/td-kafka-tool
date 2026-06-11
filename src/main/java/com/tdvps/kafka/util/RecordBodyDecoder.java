package com.tdvps.kafka.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;

/**
 * Decodes the record_body field from raw bytes to a displayable JSON string.
 *
 * Steps (per the document spec):
 *  1. If record_body_compression == "GZIP", decompress first.
 *  2. Convert bytes → UTF-8 string.
 *  3. Parse as JSON (for pretty printing and field extraction).
 */
public class RecordBodyDecoder {

    private static final Logger log = LoggerFactory.getLogger(RecordBodyDecoder.class);

    private final ObjectMapper objectMapper;
    private final ObjectMapper prettyMapper;

    public RecordBodyDecoder() {
        this.objectMapper = new ObjectMapper();
        this.prettyMapper = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT);
    }

    /**
     * Decode raw bytes to a parsed JsonNode.
     *
     * @param bodyBytes          raw bytes from Kafka record_body field
     * @param compressionType    value of record_body_compression ("NONE", "GZIP", null)
     * @return parsed JsonNode, or null on failure
     */
    public JsonNode decode(byte[] bodyBytes, String compressionType) {
        if (bodyBytes == null || bodyBytes.length == 0) {
            log.warn("record_body is null or empty — skipping decode");
            return null;
        }

        try {
            byte[] uncompressed = decompress(bodyBytes, compressionType);
            String json = new String(uncompressed, StandardCharsets.UTF_8);
            return objectMapper.readTree(json);
        } catch (IOException e) {
            log.error("Failed to decode record_body: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Decompress bytes if needed.
     */
    private byte[] decompress(byte[] bytes, String compressionType) throws IOException {
        if (compressionType == null || compressionType.isBlank()
                || compressionType.equalsIgnoreCase("NONE")) {
            return bytes;
        }

        if (compressionType.equalsIgnoreCase("GZIP")) {
            try (GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(bytes))) {
                return gis.readAllBytes();
            }
        }

        log.warn("Unknown compression type '{}' — treating as uncompressed", compressionType);
        return bytes;
    }

    /**
     * Pretty-print a JsonNode.
     */
    public String toPrettyString(JsonNode node) {
        if (node == null) return "<null>";
        try {
            return prettyMapper.writeValueAsString(node);
        } catch (IOException e) {
            return node.toString();
        }
    }

    /**
     * Compact single-line JSON string.
     */
    public String toCompactString(JsonNode node) {
        if (node == null) return "<null>";
        return node.toString();
    }
}
