package com.tdvps.kafka.model;

/**
 * Encapsulates all filtering / seek options that can be applied to the consumer.
 *
 * Options mirror what the document specifies:
 *   --topic          Kafka topic name
 *   --partition      Specific partition (-1 = all)
 *   --offset         Start offset (-1 = latest, 0 = earliest, N = exact)
 *   --at-time        Seek to offset-for-time (epoch millis)
 *   --max-records    Stop after N records (0 = unlimited)
 *   --filter-key     Only print records whose key matches this string
 *   --pretty         Pretty-print JSON (default: true)
 */
public class ConsumerOptions {

    private String topic;
    private int partition = -1;          // -1 = all partitions
    private long offset = -1;            // -1 = latest
    private long atTimeMs = -1;          // -1 = not set (use offset instead)
    private int maxRecords = 0;          // 0 = unlimited
    private String filterKey = null;
    private boolean pretty = true;
    private String bootstrapServers = "localhost:9092";
    private String groupId = "tdvps-support-tool";

    // -------------------------------------------------------------------------
    // Builder-style setters (fluent)
    // -------------------------------------------------------------------------

    public ConsumerOptions topic(String topic) { this.topic = topic; return this; }
    public ConsumerOptions partition(int partition) { this.partition = partition; return this; }
    public ConsumerOptions offset(long offset) { this.offset = offset; return this; }
    public ConsumerOptions atTimeMs(long atTimeMs) { this.atTimeMs = atTimeMs; return this; }
    public ConsumerOptions maxRecords(int max) { this.maxRecords = max; return this; }
    public ConsumerOptions filterKey(String key) { this.filterKey = key; return this; }
    public ConsumerOptions pretty(boolean pretty) { this.pretty = pretty; return this; }
    public ConsumerOptions bootstrapServers(String s) { this.bootstrapServers = s; return this; }
    public ConsumerOptions groupId(String g) { this.groupId = g; return this; }

    // -------------------------------------------------------------------------
    // Getters
    // -------------------------------------------------------------------------

    public String getTopic() { return topic; }
    public int getPartition() { return partition; }
    public long getOffset() { return offset; }
    public long getAtTimeMs() { return atTimeMs; }
    public int getMaxRecords() { return maxRecords; }
    public String getFilterKey() { return filterKey; }
    public boolean isPretty() { return pretty; }
    public String getBootstrapServers() { return bootstrapServers; }
    public String getGroupId() { return groupId; }

    public boolean hasAtTime() { return atTimeMs >= 0; }
    public boolean hasSpecificPartition() { return partition >= 0; }
    public boolean hasSpecificOffset() { return offset >= 0; }
    public boolean hasKeyFilter() { return filterKey != null && !filterKey.isBlank(); }
    public boolean isLimited() { return maxRecords > 0; }
}
