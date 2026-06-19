package com.tdvps.kafka.consumer;

import com.tdvps.kafka.model.ConsumerOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * CLI subcommand: consume
 *
 * Usage examples:
 *
 *   # Read from the TDVPS positions topic, latest messages
 *   java -jar tdvps-kafka-tool.jar consume \
 *       --topic cpa02_app_tds_tdvps_positions_egress_ged_positions
 *
 *   # Read from beginning (all history)
 *   java -jar tdvps-kafka-tool.jar consume \
 *       --topic cpa02_app_tds_tdvps_positions_egress_ged_positions \
 *       --offset 0
 *
 *   # Read a specific partition from offset 1000
 *   java -jar tdvps-kafka-tool.jar consume \
 *       --topic cpa02_app_tds_tdvps_positions_egress_ged_positions \
 *       --partition 3 --offset 1000
 *
 *   # Seek to a specific time (epoch millis) — atTime
 *   java -jar tdvps-kafka-tool.jar consume \
 *       --topic cpa02_app_tds_tdvps_positions_egress_ged_positions \
 *       --at-time 1717891200000
 *
 *   # Read only 5 messages, compact output
 *   java -jar tdvps-kafka-tool.jar consume \
 *       --topic my-topic --max-records 5 --no-pretty
 *
 *   # Filter by key substring
 *   java -jar tdvps-kafka-tool.jar consume \
 *       --topic my-topic --filter traderID:SUPPORT1
 */
@Command(
        name = "consume",
        mixinStandardHelpOptions = true,
        description = {
                "",
                "Read and decode records from a TDVPS Kafka topic.",
                "Decodes record_body (bytes → UTF-8 → JSON) and pretty-prints payloads.",
                "Shuts down after @|yellow " + ConsumeCommand.MAX_EMPTY_POLLS_DESC + " consecutive empty polls|@.",
                ""
        }
)
public class ConsumeCommand implements Runnable {

    static final String MAX_EMPTY_POLLS_DESC = "10";
    private static final Logger log = LoggerFactory.getLogger(ConsumeCommand.class);

    // ── Required ──────────────────────────────────────────────────────────────

    @Option(names = {"-t", "--topic"}, required = true,
            description = "Kafka topic to consume from.\n"
                    + "Example: cpa02_app_tds_tdvps_positions_egress_ged_positions")
    private String topic;

    // ── Connection ───────────────────────────────────────────────────────────

    @Option(names = {"-b", "--bootstrap-servers"},
            description = "Kafka bootstrap servers. Default: ${DEFAULT-VALUE}",
            defaultValue = "localhost:9092")
    private String bootstrapServers;

    @Option(names = {"-g", "--group-id"},
            description = "Consumer group ID. Default: ${DEFAULT-VALUE}",
            defaultValue = "tdvps-support-tool")
    private String groupId;

    // ── Seek / Filter ────────────────────────────────────────────────────────

    @Option(names = {"-p", "--partition"},
            description = "Specific partition to read (default: all partitions). "
                    + "Use -1 for all.",
            defaultValue = "-1")
    private int partition;

    @Option(names = {"-o", "--offset"},
            description = "Start offset. 0 = earliest (beginning), N = exact offset. "
                    + "Omit to start from latest.",
            defaultValue = "-1")
    private long offset;

    @Option(names = {"--at-time"},
            description = "Seek to first record at or after this epoch-millisecond timestamp. "
                    + "Overrides --offset.",
            defaultValue = "-1")
    private long atTimeMs;

    @Option(names = {"-m", "--max-records"},
            description = "Stop after reading N records. 0 = unlimited.",
            defaultValue = "0")
    private int maxRecords;

    @Option(names = {"--filter"},
            description = "Filter by field in record_body. Format: key:value\n"
                    + "Example: --filter workflowStatus:Committed\n"
                    + "Matches exact field value (case-insensitive).")
    private String filterKey;

    // ── Output ───────────────────────────────────────────────────────────────

    @Option(names = {"--no-pretty"},
            description = "Compact JSON output (no indentation).")
    private boolean noP;

    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public void run() {
        ConsumerOptions opts = new ConsumerOptions()
                .topic(topic)
                .bootstrapServers(bootstrapServers)
                .groupId(groupId)
                .partition(partition)
                .offset(offset)
                .atTimeMs(atTimeMs)
                .maxRecords(maxRecords)
                .filterKey(filterKey)
                .pretty(!noP);

        log.info("Starting TDVPS consumer — topic={} partition={} offset={} atTimeMs={}",
                topic, partition, offset, atTimeMs);

        try (TdvpsKafkaConsumer consumer = new TdvpsKafkaConsumer(opts)) {
            consumer.consume();
        } catch (Exception e) {
            log.error("Consumer error: {}", e.getMessage(), e);
            System.err.println("ERROR: " + e.getMessage());
            System.exit(1);
        }
    }
}
