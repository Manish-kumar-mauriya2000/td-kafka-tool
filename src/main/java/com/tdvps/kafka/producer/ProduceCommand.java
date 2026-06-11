package com.tdvps.kafka.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * CLI subcommand: produce
 *
 * Sends sample TDVPS-schema messages to a topic for local testing.
 *
 * Usage examples:
 *
 *   # Send 5 position messages (default)
 *   java -jar tdvps-kafka-tool.jar produce \
 *       --topic cpa02_app_tds_tdvps_positions_egress_ged_positions
 *
 *   # Send 10 trade messages
 *   java -jar tdvps-kafka-tool.jar produce \
 *       --topic cpa02_app_tds_tdvps_positions_egress_ged_positions \
 *       --count 10 --type trade
 *
 *   # Custom bootstrap server
 *   java -jar tdvps-kafka-tool.jar produce \
 *       --topic my-topic --bootstrap-servers broker1:9092 --count 3
 */
@Command(
    name = "produce",
    mixinStandardHelpOptions = true,
    description = {
        "",
        "@|bold,yellow [TEST ONLY]|@ Sends sample TDVPS-schema messages to a Kafka topic.",
        "Use this to populate a local topic before running the consumer.",
        ""
    }
)
public class ProduceCommand implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(ProduceCommand.class);

    @Option(names = {"-t", "--topic"}, required = true,
            description = "Target Kafka topic.")
    private String topic;

    @Option(names = {"-b", "--bootstrap-servers"},
            description = "Kafka bootstrap servers. Default: ${DEFAULT-VALUE}",
            defaultValue = "localhost:9092")
    private String bootstrapServers;

    @Option(names = {"-c", "--count"},
            description = "Number of messages to send. Default: ${DEFAULT-VALUE}",
            defaultValue = "5")
    private int count;

    @Option(names = {"--type"},
            description = "Message type: position, trade, generic. Default: ${DEFAULT-VALUE}",
            defaultValue = "position")
    private String messageType;

    @Override
    public void run() {
        log.info("Producing {} '{}' messages to topic '{}' on {}",
            count, messageType, topic, bootstrapServers);

        try (TdvpsKafkaProducer producer = new TdvpsKafkaProducer(bootstrapServers)) {
            producer.init();
            producer.sendSamples(topic, count, messageType);
            System.out.printf("✓ Sent %d '%s' messages to topic '%s'%n",
                count, messageType, topic);
        } catch (Exception e) {
            log.error("Producer error: {}", e.getMessage(), e);
            System.err.println("ERROR: " + e.getMessage());
            System.exit(1);
        }
    }
}
