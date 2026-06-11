package com.tdvps.kafka;

import com.tdvps.kafka.consumer.ConsumeCommand;
import com.tdvps.kafka.producer.ProduceCommand;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;

/**
 * TDVPS Kafka Admin/Support Utility
 *
 * Entry point. Dispatches to subcommands:
 *   consume  — flexible Kafka topic reader with filtering
 *   produce  — test message producer
 *
 * Build:  mvn clean package
 * Run:    java -jar target/tdvps-kafka-tool-1.0.0-SNAPSHOT.jar <subcommand> [options]
 */
@Command(
    name = "tdvps-kafka",
    mixinStandardHelpOptions = true,
    version = "1.0.0",
    description = {
        "",
        "@|bold,cyan TDVPS Kafka Admin / Support Utility|@",
        "Reads, decodes and displays TDVPS Kafka records.",
        "Supports filtering by topic, partition, offset and timestamp.",
        ""
    },
    subcommands = {
        ConsumeCommand.class,
        ProduceCommand.class,
        HelpCommand.class
    }
)
public class TdvpsKafkaTool implements Runnable {

    @Override
    public void run() {
        // No subcommand → print help
        CommandLine.usage(this, System.out);
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new TdvpsKafkaTool())
            .setColorScheme(CommandLine.Help.defaultColorScheme(CommandLine.Help.Ansi.AUTO))
            .execute(args);
        System.exit(exitCode);
    }
}
