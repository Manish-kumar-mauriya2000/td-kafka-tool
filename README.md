# TDVPS Kafka Admin / Support Utility

A local Kafka consumer/producer tool for TDVPS PAT ecosystem support and troubleshooting.
Built while waiting for full project/environment access.

---

## What it does

| Component | Purpose |
|-----------|---------|
| **Consumer** | Reads records from a TDVPS Kafka topic, decodes `record_body` (bytes → UTF-8 → JSON), pretty-prints payloads with key support fields highlighted |
| **Producer** | Sends sample TDVPS-schema messages for local testing (position, trade, generic types) |
| **Empty-poll shutdown** | Consumer stops gracefully after 10 consecutive empty polls |
| **Seek options** | Start from earliest, exact offset, specific partition, or timestamp (`atTime`) |

---

## Project structure

```
tdvps-kafka-tool/
├── pom.xml
├── docker-compose.yml              ← Local Kafka (KRaft, no ZooKeeper) + Kafka UI
├── scripts/
│   └── tdvps-kafka.sh             ← Convenience wrapper script
└── src/
    └── main/java/com/tdvps/kafka/
        ├── TdvpsKafkaTool.java     ← Main entry point (PicoCLI dispatcher)
        ├── consumer/
        │   ├── ConsumeCommand.java ← CLI subcommand wiring
        │   └── TdvpsKafkaConsumer.java  ← Core consumer logic
        ├── producer/
        │   ├── ProduceCommand.java
        │   └── TdvpsKafkaProducer.java
        ├── model/
        │   ├── TdvpsRecord.java    ← TDVPS message schema
        │   └── ConsumerOptions.java
        └── util/
            ├── RecordBodyDecoder.java  ← bytes → UTF-8 → JSON
            └── RecordPrinter.java      ← formatted output
```

---

## Prerequisites

- Java 17+
- Maven 3.8+
- Docker + Docker Compose (for local Kafka)

---

## Quick start

### 1. Build

```bash
mvn clean package
# or
./scripts/tdvps-kafka.sh build
```

Produces: `target/tdvps-kafka-tool-1.0.0-SNAPSHOT.jar` (fat/uber JAR)

---

### 2. Start local Kafka

```bash
./scripts/tdvps-kafka.sh start-local
# or directly:
docker-compose up -d
```

- Kafka: `localhost:9092`
- Kafka UI: http://localhost:8080

---

### 3. Send test messages (producer)

```bash
# 5 position messages (default)
java -jar target/tdvps-kafka-tool-1.0.0-SNAPSHOT.jar produce \
  --topic cpa02_app_tds_tdvps_positions_egress_ged_positions

# 10 trade messages
java -jar target/tdvps-kafka-tool-1.0.0-SNAPSHOT.jar produce \
  --topic cpa02_app_tds_tdvps_positions_egress_ged_positions \
  --count 10 --type trade
```

Available `--type` values: `position`, `trade`, `generic`

---

### 4. Consume messages

#### Read latest (default — waits for new messages)
```bash
java -jar target/tdvps-kafka-tool-1.0.0-SNAPSHOT.jar consume \
  --topic cpa02_app_tds_tdvps_positions_egress_ged_positions
```

#### Read from beginning (all history)
```bash
java -jar target/tdvps-kafka-tool-1.0.0-SNAPSHOT.jar consume \
  --topic cpa02_app_tds_tdvps_positions_egress_ged_positions \
  --offset 0
```

#### Read a specific partition from a specific offset
```bash
java -jar target/tdvps-kafka-tool-1.0.0-SNAPSHOT.jar consume \
  --topic cpa02_app_tds_tdvps_positions_egress_ged_positions \
  --partition 3 --offset 1000
```

#### Seek to a timestamp (`atTime`)
```bash
# Convert datetime to epoch millis first, e.g. 2026-06-09T04:00:00Z → 1749434400000
java -jar target/tdvps-kafka-tool-1.0.0-SNAPSHOT.jar consume \
  --topic cpa02_app_tds_tdvps_positions_egress_ged_positions \
  --at-time 1749434400000
```

#### Read only N records
```bash
java -jar target/tdvps-kafka-tool-1.0.0-SNAPSHOT.jar consume \
  --topic cpa02_app_tds_tdvps_positions_egress_ged_positions \
  --offset 0 --max-records 5
```

#### Filter by key
```bash
java -jar target/tdvps-kafka-tool-1.0.0-SNAPSHOT.jar consume \
  --topic cpa02_app_tds_tdvps_positions_egress_ged_positions \
  --filter-key SUPPORT1
```

#### Compact output (no pretty-print)
```bash
java -jar target/tdvps-kafka-tool-1.0.0-SNAPSHOT.jar consume \
  --topic cpa02_app_tds_tdvps_positions_egress_ged_positions \
  --no-pretty
```

---

### 5. Full local smoke test

```bash
./scripts/tdvps-kafka.sh test-local
```

Starts Kafka → sends 5 messages → consumes them all → shuts down.

---

## Consumer output format

```
─────────────────────────────────────────────────────────────────────────────
  Record #1      Topic: cpa02_app_tds_tdvps_positions_egress_ged_positions   Partition: 0  Offset: 42
─────────────────────────────────────────────────────────────────────────────
  [Envelope]
    record_uuid           : f3a1b2c4-...
    dataset_storage_uuid  : d5e6f7a8-...
    record_body_compression: NONE
    record_timestamp      : 1749434400.000000000s (epoch)  [2026-06-09 04:00:00.000 UTC]
  [Kafka Metadata]
    kafka_timestamp       : 2026-06-09 04:00:35.999 UTC
  [record_body — Decoded Payload]
  ┌─ Key fields:
  │  status                   : Settled
  │  triggerEventId           : 298974559
  │  instrumentId             : 697827417
  │  kafkaTopic               : cpa02_app_tds_tdvps_positions_egress_ged_positions
  │  kafkaOffset              : 148251599
  │  kafkaPartition           : 8
  │  settledCash:
  │    amount=-9656932.397  currency=USD
  └─
  [Full JSON]
    {
      "status" : "Settled",
      ...
    }
```

---

## TDVPS Topic Schema

```
Schema.newBuilder()
  .column("record_uuid",             DataTypes.STRING())
  .column("record_timestamp",        DataTypes.ROW(
      DataTypes.FIELD("seconds",     DataTypes.BIGINT()),
      DataTypes.FIELD("nanos",       DataTypes.INT())
  ))
  .column("dataset_storage_uuid",    DataTypes.STRING())
  .column("record_body",             DataTypes.BYTES())
  .column("record_body_compression", DataTypes.STRING())
  .build();
```

`record_body` is the primary payload: UTF-8 encoded JSON string on the wire.

---

## CLI reference

```
java -jar tdvps-kafka-tool.jar --help
java -jar tdvps-kafka-tool.jar consume --help
java -jar tdvps-kafka-tool.jar produce --help
```

---

## Running tests

```bash
mvn test
```

Tests cover `RecordBodyDecoder`: plain JSON, GZIP compressed, null/empty input, invalid JSON, unknown compression.

---

## Connecting to a real environment (future)

When you get access to the PAT environment, update `--bootstrap-servers`:

```bash
java -jar tdvps-kafka-tool.jar consume \
  --bootstrap-servers broker1.internal:9092,broker2.internal:9092 \
  --topic cpa02_app_tds_tdvps_positions_egress_ged_positions \
  --offset 0
```

SSL/SASL config can be added to `TdvpsKafkaConsumer.buildKafkaConsumer()` using standard Kafka client properties.

---

## Stop local Kafka

```bash
./scripts/tdvps-kafka.sh stop-local
# or
docker-compose down
```
