#!/usr/bin/env bash
# =============================================================================
# tdvps-kafka.sh — convenience wrapper for the TDVPS Kafka utility
#
# Usage:
#   ./scripts/tdvps-kafka.sh build
#   ./scripts/tdvps-kafka.sh start-local
#   ./scripts/tdvps-kafka.sh produce --topic <topic> [--count N] [--type position|trade]
#   ./scripts/tdvps-kafka.sh consume --topic <topic> [options...]
#   ./scripts/tdvps-kafka.sh consume-from-start --topic <topic>
#   ./scripts/tdvps-kafka.sh consume-at-time --topic <topic> --at-time <epoch-ms>
#   ./scripts/tdvps-kafka.sh stop-local
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
JAR="$PROJECT_DIR/target/tdvps-kafka-tool-1.0.0-SNAPSHOT.jar"
DEFAULT_TOPIC="cpa02_app_tds_tdvps_positions_egress_ged_positions"
BOOTSTRAP="localhost:9092"

# ── Colours ──────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*" >&2; exit 1; }

# ── Helpers ───────────────────────────────────────────────────────────────────
require_jar() {
  if [[ ! -f "$JAR" ]]; then
    warn "JAR not found — building first..."
    do_build
  fi
}

do_build() {
  info "Building TDVPS Kafka tool..."
  cd "$PROJECT_DIR"
  mvn clean package -q -DskipTests
  info "Build complete → $JAR"
}

do_start_local() {
  info "Starting local Kafka (docker-compose)..."
  cd "$PROJECT_DIR"
  docker-compose up -d
  info "Waiting for Kafka to be ready..."
  sleep 5
  info "Kafka UI available at http://localhost:8080"
  info "Bootstrap: localhost:9092"
}

do_stop_local() {
  info "Stopping local Kafka..."
  cd "$PROJECT_DIR"
  docker-compose down
  info "Stopped."
}

# ── Command dispatch ──────────────────────────────────────────────────────────
CMD="${1:-help}"
shift || true

case "$CMD" in

  build)
    do_build
    ;;

  start-local)
    do_start_local
    ;;

  stop-local)
    do_stop_local
    ;;

  produce)
    require_jar
    info "Producing messages..."
    java -jar "$JAR" produce --bootstrap-servers "$BOOTSTRAP" "$@"
    ;;

  consume)
    require_jar
    info "Starting consumer (Ctrl-C to stop)..."
    java -jar "$JAR" consume --bootstrap-servers "$BOOTSTRAP" "$@"
    ;;

  # Shortcut: read all messages from beginning
  consume-from-start)
    require_jar
    TOPIC="${2:-$DEFAULT_TOPIC}"
    info "Reading all messages from beginning of '$TOPIC'..."
    java -jar "$JAR" consume \
      --bootstrap-servers "$BOOTSTRAP" \
      --topic "$TOPIC" \
      --offset 0
    ;;

  # Shortcut: read from a timestamp
  consume-at-time)
    require_jar
    TOPIC=""
    AT_TIME=""
    while [[ $# -gt 0 ]]; do
      case "$1" in
        --topic)    TOPIC="$2";   shift 2 ;;
        --at-time)  AT_TIME="$2"; shift 2 ;;
        *) shift ;;
      esac
    done
    [[ -z "$TOPIC" ]]   && error "--topic is required"
    [[ -z "$AT_TIME" ]] && error "--at-time (epoch ms) is required"
    info "Reading '$TOPIC' from epoch-ms $AT_TIME..."
    java -jar "$JAR" consume \
      --bootstrap-servers "$BOOTSTRAP" \
      --topic "$TOPIC" \
      --at-time "$AT_TIME"
    ;;

  # Quick local test: produce then consume
  test-local)
    TOPIC="${1:-$DEFAULT_TOPIC}"
    require_jar
    info "=== LOCAL TEST ==="
    info "Step 1: Starting local Kafka..."
    do_start_local

    info "Step 2: Sending 3 position + 2 trade messages..."
    java -jar "$JAR" produce \
      --bootstrap-servers "$BOOTSTRAP" \
      --topic "$TOPIC" --count 3 --type position
    java -jar "$JAR" produce \
      --bootstrap-servers "$BOOTSTRAP" \
      --topic "$TOPIC" --count 2 --type trade

    info "Step 3: Consuming (will stop after 10 empty polls)..."
    java -jar "$JAR" consume \
      --bootstrap-servers "$BOOTSTRAP" \
      --topic "$TOPIC" \
      --offset 0
    ;;

  help|*)
    echo ""
    echo "  TDVPS Kafka Admin/Support Utility — Shell Wrapper"
    echo ""
    echo "  Commands:"
    echo "    build                          Build the fat JAR"
    echo "    start-local                    Start local Kafka via docker-compose"
    echo "    stop-local                     Stop local Kafka"
    echo "    produce  --topic T [opts]      Send test messages"
    echo "    consume  --topic T [opts]      Read and decode messages"
    echo "    consume-from-start --topic T   Read all messages from offset 0"
    echo "    consume-at-time --topic T --at-time EPOCH_MS"
    echo "    test-local [topic]             Full local smoke test"
    echo ""
    echo "  Consumer options:"
    echo "    --topic         Topic name (required)"
    echo "    --partition     Specific partition (-1 = all)"
    echo "    --offset        Start offset  (0 = earliest)"
    echo "    --at-time       Epoch-ms timestamp seek"
    echo "    --max-records   Stop after N records"
    echo "    --filter-key    Only show records matching key"
    echo "    --no-pretty     Compact JSON output"
    echo ""
    echo "  Producer options:"
    echo "    --topic         Topic name (required)"
    echo "    --count         Number of messages (default: 5)"
    echo "    --type          position | trade | generic (default: position)"
    echo ""
    ;;
esac
