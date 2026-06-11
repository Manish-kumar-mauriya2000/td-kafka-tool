#!/usr/bin/env bash
# =============================================================================
# run-kafka-ui.sh — Start Kafka UI locally without Docker
#
# Prerequisites:
#   Java 11+  (java -version to check)
#
# Usage:
#   chmod +x run-kafka-ui.sh
#   ./run-kafka-ui.sh
#
# Then open: http://localhost:8080
# =============================================================================

set -euo pipefail

KAFKA_UI_VERSION="0.7.2"
JAR_NAME="kafka-ui-api-v${KAFKA_UI_VERSION}.jar"
DOWNLOAD_URL="https://github.com/provectuslabs/kafka-ui/releases/download/v${KAFKA_UI_VERSION}/${JAR_NAME}"

# ── Change these if your Kafka is on a different host/port ───────────────────
BOOTSTRAP_SERVERS="localhost:9092"
CLUSTER_NAME="local-kraft"
UI_PORT="8080"
# ─────────────────────────────────────────────────────────────────────────────

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }

# Download JAR if not already present
if [[ ! -f "$JAR_NAME" ]]; then
  info "Downloading Kafka UI v${KAFKA_UI_VERSION}..."
  info "URL: $DOWNLOAD_URL"

  if command -v curl &>/dev/null; then
    curl -L --progress-bar -o "$JAR_NAME" "$DOWNLOAD_URL"
  elif command -v wget &>/dev/null; then
    wget --show-progress -O "$JAR_NAME" "$DOWNLOAD_URL"
  else
    echo "ERROR: Neither curl nor wget found. Please download manually:"
    echo "  $DOWNLOAD_URL"
    exit 1
  fi
  info "Downloaded → $JAR_NAME"
else
  info "JAR already present → $JAR_NAME"
fi

info "Starting Kafka UI..."
info "  Cluster : $CLUSTER_NAME"
info "  Kafka   : $BOOTSTRAP_SERVERS"
info "  UI      : http://localhost:${UI_PORT}"
info ""
info "Press Ctrl-C to stop."
echo ""

java -Xms64m -Xmx256m \
  -Dspring.config.additional-location=optional:./kafka-ui.yml \
  -Dkafka.clusters.0.name="${CLUSTER_NAME}" \
  -Dkafka.clusters.0.bootstrapServers="${BOOTSTRAP_SERVERS}" \
  -Dserver.port="${UI_PORT}" \
  -jar "$JAR_NAME"
