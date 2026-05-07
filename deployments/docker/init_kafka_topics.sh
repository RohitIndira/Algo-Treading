#!/bin/bash
# init_kafka_topics.sh — create all Kafka topics with correct partition counts.
# Run once after Kafka is healthy, before starting any services.
#
# Usage:
#   ./init_kafka_topics.sh                         # uses kafka:29092 (Docker network)
#   KAFKA_BROKER=localhost:9092 ./init_kafka_topics.sh  # local dev

set -e

KAFKA_BROKER="${KAFKA_BROKER:-kafka:29092}"
REPLICATION_FACTOR="${REPLICATION_FACTOR:-1}"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()  { echo -e "${GREEN}[KAFKA-INIT]${NC} $*"; }
warn()  { echo -e "${YELLOW}[KAFKA-INIT]${NC} $*"; }

wait_for_kafka() {
    info "Waiting for Kafka at ${KAFKA_BROKER}..."
    local attempts=0
    until kafka-topics --bootstrap-server "${KAFKA_BROKER}" --list &>/dev/null; do
        attempts=$(( attempts + 1 ))
        if [ "${attempts}" -ge 30 ]; then
            echo "[KAFKA-INIT] ERROR: Kafka not reachable after 60s" >&2
            exit 1
        fi
        sleep 2
    done
    info "Kafka is ready."
}

create_topic() {
    local topic="$1"
    local partitions="$2"
    local retention_ms="${3:--1}"   # -1 = keep forever (use topic default)

    if kafka-topics --bootstrap-server "${KAFKA_BROKER}" --describe --topic "${topic}" &>/dev/null; then
        warn "Topic '${topic}' already exists — skipping."
        return 0
    fi

    kafka-topics \
        --bootstrap-server "${KAFKA_BROKER}" \
        --create \
        --topic "${topic}" \
        --partitions "${partitions}" \
        --replication-factor "${REPLICATION_FACTOR}" \
        --config "retention.ms=${retention_ms}"

    info "Created topic '${topic}' with ${partitions} partitions."
}

wait_for_kafka

# ── Topics ───────────────────────────────────────────────────────────────────
# news-events: rules-engine reads this — 10 partitions supports up to 10
#              parallel consumer instances (2 PM2 × 5 partitions each).
create_topic "news-events"          10

# trade-signals: trade-execution reads this — 20 partitions supports up to
#                20 parallel consumer instances.
create_topic "trade-signals"        20

# user-config-events: consumed by rules-engine + trade-execution for config sync.
# Low volume, 5 partitions is plenty.
create_topic "user-config-events"   5

# order-updates: published by trade-execution for downstream consumers.
create_topic "order-updates"        10

info "All topics created."
kafka-topics --bootstrap-server "${KAFKA_BROKER}" --list
