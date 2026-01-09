#!/usr/bin/env bash

# Simple helper script to start the Kafka stack for the trading system
# **and** make sure all core topics used in this codebase exist.
#
# Usage:
#   cd "$(dirname "$0")"
#   ./run_kafka.sh
#
# This will:
#   1) Start Zookeeper + Kafka from docker-compose-kafka.yml
#   2) Wait for Kafka to be ready
#   3) Create (if missing) the main topics:
#        - market.data.news
#        - market.data.52w_breakouts
#        - user-configs
#        - trade-signals

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "[run_kafka] Starting Kafka using docker-compose-kafka.yml ..."
docker compose -f docker-compose-kafka.yml up -d

echo "[run_kafka] Waiting for Kafka broker to become ready ..."
# Simple readiness loop: try listing topics until it succeeds or timeout
for i in {1..30}; do
  if docker compose -f docker-compose-kafka.yml exec -T kafka \
    kafka-topics --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
    echo "[run_kafka] Kafka is ready."
    break
  fi
  echo "[run_kafka] Kafka not ready yet, retrying ($i/30) ..."
  sleep 2
done

TOPICS=(
  "market.data.news"
  "market.data.52w_breakouts"
  "user-configs"
  "trade-signals"
  "portfolio.allocations"
)

echo "[run_kafka] Ensuring core topics exist ..."
for topic in "${TOPICS[@]}"; do
  echo "[run_kafka] Ensuring topic '$topic' exists ..."
  docker compose -f docker-compose-kafka.yml exec -T kafka \
    kafka-topics --create --if-not-exists \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 --partitions 3 \
    --topic "$topic"
done

echo "[run_kafka] Kafka stack and topics are ready. You can now start services."
