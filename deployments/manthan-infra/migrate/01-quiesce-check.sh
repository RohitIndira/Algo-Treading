#!/usr/bin/env bash
# Phase 3 gate — run ON THE OLD BOX after `pm2 stop data-ingestion rules-engine`.
# Verifies the pipeline is fully drained so DB+Kafka+Redis can move as one
# consistent snapshot (2026-07-17 lesson: a half-drained move replays stale
# events into positions_db). Exits non-zero if ANY check fails.
set -uo pipefail
PG="docker exec algo-dev-postgres psql -U postgres -Atc"
FAIL=0

check() { # <label> <got> <want>
  if [ "$2" = "$3" ]; then echo "  OK   $1 = $2"
  else echo "  FAIL $1 = $2 (want $3)"; FAIL=1; fi
}

echo "== 1. signal producers stopped =="
for s in data-ingestion rules-engine; do
  st=$(pm2 jlist | jq -r ".[] | select(.name==\"$s\") | .pm2_env.status")
  check "pm2 $s" "$st" "stopped"
done

echo "== 2. signal inbox drained =="
# Terminal inbox states are DONE and DLQ; PENDING/RUNNING are in-flight and
# FAILED rows still get retried — none of the three may exist at cutover.
pending=$($PG -d execution_db "SELECT COUNT(*) FROM signal_inbox WHERE status NOT IN ('DONE','DLQ');" 2>/dev/null || echo "ERR")
check "in-flight/retryable inbox rows" "$pending" "0"

echo "== 3. no in-flight orders today =="
inflight=$($PG -d execution_db "SELECT COUNT(*) FROM manthan_orders WHERE trade_date = CURRENT_DATE AND status IN ('PENDING','PLACED','PARTIAL','SL_TRIGGERED');")
check "non-terminal orders today" "$inflight" "0"

echo "== 4. consumer lag zero on every group =="
LAG=$(docker exec algo-dev-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --all-groups 2>/dev/null |
      awk 'NR>1 && $6 ~ /^[0-9]+$/ { sum += $6 } END { print sum+0 }')
check "total consumer lag" "$LAG" "0"

echo "== 5. redis persisted =="
docker exec algo-dev-redis redis-cli BGSAVE >/dev/null
sleep 2
persist=$(docker exec algo-dev-redis redis-cli LASTSAVE)
echo "  OK   redis LASTSAVE = $persist"

echo
if [ "$FAIL" = 0 ]; then
  echo "QUIESCED — safe to 'pm2 stop all' and run 02-dump.sh"
else
  echo "NOT QUIESCED — fix the FAIL lines before dumping. Do not proceed."
  exit 1
fi
