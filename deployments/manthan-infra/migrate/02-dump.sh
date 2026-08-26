#!/usr/bin/env bash
# Phase 3 — run ON THE OLD BOX after 01-quiesce-check.sh passes AND `pm2 stop all`.
# Produces one timestamped bundle: 6 Postgres dumps + the Redis RDB + row-count
# manifest for verification on the new box. Kafka data is deliberately NOT
# dumped: after a verified drain there is nothing unconsumed, topics auto-create
# on the new broker, and consumers resume cleanly from empty.
set -euo pipefail

TS=$(date +%Y%m%d-%H%M)
OUT=~/manthan-migration-$TS
mkdir -p "$OUT"
DBS="trading_db signals_db execution_db positions_db order_status_db stockk_market"

echo "== guard: everything stopped? =="
running=$(pm2 jlist | jq '[.[] | select(.pm2_env.status=="online" and .name!="pm2-logrotate")] | length')
if [ "$running" != "0" ]; then
  echo "ABORT: $running PM2 services still online. Run 'pm2 stop all' first."
  exit 1
fi

echo "== postgres dumps =="
for db in $DBS; do
  echo "  dumping $db"
  docker exec algo-dev-postgres pg_dump -U postgres -Fc "$db" > "$OUT/$db.dump"
done

echo "== redis RDB (durable manthan:* state) =="
docker exec algo-dev-redis redis-cli BGSAVE >/dev/null && sleep 2
docker cp algo-dev-redis:/data/dump.rdb "$OUT/redis-dump.rdb"

echo "== row-count manifest =="
{
  for db in $DBS; do
    docker exec algo-dev-postgres psql -U postgres -d "$db" -Atc \
      "SELECT '$db.' || relname || '=' || n_live_tup FROM pg_stat_user_tables ORDER BY relname;"
  done
  echo "redis.keys=$(docker exec algo-dev-redis redis-cli DBSIZE | tr -dc 0-9)"
} > "$OUT/manifest.txt"

sha256sum "$OUT"/* > "$OUT/SHA256SUMS"
echo
echo "Bundle ready: $OUT"
du -sh "$OUT"
echo "Transfer:  scp -r $OUT ubuntu@<NEW-IP>:~/"
echo "Then on the new box:  ./03-restore.sh ~/manthan-migration-$TS"
