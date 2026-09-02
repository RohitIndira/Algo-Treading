#!/usr/bin/env bash
# Phase 3 — run ON THE NEW BOX with the transferred bundle:
#   ./03-restore.sh ~/manthan-migration-YYYYMMDD-HHMM
# Restores the 6 Postgres DBs and the Redis RDB into the compose containers,
# then verifies every table's row count against the manifest from the old box.
# Requires: docker compose stack up (00-new-server-setup.sh), PM2 services NOT started.
set -euo pipefail

BUNDLE=${1:?usage: 03-restore.sh <bundle-dir>}
DBS="trading_db signals_db execution_db positions_db order_status_db stockk_market"

echo "== integrity =="
(cd "$BUNDLE" && sha256sum -c SHA256SUMS --quiet) && echo "  checksums OK"

echo "== guard: no PM2 services running =="
running=$(pm2 jlist 2>/dev/null | jq '[.[] | select(.pm2_env.status=="online" and .name!="pm2-logrotate")] | length' || echo 0)
[ "$running" = "0" ] || { echo "ABORT: stop PM2 services before restoring."; exit 1; }

echo "== postgres restore =="
for db in $DBS; do
  echo "  restoring $db"
  docker exec algo-dev-postgres psql -U postgres -Atc \
    "SELECT 'CREATE DATABASE $db' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname='$db')\gexec" >/dev/null
  # --clean --if-exists: idempotent over the empty init-created schema
  docker exec -i algo-dev-postgres pg_restore -U postgres -d "$db" --clean --if-exists --no-owner < "$BUNDLE/$db.dump"
done

echo "== redis restore (manthan:* durable state) =="
# The compose service runs with --appendonly yes, so on a normal start Redis
# loads the (empty) AOF and IGNORES the RDB — and `docker cp` into the minimal
# alpine image fails ("Could not find /proc/self/fd"). So: drop the RDB into the
# named volume via its host path, load it in a TRANSIENT no-AOF container, enable
# AOF at runtime (which writes the loaded keys into a fresh AOF), then bring the
# real service up so it loads the now-populated AOF. (2026-09-02: the old
# cp+start+BGREWRITEAOF sequence silently produced an empty DB.)
COMPOSE=~/Algo-Treading/deployments/manthan-infra/docker-compose.yml
REDIS_VOL=$(docker inspect algo-dev-redis --format '{{range .Mounts}}{{.Name}}{{end}}')
REDIS_MP=$(docker volume inspect "$REDIS_VOL" -f '{{.Mountpoint}}')
docker compose -f "$COMPOSE" rm -sf algo-dev-redis >/dev/null 2>&1 || true
sudo rm -rf "$REDIS_MP/appendonlydir"
sudo cp "$BUNDLE/redis-dump.rdb" "$REDIS_MP/dump.rdb"
docker run -d --name redis-seed -v "${REDIS_VOL}":/data redis:7-alpine redis-server --appendonly no >/dev/null
sleep 3
seeded=$(docker exec redis-seed redis-cli DBSIZE)
docker exec redis-seed redis-cli CONFIG SET appendonly yes >/dev/null; sleep 1
docker exec redis-seed redis-cli BGREWRITEAOF >/dev/null; sleep 2
docker rm -f redis-seed >/dev/null
docker compose -f "$COMPOSE" up -d algo-dev-redis >/dev/null; sleep 3
echo "  redis loaded: $(docker exec algo-dev-redis redis-cli DBSIZE) keys (seed saw $seeded)"

echo "== verify against manifest =="
{
  for db in $DBS; do
    docker exec algo-dev-postgres psql -U postgres -d "$db" -Atc \
      "SELECT '$db.' || relname || '=' || n_live_tup FROM pg_stat_user_tables ORDER BY relname;"
  done
  echo "redis.keys=$(docker exec algo-dev-redis redis-cli DBSIZE | tr -dc 0-9)"
} > /tmp/manifest.new.txt

if diff -u "$BUNDLE/manifest.txt" /tmp/manifest.new.txt; then
  echo
  echo "RESTORE VERIFIED — every table and the redis keyspace match the source."
  echo "Next: start PM2 services, watch recovery/reconciler/safety-monitor startup,"
  echo "then switch the Cloudflare A record."
else
  echo
  echo "MISMATCH — see diff above. n_live_tup is an estimate; for any diverging"
  echo "table confirm with SELECT COUNT(*) on both sides before judging."
  exit 1
fi
