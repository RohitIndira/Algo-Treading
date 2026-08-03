#!/usr/bin/env bash
# Apply each service's SQL migrations to the DB it owns, in the algo-dev
# separate-namespace Postgres. Run ONCE against fresh DBs (migrations are not
# all idempotent). Run from the repo root, after `docker compose up postgres`.
#
#   cd deployments/separate-namespace && ./apply-migrations.sh
#
# Ownership (docs/db_ownership.md):
#   trading_db      <- user-config (strategies/configs) THEN rules-engine (manthan_*)
#   execution_db    <- trade-execution (orders, user_credentials, manthan_orders)
#   order_status_db <- orderstatus (broker_events)
#   positions_db    <- positions
#   signals_db      <- data-ingestion (so rules-engine reads don't error)
set -euo pipefail

PG=algo-dev-postgres
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

apply() {
  local db="$1"; shift
  echo ""
  echo "═══ applying to $db ═══"
  for svc in "$@"; do
    for f in $(ls "services/$svc/migrations/"*.sql 2>/dev/null | sort); do
      echo "  → $db  <=  $svc/$(basename "$f")"
      docker exec -i "$PG" psql -v ON_ERROR_STOP=1 -U postgres -d "$db" < "$f"
    done
  done
}

# Order matters where two services co-own a DB: user-config lays the base
# strategy schema, then rules-engine adds its disjoint manthan_* tables.
apply trading_db      user-config rules-engine
apply execution_db    trade-execution
apply order_status_db orderstatus
apply positions_db    positions
apply signals_db      data-ingestion

echo ""
echo "✓ migrations applied. Per-DB table counts:"
for db in trading_db execution_db order_status_db positions_db signals_db; do
  n=$(docker exec -i "$PG" psql -tAqc "SELECT count(*) FROM information_schema.tables WHERE table_schema='public'" -U postgres -d "$db")
  echo "   $db: $n tables"
done
