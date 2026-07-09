#!/usr/bin/env bash
# sync_from_staging.sh — one-way sync of small tables from staging to local.
#
# Read-only on staging (pg_dump --data-only).
# Truncate + insert locally, only for the specific tables listed.
#
# Prereqs:
#   - Local Postgres running on localhost:5432 with user 'postgres' / pass 'postgres'
#   - SSH access to `manthan` via ssh-agent (`ssh-add ~/Downloads/websocket-stagging.pem`)
#   - Local Postgres already has the same schemas (created via app migrations)
#
# Usage:
#   ./scripts/sync_from_staging.sh
#
# Safety:
#   - Never writes to staging
#   - Only truncates the exact tables listed in the JOBS array
#   - Uses --set ON_ERROR_STOP=on so partial failures roll back

set -euo pipefail

# ────────────────────────────────────────────────────────────────────
# Config
# ────────────────────────────────────────────────────────────────────
STAGING_SSH_HOST="${STAGING_SSH_HOST:-manthan}"
STAGING_PG_CONTAINER="${STAGING_PG_CONTAINER:-tsdb_live}"

LOCAL_PG_HOST="${LOCAL_PG_HOST:-localhost}"
LOCAL_PG_PORT="${LOCAL_PG_PORT:-5432}"
LOCAL_PG_USER="${LOCAL_PG_USER:-postgres}"
LOCAL_PG_PASS="${LOCAL_PG_PASS:-postgres}"

DUMPDIR="${DUMPDIR:-/tmp/staging_sync_$(date +%Y%m%d_%H%M%S)}"
mkdir -p "$DUMPDIR"

# SSH options: identity from agent (not the encrypted file); no shell input; batch mode
SSH_OPTS=(-o IdentitiesOnly=no -o BatchMode=yes -o ConnectTimeout=10)

# ────────────────────────────────────────────────────────────────────
# Tables to sync — format: "source_db|target_db|table"
#
# Staging uses 3 separate DBs (trading_db, trading_execution, market_data).
# Locally we consolidate everything into the stockk_* family so all
# downstream code references only stockk_trading + stockk_market. The
# staging PM2 config keeps its original DB names — this is a local-only
# organizational choice (2026-07-06).
#
# Ordering matters for FK constraints — parent tables first.
# ────────────────────────────────────────────────────────────────────
JOBS=(
  "trading_db|stockk_trading|strategies"
  "trading_db|stockk_trading|trade_configs"
  "trading_db|stockk_trading|manthan_positions"
  "trading_execution|stockk_trading|manthan_orders"
  "market_data|stockk_market|manthan_stocks"
)

# ────────────────────────────────────────────────────────────────────
# Colours (nice for scanning the log)
# ────────────────────────────────────────────────────────────────────
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log()  { printf "${GREEN}[sync] %s${NC}\n" "$*"; }
warn() { printf "${YELLOW}[sync] %s${NC}\n" "$*" >&2; }
err()  { printf "${RED}[sync] %s${NC}\n" "$*" >&2; }

# ────────────────────────────────────────────────────────────────────
# Pre-flight checks
# ────────────────────────────────────────────────────────────────────
log "── pre-flight ──"

# Verify SSH works (silent, non-interactive)
if ! DISPLAY= SSH_ASKPASS= ssh "${SSH_OPTS[@]}" "$STAGING_SSH_HOST" \
    "docker exec -i $STAGING_PG_CONTAINER psql -U postgres -c 'SELECT 1' > /dev/null 2>&1"; then
  err "SSH or staging Postgres check failed."
  err "Make sure: (1) ssh-add ~/Downloads/websocket-stagging.pem has been run"
  err "           (2) the tsdb_live container is running on staging"
  exit 1
fi
log "  SSH + staging Postgres reachable ✓"

# Verify local Postgres
if ! PGPASSWORD="$LOCAL_PG_PASS" psql -h "$LOCAL_PG_HOST" -p "$LOCAL_PG_PORT" -U "$LOCAL_PG_USER" \
    -d postgres -c 'SELECT 1' > /dev/null 2>&1; then
  err "Local Postgres check failed at $LOCAL_PG_HOST:$LOCAL_PG_PORT"
  exit 1
fi
log "  Local Postgres reachable ✓"

# ────────────────────────────────────────────────────────────────────
# Sync loop
# ────────────────────────────────────────────────────────────────────
TOTAL_ROWS_COPIED=0

for job in "${JOBS[@]}"; do
  IFS='|' read -r src_db tgt_db tbl <<< "$job"

  log "── ${src_db}.${tbl}  →  ${tgt_db}.${tbl} ──"

  # 1. dump from staging (read-only pg_dump)
  dump_file="$DUMPDIR/${tgt_db}__${tbl}.sql"
  DISPLAY= SSH_ASKPASS= ssh "${SSH_OPTS[@]}" "$STAGING_SSH_HOST" \
    "docker exec -i $STAGING_PG_CONTAINER pg_dump \
       -U postgres -d $src_db \
       --data-only --no-owner --column-inserts \
       -t public.$tbl" \
    > "$dump_file"

  bytes=$(wc -c < "$dump_file")
  log "  dumped $bytes bytes to $dump_file"

  # 2. TRUNCATE the specific table locally (only this table, not CASCADE)
  #    Wrapped in DO block so FK-related errors are surfaced clearly.
  PGPASSWORD="$LOCAL_PG_PASS" psql \
    -h "$LOCAL_PG_HOST" -p "$LOCAL_PG_PORT" -U "$LOCAL_PG_USER" \
    -d "$tgt_db" \
    --set ON_ERROR_STOP=on \
    -c "TRUNCATE TABLE public.$tbl RESTART IDENTITY CASCADE;" \
    > /dev/null
  log "  local $tbl truncated ✓"

  # 3. Restore
  #    ON_ERROR_STOP so a broken INSERT fails the whole run.
  #    session_replication_role=replica disables FK + CHECK triggers just
  #    for this session — critical for tables with self-FK (e.g.
  #    manthan_orders.parent_order_id → manthan_orders.id) where the
  #    row-by-row insert order can't satisfy the constraint mid-file.
  {
    echo "SET session_replication_role = replica;"
    cat "$dump_file"
  } | PGPASSWORD="$LOCAL_PG_PASS" psql \
    -h "$LOCAL_PG_HOST" -p "$LOCAL_PG_PORT" -U "$LOCAL_PG_USER" \
    -d "$tgt_db" \
    --set ON_ERROR_STOP=on \
    -q

  # 4. Verify
  rows=$(PGPASSWORD="$LOCAL_PG_PASS" psql \
    -h "$LOCAL_PG_HOST" -p "$LOCAL_PG_PORT" -U "$LOCAL_PG_USER" \
    -d "$tgt_db" -t -c "SELECT COUNT(*) FROM public.$tbl;" | tr -d ' ')

  log "  restored: ${rows} rows ✓"
  TOTAL_ROWS_COPIED=$(( TOTAL_ROWS_COPIED + rows ))
done

echo
log "────────────────────────────────────────"
log "sync complete — $TOTAL_ROWS_COPIED rows total across ${#JOBS[@]} tables"
log "dumps saved to: $DUMPDIR"
log "────────────────────────────────────────"
