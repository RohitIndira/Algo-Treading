#!/usr/bin/env bash
#
# Manthan fresh-test reset
#
# Wipes per-test data across all 3 Postgres databases + local Redis so a fresh
# trading day can start from a known clean state. Reference data is preserved.
#
# What gets WIPED:
#   trading_db        — strategies + child tables (configs/limits/conditions),
#                       execution_outbox, trade_signals, all manthan_* runtime
#                       tables (signal_decisions / position_events / positions /
#                       portfolio_state / cooldown)
#   trading_execution — manthan_orders + manthan_order_events, orders +
#                       execution_events, optionally user_credentials
#   market_data       — manthan_signals (today's eligibility), manthan_stocks
#                       (per-day audit)
#   Local Redis       — manthan:* and isin:* keys (only if local Redis is up)
#
# What stays UNTOUCHED:
#   trading_db schemas + indexes + migrations
#   market_data.instruments (NSE/BSE master)
#   market_data.daily_ohlcv + partitions (price history)
#   market_data.breakout_events, data_load_log
#   Indira prod Redis (15.207.203.46) — never touched, that's their prod
#
# Usage:
#   ./scripts/manthan-fresh-test.sh                    # interactive prompt
#   ./scripts/manthan-fresh-test.sh --yes              # skip confirmation
#   ./scripts/manthan-fresh-test.sh --keep-creds       # don't flush user_credentials
#   ./scripts/manthan-fresh-test.sh --dry-run          # report only, no DELETEs
#   ./scripts/manthan-fresh-test.sh --yes --keep-creds # safe re-runnable mode
#
# Safety:
#   - Refuses to run in --yes mode unless a confirmation flag (--yes) is set.
#   - Each TRUNCATE wraps in BEGIN/COMMIT — partial failure leaves data intact.
#   - --dry-run prints row counts but never modifies anything.
#   - Indira prod Redis is read-only verified at the end (sanity check).

set -euo pipefail

# --- defaults ---
ASSUME_YES=0
KEEP_CREDS=0
DRY_RUN=0
PG_HOST="${PG_HOST:-localhost}"
PG_USER="${PG_USER:-postgres}"
PG_PASS="${PG_PASS:-postgres}"
LOCAL_REDIS_HOST="${LOCAL_REDIS_HOST:-localhost}"
INDIRA_REDIS_HOST="${INDIRA_REDIS_HOST:-15.207.203.46}"
INDIRA_REDIS_PASS="${INDIRA_REDIS_PASS:-R3d1s@Prod#2026}"

# --- argv parsing ---
for arg in "$@"; do
    case "$arg" in
        --yes)         ASSUME_YES=1 ;;
        --keep-creds)  KEEP_CREDS=1 ;;
        --dry-run)     DRY_RUN=1 ;;
        -h|--help)
            sed -n '2,40p' "$0"
            exit 0 ;;
        *)
            echo "unknown flag: $arg (use --help)" >&2
            exit 2 ;;
    esac
done

# --- colors ---
if [[ -t 1 ]]; then
    R='\033[0;31m' G='\033[0;32m' Y='\033[1;33m' B='\033[1;34m' NC='\033[0m'
else
    R='' G='' Y='' B='' NC=''
fi

# --- pg helper: run SQL + print ---
pg() {
    local db="$1" sql="$2"
    PGPASSWORD="$PG_PASS" psql -h "$PG_HOST" -U "$PG_USER" -d "$db" -v ON_ERROR_STOP=1 -c "$sql"
}

# --- step header ---
hdr() { echo -e "\n${B}=== $* ===${NC}"; }

# ---------------------------------------------------------
# Step 1 — Show what's about to happen
# ---------------------------------------------------------
hdr "Pre-flight — current row counts"

pg trading_db "
SELECT 'trading_db' AS db,
  (SELECT count(*) FROM strategies)               AS strategies,
  (SELECT count(*) FROM trade_configs)            AS trade_cfg,
  (SELECT count(*) FROM execution_outbox)         AS outbox,
  (SELECT count(*) FROM trade_signals)            AS trade_sig,
  (SELECT count(*) FROM manthan_signal_decisions) AS msd,
  (SELECT count(*) FROM manthan_position_events)  AS mpe,
  (SELECT count(*) FROM manthan_positions)        AS mp,
  (SELECT count(*) FROM manthan_portfolio_state)  AS mps,
  (SELECT count(*) FROM manthan_cooldown)         AS mc;"

pg trading_execution "
SELECT 'trading_execution' AS db,
  (SELECT count(*) FROM manthan_orders)         AS m_orders,
  (SELECT count(*) FROM manthan_order_events)   AS m_events,
  (SELECT count(*) FROM orders)                 AS orders,
  (SELECT count(*) FROM execution_events)       AS exec_events,
  (SELECT count(*) FROM user_credentials)       AS creds;"

pg market_data "
SELECT 'market_data' AS db,
  (SELECT count(*) FROM manthan_signals) AS signals,
  (SELECT count(*) FROM manthan_stocks)  AS stocks,
  (SELECT count(*) FROM instruments)     AS instruments_KEPT;"

if redis-cli -h "$LOCAL_REDIS_HOST" PING > /dev/null 2>&1; then
    local_manthan=$(redis-cli -h "$LOCAL_REDIS_HOST" --scan --pattern 'manthan:*' | wc -l)
    local_isin=$(redis-cli -h "$LOCAL_REDIS_HOST" --scan --pattern 'isin:*' | wc -l)
    echo -e "Local Redis (${LOCAL_REDIS_HOST})  manthan:* = ${local_manthan}   isin:* = ${local_isin}"
else
    echo -e "${Y}Local Redis (${LOCAL_REDIS_HOST}) not reachable — Redis flush will be skipped${NC}"
fi

# ---------------------------------------------------------
# Step 2 — Confirmation
# ---------------------------------------------------------
if [[ $DRY_RUN -eq 1 ]]; then
    echo -e "\n${Y}DRY-RUN — nothing will be modified. Re-run without --dry-run to commit.${NC}"
    exit 0
fi

if [[ $KEEP_CREDS -eq 1 ]]; then
    echo -e "\n${Y}Will KEEP trading_execution.user_credentials (broker auth).${NC}"
else
    echo -e "\n${R}Will FLUSH trading_execution.user_credentials too — you'll need to re-auth.${NC}"
fi

if [[ $ASSUME_YES -eq 0 ]]; then
    echo -en "\n${R}Type 'yes' to proceed with the flush:${NC} "
    read -r answer
    if [[ "$answer" != "yes" ]]; then
        echo "aborted."
        exit 1
    fi
fi

# ---------------------------------------------------------
# Step 3 — trading_db
# ---------------------------------------------------------
hdr "Flushing trading_db"
pg trading_db "
BEGIN;
TRUNCATE TABLE
    manthan_position_events,
    manthan_signal_decisions,
    manthan_positions,
    manthan_portfolio_state,
    manthan_cooldown,
    trade_signals,
    execution_outbox,
    risk_limits,
    trade_configs,
    strategy_conditions,
    strategies
RESTART IDENTITY CASCADE;
COMMIT;"
echo -e "${G}✓ trading_db flushed${NC}"

# ---------------------------------------------------------
# Step 4 — trading_execution
# ---------------------------------------------------------
hdr "Flushing trading_execution"
if [[ $KEEP_CREDS -eq 1 ]]; then
    pg trading_execution "
BEGIN;
TRUNCATE TABLE
    manthan_order_events,
    manthan_orders,
    execution_events,
    orders
RESTART IDENTITY CASCADE;
COMMIT;"
    echo -e "${G}✓ trading_execution flushed (user_credentials KEPT)${NC}"
else
    pg trading_execution "
BEGIN;
TRUNCATE TABLE
    manthan_order_events,
    manthan_orders,
    execution_events,
    orders,
    user_credentials
RESTART IDENTITY CASCADE;
COMMIT;"
    echo -e "${G}✓ trading_execution flushed (user_credentials WIPED)${NC}"
fi

# ---------------------------------------------------------
# Step 5 — market_data (per-test only, reference kept)
# ---------------------------------------------------------
hdr "Flushing market_data per-test tables"
pg market_data "
BEGIN;
TRUNCATE TABLE
    manthan_signals,
    manthan_stocks
RESTART IDENTITY CASCADE;
COMMIT;"
echo -e "${G}✓ market_data per-test tables flushed${NC} (instruments / daily_ohlcv kept)"

# ---------------------------------------------------------
# Step 6 — Local Redis (surgical, not FLUSHDB)
# ---------------------------------------------------------
hdr "Flushing local Redis manthan:* and isin:* keys"
if redis-cli -h "$LOCAL_REDIS_HOST" PING > /dev/null 2>&1; then
    deleted_m=$(redis-cli -h "$LOCAL_REDIS_HOST" --scan --pattern 'manthan:*' | xargs -r redis-cli -h "$LOCAL_REDIS_HOST" DEL | tail -1)
    deleted_i=$(redis-cli -h "$LOCAL_REDIS_HOST" --scan --pattern 'isin:*'    | xargs -r redis-cli -h "$LOCAL_REDIS_HOST" DEL | tail -1)
    echo -e "${G}✓ local Redis flushed${NC}  manthan:* deleted=${deleted_m:-0}  isin:* deleted=${deleted_i:-0}"
else
    echo -e "${Y}skipped — local Redis not reachable${NC}"
fi

# ---------------------------------------------------------
# Step 7 — Sanity check Indira prod Redis (read-only)
# ---------------------------------------------------------
hdr "Sanity — Indira prod Redis (read-only check, never written)"
if [[ -n "$INDIRA_REDIS_PASS" ]]; then
    sample=$(redis-cli -a "$INDIRA_REDIS_PASS" -h "$INDIRA_REDIS_HOST" -p 6379 --no-auth-warning GET "isin:INE139A01034" 2>/dev/null || true)
    if [[ -n "$sample" ]]; then
        echo -e "${G}✓ Indira prod Redis reachable & isin keys still present${NC}"
        echo "  sample: ${sample:0:100}..."
    else
        echo -e "${Y}prod Redis reachable but isin:INE139A01034 not present (probably fine — different ISIN universe)${NC}"
    fi
else
    echo "INDIRA_REDIS_PASS unset — skipped"
fi

# ---------------------------------------------------------
# Step 8 — Post-flush verification + next steps
# ---------------------------------------------------------
hdr "Post-flush — confirm zero rows"
pg trading_db "
SELECT 'trading_db' AS db,
  (SELECT count(*) FROM strategies) AS strategies,
  (SELECT count(*) FROM manthan_positions) AS mp,
  (SELECT count(*) FROM manthan_signal_decisions) AS msd,
  (SELECT count(*) FROM manthan_position_events) AS mpe;"
pg trading_execution "
SELECT 'trading_execution' AS db,
  (SELECT count(*) FROM manthan_orders) AS m_orders,
  (SELECT count(*) FROM user_credentials) AS creds;"
pg market_data "
SELECT 'market_data' AS db,
  (SELECT count(*) FROM manthan_signals) AS signals,
  (SELECT count(*) FROM instruments) AS instruments_KEPT;"

echo -e "\n${G}=== Flush complete. ===${NC}"
echo -e "Next steps for a fresh test:"
echo -e "  1. Restart user-config / rules-engine / trade-execution"
echo -e "     ${B}go run ./services/user-config/cmd/${NC}"
echo -e "     ${B}go run ./services/rules-engine/cmd/${NC}"
echo -e "     ${B}go run ./services/trade-execution/cmd/${NC}"
echo -e "  2. (Re-)create a strategy via gRPC for ND03920 with fresh JWT"
echo -e "  3. Refresh today's signals + Redis ISIN keys:"
echo -e "     ${B}go run ./services/data-ingestion/cmd/manthan-live/${NC}"
echo -e "  4. Dry-run the rebalancer:"
echo -e "     ${B}go run ./services/rebalancer/cmd/ --dry-run${NC}"
echo -e "  5. Real run when output looks good:"
echo -e "     ${B}go run ./services/rebalancer/cmd/${NC}"
