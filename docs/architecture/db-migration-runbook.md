# DB Migration Runbook — 4 legacy DBs → 3 domain DBs

**Audience:** the engineer executing the migration at 3 AM on a planned
maintenance window. Copy-paste friendly. Every step is reversible.

**Companion:** [`database-redesign-plan.md`](./database-redesign-plan.md)
holds the *why* (bounded contexts, ownership matrix, role grants).
This file holds the *how*.

---

## At a glance — the shape of the migration

```
   BEFORE                          AFTER
   ──────                          ─────
   trading_execution ┐
   trading_db        ├──────►      stockk_auth      (auth boundary)
   market_data       │             stockk_trading   (trading boundary)
   market (tick)     ┘             stockk_market    (market data boundary)
   (4 DBs, leaky)                  (3 DBs, bounded)
```

| New DB           | What it holds                                          | Source(s)                    |
|------------------|--------------------------------------------------------|------------------------------|
| `stockk_auth`    | `user_credentials` only                                | trading_execution            |
| `stockk_trading` | Orders, positions, strategies, decisions, signals dec. | trading_execution + trading_db |
| `stockk_market`  | Signals, instruments, OHLCV, tick data                 | market_data + market         |

---

## Pre-flight — Phase 0 (already done locally)

- [x] All 4 source DBs backed up to `backups/*.dump` (custom format).
- [x] Restore test passed: `trading_db` round-tripped to a throwaway DB
      with identical table count + row counts.
- [x] Every service is env-driven for DB names (no hardcoded literals).
      See `database-redesign-plan.md` "Code changes required" section.
- [ ] **For staging/prod**: confirm out-of-market-hours window (>15:30 IST).
- [ ] **For staging/prod**: take a fresh backup right before Phase 1
      (the one in `backups/` is from `date +%Y%m%d-%H%M%S` — see file).

### Re-take a backup (any environment)

```bash
mkdir -p backups
STAMP=$(date +%Y%m%d-%H%M%S)
for DB in trading_execution trading_db market_data market; do
  PGPASSWORD=$PGPASSWORD pg_dump \
    -h $POSTGRES_HOST -U $POSTGRES_USER -p ${POSTGRES_PORT:-5432} \
    --format=custom --no-owner --no-acl "$DB" \
    > "backups/${DB}-baseline-${STAMP}.dump" 2>&1
  ls -lh "backups/${DB}-baseline-${STAMP}.dump"
done
```

### Restore-test gate (mandatory before Phase 1)

Pick one source DB, restore to a throwaway, compare row counts. If any
mismatch, STOP and investigate.

```bash
TMP="restore_test_$(date +%s)"
PGPASSWORD=$PGPASSWORD psql -h $POSTGRES_HOST -U $POSTGRES_USER -c "CREATE DATABASE $TMP"
PGPASSWORD=$PGPASSWORD pg_restore -h $POSTGRES_HOST -U $POSTGRES_USER \
  -d "$TMP" --no-owner --no-acl backups/trading_db-baseline-*.dump

# Compare table count + a few critical row counts
for tbl in strategies manthan_positions manthan_signal_decisions; do
  S=$(psql -tAc "SELECT count(*) FROM $tbl" trading_db)
  R=$(psql -tAc "SELECT count(*) FROM $tbl" "$TMP")
  [ "$S" = "$R" ] && echo "✓ $tbl  $S=$R" || echo "✗ $tbl  src=$S restored=$R"
done

PGPASSWORD=$PGPASSWORD psql -c "DROP DATABASE $TMP"
```

---

## Phase 1 — Create the 3 domain DBs (zero impact)

Run as superuser. Nothing yet uses these — apps keep running on legacy DBs.

```sql
CREATE DATABASE stockk_auth;
CREATE DATABASE stockk_trading;
CREATE DATABASE stockk_market;
```

**Verify:**

```sql
SELECT datname, pg_size_pretty(pg_database_size(datname))
FROM pg_database
WHERE datname LIKE 'stockk_%'
ORDER BY datname;
-- Expected: 3 rows, each ~8 MB (empty)
```

**Rollback** (if you decide to abort the whole migration here):

```sql
DROP DATABASE stockk_auth;
DROP DATABASE stockk_trading;
DROP DATABASE stockk_market;
```

---

## Phase 2 — Per-domain data migration

Each domain is independent. Do them in this order (smallest → largest):

1. **`stockk_auth`** — 1 table, simplest, validates the pipeline.
2. **`stockk_market`** — read-heavy, no live writes during this step
                         (market data ingestion is paused if mid-day).
3. **`stockk_trading`** — most tables; do last so any pattern learned
                         from the first two applies here.

### 2A. Migrate `stockk_auth`

**Goal:** move `user_credentials` from `trading_execution` to `stockk_auth`.
This is the auth boundary — no other table belongs here.

```bash
# Copy schema + data
PGPASSWORD=$PGPASSWORD pg_dump \
  -h $POSTGRES_HOST -U $POSTGRES_USER \
  -t user_credentials --no-owner --no-acl \
  trading_execution \
| PGPASSWORD=$PGPASSWORD psql -h $POSTGRES_HOST -U $POSTGRES_USER \
  -d stockk_auth
```

**Validation gate** — row count parity:

```bash
SRC=$(psql -tAc "SELECT count(*) FROM user_credentials" trading_execution)
DST=$(psql -tAc "SELECT count(*) FROM user_credentials" stockk_auth)
[ "$SRC" = "$DST" ] && echo "✓ stockk_auth.user_credentials  $SRC rows" \
                   || { echo "✗ MISMATCH src=$SRC dst=$DST"; exit 1; }
```

**Spot-check** — pick a known user, confirm the encrypted token survived:

```sql
-- In trading_execution (source):
SELECT user_id, length(encrypted_access_token), updated_at
FROM user_credentials WHERE user_id = 'S4450';
-- In stockk_auth (target): same query, same result.
```

### 2B. Migrate `stockk_market`

**Goal:** consolidate everything market-data-shaped: signals,
instruments, OHLCV (all year partitions), tick data.

```bash
# Bulk copy market_data → stockk_market (skip operational/probe tables)
PGPASSWORD=$PGPASSWORD pg_dump \
  -h $POSTGRES_HOST -U $POSTGRES_USER \
  --exclude-table=health_probes \
  --exclude-table=data_load_log \
  --no-owner --no-acl \
  market_data \
| PGPASSWORD=$PGPASSWORD psql -h $POSTGRES_HOST -U $POSTGRES_USER \
  -d stockk_market

# If a separate `market` DB with tick_data exists (it does NOT on
# local dev — only on staging/prod, see Open Discrepancies below):
PGPASSWORD=$PGPASSWORD pg_dump \
  -h $POSTGRES_HOST -U $POSTGRES_USER \
  -t tick_data --no-owner --no-acl market \
| PGPASSWORD=$PGPASSWORD psql -h $POSTGRES_HOST -U $POSTGRES_USER \
  -d stockk_market
```

**Validation gate** — table-by-table row counts:

```bash
for tbl in manthan_signals instruments manthan_stocks breakout_events \
           daily_ohlcv daily_ohlcv_2025 daily_ohlcv_2026; do
  SRC=$(psql -tAc "SELECT count(*) FROM $tbl" market_data 2>/dev/null || echo 0)
  DST=$(psql -tAc "SELECT count(*) FROM $tbl" stockk_market 2>/dev/null || echo 0)
  [ "$SRC" = "$DST" ] && echo "✓ $tbl  $SRC" \
                     || echo "✗ $tbl  src=$SRC dst=$DST"
done
```

**Spot-check** — today's manthan_signals must be present:

```sql
SELECT count(*) FROM manthan_signals
WHERE signal_date = CURRENT_DATE;
-- Must equal the count in market_data.manthan_signals same query.
```

### 2C. Migrate `stockk_trading`

**Goal:** every trading-state table from BOTH source DBs lands here.
This is the trickiest because two sources merge into one — namespace
collisions are possible.

#### Step 1: Copy trading_execution tables (excluding ones moving elsewhere)

```bash
# Skip user_credentials (went to stockk_auth in Phase 2A)
# Skip health_probes (operational, doesn't need to move)
PGPASSWORD=$PGPASSWORD pg_dump \
  -h $POSTGRES_HOST -U $POSTGRES_USER \
  --exclude-table=user_credentials \
  --exclude-table=health_probes \
  --no-owner --no-acl \
  trading_execution \
| PGPASSWORD=$PGPASSWORD psql -h $POSTGRES_HOST -U $POSTGRES_USER \
  -d stockk_trading
```

Tables landing in `stockk_trading` from this step:
- `execution_events`
- `hft_audit_orders`
- `manthan_order_events`
- `manthan_orders`
- `orders`
- `signal_inbox`

#### Step 2: Copy trading_db tables

```bash
PGPASSWORD=$PGPASSWORD pg_dump \
  -h $POSTGRES_HOST -U $POSTGRES_USER \
  --exclude-table=health_probes \
  --no-owner --no-acl \
  trading_db \
| PGPASSWORD=$PGPASSWORD psql -h $POSTGRES_HOST -U $POSTGRES_USER \
  -d stockk_trading
```

Tables landing in `stockk_trading` from this step:
- `execution_outbox`
- `manthan_cooldown`
- `manthan_portfolio_state`
- `manthan_position_events`
- `manthan_positions`
- `manthan_positions_with_intent`
- `manthan_signal_decisions`
- `risk_limits`
- `strategies`
- `strategy_conditions`
- `trade_configs`
- `trade_signals`

#### Pre-merge collision check

Before running Step 2, verify Step 1's tables don't share names with
Step 2's tables. If they do, the second `pg_dump | psql` will fail with
"already exists" errors and corruption risk.

```sql
-- In stockk_trading after Step 1:
SELECT table_name FROM information_schema.tables
WHERE table_schema='public' ORDER BY table_name;
-- Compare against the trading_db table list above. Zero overlap expected.
```

If there *is* overlap, STOP. Document the colliding table and decide
which source wins before continuing (this is a design problem, not a
migration problem — fix it in `data-ownership.md` first).

#### Validation gate — every critical table

```bash
echo "=== trading_execution → stockk_trading ==="
for tbl in execution_events manthan_orders manthan_order_events \
           hft_audit_orders signal_inbox; do
  SRC=$(psql -tAc "SELECT count(*) FROM $tbl" trading_execution)
  DST=$(psql -tAc "SELECT count(*) FROM $tbl" stockk_trading)
  [ "$SRC" = "$DST" ] && echo "✓ $tbl  $SRC" || echo "✗ $tbl  src=$SRC dst=$DST"
done

echo "=== trading_db → stockk_trading ==="
for tbl in strategies trade_configs manthan_positions manthan_signal_decisions \
           manthan_cooldown manthan_portfolio_state risk_limits \
           strategy_conditions trade_signals execution_outbox; do
  SRC=$(psql -tAc "SELECT count(*) FROM $tbl" trading_db)
  DST=$(psql -tAc "SELECT count(*) FROM $tbl" stockk_trading)
  [ "$SRC" = "$DST" ] && echo "✓ $tbl  $SRC" || echo "✗ $tbl  src=$SRC dst=$DST"
done
```

**A SINGLE ✗ row aborts Phase 2.** Investigate before any cutover.

#### Foreign-key spot-check

Some tables FK across what used to be two DBs (you couldn't enforce
those before; you CAN now):

```sql
-- manthan_signal_decisions.signal_id must exist in manthan_signals
-- (it referenced market_data.manthan_signals before — uncrossable FK).
-- After cutover, both live in stockk_trading + stockk_market, still
-- cross-DB → still uncrossable. Application keeps integrity, not the DB.
SELECT d.signal_id
FROM stockk_trading.public.manthan_signal_decisions d
WHERE NOT EXISTS (
  SELECT 1 FROM stockk_market.public.manthan_signals s
  WHERE s.signal_id = d.signal_id
)
LIMIT 5;
-- (this query won't actually run cross-DB without dblink — run in two
-- shells and diff the signal_id sets if you need formal verification)
```

---

## Phase 3 — Service cutover order (least risky first)

Each step = flip one `.env`, restart that ONE service, verify, move on.
Detail per service is in
[`database-redesign-plan.md`](./database-redesign-plan.md#per-service-env-mapping-for-phase-3-cutover).

```
1. data-ingestion    → MARKET_DATA_DB_NAME=stockk_market
2. risk-management   → DB_NAME=stockk_trading
3. rebalancer        → TRADING_DB=stockk_trading  MARKET_DB=stockk_market
4. user-config       → DB_NAME=stockk_trading     EXECUTION_DB_NAME=stockk_auth
5. rules-engine      → POSTGRES_DB=stockk_trading MANTHAN_SIGNALS_DB=stockk_market
6. api-gateway       → MANTHAN_SIGNALS_DB=stockk_market
                       MANTHAN_POSITIONS_DB=stockk_trading
                       MANTHAN_ORDERS_DB=stockk_trading
7. trade-execution   → POSTGRES_DB=stockk_trading
                       (also reads user_credentials from stockk_auth)
```

**Between every step**, confirm the service landed on the new DB:

```bash
# In a fresh psql, look at active connections:
psql -h $POSTGRES_HOST -U postgres -c "
  SELECT datname, application_name, state, count(*)
  FROM pg_stat_activity
  WHERE state IS NOT NULL AND datname IS NOT NULL
  GROUP BY datname, application_name, state
  ORDER BY datname, application_name"
```

If a service still has connections on `trading_db` / `trading_execution`
/ `market_data` AFTER restart, the `.env` didn't load — fix before
moving on.

---

## Phase 4 — Decommission legacy DBs

After **1 week** of stable production on the new DBs:

```sql
-- Rename first (quick rollback path):
ALTER DATABASE trading_execution RENAME TO trading_execution_decommissioned;
ALTER DATABASE trading_db        RENAME TO trading_db_decommissioned;
ALTER DATABASE market_data       RENAME TO market_data_decommissioned;

-- Wait 2 weeks watching the apps. If anything connects, the app
-- will fail loudly because the DB name is gone.

-- Final drop (UNRECOVERABLE without backup):
DROP DATABASE trading_execution_decommissioned;
DROP DATABASE trading_db_decommissioned;
DROP DATABASE market_data_decommissioned;
```

---

## Phase 5 — Role grants (per-service Postgres roles)

See `database-redesign-plan.md` "Phase 5" — full SQL there.
Run AFTER Phase 4 only — until then the legacy DBs need superuser to
clean up.

---

## Rollback procedures

### Mid-Phase-2 rollback (data mismatch detected)

```sql
-- The legacy DBs are untouched — apps still running on them.
-- Just nuke the partial new DBs and start over after fixing root cause:
DROP DATABASE stockk_auth;
DROP DATABASE stockk_trading;
DROP DATABASE stockk_market;
-- Re-run Phase 1 + Phase 2 after the fix.
```

### Mid-Phase-3 rollback (one service broke after cutover)

```bash
# Revert that one service's .env to the old DB name and restart it.
# Other services continue on the new DBs.
git checkout services/<broken-service>/.env  # or edit by hand
docker compose restart <broken-service>
# Investigate. Re-cut-over after fix.
```

Because Phase 2 only COPIES (never moves), the legacy DBs are still
intact and writable. Single-service rollback is safe at any point
during Phase 3.

### Post-Phase-4 rollback (rare — after decommission)

You have the `backups/*.dump` files. Restore the legacy DBs from
backup, flip every service's `.env` back. Plan for ~30 min downtime.

```bash
for DB in trading_execution trading_db market_data; do
  psql -c "CREATE DATABASE $DB"
  pg_restore -d "$DB" --no-owner --no-acl \
    backups/${DB}-baseline-*.dump
done
```

---

## Open discrepancies (resolve BEFORE staging/prod migration)

These were flagged by the audit. **Don't migrate staging until they're
closed.**

### 1. `market` DB existence

The plan references a 4th source DB called `market` containing
`tick_data`. It does NOT exist on local dev (only `market_data`).

**Resolve:** confirm whether `market` exists on staging/prod by running
`\l` on those Postgres instances. If yes, the runbook above already
includes the `tick_data` migration step. If no, remove that step.

### 2. user-config `strategies` writes

`data-ownership.md` says user-config doesn't write to `stockk_trading`,
but user-config exposes `CreateStrategy / UpdateStrategy / DeleteStrategy`
gRPC RPCs. Either those RPCs are Kafka-proxy stubs (and rules-engine
applies the writes) or the ownership matrix is wrong.

**Resolve:** grep `services/user-config/internal/` for direct INSERT/
UPDATE on `strategies`. If found → matrix is wrong, user-config needs
INSERT/UPDATE grant on `stockk_trading.strategies` in Phase 5. If not
found → matrix is correct, user-config gets SELECT-only.

---

## Sign-off — gate to start

The person executing this runbook must check ALL of these before
typing the first `CREATE DATABASE`:

- [ ] Fresh backup taken (Phase 0) within the last 1 hour.
- [ ] Restore-test gate passed (one DB round-tripped to throwaway).
- [ ] Both open discrepancies above are resolved.
- [ ] Out of market hours (>15:30 IST) — or willing to accept the
      consequences during market hours.
- [ ] On-call has been told a migration is starting.
- [ ] You have a terminal in `~/Algo-Treading` with the right
      Postgres credentials exported.
