# Database Redesign Plan

> Status: **Proposal — not yet executed**
> Author: dev team
> Last updated: 2026-06-24

> **Executing the migration?** This document is the *design rationale*.
> For copy-paste commands, validation gates, and rollback procedures,
> see the companion [`db-migration-runbook.md`](./db-migration-runbook.md).
> Phase 0 baseline backups completed locally on 2026-06-24
> (`backups/*-baseline-20260624-165537.dump`); restore-test gate passed.

## Pre-Phase-2 audit findings (2026-06-24) — what's settled since the plan was drafted

Between the original 2026-06-16 draft and the current state, three Phase 0
gRPC migrations shipped (rules-engine + rebalancer + trade-execution
credential / strategies refactors) and a pre-Phase-2 audit was completed.
Resulting REQUIREMENTS for the role-grant design in Phase 5 below:

  1. **`manthan_signal_decisions` has a documented co-INSERT pattern.**
     rebalancer INSERTs PROPOSED rows, rules-engine UPDATEs them through
     the lifecycle. The Postgres role for rebalancer must grant
     **INSERT-only** on this specific table (no UPDATE / DELETE), to
     enforce the lifecycle boundary at the DB layer even when code review
     misses it. Detail in
     [data-ownership.md § Co-INSERT pattern](data-ownership.md).

  2. **hft-engine is FROZEN.** Its current direct reads of
     `user_credentials` and `strategies` stay. The Postgres role for
     hft-engine is created with **its current grants** (cross-DB SELECT
     on those tables), not the target-state minimal grants. Tightening
     happens after hft-engine is unfrozen (post-current-sweep).

  3. **api-gateway exception is permanent.** api-gateway is the
     translation layer; it gets a **SELECT-only** role across
     `stockk_trading` (read replica preferred at scale). Direct DB
     reads from api-gateway are NOT migration debt — they are policy.

  4. **Backend → backend raw reads are policy-acceptable.** rebalancer
     reading rules-engine's manthan_positions / _cooldown /
     _portfolio_state / _signal_decisions remains direct DB with a
     SELECT-only role on those tables. Same for rules-engine reading
     data-ingestion's manthan_signals.

These four findings are baked into Phase 5 (role grants) below.

## Why we're doing this

Today we have 4 databases with overlapping names and tangled ownership:

```
trading_execution   ← 10 tables, used by trade-execution + 3 others
trading_db          ← 12 tables, used by user-config + 4 others
market_data         ← 8 tables, used by data-ingestion + 3 others
market              ← 1 table (tick_data), used by data-ingestion
```

Specific problems we've hit or will hit:

- `user_credentials` is touched by **4 services** (hft-engine, rebalancer,
  trade-execution, user-config). Each has its own struct, its own encryption
  key handling, its own schema assumptions. A schema change breaks the others
  silently.

- `manthan_signals` is in `market_data` but `manthan_orders` is in
  `trading_execution`. You can't write a Postgres transaction across them.
  When a signal generates an order, you have to handle partial failures in
  application code.

- `execution_outbox` lives in `trading_db` (owned by user-config) but the
  events being published are about `trade-execution`'s order flow. Wrong
  owner.
  
- Three different `health_probes` tables — one per DB. Symptom of "I can't
  see the other DB so I'll just make my own."

## Target architecture — three bounded contexts

```
┌─────────────────────────────────────────────────────────────┐
│  stockk_auth                                                │
│  Owner: user-config                                         │
│  Tables:                                                    │
│   - user_credentials   (encrypted broker JWTs)              │
│   - users              (future)                             │
│   - sessions           (future)                             │
│                                                             │
│  Access pattern: user-config writes, others read via gRPC   │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  stockk_trading                                             │
│  Owners: trade-execution + rules-engine + rebalancer +      │
│          hft-engine (shared "trading domain")               │
│  Tables:                                                    │
│   Order/execution side:                                     │
│    - orders                                                 │
│    - manthan_orders, manthan_order_events                   │
│    - manthan_arm_retries                                    │
│    - signal_inbox                                           │
│    - execution_events                                       │
│    - execution_outbox     (moved from trading_db)           │
│    - hft_audit_orders, hft_runtime_state                    │
│   Strategy/rules side:                                      │
│    - strategies, strategy_conditions                        │
│    - trade_configs, risk_limits                             │
│    - trade_signals                                          │
│   Manthan portfolio/state:                                  │
│    - manthan_positions, manthan_position_events             │
│    - manthan_portfolio_state                                │
│    - manthan_signal_decisions                               │
│    - manthan_cooldown                                       │
│                                                             │
│  Access pattern: cross-table reads OK (same DB),            │
│                  cross-service writes go through            │
│                  service-specific tables only.              │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  stockk_market                                              │
│  Owner: data-ingestion (write-heavy time-series)            │
│  Tables:                                                    │
│   - tick_data            (TimescaleDB hypertable)           │
│   - daily_ohlcv_2025/26/27  (TimescaleDB hypertable)        │
│   - instruments          (symbol master)                    │
│   - manthan_stocks       (Manthan-eligible symbols)         │
│   - manthan_signals      (output of Manthan algo)           │
│   - breakout_events                                         │
│                                                             │
│  Access pattern: data-ingestion writes, everyone reads.     │
│                  Read-only DB user for non-owners.          │
└─────────────────────────────────────────────────────────────┘
```

## What each service connects to

| Service           | stockk_auth | stockk_trading | stockk_market |
|-------------------|:-----------:|:--------------:|:-------------:|
| user-config       |     R/W     |      R         |       —       |
| trade-execution   |      —      |     R/W        |       R       |
| rules-engine      |      —      |     R/W        |       R       |
| rebalancer        |      —      |     R/W        |       R       |
| hft-engine        |      —      |     R/W        |       R       |
| data-ingestion    |      —      |      —         |      R/W      |
| api-gateway       |      —      |      R         |       R       |
| risk-management   |      —      |      R         |       R       |

(Notice: user-config does NOT write to stockk_trading — only reads strategies
to support its config UI. Strategies are owned by rules-engine.)

> ⚠ **Discrepancy to resolve before Phase 3**: today's user-config service
> exposes `CreateStrategy / UpdateStrategy / DeleteStrategy` gRPC RPCs and
> writes `user_credentials` directly. The matrix above says it only reads
> strategies. Either (a) the gRPC handlers are thin proxies that publish
> Kafka events for rules-engine to apply, or (b) the matrix needs updating
> to mark user-config as R/W on stockk_trading. **Verify in code before
> Phase 3 cutover** — the answer affects the role grants in Phase 5.

## Per-service `.env` mapping for Phase 3 cutover

This table is what makes Phase 3 mechanical. For each service, here are
the **exact** env var names that drive DB connection + the values they
must hold before vs after cutover.

### Env-only changes (most services)

| Service          | Env var                  | Before                | After (Phase 3)    | Notes                          |
|------------------|--------------------------|-----------------------|--------------------|--------------------------------|
| api-gateway      | `MANTHAN_SIGNALS_DB`     | `market_data`         | `stockk_market`    |                                |
| api-gateway      | `MANTHAN_POSITIONS_DB`   | `trading_db`          | `stockk_trading`   |                                |
| api-gateway      | `MANTHAN_ORDERS_DB`      | `trading_execution`   | `stockk_trading`   | Same DB as positions now       |
| data-ingestion   | `MARKET_DATA_DB_NAME`    | `market_data`         | `stockk_market`    |                                |
| hft-engine       | `TRADING_DB`             | `trading_db`          | `stockk_trading`   |                                |
| hft-engine       | `TRADING_EXEC_DB`        | `trading_execution`   | `stockk_trading`*  | * if user_credentials moved; see Phase 5 hft freeze note |
| rules-engine     | `POSTGRES_DB`            | `trading_db`          | `stockk_trading`   |                                |
| trade-execution  | `POSTGRES_DB`            | `trading_execution`   | `stockk_trading`   |                                |
| user-config      | `DB_NAME`                | `trading_db`          | `stockk_trading`   | reads/writes strategies        |
| user-config      | `EXECUTION_DB_NAME`      | `trading_execution`   | `stockk_auth`      | writes user_credentials        |
| user-config      | `EXECUTION_DB_HOST`      | `localhost`           | `localhost`        | (no change in our setup)       |

### Code changes required (NOT env-only)

**Status (2026-06-24): RESOLVED.** Audit of every service for hardcoded
DB literals found exactly one violation — rebalancer. Fixed in the same
session it was discovered. Every service is now env-driven; Phase 3 is
mechanical `.env` swaps with zero code edits.

```
─── rebalancer (services/rebalancer/cmd/main.go) — FIXED ───
  Was (hardcoded string literals):
      tradingDB := mustOpen(logger, "trading_db", buildDSN("trading_db"))
      marketDB  := mustOpen(logger, "market_data", buildDSN("market_data"))
  Now (env-driven, defaults preserve current dev/staging behaviour):
      tradingDBName := getEnv("TRADING_DB", "trading_db")
      marketDBName  := getEnv("MARKET_DB",  "market_data")
      tradingDB := mustOpen(logger, tradingDBName, buildDSN(tradingDBName))
      marketDB  := mustOpen(logger, marketDBName,  buildDSN(marketDBName))
```

Phase 3 cutover for rebalancer now needs only:
```bash
# Add to services/rebalancer/.env (or shared .env)
TRADING_DB=stockk_trading
MARKET_DB=stockk_market
```

Full audit results (`grep -rn '"trading_db"|"market_data"|"trading_execution"' services/`):

| Service          | Verdict     | How DB name is resolved                                      |
|------------------|-------------|--------------------------------------------------------------|
| api-gateway      | ✅ env       | `envOr("MANTHAN_SIGNALS_DB", ...)` etc. (cmd/main.go:143-145) |
| data-ingestion   | ✅ env       | `getEnv("MARKET_DATA_DB_NAME", ...)` (config.go:135)         |
| hft-engine       | ✅ env       | `env("TRADING_DB", ...)`, `env("TRADING_EXEC_DB", ...)`      |
| rebalancer       | ✅ env (NEW) | `getEnv("TRADING_DB", ...)`, `getEnv("MARKET_DB", ...)`      |
| risk-management  | ✅ env       | `getEnv("DB_NAME", ...)` (config.go:39)                      |
| rules-engine     | ✅ env       | `getEnv("POSTGRES_DB", ...)` + `MANTHAN_SIGNALS_DB`          |
| trade-execution  | ✅ env       | `getEnv("POSTGRES_DB", ...)` (cmd/main.go:883)               |
| user-config      | ✅ env       | `getEnv("DB_NAME", ...)`, `getEnv("EXECUTION_DB_NAME", ...)` |

Literal-string DB names *do* still appear in code, but only as the
`default` argument to `getEnv` — the env var overrides them at runtime.
That's the correct pattern (env wins, default is a dev convenience).

### Notable consequence — service connection count changes

Some services connect to FEWER databases after migration. Either by
consolidation or because we removed a connection in Phase 0:

| Service          | Connections before    | Connections after Phase 4   | Why                            |
|------------------|-----------------------|------------------------------|--------------------------------|
| api-gateway      | 3 (market_data, trading_db, trading_execution) | 2 (stockk_market, stockk_trading) | manthan_positions + manthan_orders both in stockk_trading |
| user-config      | 2 (trading_db, trading_execution) | 2 (stockk_trading, stockk_auth) | rename, no consolidation       |
| rebalancer       | 2 (trading_db, market_data) | 2 (stockk_trading, stockk_market) | rename + Phase 0.6a already dropped trading_execution |
| trade-execution  | 1 (trading_execution) | 1 (stockk_trading)           | rename                         |
| rules-engine     | 2 (trading_db, market_data) | 2 (stockk_trading, stockk_market) | rename                         |
| data-ingestion   | 1 (market_data)       | 1 (stockk_market)            | rename                         |
| hft-engine       | 2 (trading_db, trading_execution) | 2 (stockk_trading, stockk_auth) | rename until unfrozen          |
| risk-management  | 1 (presumed trading_db) | 1 (stockk_trading)          | rename                         |

### Verification command for each cutover step

After flipping each service's `.env`, verify the connection landed on
the new DB before declaring the cutover done:

```bash
# Run after each service restart in Phase 3:
docker exec postgres_container psql -U postgres -c "
  SELECT datname, count(*) AS conns
  FROM pg_stat_activity
  WHERE application_name LIKE '<service>%' OR usename = '<service>_svc'
  GROUP BY datname"
# Expected: rows show ONLY the new stockk_* databases. Any row with
# trading_db / trading_execution / market_data means the cutover is
# incomplete.
```

## Migration phases — safe, reversible, no big-bang

We do this in small steps. Each step is reversible if something breaks.

### Phase 0 — Pre-flight (today, no DB changes)

- [x] Document this plan (this file)
- [ ] Take a baseline backup of all 4 current DBs
  ```bash
  for DB in trading_execution trading_db market market_data; do
    docker exec tsdb_live pg_dump -U postgres --format=custom $DB > \
      backups/$DB-baseline-$(date +%Y%m%d-%H%M%S).dump
  done
  ```
- [ ] Test restoring ONE backup to verify the process works
- [ ] Confirm staging is NOT in active trading hours (after 15:30 IST)

### Phase 1 — Create new DBs alongside old (zero-impact, no downtime)

```sql
CREATE DATABASE stockk_auth;
CREATE DATABASE stockk_trading;
CREATE DATABASE stockk_market;
```

At this point nothing uses them. Apps keep running on old DBs.

### Phase 2 — Copy schemas + data (still zero-impact)

For each new DB, copy structure + current data from the source(s):

```bash
# stockk_auth — just user_credentials from trading_execution
docker exec tsdb_live pg_dump -U postgres -t user_credentials trading_execution | \
  docker exec -i tsdb_live psql -U postgres -d stockk_auth

# stockk_trading — most of trading_execution + most of trading_db
docker exec tsdb_live pg_dump -U postgres \
  --exclude-table=user_credentials \
  --exclude-table=health_probes \
  trading_execution | \
  docker exec -i tsdb_live psql -U postgres -d stockk_trading

docker exec tsdb_live pg_dump -U postgres \
  --exclude-table=health_probes \
  trading_db | \
  docker exec -i tsdb_live psql -U postgres -d stockk_trading

# stockk_market — market_data + tick_data from market
docker exec tsdb_live pg_dump -U postgres \
  --exclude-table=health_probes \
  market_data | \
  docker exec -i tsdb_live psql -U postgres -d stockk_market

docker exec tsdb_live pg_dump -U postgres -t tick_data market | \
  docker exec -i tsdb_live psql -U postgres -d stockk_market
```

**Validation gate**: row counts must match.

```sql
-- In old DB:
SELECT 'manthan_orders' AS t, count(*) FROM manthan_orders;
-- In new DB:
SELECT 'manthan_orders' AS t, count(*) FROM manthan_orders;
-- Counts must match exactly before continuing.
```

### Phase 3 — Service-by-service cutover (one at a time)

Cut over the LEAST critical services first. Most critical (trade-execution)
last. The order:

1. **data-ingestion** — switch DB_NAME to `stockk_market`. If it breaks,
   we just lose some signal generation for a few mins. No money at risk.
2. **rebalancer** — switch DB_NAME. Same low-risk.
3. **rules-engine** — switch DB_NAME to `stockk_trading`.
4. **api-gateway** — switch DB connections (read-only path, lower risk).
5. **user-config** — switch DB_NAME to `stockk_auth`. (Writes go to new DB.)
6. **hft-engine** — switch DB_NAME to `stockk_trading`.
7. **trade-execution** — switch DB_NAME LAST. Highest-risk service.

Between each cutover, **soak test for ~24 hours** before moving the next
service. Watch:
- Order placement still works
- SL placement still works
- Reconciler still works
- No errors in logs

### Phase 4 — Decommission old DBs (after 1 week of stable new DBs)

```sql
-- Rename first (in case we need to roll back)
ALTER DATABASE trading_execution RENAME TO trading_execution_decom;
ALTER DATABASE trading_db RENAME TO trading_db_decom;
ALTER DATABASE market_data RENAME TO market_data_decom;
ALTER DATABASE market RENAME TO market_decom;

-- After 30 more days of stability, actually drop them:
-- DROP DATABASE trading_execution_decom;
-- ...
```

### Phase 5 — Per-service Postgres roles with minimal grants (the boundary enforcement)

Without this phase, the bounded contexts are only conceptual. Phase 5 is
what makes the boundary REAL — Postgres physically denies a service the
permission to touch data outside its lane, even if a future code bug
tries to.

Each service gets its own role with explicit GRANTs (no superuser). The
grants are derived directly from the pre-Phase-2 audit findings.

```sql
-- ─────────────────────────────────────────────────────────────────
-- Phase 5 role grants (executed after Phase 3 cutover is stable)
-- ─────────────────────────────────────────────────────────────────

-- user-config: owns stockk_auth completely
CREATE ROLE user_config_svc LOGIN PASSWORD '__from_vault__';
ALTER ROLE user_config_svc CONNECTION LIMIT 25 SET statement_timeout = '10s';
GRANT CONNECT, TEMPORARY ON DATABASE stockk_auth TO user_config_svc;
GRANT USAGE ON SCHEMA public TO user_config_svc;
GRANT ALL ON ALL TABLES IN SCHEMA public TO user_config_svc;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO user_config_svc;
-- no grant on stockk_trading / stockk_market

-- trade-execution: owns its orders + position events in stockk_trading
CREATE ROLE trade_execution_svc LOGIN PASSWORD '__from_vault__';
ALTER ROLE trade_execution_svc CONNECTION LIMIT 30 SET statement_timeout = '10s';
GRANT CONNECT ON DATABASE stockk_trading TO trade_execution_svc;
GRANT USAGE ON SCHEMA public TO trade_execution_svc;
GRANT SELECT, INSERT, UPDATE ON
    orders, manthan_orders, manthan_order_events, manthan_arm_retries,
    signal_inbox, execution_events, execution_outbox
TO trade_execution_svc;
GRANT SELECT ON strategies, trade_configs, risk_limits, trade_signals
TO trade_execution_svc;
-- DOES NOT grant write on rules-engine's tables

-- rules-engine: owns Manthan portfolio state machine
CREATE ROLE rules_engine_svc LOGIN PASSWORD '__from_vault__';
ALTER ROLE rules_engine_svc CONNECTION LIMIT 30 SET statement_timeout = '15s';
GRANT CONNECT ON DATABASE stockk_trading TO rules_engine_svc;
GRANT USAGE ON SCHEMA public TO rules_engine_svc;
GRANT SELECT, INSERT, UPDATE ON
    manthan_positions, manthan_position_events, manthan_portfolio_state,
    manthan_signal_decisions, manthan_cooldown, trade_signals,
    strategies, strategy_conditions, trade_configs, risk_limits
TO rules_engine_svc;
GRANT CONNECT ON DATABASE stockk_market TO rules_engine_svc;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO rules_engine_svc;  -- read-only on market

-- rebalancer: stricter — INSERT-only on the co-write table
CREATE ROLE rebalancer_svc LOGIN PASSWORD '__from_vault__';
ALTER ROLE rebalancer_svc CONNECTION LIMIT 5 SET statement_timeout = '30s';  -- CLI, slower OK
GRANT CONNECT ON DATABASE stockk_trading TO rebalancer_svc;
GRANT USAGE ON SCHEMA public TO rebalancer_svc;
-- Read-only on rules-engine's lifecycle tables (audit finding 4):
GRANT SELECT ON
    manthan_positions, manthan_cooldown, manthan_portfolio_state
TO rebalancer_svc;
-- INSERT-ONLY on the co-write table (audit finding 1) — lifecycle stays
-- with rules-engine, but rebalancer is allowed to propose:
GRANT SELECT, INSERT ON manthan_signal_decisions TO rebalancer_svc;
-- (Note: NO UPDATE / DELETE grant — if rebalancer ever tries to flip
-- a row's status, Postgres rejects the query. The lifecycle boundary
-- is enforced at the DB layer.)
GRANT CONNECT ON DATABASE stockk_market TO rebalancer_svc;
GRANT SELECT ON manthan_signals TO rebalancer_svc;

-- api-gateway: SELECT-only across stockk_trading (audit finding 3)
CREATE ROLE api_gateway_reader LOGIN PASSWORD '__from_vault__';
ALTER ROLE api_gateway_reader CONNECTION LIMIT 50 SET statement_timeout = '5s';
GRANT CONNECT ON DATABASE stockk_trading TO api_gateway_reader;
GRANT USAGE ON SCHEMA public TO api_gateway_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO api_gateway_reader;
REVOKE INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public FROM api_gateway_reader;
-- Auto-grant SELECT on future tables:
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO api_gateway_reader;
-- (api-gateway does NOT need write — it goes through user-config gRPC
-- for the SSO write path.)

-- data-ingestion: owns stockk_market
CREATE ROLE data_ingestion_svc LOGIN PASSWORD '__from_vault__';
ALTER ROLE data_ingestion_svc CONNECTION LIMIT 20 SET statement_timeout = '10s';
GRANT CONNECT ON DATABASE stockk_market TO data_ingestion_svc;
GRANT USAGE ON SCHEMA public TO data_ingestion_svc;
GRANT ALL ON ALL TABLES IN SCHEMA public TO data_ingestion_svc;

-- hft-engine: FROZEN per 2026-06-24 audit (finding 2). Grants kept
-- DELIBERATELY broader than target state — includes cross-DB reads of
-- user_credentials + strategies that are pending migration. Tightening
-- happens AFTER hft-engine is unfrozen.
CREATE ROLE hft_engine_svc LOGIN PASSWORD '__from_vault__';
ALTER ROLE hft_engine_svc CONNECTION LIMIT 30 SET statement_timeout = '5s';
GRANT CONNECT ON DATABASE stockk_trading TO hft_engine_svc;
GRANT USAGE ON SCHEMA public TO hft_engine_svc;
GRANT SELECT, INSERT, UPDATE ON hft_audit_orders, hft_runtime_state
    TO hft_engine_svc;
-- Frozen reads (to be tightened post-unfreeze):
GRANT SELECT ON strategies, trade_configs TO hft_engine_svc;
GRANT CONNECT ON DATABASE stockk_auth TO hft_engine_svc;
GRANT SELECT ON user_credentials TO hft_engine_svc;
-- TODO(unfreeze hft-engine): REVOKE the stockk_auth grants and the
-- direct strategies grant; replace with user-config gRPC client.

-- risk-management: SELECT-only on what it audits
CREATE ROLE risk_management_svc LOGIN PASSWORD '__from_vault__';
ALTER ROLE risk_management_svc CONNECTION LIMIT 10 SET statement_timeout = '5s';
GRANT CONNECT ON DATABASE stockk_trading TO risk_management_svc;
GRANT USAGE ON SCHEMA public TO risk_management_svc;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO risk_management_svc;
-- (risk-management is read-only by design; writes go through gRPC to
-- whichever service owns the action.)
```

**Why this matters more than the table-move work itself**: the bounded
contexts in Phase 1 + Phase 3 are conceptual until Phase 5 makes
Postgres physically deny boundary violations. Phase 5 turns code
review into an absolute guarantee.

### Lessons from the 2026-06-25 local dry-run

The Phase 5 SQL above was rehearsed on the local dev box. The runnable
version lives at [`scripts/db/phase5_roles_local.sql`](../../scripts/db/phase5_roles_local.sql).
Two findings the dry-run surfaced that this draft did NOT have:

1. **`REVOKE CONNECT ... FROM PUBLIC` is mandatory.** By default every
   role (including `data_ingestion_svc`) can CONNECT to every DB. The
   per-role CONNECT GRANTs above don't matter if PUBLIC already has
   CONNECT. Without revoking PUBLIC first, `data_ingestion_svc` could
   open a session against `stockk_trading` (it would have zero table
   grants, but the boundary leaks at the DB level). The script now
   begins with three `REVOKE CONNECT ... FROM PUBLIC` statements.

2. **`user_config_svc` needs RW on `strategies` + `strategy_conditions`
   + `trade_configs` in `stockk_trading`.** The original draft only
   granted it `stockk_auth`. The 2026-06-25 grep found 1 INSERT + 5
   UPDATE sites in `services/user-config/internal/repository/strategy_repository.go`
   — user-config IS a strategies writer. The local script grants
   `SELECT, INSERT, UPDATE, DELETE` on those three tables. (rules-engine
   is also a writer on `strategies` — confirmed co-write pattern.)

3. **Idempotency.** First-draft `DROP ROLE IF EXISTS` errors out on
   re-run because the role still owns objects. The local script
   wraps a `REASSIGN OWNED ... TO postgres; DROP OWNED ...` block in
   a `DO ... EXCEPTION WHEN undefined_object THEN NULL ... END` so
   re-runs are silent no-ops.

### 2026-06-25 boundary enforcement test results

13 cases run after applying the local script. All passed:

```
✓ rebalancer INSERT manthan_signal_decisions         allowed
✓ rebalancer UPDATE manthan_signal_decisions         denied   (lifecycle owned by rules-engine)
✓ rebalancer UPDATE manthan_positions                denied
✓ api-gateway INSERT manthan_orders                  denied
✓ trade-exec UPDATE manthan_positions                denied
✓ risk-mgmt UPDATE strategies                        denied
✓ user-config UPDATE manthan_positions               denied   (out of its lane)
✓ rules-engine UPDATE strategies (co-write)          allowed
✓ user-config UPDATE strategies (audit fix)          allowed
✓ data-ingestion CONNECT stockk_trading              denied at CONNECT (PUBLIC revoked)
✓ user-config CONNECT stockk_market                  denied at CONNECT (PUBLIC revoked)
✓ trade-exec SELECT user_credentials (cross-DB)      allowed
✓ rules-engine INSERT manthan_signals                denied   (read-only on stockk_market)
```

### Test before declaring Phase 5 done

For each service role, verify the role CANNOT do what it shouldn't:

```sql
-- e.g. rebalancer must NOT be able to UPDATE manthan_signal_decisions:
SET ROLE rebalancer_svc;
UPDATE manthan_signal_decisions SET status='DISPATCHED' WHERE 1=0;
-- Expected: ERROR: permission denied for table manthan_signal_decisions

-- api-gateway must NOT be able to INSERT into manthan_orders:
SET ROLE api_gateway_reader;
INSERT INTO manthan_orders(symbol) VALUES ('TEST') ON CONFLICT DO NOTHING;
-- Expected: ERROR: permission denied for table manthan_orders
```

These negative tests live in the deployment scripts so a future role
config drift is caught at deploy time, not at incident time.

## What can go wrong (the veteran always asks this)

| Risk | Likelihood | Mitigation |
|------|-----------|------------|
| pg_dump misses a sequence value, next INSERT fails on PK collision | Medium | Use `pg_dump --create` and explicitly reset sequences after restore |
| Foreign key violation when restoring (cross-table refs) | High | Restore in dependency order. Use `--disable-triggers`. |
| A service's `.env` still points to old DB after restart | High | Use a deploy checklist. Verify `pg_stat_activity` shows new DB name. |
| Concurrent writes during dump → inconsistent snapshot | Medium | Take dumps during off-hours (after 15:30 IST). Use `--no-locks` with caution. |
| TimescaleDB hypertable doesn't dump/restore cleanly | High | Use `timescaledb-backup` tool, not raw pg_dump. |
| Some service has hardcoded DB name in Go source (not .env) | Medium | Grep all services for hardcoded DB names before cutover |

## Rollback plan (if anything breaks)

If a service breaks after cutover:
1. Revert that service's `.env` to point to OLD DB
2. Restart service
3. OLD DB is still there (Phase 4 hasn't dropped them yet)
4. Debug what went wrong
5. Retry cutover later

Worst case: revert ALL `.env` files, restart everything. We're back to where
we started (the old DBs still have all the data).

## Open questions (TBD before execution)

1. **TimescaleDB hypertables** — does `pg_dump` handle them, or do we need
   `timescaledb-backup`? Need to test.
2. **execution_outbox migration** — does any consumer still read from the
   old location? Need to map outbox consumers.
3. **health_probes** — should we keep one in each new DB, or drop entirely
   and use a Grafana liveness check instead?
4. **Schema-per-service vs DB-per-domain** — confirmed bounded-contexts
   (DB-per-domain), but should we add Postgres schemas inside each DB for
   sub-domain organization? (e.g., `stockk_trading.manthan.orders` instead
   of just `stockk_trading.manthan_orders`)

## When to do this

**Not during market hours (09:15 IST – 15:30 IST).**

Best time: Friday 16:00 IST onwards, gives the whole weekend to validate
before Monday market open.

Worst time: Tuesday morning before 09:14 IST cron fires. Don't be that team.

## Sign-off checklist (before Phase 3 starts)

- [ ] Baseline backup taken AND restore tested
- [ ] All 4 current DBs have row-count snapshot saved
- [ ] Plan reviewed by team
- [ ] Pager/oncall coverage during cutover window
- [ ] Communication sent to anyone using the API
- [ ] Time slot reserved (outside trading hours)

## Sign-off checklist (before Phase 5 starts)

- [ ] All services running stably on new DBs for ≥ 1 week
- [ ] Old DBs renamed `_decom` (Phase 4), still queryable for rollback
- [ ] Each per-service `.env` has the role-specific password ready
      (not the shared `postgres` superuser)
- [ ] Negative test scripts written (verifying each role CANNOT do
      what it shouldn't, per the table above)
- [ ] Maintenance window scheduled — Phase 5 role swap takes a brief
      reconnect per service
- [ ] Rollback plan: keep a temporary `postgres` superuser env var ready
      to swap back if any service unexpectedly fails with a permission
      error in production
