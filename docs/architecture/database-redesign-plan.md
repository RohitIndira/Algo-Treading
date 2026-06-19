# Database Redesign Plan

> Status: **Proposal — not yet executed**
> Author: dev team
> Last updated: 2026-06-16

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
