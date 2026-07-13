# Database Ownership Map

Author: rohitt
Last updated: 2026-07-13 (DB.1 / DB.2)

## Purpose

The single source of truth for **which service owns writes on which Postgres
database**. Without this map, the codebase drifts back into the "one DB shared
by many writers" pattern that made pre-CQRS refactors risky.

## Rules

1. **One writer per database.** The service that owns writes owns the schema.
   Everyone else is read-only.
2. **Cross-service reads over gRPC by default.** DB read-only role only when
   the read is too hot for gRPC (e.g. portfolio svc reading positions
   directly instead of round-tripping).
3. **DB name = domain, not just the primary owner.** `positions_db` is owned
   by positions svc; `execution_db` is owned by trade-execution. But when
   two services co-own disjoint tables in the same DB, the name reflects the
   shared domain — e.g. `trading_db` hosts BOTH user-config's strategy tables
   AND rules-engine's manthan_* tables (no table overlaps).
4. **All DBs created by `deployments/docker/init_databases.sh`.** Fresh
   `docker-compose up` boots every service.
5. **Zero-usage DBs are technical debt — drop them.** Nothing referenced by
   any service should exist on the Postgres.

## Canonical map

| Database              | Writer(s)                    | Read-only consumers                                           | Purpose                                                    |
|-----------------------|------------------------------|--------------------------------------------------------------|------------------------------------------------------------|
| `trading_db`          | rules-engine + user-config   | api-gateway (Manthan handler, live-algos store), rebalancer, hft-engine | Strategies + trade_configs (user-config); manthan_positions, manthan_cooldown, manthan_signal_decisions, manthan_portfolio_state (rules-engine) |
| `execution_db`   | trade-execution + user-config (only writes user_credentials) | api-gateway (Manthan handler, live-algos store, portfolio token lookup), hft-engine | manthan_orders, manthan_order_events (trade-execution's audit trail); user_credentials |
| `signals_db`         | data-ingestion               | api-gateway (Manthan handler), rules-engine (via MANTHAN_SIGNALS_DB), rebalancer | Instruments, daily_ohlcv, manthan_signals, manthan_stocks, breakout_events |
| `positions_db`        | positions svc                | portfolio svc (via `positions_reader` role)                  | positions, position_events (CQRS query side for portfolio) |
| `order_status_db`     | orderstatus svc              | (none yet)                                                    | broker_events (WSS + REST reconciler audit)                |
| `stockk_market`       | (nothing in-code; populated by external ETL) | api-gateway (performance handler) | algo_performance_daily, benchmark_daily |

**Total: 6 canonical DBs.** (5 of them are created by `init_databases.sh`;
`stockk_market` is populated externally so it's a legacy read-only source —
scheduled for review; see "Under consideration" below.)

## Deprecated / dropped DBs

**Killed 2026-07-13 (DB.1):**

- `stockk_trading` — a silent replica of `manthan_positions` (trading_db)
  and `manthan_orders` (execution_db). Was read by api-gateway's
  live-algos handler + portfolio token lookup because it had both tables
  in one DB, enabling a single CTE JOIN. But it drifted from the
  authoritative sources whenever the copy job lagged, showing stale
  positions in the UI. DB.1 replaced the SQL JOIN with a Go-side merge
  reading both authoritative DBs.

**Zero-usage — safe to drop (DB.5, pending):**

- `stockk_auth`
- `stockk_trading` (see above)
- `trading_notifications`
- `trading_orders`
- `trading_platform`
- `trading_portfolio`
- `trading_strategy`
- `trading_system`
- `trading_users`
- `odin_candles` (leftover from an earlier market-data pipeline)
- `odin_streamer` (same)
- `user_login_db` (predecessor of the user-config service, now in `trading_db`)

Grep across all Go source + `deployments/` + `.env*` finds ZERO references
for any of these (only comment mentions of `stockk_auth` as a future
placeholder in `services/trade-execution/internal/repository/grpc_credentials_repository.go:8,38`).

## Renames DONE and considered

DB.3 (2026-07-13, this branch): renamed `trading_execution` → `execution_db`.
Trade-execution owns writes; the name now matches the CQRS pattern of
`positions_db` + `order_status_db`.

Not renaming `trading_db` — it's SHARED between user-config (strategy tables)
and rules-engine (manthan_* tables). Splitting it would be a real data
migration. Deferred until we have a business reason (e.g. one team wants
independent schema evolution).

`stockk_market` currently has no in-code writer — populated by an external
Python ETL script (`/tmp/perf_etl.py` — legacy). Long-term: fold into
`signals_db`. Deferred behind the ETL rewrite.

**Staging deploy note (DB.3 + DB.4):** the staging box still has the DBs
named `trading_execution` and `market_data`. To deploy this branch there,
EITHER (a) run these on staging Postgres (requires no active
connections — bounce services first):

    ALTER DATABASE trading_execution RENAME TO execution_db;
    ALTER DATABASE market_data       RENAME TO signals_db;

OR (b) set the appropriate DB-name env var in each service's
ecosystem.config.js to the old name so the code still resolves it via
env-var override. Either way, no data movement is required — just the
names.

## When you add a new service

1. Pick the DB it will WRITE — usually a new one owned solely by this service.
2. Add the DB to `deployments/docker/init_databases.sh` so fresh clones boot.
3. Add a row to the "Canonical map" table above.
4. If cross-service reads are needed, prefer gRPC. Only grant a read-only
   role on a shared DB if the read is too hot for a gRPC hop (measure before
   deciding — DB direct reads are a coupling smell).
5. Document in your service's design doc which DB you own and why.

## Historical timeline

- Pre-2026-06: monolithic `trading_db` — everything writes here.
- 2026-06-15..07-01: CQRS extraction begins. `execution_db` split out
  for trade-execution's audit trail (chunks E.*). `order_status_db` split
  for orderstatus svc.
- 2026-07-13: positions svc CQRS split — `positions_db` created + portfolio
  svc reads it via a read-only role (chunks P.A → P.G + PF.A → PF.D).
- 2026-07-13: DB.1 — killed `stockk_trading` silent replica; live-algos +
  portfolio token lookup now hit authoritative sources with Go-side JOIN.
- 2026-07-13: DB.2 — `init_databases.sh` creates all canonical DBs on
  fresh `docker-compose up`.
- 2026-07-13: DB.3 — `trading_execution` renamed to `execution_db`. All
  service code defaults + `.env` files updated. Historical arch docs
  under `docs/architecture/` kept as-is (they describe pre-rename state).
- 2026-07-13: DB.4 — `market_data` renamed to `signals_db`. WS-protocol
  `"market_data"` message-type literals in trade-execution/marketws +
  data-ingestion/ws_monitor + oco/trailing + paper/market_client left
  intact — those are wire-protocol contracts, not DB references.
