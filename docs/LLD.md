# Low-Level Design (LLD) — Algo-Trading Platform

**Status:** Code-verified · **Last updated:** 2026-07-14
**Companion docs:** [`HLD.md`](HLD.md) · [`ARCHITECTURE.md`](ARCHITECTURE.md) · [`PRD.md`](PRD.md)

> Data models below are transcribed from `deployments/docker/init_all_schemas.sql` and the service migrations; gRPC contracts from `api/proto/*`; Kafka topics/keys and algorithms from the service code.

---

## 1. Repository layout

```
Algo-Treading/  (go.work workspace, module github.com/RohitIndira/Algo-Treading)
├── api/
│   ├── gateway/                 # API Gateway (HTTP)
│   │   └── internal/{router,handlers,middleware,grpc_clients,dto,config}
│   └── proto/{common,user_config,rules_engine,trade_execution,risk_management}
├── pkg/                         # shared libs
│   ├── indira/                  # Indira broker REST + WS client (stateless, multi-user)
│   ├── database/{postgres,mongodb,redis,elasticsearch}
│   ├── kafka/  rabbitmq/  logger/  crypto/  correlation/  metrics/
├── services/
│   ├── data-ingestion/          # Mongo change-stream watcher
│   ├── user-config/             # strategy CRUD + outbox + EOD scheduler
│   ├── rules-engine/            # matching engine + AMN + consumers
│   ├── trade-execution/         # executor, OCO, multilevel, paper, scheduler, WS server
│   └── risk-management/         # pre-trade risk (Redis)
└── deployments/docker/, scripts/, docs/
```

Each service follows `cmd/main.go` (composition root) + `config/` + `internal/{...}` (repository / server / consumer / executor / …).

## 2. Data model

Two logical PostgreSQL databases (co-located on one instance in dev via `init_all_schemas.sql`):

- **`trading_db`** — owned by **user-config**; audited by rules-engine.
- **`trading_execution`** — owned by **trade-execution**; creds written by user-config.

### 2.1 `trading_db` (config)

**`strategies`** — one row per strategy.
`strategy_id UUID PK` · `user_id` · `strategy_name` · `active bool` · `trading_mode ∈ {PAPER,LIVE}` · `version` · `created_at/updated_at/deleted_at` (soft delete).
Unique `(user_id, strategy_name)` among non-deleted rows.

**`strategy_conditions`** — 1:1 with strategy. Matching filters:
`match_all_news` · `impact_score_min/max` · `sentiments TEXT[]` · `news_categories TEXT[]` · `stock_codes BIGINT[]` · `min/max_market_cap` · `market_cap_types TEXT[]` · `min/max_price_change_pct` · `min_volume` · `exchanges TEXT[]`.

**`trade_configs`** — 1:1. Order template:
`order_type` · `product_type` · `validity` · `quantity` · `exchange` · `order_side` (default BUY) · `limit_price` · `stop_loss_pct` · `take_profit_pct` · `trailing_sl_pct` · `stop_loss_type` · `take_profit_type` · `multi_level_sl JSONB` · `multi_level_tp JSONB` · `trade_window_start/end`.

**`risk_limits`** — 1:1:
`max_daily_trades` · `max_per_trade_risk` · `max_portfolio_exposure_pct` · `max_loss_per_day` · `enable_risk_checks` · `enable_auto_square_off` · `auto_square_off_time` (default `15:15`) · `max_amount_per_stock` · `max_trades_per_strategy`.

**`execution_outbox`** — transactional outbox:
`id BIGSERIAL` · `aggregate_id UUID` · `event_type` · `payload JSONB` · `processed bool` (partial index on `processed=false`).

### 2.2 `trading_execution` (orders)

**`user_credentials`** — Indira broker auth (AES-encrypted `indira_bearer_token`):
`user_id UNIQUE` · `indira_user_id` · `indira_app_id` · `indira_source` · `indira_bearer_token`.

**`orders`** — the central table (see `init_all_schemas.sql:149`). Notable column groups:
- Identity: `order_id UUID PK` · `user_id` · `strategy_id` · `strategy_name` · `event_id` · `signal_id` (unique when set → **dedup**).
- Instrument: `stock_code BIGINT` · `exchange` · `symbol`.
- Order: `order_type` · `order_side` · `quantity` · `price` · `stop_loss` · `take_profit` · `validity` · `product_type` · `target_price`.
- Status: `status` (default `RECEIVED`) · `broker_status` · `broker_ws_data JSONB` · `indira_order_id` · `exchange_order_number`.
- Auth passthrough: `bearer_token` · `app_id` · `source`.
- Exit config: `stop_loss_type` · `trailing_sl_pct` · `highest_price`.
- Square-off: `is_square_off_order` · `auto_square_off_time` (per-order/user override; NULL ⇒ global default).
- Paper vs live: `is_paper_trade` · `trading_mode` · `paper_exit_price/pnl` · `live_exit_price/pnl`.
- OCO: `oco_group_id` · `oco_role` · `parent_order_id`.
- Fill: `filled_quantity` · `filled_price` · `commission` · `total_cost`.
- Risk: `risk_approved` · `risk_score`.
- Constraints: `quantity>0`, `price>0 or null`, `0 ≤ filled_quantity ≤ quantity`.

**`execution_events`** — per-order lifecycle log (`order_id` FK, `event_type`, `event_data JSONB`).
**`multi_level_exit_levels`** — runtime state of each partial SL/TP level: `entry_order_id` · `exit_type ∈ {SL,TP}` · `level_num 1..5` · `price_pct` · `qty_pct` · `trigger_price` · `exit_qty` · `status` · `exit_order_id` · `broker_order_id` · `exit_price`. Unique `(entry_order_id, exit_type, level_num)`.
**`user_square_off_config`** — per-user square-off time (native to trade-execution).

> trade-execution also has a **`v2` model package** (`internal/models/v2/*`: broker_account, order, fill, position, order_group, daily_pnl, instrument, status_history, stop_loss_config) used by the v2 repositories — an evolving normalized schema alongside the flat `orders` table.

### 2.3 `trade_signals` (rules-engine audit, in `trading_db`)

`signal_id UUID PK` · `order_id UNIQUE` · `user_id` · `strategy_id` · `strategy_name` · `event_id` · stock fields · order fields · `match_score` · `impact_score` · `sentiment` · `news_category` · `status` (default PENDING) · `execution_price/time` · `broker_order_id` · `metadata JSONB`. **Auditing only** — not the strategy source and non-fatal if unavailable.

## 3. gRPC contracts (`api/proto`)

**`UserConfigService`** (`:50051`, the only server actually running of these):
`CreateStrategy` · `UpdateStrategy` · `DeleteStrategy` · `GetStrategy` · `ListUserStrategies` · `ActivateStrategy` · `DeactivateStrategy` · `GetStrategiesByIDs` · `GetAllActiveStrategies` · `UpdateUserCredentials` · `GetUserCredentials` · `HealthCheck`.

**`RiskManagementService`** (`:9005`, defined; service not deployed):
`CheckPreTradeRisk` · `UpdatePostTradeMetrics` · `GetRiskMetrics` · `SetRiskLimits` · `ResetDailyCounters` · `GetUserPositions` · `HealthCheck`.

**`TradeExecutionService`** (`:9004`):
`GetOrderStatus` · `GetUserOrders` · `CancelOrder` · `ModifyOrder` · `GetOrderHistory` · `GetOrderStatistics` · `HealthCheck`.

**`RulesEngineService`** — proto exists (`EvaluateEvent`, `ReloadUserRules`, `GetMatchingStats`, `GetActiveRulesCount`, `HealthCheck`) but **no gRPC server is started** in `rules-engine/cmd/main.go`.

Every server chains a **recovery interceptor** (per-RPC panic → `codes.Internal`) and a **correlation interceptor** (propagates/creates `X-Correlation-ID`).

## 4. Kafka message contracts

| Topic | Key | Producer | Consumer group(s) | Body (Go type) |
|---|---|---|---|---|
| `news-events` | Mongo `_id` | data-ingestion | `rule-engine-news-processor` | Enriched news doc (Extended JSON) |
| `user-config-events` | strategy id | user-config outbox | `rule-engine-config-sync`, `trade-execution-strategy-events` | Config event (created/updated/deleted/activated/deactivated) |
| `trade-signals` | order/signal id | rules-engine | `trade-execution-service` | Order signal (`models.Order`-shaped) |
| `trade-executions` | order id | trade-execution | *(none in-repo)* | Execution report |
| `order-updates` | order id | trade-execution | *(none in-repo)* | Order status change |

Producers use `RequiredAcks` + `LeastBytes` balancing; topics auto-create with 1 partition by default (`EnsureTopicExists(...,1,1)`).

## 5. Redis key schema

| DB | Key pattern | Written by | Read by | Purpose |
|---|---|---|---|---|
| DB0 | `isin:{ISIN}` | data-ingestion | data-ingestion | Company master cache |
| DB0 | `market:{exch}:{token}` | **external market feed** | rules-engine, trade-execution | LTP, tick size, DPR, prev-close (`paper.MarketTick`) |
| DB0 | `user:{userId}:matches` (pub/sub) | *(unwired — `RedisCache.Publish` never called)* | API Gateway `/ws/matches` | Live match feed (dormant) |
| DB1 | `ticks:{exch}:{token}` (TTL 12h) | trade-execution tickstore | rules-engine velocity check | Recent tick stream |

## 6. Per-service internals & algorithms

### 6.1 data-ingestion
`watcher/mongo_watcher.go`: change stream on `CAG_CHATBOT.NewsImpactDashboard` (INSERT only) → `enrichAndPublish`: look up company by ISIN in Redis (`data/redis_manager.go`, load-through from Mongo), **skip** if unknown/inactive → validate (`watcher/validator.go`) → dedupe by document id → `publisher/publisher.go` publishes to `news-events`. A scheduler periodically refreshes the company master into Redis. **Fail-fast:** if the watcher goroutine dies, `main` exits so PM2 restarts it.

### 6.2 user-config
`server/grpc_server.go` implements `UserConfigService`; `service/strategy_service.go` orchestrates; `repository/{strategy,credentials}_repository.go` persist to `trading_db` / `trading_execution`. **Outbox pattern:** a strategy write and its `execution_outbox` row commit in one transaction; `worker/outbox_worker.go` polls unprocessed rows every 500ms and publishes to `user-config-events`. `scheduler/eod_deactivation.go` deactivates PAPER strategies at 15:00 and LIVE at 15:05 IST (env-overridable) plus per-strategy custom `auto_square_off_time`. Credentials encrypted via `pkg/crypto` (AES).

### 6.3 rules-engine
- **Bootstrap** (`startup/bootstrap.go`): gRPC `BulkLoad` all active strategies from user-config into `configstore` (in-memory). Hard requirement.
- **Config sync** (`configsync/`, `kafka/config_consumer.go`): apply `user-config-events` to the store live.
- **Matching** (`engine/engine.go` worker pool → `matcher/evaluator.go` + `matcher/scorer.go`): evaluate each news event against candidate strategies; weighted **score** with a minimum-score threshold; `engine/deduper.go` suppresses duplicate matches.
- **Gates:** `holiday/checker.go` (Mongo `OdinMasterData` holidays, 24h refresh) + `utils/market_hours.go` (09:15–15:30 IST, Saturday-mock flag) + `consumer/tick_history.go` velocity check (Redis DB1) + compliance caps (`consumer` package env: `MAX_ORDER_VALUE`, `MAX_EXPOSURE_LIMIT`, `BANNED_TOKENS`, velocity).
- **Risk** (`risk/client.go`): `CheckPreTradeRisk`; on client-init failure `riskClient=nil` ⇒ **fail-open auto-approve**.
- **Publish** (`publisher/kafka_publisher.go`): matched order → `trade-signals`; `repository/trade_signal_repository.go` audits to Postgres.
- **AMN** (`backfill/*`): historical news backfill + a preview HTTP endpoint (`:8082`) proxied by the gateway.

### 6.4 trade-execution (largest service)
Composition root `cmd/main.go` wires ~15 subsystems. Key ones:
- **Executor** (`executor/executor.go`, `signal_processor.go`, `routing_executor.go`, `paper_executor.go`): consume `trade-signals`; route **live vs paper** by `is_paper_trade`; retries; publish `order-updates`.
- **Order-status service** (`statusservice/service.go`): one Indira order-status **WebSocket per user**; applies fills/rejects to the order state machine; drives OCO/ML handlers; broadcasts to the frontend WS.
- **OCO** (`oco/manager.go`, `trailing.go`): one-cancels-the-other groups, trailing SL via WSS ticks (Redis fallback), fill-price resolution from the broker trade book, restart reload from DB.
- **Multi-level SL/TP** (`multilevel/manager.go`): partial exits across up to 5 SL + 5 TP levels; SL moves to breakeven / prior-TP as TP levels fire; paper + live.
- **Paper layer** (`paper/*`): paper executor, market-data WSS client (enhanced-stream binary decode), monitors, `ws_server.go` = the **frontend WebSocket server** (`/ws/live-orders`, `/ws/paper-trades`) and REST endpoints proxied by the gateway.
- **Schedulers** (`scheduler/*`): auto square-off (15:05 live / 15:00 paper, per-user/per-strategy overrides, SL/TP teardown before reverse order, broker net-qty check), price monitor (WSS-primary + Redis poll fallback), position check.
- **Tickstore** (`tickstore/writer.go`): drains socket ticks → Redis DB1.
- **Lifecycle** (`lifecycle/lifecycle.go`): ordered graceful shutdown.

### 6.5 risk-management
`server/server.go` implements `RiskManagementService`; `checker/pre_trade.go` evaluates limits; `repository/redis.go` stores limits + daily counters. **Redis-only; not deployed.**

## 7. Order state machine (orders.status)

```
RECEIVED ─▶ (validate/route) ─▶ SUBMITTED ─▶ PARTIALLY_FILLED ─▶ FILLED
                                    │                              │
                                    ├─▶ REJECTED / A.REJECTED      └─▶ (exit: SL/TP/OCO/ML/square-off)
                                    └─▶ CANCELLED / FAILED
```
- Paper orders skip the broker and are filled by the paper executor/monitor.
- Terminal for OCO liveness = `{CANCELLED, REJECTED, A.REJECTED, FAILED}`; "open" live = `{FILLED, PARTIALLY_FILLED}`.
- Dedup: unique index on `orders.signal_id` prevents double-processing the same signal.

## 8. Configuration reference (selected env)

| Service | Key vars (defaults) |
|---|---|
| gateway | `HTTP_PORT=8081`, `USER_CONFIG_GRPC_ADDR=:50051`, `TRADE_EXECUTION_PAPER_URL`, `RULES_ENGINE_URL=:8082`, `AUTH_VERIFY_URL`, `AUTH_BYPASS` |
| user-config | `GRPC_PORT/Server.Port=50051`, `KAFKA_*` (topic `user-config-events`), execution-DB creds, `ENCRYPTION_KEY`, `EOD_PAPER/LIVE_DEACTIVATION_TIME` |
| data-ingestion | `MONGO_URI`, `MONGO_DATABASE=CAG_CHATBOT`, `MONGO_NEWS_COLLECTION=NewsImpactDashboard`, `KAFKA_TOPIC_NEWS=news-events`, `REDIS_URI` |
| rules-engine | `KAFKA_TOPIC=news-events`, `CONFIG_KAFKA_TOPIC=user-config-events`, `USER_CONFIG_GRPC_ADDR`, `RISK_MANAGEMENT_SERVICE_ADDR=:9005`, `REDIS_ADDRS`, `TICKSTORE_REDIS_DB=1`, `MONGODB_URI`, `MARKET_*`, `WORKER_COUNT=50`, `PREVIEW_HTTP_PORT=8082` |
| trade-execution | `SERVICE_PORT=9004`, `METRICS_PORT=9090`, `PAPER_WS_PORT=8081`, `KAFKA_TOPIC=trade-signals`, `POSTGRES_DB=trading_execution`, `PAPER_MARKET_WS_URL`, `REDIS_*`, `AUTO_SQUARE_OFF_TIME=15:05`, `ENABLE_STARTUP_RECONCILE`, `ENABLE_ML_RELOAD`, `FILL_RECONCILE_INTERVAL_SEC=30` |
| risk-management | `GRPC_PORT=9005`, `REDIS_HOST/PORT` |

## 9. Error handling & resilience patterns

- **Panic isolation:** gRPC recovery interceptors; `mainRecover`/`defer recover` around every background goroutine in trade-execution; watcher crash → process exit (PM2 restart).
- **Fail-open by design:** risk check, velocity check, tickstore, Redis price client, holiday checker, signal audit — all non-fatal so the trading hot-path keeps running.
- **Idempotency:** `orders.signal_id` unique index; outbox `processed` flag.
- **Restart recovery:** broker-WS pre-warm, OCO reload, optional order reconciliation & ML reload; `/readyz` gates traffic until `broker_ws` recovery completes.
