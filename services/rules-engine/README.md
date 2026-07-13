# Rules Engine

The Manthan signal projector + strategy state owner. Consumes broker fill
events, projects them onto the Manthan portfolio tables, publishes entries
and SL modifications to trade-execution, and serves as the in-memory cache
for the strategy snapshot that other services may read via gRPC.

> **Status (2026-06-25):** rules-engine now exists to drive **Manthan only**.
> The original news-event-driven design described in older revisions of this
> README is gone — see the [trim history](#trim-history) section at the bottom.

## What it does

```
┌─────────────────────────────────────────────────────────────────┐
│                       rules-engine                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  user-config gRPC   ──bootstrap──→  ConfigStore (in-memory     │
│                                     strategy snapshot)          │
│       │                                       ↑                 │
│       │                                       │                 │
│  user-config-events Kafka ──→ ConfigConsumer ─┘                 │
│                                                                 │
│  ───── Manthan signal intake ─────                              │
│  manthan.signals Kafka ──→ manthan.Consumer ──┐                 │
│                                               │                 │
│  Indira live Redis ─poll→ LTPFeed ─tick──→ TickHandler          │
│                                               │                 │
│                                  ┌────────────┘                 │
│                                  ↓                              │
│                              Allocator + PortfolioMgr           │
│                              + TrailingSLMgr + OrderGenerator   │
│                                  │                              │
│  ───── Manthan publish path ─────┤                              │
│                                  ↓                              │
│        ManthanPublisher  ──→  Kafka trade-signals               │
│                          ──→  Kafka portfolio.allocations       │
│                                                                 │
│  ───── Manthan fill ingest ──────                               │
│  manthan.execution.events Kafka ──→ FillConsumer                │
│                                          │                      │
│                                          ↓                      │
│                                  PositionProjector              │
│                                  (the state machine —           │
│                                   ENTRY_FILLED, SL_FILLED,      │
│                                   MANUAL_EXIT_DETECTED, …)      │
│                                          │                      │
│                                          ├─→ manthan_positions  │
│                                          ├─→ manthan_signal_    │
│                                          │   decisions          │
│                                          ├─→ manthan_position_  │
│                                          │   events             │
│                                          └─→ Kafka manthan.     │
│                                              notifications      │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## External dependencies

| Dependency | Role | Used by |
|------------|------|---------|
| **PostgreSQL** | `trading_db` (today) / `stockk_trading` (after Phase 3) — Manthan tables | projector + consumer + publisher |
| **Redis** (local) | Manthan signal cache + EMA target cache + LTP cache | consumer + publisher |
| **Redis** (external, prod) | Indira live LTP feed (1-second poll) | LTPFeed → TickHandler |
| **Kafka** | 5 topics — see [Kafka topics](#kafka-topics) below | consumer + publisher + fill consumer + notifications |
| **user-config gRPC** | Bootstrap strategy snapshot + receive strategy events | startup.Bootstrapper + ConfigConsumer |

## Kafka topics

| Topic | Direction | Purpose |
|-------|-----------|---------|
| `user-config-events` | consume | Strategy CREATED / UPDATED / PAUSED / RESUMED / DELETED events from user-config |
| `manthan.signals` | consume | Today's eligible Manthan stocks from data-ingestion (1× per day after EOD calc) |
| `manthan.execution.events` | consume | Broker fill events from trade-execution (ENTRY_FILLED, SL_FILLED, MANUAL_EXIT_DETECTED, …) — drives the projector |
| `trade-signals` | publish | Entry orders + SL modifications consumed by trade-execution |
| `portfolio.allocations` | publish | Portfolio state changes (presumed consumed by frontend WS) |
| `manthan.notifications` | publish | User-facing notifications on manual interference detection (best-effort) |

## Database

Rules-engine is the **lifecycle owner** of the Manthan tables (per Phase 5
role grants — see [`scripts/db/phase5_roles_local.sql`](../../scripts/db/phase5_roles_local.sql)
and [`docs/architecture/data-ownership.md`](../../docs/architecture/data-ownership.md)).

| Table | Writer | Notes |
|-------|--------|-------|
| `manthan_positions` | rules-engine (sole) | live position state |
| `manthan_position_events` | rules-engine (append-only) | event source for the projector |
| `manthan_portfolio_state` | rules-engine (sole) | per-strategy capital snapshot |
| `manthan_signal_decisions` | rules-engine (RW lifecycle) + rebalancer (INSERT-only co-write) | proposed → dispatched → confirmed lifecycle |
| `manthan_cooldown` | rules-engine (sole) | re-entry blocking after exits |

Schema lives in [`migrations/`](migrations/) — see its [README](migrations/README.md)
for apply order, target DB per phase, and the ownership matrix.

## Packages

```
cmd/                       wire-up + lifecycle (main.go ~460 LOC)
config/                    env loader + Config struct (Redis, Postgres, Kafka,
                           UserConfigGRPCAddr — slimmed 2026-06-25)
internal/cache/            Redis client (LTP, EMA, signal cache)
internal/configstore/      in-memory strategy snapshot — what other services
                           would read via gRPC if they needed it
internal/configsync/       Kafka payload → models.Strategy mapper +
                           StrategyPayload type for user-config-events
internal/kafka/            ConfigConsumer (strategy events) + KafkaReader mock seam
internal/manthan/          THE service — 13 files, all Manthan:
                             consumer.go               — manthan.signals intake
                             fill_consumer.go          — manthan.execution.events intake
                             position_projector.go     — state machine (563 LOC, 16 event types)
                             publisher.go              — trade-signals + portfolio.allocations writer
                             notification_publisher.go — manthan.notifications writer
                             allocator.go              — sector / mcap caps + EMA sizing
                             portfolio.go              — in-memory portfolio mgr
                             order.go                  — order DTOs
                             ltp_feed.go               — Indira external Redis LTP poll
                             tick_handler.go           — TSL trail on ATH break
                             trailing_sl.go            — SL trigger logic
                             rehydrate.go              — restart-time rebuild of in-memory state
                             models.go                 — UserStrategy, FillEvent, Position, etc.
internal/models/           Strategy / TradeConfig (slimmed — no more Conditions /
                           RiskLimits / BearerToken — see Cat B trim)
internal/startup/          gRPC client + Bootstrapper (BulkLoad all strategies
                           from user-config at startup)
migrations/                schema (002–008, see migrations/README.md)
```

## Configuration

Every env var the service reads is documented in
[`.env.example`](.env.example). Highlights:

```bash
# Service identity
SERVICE_NAME, SERVICE_VERSION, ENVIRONMENT, GRPC_PORT, METRICS_PORT

# Kafka (only the live fields — Cat B removed news-consumer plumbing)
KAFKA_BROKERS
CONFIG_KAFKA_TOPIC=user-config-events
CONFIG_CONSUMER_GROUP_ID=rule-engine-config-sync
CONFIG_KAFKA_OFFSET_RESET=earliest

# user-config gRPC bootstrap
USER_CONFIG_GRPC_ADDR=localhost:50051

# Postgres — trading_db today, stockk_trading after Phase 3 cutover
POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD,
POSTGRES_DB, POSTGRES_SSLMODE
MANTHAN_SIGNALS_DB=signals_db    # source DB for the manthan_signals table

# Redis — local (publisher cache) + external (Indira LTP feed)
REDIS_URI / REDIS_ADDRS, REDIS_PASSWORD, REDIS_DB, …
EXT_REDIS_ADDR, EXT_REDIS_PASSWORD

# Manthan feature gates
MANTHAN_DECISION_LOG_ENABLED=true       # CQRS write path: decisions log + projector
MANTHAN_NOTIFICATIONS_ENABLED=true      # publish manthan.notifications on manual interference
```

## Running locally

```bash
# 1. Up: Postgres + Kafka (use repo helpers)
bash deployments/docker/setup_kafka.sh

# 2. Apply migrations (raw psql — no migration runner is wired today)
for f in services/rules-engine/migrations/*.sql; do
  PGPASSWORD=postgres psql -h localhost -U postgres -d trading_db -f "$f"
done

# 3. Run the service
cd services/rules-engine
cp .env.example .env   # then edit if you need non-default endpoints
go run ./cmd/
```

## Architecture references

- [`docs/architecture/data-ownership.md`](../../docs/architecture/data-ownership.md)
  — bounded-context table ownership matrix
- [`docs/architecture/communication-patterns.md`](../../docs/architecture/communication-patterns.md)
  — when to use gRPC vs Kafka vs direct DB
- [`docs/architecture/database-redesign-plan.md`](../../docs/architecture/database-redesign-plan.md)
  — the 4→3 DB consolidation (in progress; rules-engine cuts over in Phase 3)
- [`docs/architecture/db-migration-runbook.md`](../../docs/architecture/db-migration-runbook.md)
  — operator runbook for the cutover

## Trim history

Rules-engine was originally designed as a generic news-event matching
service that also happened to ship Manthan. Across 2026-06-25 we trimmed it
down to Manthan only:

| Commit | What was removed | LOC |
|--------|------------------|-----|
| `66b291a` | BearerToken on Manthan Kafka publishers (security: tokens off the wire) | -28 |
| `b96ef72` | RabbitMQ publisher stack — was wired with nil arg, never ran | -461 |
| `671f970` | Legacy news-event path (handler, engine, matcher, kafka_publisher, repository, news_consumer, holiday, marketHours, risk client, OrderRequest, MongoDBEvent) | -4,147 |
| `aca9695` | Residual dead code in configsync (Consumer class, ConditionsPayload, RiskLimitsPayload, dead helpers) | -255 |
| `952431e` | Dead sub-configs in config.go (MongoDB, MarketHours, Logging, GRPCClients, Performance) + misleading log line | -162 |
| `4f73dca` | Legacy migration 001 (created dead `trade_signals` table); added migration 008 to drop it | n/a |
| `13422ad` | Legacy `_test.go` files testing the slimmed surface | -501 |

For the audit + rationale behind these, see commit messages.
