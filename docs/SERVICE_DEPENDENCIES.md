# Service Dependencies & Failure Impact Analysis

This document maps every real dependency found in the codebase, what breaks when each component goes down, and the recommended solution.

---

## Table of Contents

1. [Infrastructure & External Components](#1-infrastructure--external-components)
2. [Kafka Topics](#2-kafka-topics)
3. [Service-by-Service Dependencies](#3-service-by-service-dependencies)
   - [API Gateway](#31-api-gateway)
   - [Order Status WebSocket](#32-order-status-websocket--apiv1wsordersuserid)
   - [User Config Service](#33-user-config-service)
   - [Data Ingestion Service](#34-data-ingestion-service)
   - [Rules Engine Service](#35-rules-engine-service)
   - [Trade Execution Service](#36-trade-execution-service)
   - [Risk Management Service](#37-risk-management-service)
4. [Failure Impact Matrix](#4-failure-impact-matrix)
5. [Per-Component Failure Details & Solutions](#5-per-component-failure-details--solutions)

---

## 1. Infrastructure & External Components

| # | Component | Type | Endpoint / Port | Notes |
|---|-----------|------|-----------------|-------|
| 1 | **PostgreSQL 15** | Database | `:5432` | Strategies, orders, risk metrics, credentials |
| 2 | **MongoDB 7** | Database | `:27017` | News/market source (change streams) + `OdinMasterData.HolidayMaster` |
| 3 | **Redis 7** | Cache / Pub-Sub | `:6379` | LTP price cache, strategy cache, WebSocket pub/sub, risk counters |
| 4 | **Kafka** | Message Broker | `:9092` / `:29092` | Full trading pipeline event bus |
| 5 | **Zookeeper** | Coordination | `:2181` | Kafka cluster coordination |
| 6 | **LTP Market Data Socket** | External WSS | `wss://stockkaskwebsocket.indiratrade.com/enhanced-stream` | Real-time tick feed for SL/TP price monitoring — **binary + JSON frames** |
| 7 | **Order Status Socket** | External WSS | `wss://livemiddleware.indiratrade.com/order-notify/websocket` | Per-user order execution confirmations from broker |
| 8 | **Indira REST API** | External HTTPS | `INDIRA_BASE_URL` (configured) | Order place / modify / cancel / portfolio / fund limits |
| 9 | **Auth Service** | External HTTPS | `AUTH_VERIFY_URL` — code default `https://trade.indiratrade.com/auth-services/api/auth/verify/token` | Bearer token verification on API requests. Note: when the URL contains `trade.indiratrade.com` the gateway sets `Bypass=true` (see `api/gateway/config/config.go`) |

> **Note on LTP Socket vs Order Status Socket:**  
> These are **two separate WebSocket connections to two different Indira hosts.**  
> - `stockkaskwebsocket` → price ticks (LTP, exchange, token) — used by Price Monitor and Paper Trading  
> - `livemiddleware` → order fills, rejections, status changes — used for execution confirmations

---

## 2. Kafka Topics

| Topic | Partitions | Producer | Consumer(s) | Payload |
|-------|-----------|---------|------------|---------|
| `news-events` | 10 | Data Ingestion | Rules Engine | `NewsPayload` — stock symbol, NSE/BSE codes, impact score, sentiment, category, market cap |
| `user-config-events` | 5 | User Config | Rules Engine, Trade Execution | Strategy created / updated / deleted / activated / deactivated |
| `trade-signals` | 20 | Rules Engine | Trade Execution | Full `Order` object with matched strategy metadata |
| `trade-executions` | — | Trade Execution | Notification system | Order placed at broker; headers: `notification_channel` (push, email, in_app) |
| `order-updates` | 10 | Trade Execution | Analytics / monitoring | `ExecutionReport` — status changes (filled, cancelled, rejected) |

---

## 3. Service-by-Service Dependencies

---

### 3.1 API Gateway

**Port:** `8081` (HTTP/REST + WebSocket)  
**Role:** Single entry point for the frontend. It speaks gRPC to **user-config only**; every other backend interaction is an **HTTP reverse-proxy** or a client-facing WebSocket. It is otherwise DB-free.

> **Corrected against code (`api/gateway`):** the gateway constructs exactly one gRPC client — `grpc_clients.NewUserConfigClient` (`cmd/main.go`). It does **not** hold gRPC clients for rules-engine, trade-execution, or risk-management. Paper/live order endpoints and the AMN preview are HTTP proxies (`config.go`: `TRADE_EXECUTION_PAPER_URL`, `RULES_ENGINE_URL`). Risk-management is never contacted by the gateway.

#### Dependency Table

| Dependency | Type | Why It's Needed | Failure Mode if Down |
|------------|------|-----------------|----------------------|
| **Auth Service** | External HTTPS | Auth middleware verifies the Bearer token on `/api/v1/*` (skipped for health/OPTIONS, and when `Bypass` is set) | All users get 401 (unless bypassed) |
| **User Config Service** (gRPC `:50051`) | Microservice | Strategy CRUD via `UserConfigService` | All `/api/v1/strategies*` endpoints fail |
| **Trade Execution** (HTTP proxy, `TRADE_EXECUTION_PAPER_URL`) | Microservice | Proxies `/paper-trades/*`, `/live-orders/*`, `/auto-square-off/*`, `/dashboard-stats` to trade-execution's paper WS/HTTP server | Paper & live order management endpoints fail |
| **Rules Engine** (HTTP proxy, `RULES_ENGINE_URL`) | Microservice | Proxies `POST /amn-preview` to the rules-engine preview HTTP server (`:8082`) | AMN preview fails; trading unaffected |
| **Redis** | Pub-Sub | Backs the **match feed** WebSockets: `/ws/matches` subscribes to `user:{userId}:matches`, `/ws/matches/all` pattern-subscribes to `user:*:matches` | Match-feed sockets go silent (order/paper feeds are unaffected — see 3.2) |

---

### 3.2 Client-facing WebSockets

**Corrected against code.** There is **no** `/api/v1/ws/orders/{userId}` route and no `orders:{userId}` Redis relay. Two distinct WebSocket surfaces exist:

**(a) Match feed — served by the API Gateway** (`/ws/matches`, `/ws/matches/all`):

```
rules-engine  ──PUBLISH user:{userId}:matches──►  Redis  ──►  API Gateway (SUBSCRIBE / PSUBSCRIBE)  ──►  Frontend
```

**(b) Live/paper order feed — served directly by trade-execution's paper WS server** (`/ws/live-orders`, `/ws/paper-trades`), **not** the gateway and **not** via Redis pub/sub:

```
Indira Order-Status WS (per-user)  ──►  trade-execution OrderStatusService
        └─► in-process broadcast (paper.PaperWSServer)  ──►  Frontend WebSocket
Market-data WS ticks  ──►  live/paper monitors  ──►  pnl_update / position_exit broadcasts  ──►  Frontend
```

| Dependency | Type | Why It's Needed | Failure Mode if Down |
|------------|------|-----------------|----------------------|
| **Indira Order-Status Socket** (per-user WSS) | External WSS | Source of fill/rejection/status events per user | Order feed stalls; orders stuck at `SUBMITTED` |
| **Trade Execution Service** | Microservice | Hosts the live/paper WS server; broadcasts broker events + monitor P&L in-process | Live/paper order sockets go silent |
| **Redis** | Pub-Sub | Needed **only** for the gateway match feed, not for the order feed | Match feed silent; order feed unaffected |

#### Token Lifecycle (Indira Order-Status Socket)

The Indira order WS uses a short-lived token (refreshed periodically). The `proxy-server.js` helper (`:3001`) fetches the WS token (`/order-notify/ws/createWsToken`) for the frontend. If token refresh fails → the socket disconnects → order updates stop.

---

### 3.3 User Config Service

**Port:** `50051` (gRPC)  
**Role:** Strategy CRUD, user credential management, EOD auto-deactivation scheduler.

#### Dependency Table

| Dependency | Type | Why It's Needed | Failure Mode if Down |
|------------|------|-----------------|----------------------|
| **PostgreSQL** (`trading_db`) | Database | Primary store for strategies + transactional outbox | All strategy reads/writes fail; service cannot start without DB |
| **PostgreSQL** (`trading_execution`) | Database | Stores AES-encrypted broker credentials (written here so trade-execution can read them). Non-fatal: falls back to a no-op creds repo if unreachable | Broker credentials are not persisted |
| **Kafka** (`user-config-events`) | Message Broker | Outbox worker publishes strategy lifecycle events | Rules Engine and Trade Execution don't learn about config changes until restart/poll |

> **Corrected against code:** user-config does **not** use Redis (no Redis client in `cmd/main.go`). Strategy state reaches rules-engine via gRPC bootstrap + the `user-config-events` Kafka topic, not a Redis cache.

#### Built-in Scheduled Tasks (`internal/scheduler/eod_deactivation.go`)

| Task | Schedule | What It Does |
|------|---------|--------------|
| **EOD Paper Deactivation** | `EOD_PAPER_DEACTIVATION_TIME`, default **15:00 IST** weekdays | Deactivates all active `PAPER` strategies globally |
| **EOD Live Deactivation** | `EOD_LIVE_DEACTIVATION_TIME`, default **15:05 IST** weekdays | Deactivates all active `LIVE` strategies globally |
| **Per-User Custom Square-Off** | Each strategy's `auto_square_off_time` | Deactivates strategies whose `enable_auto_square_off=true` when the current minute matches their time |

---

### 3.4 Data Ingestion Service

**Port:** none — this service runs **no gRPC/HTTP server** (it is a MongoDB change-stream watcher). The `:50052` in docker-compose is unused.  
**Role:** Watches a MongoDB change stream for new news documents, enriches them from a Redis company master, and publishes to Kafka `news-events`.

#### Dependency Table

| Dependency | Type | Why It's Needed | Failure Mode if Down |
|------------|------|-----------------|----------------------|
| **MongoDB** (`MONGO_DATABASE` default `CAG_CHATBOT`, collection `NewsImpactDashboard`) | Database | Change-stream source — watches for new news inserts | No new events detected; downstream pipeline stalls |
| **Redis** (DB0) | Cache | Company-master lookup by ISIN (`isin:{ISIN}`); a scheduler loads/refreshes the master from MongoDB. News for companies not in the master (or inactive) is skipped | Enrichment fails; matching news is dropped |
| **Kafka** (`news-events` producer) | Message Broker | Publishes enriched news events downstream | Events drop; Rules Engine starves |

> **Note:** Data Ingestion has a **replay mode** (`cmd/replay/main.go`) that can re-publish historical events from MongoDB into Kafka — useful for recovering from extended Kafka outages.

---

### 3.5 Rules Engine Service

**Port:** no gRPC server runs (`cmd/main.go`: "currently none in rules-engine"). It exposes a Prometheus metrics port (`:9103`) and a lightweight **AMN preview HTTP server** (`:8082`). Runs as a set of Kafka consumers + an in-memory matching engine.  
**Role:** Evaluates active strategies against incoming news events; emits trade signals when a strategy matches.

#### Dependency Table

| Dependency | Type | Why It's Needed | Failure Mode if Down |
|------------|------|-----------------|----------------------|
| **User Config Service** (gRPC `:50051`) | Microservice | **Hard startup requirement** — `BulkLoad`s all active strategies into the in-memory config store before consuming (`startup.Bootstrapper`) | Service `Fatal`s at startup; will not run |
| **Kafka** (`news-events` consumer + `user-config-events` consumer + `trade-signals` producer) | Message Broker | Consumes news; keeps the config store in sync on strategy events; publishes matched signals | No signals generated; pipeline stops |
| **Redis** (DB0) | Cache / Pub-Sub | LTP lookup (`nse:2475` → `{ltp, tick_size, prev_close}`); publishes matches to `user:{userId}:matches` for the gateway feed | LTP conditions evaluate stale/zero; match feed silent |
| **Redis** (DB1 tickstore, `TICKSTORE_REDIS_DB`) | Cache | Market-price-protection (velocity) check reads the recent tick stream written by trade-execution | Velocity check disabled (fail-open) — never blocks order generation |
| **PostgreSQL** (`trading_db`) | Database | **Trade-signal tracking only** (audit of generated signals). *Not* a strategy source | Signals aren't recorded in DB; matching/publishing continues (`signalRepo=nil`) |
| **MongoDB** | Database | Trading-holiday check + AMN backfill/preview (reads `CAG_CHATBOT` news + `OdinMasterData`) | Non-fatal: holiday check disabled (may process on holidays); AMN backfill/preview disabled |
| **Risk Management Service** (gRPC `:9005`) | Microservice | Pre-signal `CheckPreTradeRisk` before publishing to `trade-signals` | **Fail-open** — if the risk client can't init, `riskClient=nil` and orders are **auto-approved** |

> **Corrected against code:** strategies are held **in-memory** (`configstore`), seeded by a gRPC `BulkLoad` from user-config and kept current via the `user-config-events` Kafka topic. There is **no Redis strategy cache and no PostgreSQL strategy fallback**. Risk checking is **fail-open**, not fail-closed.

#### Built-in Scheduled Tasks

| Task | File | Schedule | What It Does |
|------|------|---------|--------------|
| **Holiday Cache Refresh** | `internal/holiday/checker.go` | Every 24 hours | Re-fetches trading holiday list from MongoDB |

---

### 3.6 Trade Execution Service

**Port:** gRPC `:9004` (`SERVICE_PORT`); Prometheus metrics `:9090`; frontend paper/live WS server `:8081` (`PAPER_WS_PORT`). docker-compose maps `:50054`.  
**Role:** Consumes trade signals, places orders at Indira, monitors live prices for SL/TP triggers, manages the full order lifecycle (OCO/trailing SL, multi-level SL/TP, auto square-off).

> **Corrected against code:** trade-execution does **not** call risk-management. There is no risk-management gRPC client anywhere in `services/trade-execution` — pre-trade risk is enforced upstream in rules-engine. It also consumes `user-config-events` (to close positions on strategy deactivate/delete and to pre-open per-user broker WS).

#### Dependency Table

| Dependency | Type | Why It's Needed | Failure Mode if Down |
|------------|------|-----------------|----------------------|
| **LTP Market Data Socket** (`stockkaskwebsocket WSS`) | External WSS | Real-time price ticks (LTP as float32, per exchange:token) for triggering SL/TP on open orders. Binary frame `0x01` = MARKET_DATA | Price Monitor falls back to Redis polling (100ms); if Redis LTP also stale, SL/TP triggers are delayed or missed entirely |
| **Kafka** (`trade-signals` consumer + `trade-executions` + `order-updates` producers) | Message Broker | Receives trade signals; publishes execution events and notifications | Signals queue in Kafka; no orders placed; notifications not sent |
| **PostgreSQL** | Database | Orders table, execution events, transactional outbox, fills, positions | Cannot persist orders; history/status queries fail; duplicate orders possible on restart |
| **Redis** (DB0) | Cache | LTP / tick-size / DPR lookup for fill price + limit rounding; credential cache warm-up. Non-fatal: runs with hardcoded tick sizes if down | LTP-based fills degrade; every order needs a PostgreSQL credential lookup |
| **Redis** (DB1) | Cache | Tickstore writer persists every socket tick (`ticks:{exch}:{token}`, TTL 12h) — read by rules-engine's velocity check | Tick history unavailable; algo runs unchanged |
| **Indira REST API** | External HTTPS | Place / cancel / modify orders, position book, order/trade book | Orders queued but not submitted; all live trades blocked |
| **Indira Order-Status Socket** (per-user WSS) | External WSS | Execution confirmations (fills, partials, rejections) → broadcast **in-process** to the paper WS server (no Redis relay) | Orders stuck at `SUBMITTED`; no fill confirmations; P&L/positions not updated |

#### LTP Distribution Chain (from code)

```
LTP Market Data Socket (stockkaskwebsocket WSS)
    └─► marketws/client.go   [binary frame decode: symbol, token, exchange, LTP float32]
            ├─► ltpCache (in-memory map: "exchange:token" → float64)
            ├─► Redis SET  nse:2475 = {"ltp": 2450.75, "tick_size": 0.05, "prev_close": 2445.0}
            └─► OnPriceUpdate callback
                    └─► PriceMonitor (32 shards, 16-64 workers)
                            └─► Evaluates open orders for SL/TP trigger
                                    └─► Places exit order via Indira REST
```

**LTP also used for order placement:**  
`BUY limit price = LTP × 1.005` (0.5% above LTP for faster fills) — from `internal/indira/client.go`

#### Paper Trading — LTP Dependency

Paper trading (`internal/paper/market_client.go`) connects to the **same `stockkaskwebsocket` LTP socket** for mark-to-market pricing. If the LTP socket goes down, paper trading SL/TP also stops triggering.

#### Built-in Scheduled Tasks

| Task | File | Schedule | What It Does |
|------|------|---------|--------------|
| **Price Monitor** | `internal/scheduler/price_monitor.go` | Event-driven (WSS tick) + 100ms Redis poll fallback | Continuously checks open order SL/TP thresholds |
| **Auto Square-Off (Paper)** | `internal/scheduler/auto_square_off.go` | 15:00 IST weekdays | MARKET order to close all paper positions |
| **Auto Square-Off (Live)** | same file | 15:05 IST weekdays | MARKET/IOC order to close all intraday live positions |

---

### 3.7 Risk Management Service

**Port:** gRPC `:9005` (`GRPC_PORT`). docker-compose maps `:50055`.  
**Role:** Pre-trade risk validation, post-trade metric updates, risk dashboard data.

> **Corrected against code:** `cmd/main.go` wires **only** a Redis repository (`repository.NewRedisRepository`). It does **not** open a PostgreSQL connection (the `DB*` config fields exist but are unused at runtime). This service is also **excluded from the PM2 deployment** (`deploy-pm2.sh`), so in practice rules-engine runs with risk auto-approve.

#### Dependency Table

| Dependency | Type | Why It's Needed | Failure Mode if Down |
|------------|------|-----------------|----------------------|
| **Redis** | Cache | Risk limits + real-time daily counters (trade count, daily P&L, drawdown) via the Redis repository | Counters/limits unavailable; pre-trade checks cannot be evaluated |

---

## 4. Failure Impact Matrix

> **Severity Key:**  
> `CRITICAL` — trading fully stops or data loss risk  
> `HIGH` — major feature broken, degraded fallback  
> `MEDIUM` — partial feature loss, workaround exists  
> `LOW` — minor UX impact, non-blocking  
> `—` — no direct dependency

> Cells marked ✎ were corrected against the code (see notes below the table).

| Component Down | API Gateway | Order/Match WS | User Config | Data Ingestion | Rules Engine | Trade Execution | Risk Mgmt |
|----------------|:-----------:|:---------------:|:-----------:|:--------------:|:------------:|:---------------:|:---------:|
| **PostgreSQL** | ✎— | — | CRITICAL | — | ✎LOW | CRITICAL | ✎— |
| **MongoDB** | — | — | — | CRITICAL | MEDIUM | — | — |
| **Redis** | ✎MEDIUM | CRITICAL | ✎— | ✎HIGH | HIGH | HIGH | HIGH |
| **Kafka** | — | — | HIGH | CRITICAL | CRITICAL | CRITICAL | — |
| **Zookeeper** | — | — | HIGH | CRITICAL | CRITICAL | CRITICAL | — |
| **Market Data Socket** | — | — | — | — | MEDIUM | CRITICAL | — |
| **Order Status Socket** | — | CRITICAL | — | — | — | HIGH | ✎— |
| **Indira REST API** | — | — | — | — | — | CRITICAL | — |
| **Auth Service** | CRITICAL | CRITICAL | — | — | — | — | — |
| **User Config Svc** | HIGH | — | self | — | ✎HIGH¹ | LOW | — |
| **Rules Engine Svc** | LOW | — | — | — | self | CRITICAL | — |
| **Trade Exec Svc** | HIGH | CRITICAL | — | — | — | self | ✎— |
| **Risk Mgmt Svc** | LOW | — | — | — | ✎LOW² | ✎— | self |

**Code-verified corrections:**
- **PostgreSQL** is not a dependency of the API Gateway (DB-free) or Risk Management (Redis-only). Rules Engine uses PG only for non-fatal trade-signal auditing → LOW.
- **Redis**: API Gateway match feed (MEDIUM); User Config has no Redis client (—); Data Ingestion needs Redis for company-master enrichment (HIGH).
- **Order-Status Socket → Risk Mgmt**: risk-management does not consume order-status events (—).
- ¹ Rules Engine hard-requires User Config at **startup** (`BulkLoad`) — it `Fatal`s if unreachable; steady-state sync is via Kafka.
- ² Risk Mgmt down → Rules Engine is **fail-open** (auto-approves) → LOW; Trade Execution has no risk dependency (—).

---

## 5. Per-Component Failure Details & Solutions

---

### PostgreSQL Down

**Affected:** User Config (CRITICAL), Trade Execution (CRITICAL), Rules Engine (LOW). **Not affected:** Risk Management (Redis-only, no PG connection in code).

| Service | Exact Impact |
|---------|-------------|
| User Config | Strategy CRUD fails; service cannot start cold |
| Trade Execution | Orders cannot be persisted; duplicate orders possible on restart; history/status queries fail |
| Rules Engine | Only trade-signal **auditing** stops (`signalRepo=nil`); strategy load is via gRPC/Kafka, so matching and signal publishing continue |
| Risk Management | No impact — it uses Redis only |

**Solution:**
- PostgreSQL in **HA mode** (Patroni streaming replication or AWS RDS Multi-AZ) — automatic failover in < 30 seconds
- **Read replica** for history/metrics queries (order history, risk metrics, match stats)
- Each service implements **retry with exponential backoff** (100ms → 500ms → 2s, max 3 retries) on transient connection errors
- Trade Execution uses **transactional outbox pattern** (already partially implemented) — committed rows are guaranteed to produce Kafka events even after partial DB failures

---

### MongoDB Down

**Affected:** Data Ingestion (CRITICAL), Rules Engine (MEDIUM — holiday checker)

| Service | Exact Impact |
|---------|-------------|
| Data Ingestion | Change stream breaks; no new market events → pipeline freezes at the source |
| Rules Engine | Holiday checker cannot refresh → service processes events on market holidays (false signals) |

**Solution:**
- MongoDB as **3-node replica set** (Primary + 2 Secondaries) — change streams survive primary failover automatically
- Data Ingestion persists the **resume token** (last change stream position) to Redis/PostgreSQL → on reconnect, resumes from exact position without replaying all history
- Rules Engine **caches the holiday list in memory** for the current trading day so a MongoDB outage during market hours doesn't affect the holiday check
- Add a **dead-letter topic** in Kafka for events that fail validation so nothing is silently dropped

---

### Redis Down

**Affected:** Order Status WS (CRITICAL), Rules Engine (HIGH), Risk Management (HIGH), Trade Execution (HIGH), User Config (MEDIUM)

| Service | Exact Impact |
|---------|-------------|
| Order Status WS | Pub/sub channels `orders:{userId}` and `matches:all` go silent; frontend sees stale state |
| Rules Engine | Strategy cache cold → falls back to PostgreSQL (10–100× slower); under high event load, PostgreSQL may be overwhelmed; LTP price lookups return stale/zero |
| Risk Management | Daily counters (trade count, P&L, drawdown) lost; limits may not be enforced until next EOD reset |
| Trade Execution | LTP cache stale → SL/TP triggers delayed; Price Monitor falls back to 100ms Redis poll (which also fails); credential lookups hit PostgreSQL every order |
| User Config | Cache invalidation fails; stale active strategies may linger in Rules Engine |

**Solution:**
- **Redis Sentinel** (1 primary + 2 replicas + 3 sentinels) or **Redis Cluster** — automatic failover in < 10 seconds
- Order Status WS: implement a **polling fallback** — if Redis pub/sub subscription fails, the client-facing WebSocket polls `GET /api/v1/orders/{orderId}/status` gRPC every 2 seconds
- Risk Management: **snapshot daily counters to PostgreSQL every 60 seconds** so counters can be restored within ±60s accuracy after Redis restart
- Rules Engine: pre-warm Redis cache from PostgreSQL at startup with a generous TTL (30 minutes) to absorb brief Redis outages
- Trade Execution Price Monitor: when Redis LTP is unavailable, fall back to **Indira REST `GET /portfolio/position-book`** to fetch last known prices for open positions

---

### LTP Market Data Socket Down (`stockkaskwebsocket WSS`)

**Affected:** Trade Execution (CRITICAL), Paper Trading (CRITICAL), Rules Engine (MEDIUM — via Redis)

| Service | Exact Impact |
|---------|-------------|
| Trade Execution | `ltpCache` stops updating; Redis LTP keys go stale; Price Monitor falls back to Redis poll but data is frozen; **SL/TP triggers stop firing** on open orders |
| Paper Trading | Same LTP socket — paper SL/TP triggers also stop; mark-to-market P&L freezes |
| Rules Engine | Reads LTP from Redis (written by Trade Execution from this socket); after cache TTL, LTP conditions evaluate stale prices |
| Order Placement | `BUY limit = LTP × 1.005` uses stale LTP → orders may be placed at wrong prices |

**Solution:**
- **Reconnect with exponential backoff + jitter** (1s → 2s → 4s … max 60s) — already partially implemented; ensure it fires on all disconnect codes including silent TCP drops
- **Heartbeat probe**: send ping frame every 30 seconds; if no pong in 10 seconds, treat as disconnected and reconnect proactively
- On reconnect, immediately **re-subscribe to all active instrument tokens** so no price updates are missed
- While disconnected, Price Monitor should **pause new SL/TP order triggers** and log a `PRICE_FEED_UNAVAILABLE` warning rather than triggering on stale LTP
- **Alert** (PagerDuty / Slack) if socket is disconnected for > 60 seconds during market hours (09:15–15:30 IST)
- Secondary fallback: **poll Indira REST `GET /portfolio/position-book`** every 5 seconds for LTP of open-position instruments only (not a full replacement but keeps SL/TP functional during brief outages)

---

### Order Status Socket Down (`livemiddleware WSS`)

**Affected:** Order Status WS (CRITICAL), Trade Execution (HIGH), Risk Management (MEDIUM)

| Service | Exact Impact |
|---------|-------------|
| Order Status WS | No fill/rejection events → frontend order status stuck at `SUBMITTED` |
| Trade Execution | Cannot update order status to `FILLED`/`REJECTED`; Redis `orders:{userId}` channel goes silent; post-trade metrics not updated |
| Risk Management | Post-trade P&L and position updates delayed |

**Token refresh failure also causes disconnection** — if `GET /order-notify/ws/createWsToken` REST call fails, the token cannot be refreshed before 1-hour expiry and the socket disconnects.

**Solution:**
- Reconnect logic with **exponential backoff** (30s → 5min → 10min max) — already implemented; verify it handles 401 specifically
- On reconnect: immediately call **`GET /order-book`** REST endpoint to fetch latest status of all `SUBMITTED` orders and reconcile without waiting for WebSocket events
- Send frontend clients a `{"type":"feed_interrupted","message":"Order feed reconnecting"}` WebSocket message so the UI shows a warning banner instead of silently hanging
- **Token pre-refresh**: refresh the `orderToken` every 50 minutes (before 60-minute expiry) regardless of whether the socket is healthy — implemented; ensure the REST call failure also triggers reconnect
- If Indira Order Status Socket is down for > 2 minutes: **poll `GET /order-trail`** every 30 seconds for each `SUBMITTED` order as a manual reconciliation path

---

### Kafka Down

**Affected:** Data Ingestion (CRITICAL), Rules Engine (CRITICAL), Trade Execution (CRITICAL), User Config (HIGH)

| Service | Exact Impact |
|---------|-------------|
| Data Ingestion | Cannot publish market events; events in memory buffer or dropped |
| Rules Engine | Cannot consume market events → no strategy evaluation → no trade signals |
| Trade Execution | Cannot consume trade signals → no new orders placed; cannot publish execution events |
| User Config | Strategy lifecycle events not published → Rules Engine runs on stale strategy state |

**Solution:**
- **3-broker Kafka cluster** with replication factor 3 and `min.insync.replicas=2` — single broker failure is invisible to clients
- Services retry Kafka connection with backoff (not crash) and resume from committed offset when Kafka recovers
- Data Ingestion maintains an **in-memory ring buffer** (configurable, e.g., 10,000 events) for short outages; flushes to Kafka on reconnect
- Set topic retention: `news-events` = 7 days, `trade-signals` = 3 days, `order-updates` = 7 days — consumers catch up after extended outages without signal loss
- **Migrate to KRaft mode** (Kafka 3.3+) to remove Zookeeper dependency

---

### Indira REST API Down

**Affected:** Trade Execution (CRITICAL)

| Service | Exact Impact |
|---------|-------------|
| Trade Execution | Order place/cancel/modify calls fail; `GET /fund-limit` fails (margin check skipped or cached); `GET /order-book` reconciliation unavailable |

**Solution:**
- **Circuit breaker** around all Indira REST calls: open after 5 consecutive failures; half-open after 30 seconds to test recovery
- When circuit is open, store order requests in a **local retry queue** (Redis list or PostgreSQL outbox) and submit when Indira recovers
- **Cached fund limit**: store last known available margin in Redis (TTL 60 seconds) so margin checks can continue briefly during outages
- Send an **immediate alert** when the first Indira REST call fails — Indira REST and Order Status Socket are on different hosts, so REST may be down while the WebSocket is still alive

---

### Auth Service Down (`AUTH_VERIFY_URL`)

**Affected:** API Gateway (CRITICAL, unless bypassed), Order/Match WS (CRITICAL)

| Service | Exact Impact |
|---------|-------------|
| API Gateway | All authenticated `/api/v1/*` requests return 401 (health/OPTIONS exempt); no user can access REST endpoints |
| Order/Match WS | New WebSocket handshakes cannot be authenticated → new connections rejected |

**Note:** the URL is env-driven (`AUTH_VERIFY_URL`). The code default host is `trade.indiratrade.com`, and the gateway auto-sets `Bypass=true` when the URL contains `trade.indiratrade.com` (`api/gateway/config/config.go`) — so with the default config, auth verification is effectively skipped. Confirm the deployed `.env.live` value to know the real posture.

**Solution:**
- **Cache successful token verifications in Redis** with TTL = `AUTH_TIMEOUT` (default 60 seconds) — existing sessions continue working through brief outages
- If Auth Service is unreachable for > TTL: **fail-open only for read-only GET endpoints** (get strategies, get orders); **fail-closed** for all write and trade operations
- Health endpoint `GET /api/v1/health` should expose `auth_service: degraded` so monitoring can detect the outage

---

### Rules Engine Service Down

**Affected:** Trade Execution (CRITICAL — no new signals), API Gateway (LOW)

| Service | Exact Impact |
|---------|-------------|
| Trade Execution | No trade signals published; no new orders triggered automatically |
| API Gateway | Match-stat endpoints return 503; order management unaffected |
| Data Ingestion | Continues publishing to `news-events`; events accumulate safely in Kafka (up to retention period) |

**Solution:**
- Run **3 Rules Engine replicas** in the same Kafka consumer group — Kafka distributes `news-events` partitions across them; one replica crash redistributes its partitions in < 5 seconds
- Rules Engine is stateless (all state in Redis/PostgreSQL) → Kubernetes can restart crashed replicas with zero data loss
- `news-events` retention means recovered replicas **catch up from the last committed offset** without losing events

---

### Trade Execution Service Down

**Affected:** Order Status WS (CRITICAL), API Gateway (HIGH), Risk Management (MEDIUM)

| Service | Exact Impact |
|---------|-------------|
| Order Status WS | No `PUBLISH orders:{userId}` to Redis → all client sockets go silent |
| API Gateway | All order management endpoints (status, cancel, modify, history) return 503 |
| Risk Management | Post-trade metrics not updated; counters may be inconsistent |
| Rules Engine | Continues producing signals; signals queue safely in `trade-signals` Kafka topic |

**Solution:**
- Run **3 Trade Execution replicas** in the same Kafka consumer group — partition rebalancing on crash in < 5 seconds
- Use **idempotency keys** (already partially in place via outbox) to prevent duplicate orders when a replica restarts mid-signal
- `trade-signals` retention ensures queued signals are processed when service recovers with no signal loss

---

### Risk Management Service Down

**Affected:** Rules Engine (LOW). **Not affected:** Trade Execution (it never calls risk-management), API Gateway (never contacts it).

> **Reality check from code:** risk-management is **excluded from the PM2 deployment** entirely (`deploy-pm2.sh`), and rules-engine treats a missing/failed risk client as **fail-open** — `riskClient=nil` → orders auto-approved (`cmd/main.go`). So in the current deployment, pre-trade risk is effectively not enforced.

| Service | Exact Impact |
|---------|-------------|
| Rules Engine | `CheckPreTradeRisk` fails to init/call → **auto-approve** (fail-open); signals still published |
| Trade Execution | No impact — no risk-management client exists in the service |
| API Gateway | No impact — the gateway has no risk-management dependency |

**Solution (if risk enforcement is desired):**
- Actually deploy risk-management (add it to `deploy-pm2.sh` / compose) — today it is not run in production
- Decide the intended posture explicitly in rules-engine: keep **fail-open** for availability, or switch to **fail-closed** for safety when the risk client is down
- Run Risk Management with **2 replicas** — it is stateless at the compute level (state in Redis), so restarts are fast

---

## Recommended Resilience Configuration

```
Component               Min HA Setup                                  Target RTO
─────────────────────────────────────────────────────────────────────────────────
PostgreSQL              Primary + 1 Standby (Patroni / RDS Multi-AZ)  < 30 s
MongoDB                 3-node Replica Set                              < 10 s
Redis                   Sentinel (1P + 2R + 3S) or Cluster            < 10 s
Kafka                   3 brokers, RF=3, min.ISR=2                     <  5 s (broker fail)
Zookeeper               3-node ensemble  →  migrate to KRaft           < 10 s
LTP Market Data Socket  Reconnect loop + REST fallback for open pos    < 60 s
Order Status Socket     Reconnect loop + REST order-book reconcile     < 30 s
Indira REST API         Circuit breaker + retry queue (Redis/PG)       < 60 s
Auth Service            Redis token cache (60 s TTL)                   < 60 s
User Config Service     2 replicas (K8s Deployment)                    <  5 s
Data Ingestion          2 replicas (same Kafka consumer group)         <  5 s
Rules Engine            3 replicas (same Kafka consumer group)         <  5 s
Trade Execution         3 replicas (same Kafka consumer group)         <  5 s
Risk Management         2 replicas (K8s Deployment)                    <  5 s
API Gateway             2+ replicas behind Load Balancer               <  5 s
```

---

*Last updated: 2026-07-13 — re-verified against the codebase. Corrections in this pass: gateway speaks gRPC to user-config only (HTTP-proxies rules-engine & trade-execution; never contacts risk-management); there is no `/api/v1/ws/orders/{userId}` route or `orders:{userId}` Redis relay (match feed uses `user:{id}:matches`; live/paper order feed is served in-process by trade-execution); data-ingestion is a change-stream watcher (no gRPC server) reading `CAG_CHATBOT.NewsImpactDashboard` and enriching from a Redis company master; rules-engine holds strategies in-memory (gRPC bootstrap + Kafka sync), not in Redis/PostgreSQL, and risk-checks fail-open; trade-execution does not call risk-management; risk-management is Redis-only and excluded from the PM2 deployment; user-config has no Redis client; EOD live deactivation is 15:05 (not 15:30).*
