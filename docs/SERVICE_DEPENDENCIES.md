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
| 9 | **Auth Service** | External HTTPS | `https://livemiddleware.indiratrade.com/auth-services/api/auth/verify/token` | Bearer token verification on every API request |

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
**Role:** Single entry point for the frontend; translates REST → gRPC; hosts client-facing WebSocket streams.

#### Dependency Table

| Dependency | Type | Why It's Needed | Failure Mode if Down |
|------------|------|-----------------|----------------------|
| **Redis** | Cache / Pub-Sub | WebSocket relay — subscribes to `orders:{userId}` and `matches:all` Redis pub/sub channels | All client order-status sockets go silent immediately |
| **Auth Service** (`livemiddleware.indiratrade.com`) | External HTTPS | Verifies Bearer token on **every** authenticated request | All users get 401; no API access at all |
| **User Config Service** (gRPC `:50051`) | Microservice | Strategy CRUD, credential management, activate/deactivate | All `/api/v1/strategies` endpoints return 503 |
| **Rules Engine Service** (gRPC `:50053`) | Microservice | Match stats, active rule count endpoints | Match-stat endpoints fail; trading unaffected |
| **Trade Execution Service** (gRPC `:50054`) | Microservice | Order status, cancel, modify, history | All order management endpoints fail |
| **Risk Management Service** (gRPC `:50055`) | Microservice | Risk metrics dashboard, user positions | Risk dashboard endpoints fail |

---

### 3.2 Order Status WebSocket — `/api/v1/ws/orders/{userId}`

**Role:** Pushes real-time order execution updates from the broker all the way to the frontend browser.

#### Full Data Flow

```
Indira Order Status Socket (livemiddleware WSS)
    └─► Trade Execution Service
            └─► Redis PUBLISH  orders:{userId}
                    └─► API Gateway (sub)
                            └─► Frontend Browser WebSocket
```

#### Dependency Table

| Dependency | Type | Why It's Needed | Failure Mode if Down |
|------------|------|-----------------|----------------------|
| **Indira Order Status Socket** (`livemiddleware WSS`) | External WSS | Source of fill/rejection/modification events from broker | No execution updates reach the system; orders stuck at `SUBMITTED` |
| **Trade Execution Service** | Microservice | Receives broker events and publishes to Redis channel | Channel goes silent; frontend sees stale order state |
| **Redis** | Pub-Sub | Bridge between Trade Execution (publisher) and API Gateway (subscriber) | Socket connects but receives nothing; client sees stale state |
| **API Gateway** | Self | Hosts the WebSocket handler and manages client connections | All active client sockets disconnect |

#### Token Lifecycle (Indira Order Status Socket)

The Indira WSS requires a **short-lived token** refreshed every 50 minutes:
1. REST `GET /order-notify/ws/createWsToken` → get `orderToken`
2. Connect to `wss://livemiddleware.indiratrade.com/order-notify/websocket`
3. Send `{"userId": "...", "orderToken": "..."}` as first message
4. Send heartbeat `{"userId": "...", "heartbeat": "h"}` every 45 seconds

If the token refresh REST call fails → WebSocket disconnects → order updates stop.

---

### 3.3 User Config Service

**Port:** `50051` (gRPC)  
**Role:** Strategy CRUD, user credential management, EOD auto-deactivation scheduler.

#### Dependency Table

| Dependency | Type | Why It's Needed | Failure Mode if Down |
|------------|------|-----------------|----------------------|
| **PostgreSQL** | Database | Primary store for all strategies, credentials, strategy state | All strategy reads/writes fail; service cannot start without DB |
| **Redis** | Cache | Caches active strategies for fast Rules Engine queries | Cache invalidation fails on updates; stale rules in Rules Engine until TTL expires |
| **Kafka** (`user-config-events`) | Message Broker | Publishes strategy lifecycle events | Rules Engine and Trade Execution don't learn about config changes until restart/poll |

#### Built-in Scheduled Tasks

| Task | File | Schedule | What It Does |
|------|------|---------|--------------|
| **EOD Paper Deactivation** | `internal/scheduler/eod_deactivation.go` | 15:00 IST weekdays | Deactivates all paper trading strategies globally |
| **EOD Live Deactivation** | same file | 15:30 IST weekdays | Deactivates all live trading strategies globally |
| **Per-User Square-Off** | same file | User-configured `auto_square_off_time` | Deactivates individual user's strategies at their custom time |

---

### 3.4 Data Ingestion Service

**Port:** `50052` (gRPC, minimal)  
**Role:** Watches MongoDB change streams for new market/news events and publishes them to Kafka.

#### Dependency Table

| Dependency | Type | Why It's Needed | Failure Mode if Down |
|------------|------|-----------------|----------------------|
| **MongoDB** (`trading_db` collection) | Database | Change stream source — watches for new documents | No new events detected; entire downstream pipeline stalls |
| **Kafka** (`news-events` producer) | Message Broker | Publishes enriched market events downstream | Events accumulate in memory or drop; Rules Engine starves |

> **Note:** Data Ingestion has a **replay mode** (`cmd/replay/main.go`) that can re-publish historical events from MongoDB into Kafka — useful for recovering from extended Kafka outages.

---

### 3.5 Rules Engine Service

**Port:** `50053` (gRPC)  
**Role:** Evaluates active strategies against incoming market events; emits trade signals if conditions match (threshold: 80% score).

#### Dependency Table

| Dependency | Type | Why It's Needed | Failure Mode if Down |
|------------|------|-----------------|----------------------|
| **Kafka** (`news-events` consumer + `trade-signals` producer + `user-config-events` consumer) | Message Broker | Consumes market events; publishes matched signals; refreshes rules on config changes | No signals generated; trading pipeline stops completely |
| **Redis** | Cache | Active strategy cache (keyed by user); LTP price lookup (`nse:2475` → `{"ltp": ..., "tick_size": ..., "prev_close": ...}`) | Cache miss → PostgreSQL fallback (10–100× slower); LTP-based conditions evaluate stale prices |
| **PostgreSQL** | Database | Fallback for active strategy load when Redis cache is cold | If both Redis and PostgreSQL are down, service cannot evaluate any rules |
| **MongoDB** (`OdinMasterData.HolidayMaster`) | Database | Daily holiday schedule check — skips event processing on trading holidays | Processes events on market holidays (generates false signals) |
| **User Config Service** (gRPC `:50051`) | Microservice | Fetches initial full strategy list at startup | Startup fails to pre-warm strategy cache; first events miss all strategies |
| **Risk Management Service** (gRPC `:50055`) | Microservice | Pre-signal risk check before publishing to `trade-signals` | Orders blocked (fail-closed) or pass unchecked depending on config |

#### Built-in Scheduled Tasks

| Task | File | Schedule | What It Does |
|------|------|---------|--------------|
| **Holiday Cache Refresh** | `internal/holiday/checker.go` | Every 24 hours | Re-fetches trading holiday list from MongoDB |

---

### 3.6 Trade Execution Service

**Port:** `50054` (gRPC)  
**Role:** Consumes trade signals, validates risk, places orders at Indira, monitors live prices for SL/TP triggers, manages full order lifecycle.

#### Dependency Table

| Dependency | Type | Why It's Needed | Failure Mode if Down |
|------------|------|-----------------|----------------------|
| **LTP Market Data Socket** (`stockkaskwebsocket WSS`) | External WSS | Real-time price ticks (LTP as float32, per exchange:token) for triggering SL/TP on open orders. Binary frame `0x01` = MARKET_DATA | Price Monitor falls back to Redis polling (100ms); if Redis LTP also stale, SL/TP triggers are delayed or missed entirely |
| **Kafka** (`trade-signals` consumer + `trade-executions` + `order-updates` producers) | Message Broker | Receives trade signals; publishes execution events and notifications | Signals queue in Kafka; no orders placed; notifications not sent |
| **PostgreSQL** | Database | Orders table, execution events, transactional outbox, fills, positions | Cannot persist orders; history/status queries fail; duplicate orders possible on restart |
| **Redis** | Cache | LTP price cache (written by this service from WSS ticks, read by Price Monitor and Rules Engine); user credential cache; order status tracking | LTP becomes stale; SL/TP uses Redis poll fallback; every order needs PostgreSQL credential lookup |
| **Risk Management Service** (gRPC `:50055`) | Microservice | Pre-trade risk check before every order placement | Fail-closed: orders blocked. Fail-open (misconfigured): orders bypass risk limits |
| **Indira REST API** | External HTTPS | `POST /place-order`, `POST /cancel-order`, `POST /modify-order`, `GET /order-book`, `GET /fund-limit` | Orders queued but not submitted; all live trades blocked |
| **Indira Order Status Socket** (`livemiddleware WSS`) | External WSS | Execution confirmations (fills, partial fills, rejections, modifications) → published to Redis `orders:{userId}` | Orders stuck at `SUBMITTED`; no fill confirmations; P&L and positions not updated |

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

**Port:** `50055` (gRPC)  
**Role:** Pre-trade risk validation, post-trade metric updates, risk dashboard data.

#### Dependency Table

| Dependency | Type | Why It's Needed | Failure Mode if Down |
|------------|------|-----------------|----------------------|
| **PostgreSQL** | Database | Risk limits config, historical risk metrics, violation log | Cannot load limits; all pre-trade checks fail; metrics not persisted across restarts |
| **Redis** | Cache | Real-time daily counters: trade count, daily P&L, drawdown (atomic increments for concurrency safety) | Counters reset or lost; daily trade count and loss limits may not be enforced correctly until EOD reset |

---

## 4. Failure Impact Matrix

> **Severity Key:**  
> `CRITICAL` — trading fully stops or data loss risk  
> `HIGH` — major feature broken, degraded fallback  
> `MEDIUM` — partial feature loss, workaround exists  
> `LOW` — minor UX impact, non-blocking  
> `—` — no direct dependency

| Component Down | API Gateway | Order Status WS | User Config | Data Ingestion | Rules Engine | Trade Execution | Risk Mgmt |
|----------------|:-----------:|:---------------:|:-----------:|:--------------:|:------------:|:---------------:|:---------:|
| **PostgreSQL** | HIGH | — | CRITICAL | — | HIGH | CRITICAL | CRITICAL |
| **MongoDB** | — | — | — | CRITICAL | MEDIUM | — | — |
| **Redis** | — | CRITICAL | MEDIUM | — | HIGH | HIGH | HIGH |
| **Kafka** | — | — | HIGH | CRITICAL | CRITICAL | CRITICAL | — |
| **Zookeeper** | — | — | HIGH | CRITICAL | CRITICAL | CRITICAL | — |
| **LTP Market Data Socket** | — | — | — | — | MEDIUM | CRITICAL | — |
| **Order Status Socket** | — | CRITICAL | — | — | — | HIGH | MEDIUM |
| **Indira REST API** | — | — | — | — | — | CRITICAL | — |
| **Auth Service** | CRITICAL | CRITICAL | — | — | — | — | — |
| **User Config Svc** | HIGH | — | self | — | LOW | LOW | — |
| **Rules Engine Svc** | LOW | — | — | — | self | CRITICAL | — |
| **Trade Exec Svc** | HIGH | CRITICAL | — | — | — | self | MEDIUM |
| **Risk Mgmt Svc** | LOW | — | — | — | HIGH | HIGH | self |

---

## 5. Per-Component Failure Details & Solutions

---

### PostgreSQL Down

**Affected:** User Config (CRITICAL), Trade Execution (CRITICAL), Risk Management (CRITICAL), Rules Engine (HIGH)

| Service | Exact Impact |
|---------|-------------|
| User Config | Strategy CRUD fails; service cannot start cold |
| Trade Execution | Orders cannot be persisted; duplicate orders possible on restart; history/status queries fail |
| Risk Management | Risk limits cannot be loaded; all pre-trade checks fail → orders blocked |
| Rules Engine | Cold-start cannot load strategies; warm Redis cache keeps it running until TTL expires |

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

### Auth Service Down (`livemiddleware.indiratrade.com/auth-services`)

**Affected:** API Gateway (CRITICAL), Order Status WS (CRITICAL)

| Service | Exact Impact |
|---------|-------------|
| API Gateway | All authenticated requests return 401; no user can access any REST endpoint |
| Order Status WS | New WebSocket handshakes cannot be authenticated → new connections rejected |

**Note:** Auth Service is on the same `livemiddleware.indiratrade.com` host as the Order Status Socket. If that host goes down, **both** Auth and Order Status Socket fail simultaneously.

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

**Affected:** Trade Execution (HIGH), Rules Engine (HIGH), API Gateway (LOW)

| Service | Exact Impact |
|---------|-------------|
| Trade Execution | Pre-trade risk gRPC call times out; behavior is **fail-closed** (order blocked) |
| Rules Engine | Pre-signal risk check fails; signal may be dropped or passed depending on config |
| API Gateway | Risk metrics dashboard returns 503 |

**Solution:**
- Configure Trade Execution to **always fail-closed** when Risk Management is unreachable — never fail-open for risk in live trading
- Add a **fallback in-memory risk check** in Trade Execution for the most critical limits (max position size, daily loss limit) as a secondary gate when gRPC times out
- Run Risk Management with **2 replicas** — it is stateless at the compute level (state in PostgreSQL + Redis), so Kubernetes restarts are fast

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

*Last updated: 2026-05-25 — derived from actual codebase, not assumptions.*
