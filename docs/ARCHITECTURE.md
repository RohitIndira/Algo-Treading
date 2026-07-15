# Algo-Trading Platform — Architecture Documentation

> Companion to the diagram: **[`ARCHITECTURE.png`](ARCHITECTURE.png)** (rendered) and **[`ARCHITECTURE.drawio`](ARCHITECTURE.drawio)** (editable).
> Everything below is derived from the **code**, not from older design docs. Where the code contradicts other docs, the code wins (see [§4 Gotchas](#4-correctness-notes--gotchas)).

This document has two halves:

1. **[Component catalogue](#1-component-catalogue)** — what each box in the diagram *is* and *does*.
2. **[Every connection explained](#2-every-connection-explained-the-why)** — for **each arrow**: what protocol it uses, **why it exists**, and **what depends on it / breaks without it**. This is the "if the API Gateway points at Redis, *why* is it going there?" part.

Legend of arrow types used throughout: **Kafka** (async event), **gRPC** (sync RPC), **HTTP/REST**, **WebSocket** (streaming), **DB/cache** (read or write to a datastore).

---

## 1. Component catalogue

### Clients & edge

| Component | Type / Port | What it does |
|---|---|---|
| **Frontend Clients** | Web · iOS · Android | The trader-facing UI. Calls the gateway's REST API and opens WebSockets for live order/position feeds. |
| **proxy-server.js** | Node helper, `:3001` | A tiny CORS proxy whose only job is to fetch the short-lived **Indira order-notify WS token** for the browser (`/order-notify/ws/createWsToken`). It is not part of the trading path. |
| **API Gateway** | Go, HTTP `:8081` | The single REST entry point (`/api/v1/*`) and host of the `/ws/matches` WebSocket. Runs the middleware chain (Auth · CORS · RateLimit 100/s · CorrelationID · SecurityHeaders · Recovery). **It holds a gRPC client to user-config only**; rules-engine and trade-execution are reached over **HTTP proxy**, and it is otherwise DB-free except for a Redis subscription (see §2). |

### Application microservices (Go)

| Service | Port(s) | Role |
|---|---|---|
| **data-ingestion** | *no server* (docker maps `:50052` but nothing listens) | A **MongoDB change-stream watcher**. Watches `CAG_CHATBOT.NewsImpactDashboard` for new news, enriches each item from a Redis company master, dedupes/validates, and publishes to Kafka `news-events`. |
| **user-config** | gRPC `:50051` | Strategy CRUD (`UserConfigService`). Persists strategies to PostgreSQL `trading_db` with a transactional **outbox**; an outbox worker publishes lifecycle events to `user-config-events`. Also stores AES-encrypted broker credentials into `trading_execution`, and runs the EOD deactivation scheduler (paper 15:00 / live 15:05 IST + per-user times). **Uses no Redis.** |
| **rules-engine** | metrics `:9103`, AMN preview HTTP `:8082` (**no gRPC server**) | Consumes `news-events` + `user-config-events`; matches each news item against an **in-memory** strategy store (seeded by a gRPC `BulkLoad` from user-config and kept current from Kafka); gates on holiday/market-hours; calls risk-management (see bypass note); publishes matched orders to `trade-signals`. Also serves the AMN backfill/preview. |
| **risk-management** | gRPC `:9005` | `CheckPreTradeRisk` + limits/counters in Redis. **Redis-only** (no PostgreSQL at runtime) and **excluded from the PM2 deployment** — see the bypass note in §4. |
| **trade-execution** | gRPC `:9004`, metrics `:9090`, frontend WS `:8081` | Consumes `trade-signals` + `user-config-events`; executes orders live (Indira broker) or as paper trades; manages OCO/trailing-SL, multi-level SL/TP, auto square-off, and a per-user broker order-status WebSocket; writes ticks to a tickstore; and runs the **WebSocket server the frontend connects to** for live/paper order feeds. Publishes `trade-executions` and `order-updates`. |

### Message bus — Apache Kafka topics

| Topic | Producer | Consumer(s) | Payload |
|---|---|---|---|
| `news-events` | data-ingestion | rules-engine | Enriched news item (stock, codes, sentiment, impact, market data) |
| `user-config-events` | user-config (outbox) | rules-engine, trade-execution | Strategy created/updated/deleted/activated/deactivated |
| `trade-signals` | rules-engine | trade-execution | Order to place (matched strategy + instrument + qty/price) |
| `trade-executions` | trade-execution | *(downstream / audit; no in-repo consumer)* | Order placed at broker |
| `order-updates` | trade-execution | *(downstream / audit; no in-repo consumer)* | Order status changes (filled/cancelled/rejected) |

### Data stores

| Store | Holds | Notes |
|---|---|---|
| **PostgreSQL `trading_db`** | strategies, outbox | Owned by user-config; also read by rules-engine for signal auditing. |
| **PostgreSQL `trading_execution`** | orders, fills, positions, encrypted creds | Owned by trade-execution; user-config writes creds here. |
| **MongoDB** | `CAG_CHATBOT` (news source), `OdinMasterData` (instruments), trading holidays | News source is populated by an upstream news/AI system. |
| **Redis DB0** | `isin:{ISIN}` company master; `market:{exch}:{token}` live prices; `user:{id}:matches` pub/sub channel | Company master written by data-ingestion; **`market:` price keys are written by the external market feed, not by any service in this repo** — the services only read them. |
| **Redis DB1** | `ticks:{exch}:{token}` recent tick stream (TTL 12h) | Written by trade-execution's tickstore; read by rules-engine's velocity check. |

### External systems (Indira / 3rd-party)

| System | What it is |
|---|---|
| **Auth Service** | `AUTH_VERIFY_URL` (default `trade.indiratrade.com/auth/verify/token`) — bearer-token verification. **Bypassed by default** (see §4). |
| **Indira Broker REST API** | Order place/cancel/modify, position book, order & trade book. |
| **Indira Order-Status WS** | Per-user stream of fills / rejections / status changes. |
| **Indira Market Data WS** | `enhanced-stream` binary feed of LTP ticks; drives SL/TP price monitoring and the tickstore. |

---

## 2. Every connection explained (the "why")

Each arrow in the diagram is listed below with its **protocol**, **why it exists**, and **what depends on it**. Grouped by the component the arrow starts from.

### 2.1 From the Frontend

| → To | Protocol | Why it exists | What depends on it |
|---|---|---|---|
| **API Gateway** | HTTPS REST + WSS | Every user action (create/list strategies, view paper & live orders, dashboard, match feed) goes through the gateway. | The entire UI. If down, users get 503/401. |
| **proxy-server.js** | HTTP | The Indira token endpoint is CORS-restricted, so the browser can't call it directly. The proxy fetches the WS token on the browser's behalf. | The browser's ability to open the Indira order-notify socket. Without it the token request is blocked by CORS. |
| **trade-execution** (WS) | WebSocket | The frontend connects **directly** to trade-execution's WS server (`/ws/live-orders`, `/ws/paper-trades`) for real-time order/position/P&L pushes. This is **not** proxied through the gateway. | Live/paper order screens updating without a refresh. |

### 2.2 From the API Gateway

| → To | Protocol | Why it exists | What depends on it |
|---|---|---|---|
| **Auth Service** | HTTPS | The `Auth` middleware verifies the bearer token on `/api/v1/*` (health & OPTIONS exempt). | Request authentication — **when not bypassed** (it is bypassed by default, see §4). |
| **user-config** | **gRPC** `:50051` | The gateway's *only* gRPC client. Powers strategy CRUD (`/api/v1/strategies*`). | All strategy create/read/update/delete/activate endpoints. |
| **rules-engine** | HTTP proxy → `:8082` | `POST /api/v1/amn-preview` is proxied to the rules-engine preview server so the gateway stays MongoDB-free. | The AMN backfill preview screen only. Trading is unaffected if it's down. |
| **trade-execution** | HTTP proxy (`TRADE_EXECUTION_PAPER_URL`) | The gateway forwards `/paper-trades/*`, `/live-orders/*`, `/auto-square-off/*`, `/dashboard-stats` to trade-execution's HTTP/paper server. | Paper & live order **REST** endpoints (lists, force-exit, config). |
| **Redis DB0** | Pub/Sub (subscribe) | **This is the "gateway → Redis" arrow.** The `/ws/matches` handler `SUBSCRIBE`s to `user:{userId}:matches` and `/ws/matches/all` `PSUBSCRIBE`s `user:*:matches`, then forwards any received message to the browser WebSocket. So the gateway touches Redis purely to relay a **live "strategy match" feed** to the UI. | The `/ws/matches` feed. **Caveat (from code):** `RedisCache.Publish` in rules-engine is *defined but never called*, so **nothing currently publishes to this channel** — the feed is wired on the consumer side only and stays silent until a producer is added. |

### 2.3 From data-ingestion

| Arrow | Protocol | Why it exists | What depends on it |
|---|---|---|---|
| **MongoDB → data-ingestion** | DB change stream | Watches `CAG_CHATBOT.NewsImpactDashboard` for new news inserts — the entry point of the whole pipeline. | All downstream trading. No news in → nothing happens. |
| **data-ingestion → Redis DB0** | DB read/write (`isin:{ISIN}`) | Enriches each news item with company details looked up by ISIN. On a cache miss it loads from Mongo and caches. News for a company **not in the master (or inactive) is skipped**. | Whether a news item can be resolved to a tradable stock; unresolved items are dropped. |
| **data-ingestion → news-events** | Kafka publish | Emits the enriched news event for the rules-engine. | rules-engine's intake. |

### 2.4 The Kafka event pipeline

| Arrow | Why it exists | What depends on it |
|---|---|---|
| **news-events → rules-engine** | Delivers enriched news to be matched against strategies. | Signal generation. |
| **rules-engine → trade-signals** | Emits an order to place when a strategy matches. | Order execution. |
| **trade-signals → trade-execution** | Primary intake for the executor. | All order placement. |
| **user-config → user-config-events** | The outbox worker publishes strategy lifecycle changes. | Keeping rules-engine and trade-execution in sync with config. |
| **user-config-events → rules-engine** | "Config sync" — updates the **in-memory** strategy store on create/update/activate/deactivate/delete. | rules-engine matching against current strategies without a restart. |
| **user-config-events → trade-execution** | "Strategy events" — on deactivate/delete, close open positions for that strategy; on create/activate, pre-open the user's broker order-status WS. | Positions being squared off when a strategy is turned off, and order updates streaming from the moment a strategy is active. |

### 2.5 From user-config

| → To | Protocol | Why it exists | What depends on it |
|---|---|---|---|
| **PostgreSQL `trading_db`** | DB write | Primary store for strategies + the transactional outbox rows. | All strategy reads/writes; the service **cannot start** without it. |
| **PostgreSQL `trading_execution`** | DB write | Writes AES-encrypted broker credentials into trade-execution's database so the executor can read them at order time. | Live trading (credentials). Non-fatal — falls back to a no-op creds repo if unreachable. |
| **user-config-events** | Kafka publish | Outbox worker emits lifecycle events. | rules-engine & trade-execution config sync. |

### 2.6 From rules-engine

| → To | Protocol | Why it exists | What depends on it |
|---|---|---|---|
| **user-config** | **gRPC** `:50051` (BulkLoad) | **Startup bootstrap**: loads *all* active strategies into memory before consuming any news. | Having anything to match at all. This is a **hard requirement** — rules-engine `Fatal`s if user-config is unreachable at startup. |
| **risk-management** | gRPC `:9005` | Pre-trade `CheckPreTradeRisk` before a signal is published. | Risk enforcement — **but the path is bypassed** (fail-open; service not deployed). See §4. |
| **Redis DB0** | DB read (`market:{exch}:{token}`) | Reads LTP / previous close for price- and %-change strategy conditions. | LTP-based conditions; stale/missing data makes them evaluate on wrong prices. |
| **Redis DB1** | DB read (`ticks:{exch}:{token}`) | The market-price-protection (velocity) check reads the recent tick stream that trade-execution writes. | The velocity guard. Non-fatal (fail-open) — it never blocks order generation. |
| **PostgreSQL `trading_db`** | DB write | **Trade-signal auditing only** — records generated signals. *Not* a strategy source. | Nothing critical: if unreachable, `signalRepo=nil` and matching/publishing continue. |
| **MongoDB** | DB read | Trading-holiday check + AMN backfill/preview (reads `CAG_CHATBOT` + `OdinMasterData`). | Holiday gating and the AMN feature. Non-fatal (holiday check disabled if down). |
| **trade-signals** | Kafka publish | Emits the matched order. | trade-execution. |

### 2.7 From risk-management

| → To | Protocol | Why it exists | What depends on it |
|---|---|---|---|
| **Redis** | DB read/write | Its **only** dependency — stores limits and real-time daily counters (trade count, P&L, drawdown). | Risk checks *when the service is deployed*. It currently isn't (see §4). |

### 2.8 From / to trade-execution

| Arrow | Protocol | Why it exists | What depends on it |
|---|---|---|---|
| **trade-signals → trade-execution** | Kafka consume | Primary signal intake. | Order placement. |
| **trade-execution → Indira Broker REST** | HTTPS | Place / cancel / modify orders; read position book and order/trade book. | Live order placement; without it live trades are blocked. |
| **Indira Order-Status WS → trade-execution** | WebSocket (per user) | Receives fills / rejections / status changes and broadcasts them **in-process** to the frontend WS server (no Redis relay). | Order status accuracy; without it orders stay stuck at `SUBMITTED`. |
| **Indira Market Data WS → trade-execution** | WebSocket (enhanced-stream) | Binary LTP ticks drive the price monitor (SL/TP triggers), OCO trailing SL, paper mark-to-market, and the tickstore writer. | SL/TP firing and paper P&L. |
| **trade-execution → PostgreSQL `trading_execution`** | DB write | Persists orders, fills, positions, and credentials. | Order persistence; the service **won't start** without the `orders` table. |
| **trade-execution → Redis DB0** | DB read (`market:{exch}:{token}`) | Reads LTP / tick size / DPR for fill price and limit-price rounding. | Accurate fills & compliant limit prices. Non-fatal — falls back to hardcoded tick sizes. |
| **trade-execution → Redis DB1** | DB write (`ticks:{exch}:{token}`) | Tickstore writer persists every socket tick (TTL 12h). | Read back by rules-engine's velocity check. Non-fatal. |
| **trade-execution → trade-executions / order-updates** | Kafka publish | Emits execution + status-change events for downstream/audit consumers. | Downstream analytics/notification (no in-repo consumer today). |
| **trade-execution → Frontend** | WebSocket server (`:8081`) | Pushes live/paper `order_update`, `new_order`, `position_exit`, `pnl_update`, `token_expired` events straight to the browser. | Real-time live/paper order & position UI. |

---

## 3. End-to-end flows

**A. News → Order (the money path)**
```
MongoDB news insert
  → data-ingestion (change stream, Redis company enrich)
  → Kafka news-events
  → rules-engine (match in-memory strategies, holiday/hours gate, risk check*)
  → Kafka trade-signals
  → trade-execution (live via Indira REST / or paper)
  → PostgreSQL trading_execution + Kafka trade-executions/order-updates
        * risk check is currently bypassed — see §4
```

**B. Strategy config lifecycle**
```
Frontend → API Gateway → (gRPC) user-config → PostgreSQL trading_db + outbox
  → Kafka user-config-events
      → rules-engine (update in-memory strategy store)
      → trade-execution (close positions on deactivate/delete; pre-open broker WS on activate)
```

**C. Live order status → frontend**
```
Indira Order-Status WS (per user)
  → trade-execution OrderStatusService
  → in-process broadcast (PaperWSServer)
  → Frontend WebSocket (/ws/live-orders)      [no Redis, no gateway in this path]
```

**D. Match feed (currently dormant)**
```
rules-engine RedisCache.Publish("user:{id}:matches", …)   ← NOT wired (method never called)
  → Redis DB0
  → API Gateway /ws/matches (SUBSCRIBE)
  → Frontend
```
The gateway subscribes, but no producer publishes yet, so this feed does nothing until wired.

---

## 4. Correctness notes & gotchas

These are the non-obvious, code-verified facts that a reader (or the diagram) must know:

- **⚠ Risk management is bypassed.** `risk-management` is **excluded from the PM2 deployment** (`deploy-pm2.sh`), and if the rules-engine can't reach it, it sets `riskClient = nil` and **auto-approves every order (fail-open)** — see `services/rules-engine/cmd/main.go`. So in the current deployment pre-trade risk is effectively **not enforced**. **trade-execution never calls risk-management at all** (risk is only ever a rules-engine concern).
- **⚠ Auth verify is bypassed by default.** The gateway sets `Bypass = true` when `AUTH_VERIFY_URL` contains `trade.indiratrade.com` (the code default) — see `api/gateway/config/config.go`. Confirm the deployed `.env.live` to know the real posture.
- **Match feed has no producer.** `rules-engine/internal/cache/redis_cache.go` defines `Publish`, but it is **never called** anywhere in the repo. The gateway's `/ws/matches` subscription therefore has nothing to relay yet.
- **Redis `market:` price keys are external.** No service in this repo writes `market:{exch}:{token}`; they're populated by the external market-data feed. rules-engine and trade-execution only **read** them.
- **data-ingestion runs no server.** It's a change-stream watcher; the `:50052` in docker-compose is unused.
- **rules-engine runs no gRPC server** (its `.proto` exists, but `main.go` starts only Kafka consumers + the metrics/AMN HTTP servers). It also holds **no strategy DB** — strategies live in memory (gRPC bootstrap + Kafka sync).
- **user-config uses no Redis.**
- **RabbitMQ is configured but unused on the live path.** The real bus is **Kafka**; RabbitMQ settings appear in rules-engine config but no live code path uses them.
- **Ports: code defaults vs docker-compose.** Runtime code defaults (e.g. rules-engine `:9103` metrics, trade-execution `:9004` gRPC / `:9090` metrics / `:8081` WS, risk `:9005`) differ from the `50051–50055` range docker-compose maps; the real deployment sets ports via each service's `.env`.

---

*Last updated: 2026-07-13 — derived from the codebase. Pairs with `ARCHITECTURE.png` / `ARCHITECTURE.drawio`.*
