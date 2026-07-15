# High-Level Design (HLD) — Algo-Trading Platform

**Status:** Code-verified · **Last updated:** 2026-07-14
**Companion docs:** [`ARCHITECTURE.md`](ARCHITECTURE.md) · [`ARCHITECTURE.png`](ARCHITECTURE.png) · [`LLD.md`](LLD.md) · [`PRD.md`](PRD.md)

> This HLD describes the system **as built in the code**. Where it differs from older design docs, the code is authoritative. The most important such facts are collected in [§10 Known gaps & deviations](#10-known-gaps--deviations).

---

## 1. Purpose & scope

A **news-driven algorithmic trading platform** for Indian equities (NSE/BSE). Traders define strategies whose conditions match real-time news (sentiment, impact score, category, price/%-change, market cap, exchange). When a news item matches an active strategy, the system generates an order and either **paper-trades** it or places it **live** through the Indira Securities broker, then manages its full lifecycle (SL/TP, OCO, trailing SL, multi-level exits, auto square-off).

**In scope:** strategy management, news ingestion, matching, order execution (paper + live), exit management, real-time order/position feeds, broker integration.
**Out of scope (today):** enforced pre-trade risk (see §10), a notification consumer, and a wired match-feed producer (see §10).

## 2. Architecture style

- **Event-driven microservices** in Go, communicating primarily over **Apache Kafka** (async pipeline) with **gRPC** for one synchronous control call (gateway → user-config; rules-engine → user-config/risk).
- **Polyglot persistence:** PostgreSQL (relational state), MongoDB (news + instrument masters + holidays), Redis (cache, market prices, tickstore, pub/sub).
- **Single REST/WS entry point** (API Gateway) for the frontend; internal services are not exposed to clients directly except trade-execution's frontend WebSocket server.
- **Process-per-service**, deployed under **PM2** (see `deploy-pm2.sh`); infrastructure via Docker Compose.

## 3. Component overview

| # | Component | Kind | Responsibility |
|---|---|---|---|
| 1 | **API Gateway** | Go HTTP `:8081` | REST `/api/v1/*`, `/ws/matches`; auth/CORS/rate-limit middleware; gRPC → user-config; HTTP proxy → rules-engine & trade-execution; Redis subscribe for match feed |
| 2 | **user-config** | Go gRPC `:50051` | Strategy CRUD, transactional outbox → Kafka, EOD deactivation scheduler, encrypted broker credential storage |
| 3 | **data-ingestion** | Go worker (no server) | MongoDB change-stream watcher → enrich → Kafka `news-events` |
| 4 | **rules-engine** | Go consumers + HTTP `:8082` | Match news to in-memory strategies → Kafka `trade-signals`; holiday/market-hours gate; risk call; AMN backfill/preview |
| 5 | **risk-management** | Go gRPC `:9005` | Pre-trade risk check (Redis-backed). **Not deployed** — see §10 |
| 6 | **trade-execution** | Go gRPC `:9004` + WS `:8081` | Execute orders (live/paper); OCO, trailing SL, multi-level SL/TP; auto square-off; broker order-status WS; frontend WS server |

Full per-arrow rationale is in [`ARCHITECTURE.md` §2](ARCHITECTURE.md#2-every-connection-explained-the-why).

## 4. Technology stack

| Layer | Choice |
|---|---|
| Language | Go 1.23+ (workspace `go.work`) |
| RPC | gRPC + Protocol Buffers (`api/proto/*`) |
| Event bus | Apache Kafka (`segmentio/kafka-go`) + Zookeeper; Kafka-UI for ops |
| Relational DB | PostgreSQL 15 — logical DBs `trading_db` (config) & `trading_execution` (orders) |
| Document DB | MongoDB 7 — `CAG_CHATBOT` (news), `OdinMasterData` (instruments), holidays |
| Cache / stream | Redis 7 — DB0 (company master, market prices, match pub/sub), DB1 (tickstore) |
| Broker | Indira Securities REST + two WebSockets (order-status, market data) |
| Observability | Prometheus metrics (`/metrics`), zap structured logs, correlation IDs |
| Process mgmt | PM2 (app services), Docker Compose (infra) |

## 5. Runtime data flows

**Primary (news → order):**
```
Mongo news insert → data-ingestion → Kafka news-events → rules-engine
   → (holiday/hours gate, risk check*) → Kafka trade-signals → trade-execution
   → Indira REST (live) / paper executor → Postgres trading_execution
   → Kafka trade-executions / order-updates
   (* risk currently bypassed — §10)
```

**Config lifecycle:**
```
Frontend → Gateway → (gRPC) user-config → Postgres trading_db + outbox
   → Kafka user-config-events → { rules-engine: refresh in-memory store,
                                  trade-execution: close positions / pre-open broker WS }
```

**Live order status → UI:**
```
Indira Order-Status WS → trade-execution → in-process broadcast → Frontend WS
```

See [`ARCHITECTURE.md` §3](ARCHITECTURE.md#3-end-to-end-flows) for all four flows including the (dormant) match feed.

## 6. Deployment topology

- **PM2-managed services** (`deploy-pm2.sh`), started in order: `data-ingestion → user-config → rules-engine → trade-execution → api-gateway`. `rules-engine` and `trade-execution` run **2 instances each** (same Kafka consumer group → partitions auto-distributed). **`risk-management` is intentionally excluded.**
- **Docker Compose** provisions infra: postgres, mongodb, redis, kafka, zookeeper, kafka-ui (and can also run the app services in `docker-compose.yml`).
- Each service is 12-factor: config via env / `.env`, logs to stdout (PM2 captures a single combined file per instance, rotated via `pm2-logrotate`).

## 7. Scaling & availability

- **Stateless consumers scale horizontally** by adding instances to the same Kafka consumer group (already done for rules-engine & trade-execution).
- **rules-engine holds strategy state in memory**, rebuilt on start via gRPC `BulkLoad` from user-config and kept current from `user-config-events` — so a restarted instance self-heals from Kafka + gRPC.
- **trade-execution restart recovery:** broker-WS pre-warm for users with live exposure; optional in-flight order reconciliation (`ENABLE_STARTUP_RECONCILE`) and multi-level reload (`ENABLE_ML_RELOAD`); OCO group reload from DB.
- **Kafka retention** lets recovered consumers catch up from the last committed offset.

## 8. Cross-cutting concerns

- **Observability:** Prometheus `/metrics` (trade-execution `:9090`, rules-engine `:9103`), `/healthz` + `/readyz` (trade-execution), structured zap logs, `X-Correlation-ID` propagated HTTP→gRPC→logs.
- **Resilience:** per-RPC gRPC panic-recovery interceptors; goroutine panic recovery in background workers; graceful shutdown with ordered component teardown (`lifecycle` manager in trade-execution).
- **Security:** bearer-token auth middleware at the gateway (see §10 bypass), AES-encrypted broker credentials at rest, request size limits, rate limiting, security headers, optional gRPC TLS (`GRPC_TLS_CERT`/`KEY`).
- **Compliance controls (algo trading):** market-hours + trading-holiday gate; limit orders at LTP±buffer (no market orders on entry); SL-L square-off; velocity / market-price-protection check.

## 9. Key design decisions

| Decision | Rationale |
|---|---|
| Kafka as the pipeline bus | Decouples ingestion/matching/execution; replay + horizontal scale. |
| Strategies in-memory in rules-engine | Hot-path matching must be fast; source of truth stays in user-config (gRPC bootstrap + Kafka sync). |
| Transactional outbox in user-config | Guarantees a Kafka event for every committed config change. |
| Separate `trading_db` / `trading_execution` | Config vs execution isolation; user-config writes creds into the execution DB so the executor can read them locally. |
| In-process WS broadcast in trade-execution | Live order/position events reach the browser with the lowest latency (no Redis relay). |
| Custom OCO / multi-level SL/TP engine | Broker-native bracket orders are insufficient for trailing + partial multi-level exits with restart recovery. |

## 10. Known gaps & deviations

These are **code-verified** and must be understood when reading any older doc:

1. **Pre-trade risk is effectively bypassed.** `risk-management` is excluded from `deploy-pm2.sh`; when rules-engine can't reach it, `riskClient = nil` → **every order is auto-approved (fail-open)**. trade-execution never calls risk-management.
2. **Auth verify is bypassed by default** when `AUTH_VERIFY_URL` host = `trade.indiratrade.com` (gateway config default).
3. **Match feed has no producer** — `rules-engine`'s `RedisCache.Publish` is defined but never called, so the gateway `/ws/matches` subscription is dormant.
4. **RabbitMQ is configured but unused**; the live bus is Kafka.
5. **`market:` Redis price keys are populated externally** (no in-repo writer).
6. **No gRPC server** actually runs in rules-engine or data-ingestion (data-ingestion runs no server at all).
7. **Ports differ** between code defaults and docker-compose's `50051–50055` mapping; real ports come from each service's `.env`.
