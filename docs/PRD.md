# Project Requirements Document (PRD) — Algo-Trading Platform

**Version:** 2.0 (code-verified) · **Last updated:** 2026-07-14
**Companion docs:** [`HLD.md`](HLD.md) · [`LLD.md`](LLD.md) · [`ARCHITECTURE.md`](ARCHITECTURE.md)
**Supersedes for correctness:** the detailed functional spec in [`Project_Requirements_Document.md`](Project_Requirements_Document.md) (keep it for UI/module detail, but treat *this* PRD as the source of truth for what is actually built).

> Each requirement carries a **Status**: ✅ Implemented · ⚠️ Partial/Conditional · ❌ Not enforced (see [§8 Known limitations](#8-known-limitations)). Status reflects the **code**, not intent.

---

## 1. Overview

A **news-driven algorithmic trading platform** for Indian equities (NSE/BSE). Traders create strategies whose conditions match real-time news; when a match occurs during market hours, the system generates an order and executes it in **paper** or **live** mode via the Indira Securities broker, then manages the full exit lifecycle.

## 2. Goals & non-goals

**Goals**
- Let a user define news-based strategies and have matching trades placed automatically.
- Support both paper (simulated) and live (broker) execution from the same strategy definition.
- Provide real-time order/position/P&L feedback to the UI.
- Manage exits automatically: SL/TP, trailing SL, OCO, multi-level partial exits, and end-of-day square-off.

**Non-goals (current build)**
- Enforced pre-trade risk gating (present in design, not enforced — §8).
- A notification/alert delivery service (events are published to Kafka but not consumed in-repo).
- Backtesting UI (AMN backfill exists for news, not a full backtester).

## 3. Users / personas

| Persona | Needs |
|---|---|
| **Retail trader** | Create/activate strategies; watch paper results; go live; monitor orders/positions; force-exit. |
| **Operator / SRE** | Deploy & monitor services (PM2, Prometheus), manage Kafka/DB/Redis, run migrations. |
| **Upstream news/AI system** | Writes news into MongoDB `CAG_CHATBOT.NewsImpactDashboard` (external producer). |

## 4. Functional requirements

### 4.1 Authentication & session
| ID | Requirement | Status |
|---|---|---|
| FR-A1 | All `/api/v1/*` calls carry a bearer token verified by the gateway auth middleware. | ✅ Always verified — no bypass. Auth service unreachable → 503 (fail closed) |
| FR-A2 | Requests are rate-limited (100 req/s per IP, burst 200) and size-limited (1 MB). | ✅ |
| FR-A3 | Every request gets a correlation ID propagated through gRPC and logs. | ✅ |

### 4.2 Strategy management
| ID | Requirement | Status |
|---|---|---|
| FR-S1 | Create/read/update/delete strategies via REST → gRPC (`user-config`). | ✅ |
| FR-S2 | A strategy has conditions (impact, sentiment, category, stock codes, market cap, %-change, volume, exchange) and a trade config (order type, qty, product, validity, SL/TP, trailing, multi-level, trade window). | ✅ (`strategy_conditions`, `trade_configs`) |
| FR-S3 | Activate/deactivate a strategy; changes propagate to matching within seconds. | ✅ (Kafka `user-config-events` → in-memory store) |
| FR-S4 | Unique strategy name per user; soft-delete. | ✅ (unique partial index, `deleted_at`) |
| FR-S5 | `PAPER` vs `LIVE` trading mode per strategy. | ✅ |

### 4.3 News ingestion & matching
| ID | Requirement | Status |
|---|---|---|
| FR-N1 | Ingest new news from MongoDB in real time and publish to Kafka. | ✅ (change stream → `news-events`) |
| FR-N2 | Enrich news with company/instrument data; skip unknown/inactive companies. | ✅ (Redis company master) |
| FR-N3 | Match each news item against active strategies using a weighted score with a minimum threshold. | ✅ (`matcher/scorer`) |
| FR-N4 | Suppress duplicate matches. | ✅ (`engine/deduper` + `orders.signal_id` unique) |
| FR-N5 | Do not trade outside market hours or on trading holidays. | ✅ (market-hours + Mongo holiday gate; Saturday-mock flag) |

### 4.4 Risk checks
| ID | Requirement | Status |
|---|---|---|
| FR-R1 | Run a pre-trade risk check (limits: daily trades, per-trade risk, exposure, daily loss, per-stock amount, per-strategy trades) before generating an order. | ❌ **Not enforced** — `risk-management` is not deployed and rules-engine auto-approves (fail-open). §8 |
| FR-R2 | Compliance controls: no market orders on entry (limit at LTP±buffer), velocity / market-price protection, banned tokens, max order value/exposure caps. | ✅ (rules-engine compliance caps + LTP-based limit pricing) |

### 4.5 Order execution (paper & live)
| ID | Requirement | Status |
|---|---|---|
| FR-E1 | Consume signals and place orders; route paper vs live by strategy mode. | ✅ (`signal_processor` + `routing_executor`) |
| FR-E2 | Live orders go to Indira REST; retries with backoff; persist to `trading_execution`. | ✅ |
| FR-E3 | Paper orders are simulated and marked-to-market from the live price feed. | ✅ (`paper/*`) |
| FR-E4 | Track order lifecycle (RECEIVED→SUBMITTED→(PARTIALLY_)FILLED / REJECTED / CANCELLED / FAILED). | ✅ (state machine + `execution_events`) |
| FR-E5 | Idempotent processing (no duplicate order per signal). | ✅ (unique `signal_id`) |

### 4.6 Exit management
| ID | Requirement | Status |
|---|---|---|
| FR-X1 | Fixed SL/TP per order. | ✅ |
| FR-X2 | Trailing stop-loss. | ✅ (`oco/trailing`) |
| FR-X3 | OCO (one-cancels-the-other) legs with partial-fill handling. | ✅ (`oco/manager`) |
| FR-X4 | Multi-level partial SL/TP (up to 5 each) with SL breakeven/step moves. | ✅ (`multilevel/manager`, `multi_level_exit_levels`) |
| FR-X5 | Auto square-off at market close (live 15:05 / paper 15:00 IST) with per-user/per-strategy overrides; teardown of resting exit legs first; skip already-flat positions. | ✅ (`scheduler/auto_square_off`) |
| FR-X6 | Manual force-exit (all / per-strategy) via REST. | ✅ |

### 4.7 Real-time feeds
| ID | Requirement | Status |
|---|---|---|
| FR-F1 | Push live/paper order, position and P&L updates to the browser over WebSocket. | ✅ (trade-execution WS server `/ws/live-orders`, `/ws/paper-trades`) |
| FR-F2 | Stream per-user broker order-status updates. | ✅ (Indira order-status WS → in-process broadcast) |
| FR-F3 | Live "strategy match" feed to the UI. | ❌ Consumer wired (`/ws/matches`), **no producer** (§8) |

### 4.8 Broker integration
| ID | Requirement | Status |
|---|---|---|
| FR-B1 | Per-user credentials (encrypted at rest); stateless multi-user broker client. | ✅ (`pkg/indira`, AES creds) |
| FR-B2 | Place/cancel/modify orders; read position/order/trade book; two WebSockets (order-status, market data). | ✅ |
| FR-B3 | Restart recovery: pre-warm broker WS, reconcile in-flight orders, reload OCO/ML groups. | ✅ (OCO reload always; reconcile & ML reload behind env flags) |

## 5. Non-functional requirements

| ID | Requirement | Status |
|---|---|---|
| NFR-1 | **Latency:** news→signal hot path in-memory; matching parallelized (worker pools). | ✅ |
| NFR-2 | **Scalability:** stateless consumers scale by adding instances to the same Kafka group (rules-engine & trade-execution run 2 instances). | ✅ |
| NFR-3 | **Availability/recovery:** panic isolation, graceful shutdown, offset-based catch-up, startup recovery. | ✅ |
| NFR-4 | **Observability:** Prometheus `/metrics`, `/healthz` + `/readyz`, structured logs + correlation IDs. | ✅ |
| NFR-5 | **Security:** bearer auth (always verified), AES creds, rate/size limits, security headers, optional gRPC TLS. | ✅ |
| NFR-6 | **Data integrity:** ACID Postgres, transactional outbox, idempotency keys, DB constraints. | ✅ |
| NFR-7 | **Compliance:** market-hours + holiday gate, LTP-based limit pricing, velocity protection. | ✅ |

## 6. External interfaces

- **Frontend ↔ Gateway:** REST `/api/v1/*`, WS `/ws/matches`.
- **Frontend ↔ trade-execution:** WS `/ws/live-orders`, `/ws/paper-trades`.
- **Gateway ↔ services:** gRPC (user-config), HTTP proxy (rules-engine AMN, trade-execution paper/live).
- **System ↔ Indira:** REST API + order-status WS + market-data WS + auth verify.
- **System ↔ MongoDB:** news source + instrument/holiday masters (upstream-populated).

## 7. Constraints & assumptions

- News is produced into MongoDB by an **external** system; market prices in Redis (`market:*`) are populated by an **external** feed.
- Deployment is PM2 + Docker Compose infra; single Postgres instance hosts both logical DBs in dev.
- Timezone for all trading logic is **Asia/Kolkata (IST)**.

## 8. Known limitations

1. **Pre-trade risk is not enforced** (FR-R1). `risk-management` is excluded from `deploy-pm2.sh`; rules-engine runs `riskClient=nil` → auto-approve (fail-open). trade-execution never calls risk-management.
2. **Match feed has no producer** (FR-F3): `RedisCache.Publish` is defined but never called.
3. **`trade-executions` / `order-updates` have no in-repo consumer** (published for downstream/audit only).
4. **RabbitMQ configured but unused**; Kafka is the live bus.
5. Older doc drift: the detailed BRD and some KT/guide docs describe risk/gRPC/topology that the code does not match — see [`SERVICE_DEPENDENCIES.md`](SERVICE_DEPENDENCIES.md) and [`ARCHITECTURE.md`](ARCHITECTURE.md) for corrections.

## 9. Acceptance criteria (samples)

- **Strategy lifecycle:** creating a strategy returns an ID and persists conditions/config/limits; activating it makes rules-engine match new news within a few seconds without restart.
- **Paper path:** a matching news item during market hours produces a paper order that fills and marks-to-market, with `pnl_update`/`position_exit` events on `/ws/paper-trades`.
- **Live path:** with valid creds, a matching signal places a limit order at LTP±buffer via Indira, streams status on `/ws/live-orders`, and squares off intraday at 15:05 IST.
- **Idempotency:** re-delivering the same signal does not create a second order (unique `signal_id`).
- **Compliance:** no order is generated outside 09:15–15:30 IST or on a trading holiday.

## 10. Traceability (requirement → owner service)

| Area | Service(s) |
|---|---|
| Auth, routing, feeds | api-gateway |
| Strategy CRUD, config events, EOD | user-config |
| News ingest & enrich | data-ingestion |
| Matching, gating, signals, AMN | rules-engine |
| Risk (design) | risk-management |
| Execution, exits, square-off, broker, WS | trade-execution |
