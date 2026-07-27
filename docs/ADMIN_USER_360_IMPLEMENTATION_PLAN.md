# Admin Panel — Per-User 360° View: Implementation Plan

**Goal:** From the admin panel, select any user and see *everything* about them with
accurate data — strategy config, price-monitor watches, real trades, open positions,
P&L, broker credentials/session health, and **which signals were selected vs rejected
and why** — without SSH-ing into the box to read logs.

**Status:** Implemented (all phases). See §11 for what changed versus this plan.
**Owner:** TBD
**Related:** [admin-aggregator README](../../admin-aggregator/README.md)

---

## 1. The one real gap

Almost everything the admin needs is *already* in Postgres or already exposed by a
running service. Only one thing is missing, and it happens to be the most valuable
thing on the list:

> **Rejected / skipped signals exist only in log lines. Nothing is persisted.**

`rejectForCompliance` and `rejectForDPR`
([handler.go:125-143](../services/rules-engine/internal/consumer/handler.go#L125-L143))
only call `h.logger.Warn(...)`. Strategy match failures, budget skips, duplicate-stock
skips, and cap skips are the same — a log line and nothing else. `trade_signals` has
only ever held four statuses (`PENDING`, `FAILED`, `EXECUTED`, `CANCELLED`); there is no
`REJECTED` row, because a rejected signal never gets written at all.

So "why didn't my strategy fire on that news?" — the single most common support
question — is unanswerable from data today. It requires grepping 134 MB of pm2 logs,
and those logs were also masked at the time (`STRATEGY_NAME_LOG_OVERRIDE` rewrites every
`strategy_name` to a placeholder and strips `strategy_id`, see
[logmask.go](../services/rules-engine/internal/logmask/logmask.go)).

> **Update:** masking has since been turned off — `STRATEGY_NAME_LOG_OVERRIDE` is
> now empty in `services/rules-engine/.env`, so logs carry the real `strategy_id`
> and `strategy_name` again. Set a placeholder value there to re-enable it for a
> demo session.

**This plan is therefore in two halves:**
- **Phase 0** adds decision persistence. This is the only phase that touches Go
  trading code, and it is the only phase with hot-path risk.
- **Phases 1-5** are read-only aggregator + UI work over data that already exists.

Phases 1-5 are independently useful and can ship before Phase 0 lands.

---

## 2. Data inventory — what exists vs what must be built

| Admin needs | Source | Status |
|---|---|---|
| User directory | `trading_db.strategies.user_id` ∪ `trading_execution.broker_accounts.user_id` | ✅ exists |
| Strategy config | `trading_db`: `strategies`, `strategy_conditions`, `trade_configs`, `risk_limits` | ✅ exists |
| Selected signals | `trading_db.trade_signals` | ✅ exists |
| **Rejected signals + reason** | **log lines only** | ❌ **build (Phase 0)** |
| Orders + lifecycle | `trading_execution`: `orders`, `execution_events`, `order_status_history` | ✅ exists |
| Fills | `trading_execution`: `fills`, `position_fills` | ✅ exists |
| Open positions / P&L | `trading_execution`: `positions`, `daily_pnl_summary` | ✅ exists |
| Latency timeline | `trading_execution.signal_metrics` (per-stage timestamps) | ✅ exists |
| OCO groups + legs | `trading_execution`: `order_groups`, `order_group_legs` | ✅ exists |
| Broker credentials / session | `trading_execution.broker_accounts` (`bearer_token`, `is_active`, `token_updated_at`) | ✅ exists — **must mask** |
| Live price-monitor watches | in-memory; `GetWatchSnapshot(userID)` at [price_monitor.go:1036](../services/trade-execution/internal/scheduler/price_monitor.go#L1036), served by `GET /ws/live-orders/price-watches` | ✅ exists — reuse |
| Daily trade-cap usage | Redis `strat:trades:<strategy_id>:<IST-date>` ([tradecap.go:45](../pkg/tradecap/tradecap.go#L45)) | ✅ exists |

Two notes on the inventory:

- `user_credentials` in [init_all_schemas.sql:184](../deployments/docker/init_all_schemas.sql#L184)
  is **legacy and does not exist in the live DB**. The live credential table is
  `trading_execution.broker_accounts`. Do not build against the former.
- `trading_execution.orders` marks *executed* entry orders as `CANCELLED` once the
  position closes. `trading_db.trade_signals` is the accurate record of what traded.
  The admin panel must read trade outcomes from `trade_signals` / `positions`, not from
  `orders.status`, or it will report zero trades. (Tracked separately as a data bug.)

---

## 3. Phase 0 — Decision audit trail

### 3.1 Table

New table in `trading_db` (same DB as `trade_signals`, so a decision and its resulting
signal can be joined without a cross-DB query).

```sql
CREATE TABLE IF NOT EXISTS signal_decisions (
    decision_id   BIGSERIAL PRIMARY KEY,
    event_id      VARCHAR(255) NOT NULL,
    user_id       VARCHAR(255) NOT NULL,
    strategy_id   UUID         NOT NULL,
    strategy_name VARCHAR(255) NOT NULL,

    symbol        VARCHAR(50),
    stock_code    BIGINT,
    exchange      VARCHAR(20),

    -- MATCHED | REJECTED
    outcome       VARCHAR(20)  NOT NULL,
    -- which pipeline stage decided: MATCH | COMPLIANCE | SIZING | RISK | CAP | WINDOW
    stage         VARCHAR(20)  NOT NULL,
    -- machine-readable code, e.g. PCT_CHANGE_ABOVE_MAX, DPR_UPPER_BREACH
    reason_code   VARCHAR(64),
    -- human sentence for the UI
    reason_detail TEXT,

    -- what was compared, so the UI can render "needed X, got Y"
    limit_value   NUMERIC(20,4),
    actual_value  NUMERIC(20,4),

    -- full evaluator output; cheap to store, invaluable for debugging
    matched_conditions TEXT[],
    failed_conditions  TEXT[],
    condition_scores   JSONB,
    match_score        NUMERIC(5,2),

    -- news snapshot as evaluated (feed values, not re-derived later)
    impact_score  INT,
    sentiment     VARCHAR(50),
    news_category VARCHAR(255),
    pct_change    NUMERIC(10,4),
    ltp           NUMERIC(15,2),

    -- set when outcome=MATCHED and an order was published
    order_id      VARCHAR(255),

    correlation_id VARCHAR(64),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_sigdec_user_date    ON signal_decisions (user_id, created_at DESC);
CREATE INDEX idx_sigdec_strategy_date ON signal_decisions (strategy_id, created_at DESC);
CREATE INDEX idx_sigdec_event        ON signal_decisions (event_id);
CREATE INDEX idx_sigdec_outcome      ON signal_decisions (outcome, reason_code, created_at DESC);
CREATE INDEX idx_sigdec_order        ON signal_decisions (order_id) WHERE order_id IS NOT NULL;
```

**Volume:** ~325 news events/day × active strategies evaluated per event. At ~30 active
strategies that is ~10k rows/day, ~3.6M/year — comfortable for Postgres with these
indexes. Add monthly partitioning + a 90-day retention job only if it grows past that;
do not partition on day one.

### 3.2 Reason taxonomy

Derived from the actual rejection sites in the code, so the UI can offer a fixed filter
list rather than free text.

**Stage `MATCH`** — from `EvaluationResult.FailedConditions`
([evaluator.go](../services/rules-engine/internal/matcher/evaluator.go)):
`impact_score`, `sentiment`, `category`, `market_cap`, `price_range`, `pct_change`,
`exchange`, `stock_codes`, `volume`

**Stage `COMPLIANCE`** — [handler.go:840-902](../services/rules-engine/internal/consumer/handler.go#L840-L902):
`BANNED_TOKEN`, `QTY_LIMIT_EXCEEDED`, `ORDER_VALUE_LIMIT_EXCEEDED`,
`EXPOSURE_LIMIT_EXCEEDED`, `DPR_LOWER_BREACH`, `DPR_UPPER_BREACH`, `VELOCITY_BREACH`

**Stage `SIZING`** — [handler.go:788-807](../services/rules-engine/internal/consumer/handler.go#L788-L807):
`BUDGET_TOO_SMALL` (cannot afford 1 share within `max_amount_per_stock`),
`INVALID_SIZING_PRICE`

**Stage `RISK`** — `RISK_REJECTED`, plus `RISK_BYPASSED` while
[handler.go:909](../services/rules-engine/internal/consumer/handler.go#L909) still reads
`if false && h.riskClient != nil`. Surfacing `RISK_BYPASSED` in the admin UI makes that
bypass impossible to forget about.

**Stage `CAP`** — `DUPLICATE_STOCK_TODAY`, `STRATEGY_CAP_REACHED` (rules-engine at
publish time, and trade-execution at trigger time via `cancelForCapReached`)

**Stage `WINDOW`** — `MARKET_CLOSED`, `OUTSIDE_TRADE_WINDOW`, `PCT_CHANGE_ABOVE_MAX`

### 3.3 Writer — must not slow the hot path

Non-negotiable: news evaluation is latency-sensitive (`e2e_ms` is tracked and currently
~5-20 ms). The decision writer must never block it.

```
evaluation → decisionRecorder.Record(rec)   // non-blocking send to buffered chan
                     │
                     └── background goroutine: batch up to 200 rows / 1s
                                              → single multi-row INSERT
```

Rules:
- Buffered channel (say 10 000). On overflow **drop and increment a dropped counter**
  exposed on `/metrics` — never block the evaluator.
- Batch insert with `context.WithTimeout`; a DB stall must not back-pressure the caller.
- Wrap the goroutine in the project's existing panic-recovery helper (commit `cfe365f`
  added this pattern across services).
- Feature-flag it: `SIGNAL_DECISIONS_ENABLED=true`. Ship disabled, enable in staging,
  watch `e2e_ms` and the drop counter, then enable in prod.

### 3.4 Call sites

- `internal/consumer/handler.go` — one `Record(...)` at each `return nil` rejection path
  (the 7 compliance sites, 2 sizing sites, cap sites, risk site) and one on successful
  publish with `outcome=MATCHED` + `order_id`.
- `internal/kafka/news_consumer.go` — record `MATCH`-stage rejections for strategies the
  evaluator considered but did not match. **Note:** today only *matched* strategies are
  logged (`Matched strategy` lines); non-matches are invisible. The evaluator already
  computes `FailedConditions`, so this is a plumbing change, not new logic.
- `internal/scheduler/price_monitor.go` (trade-execution) — record `STRATEGY_CAP_REACHED`
  at trigger time. Cross-service write, so either a small gRPC call to rules-engine or a
  direct insert from trade-execution; **prefer a direct insert** to avoid a new dependency.

---

## 4. Phase 1 — Aggregator: user directory + detail API

All new code in `admin-aggregator/src/collectors/users.js`, wired into
[server.js](../../admin-aggregator/src/server.js). Read-only, follows the existing
lazy-pool + failure-isolated pattern in
[db.js](../../admin-aggregator/src/collectors/db.js).

| Method | Path | Returns |
|---|---|---|
| GET | `/api/users?q=&limit=` | directory: user_id, #strategies, #active, mode, last activity, broker status |
| GET | `/api/users/:id/summary` | KPI header: today's signals/trades/P&L, open positions, cap usage, credential health |
| GET | `/api/users/:id/strategies` | full config per strategy — conditions + trade config + risk limits, joined |
| GET | `/api/users/:id/signals?from=&to=&status=` | `trade_signals` rows |
| GET | `/api/users/:id/decisions?from=&to=&outcome=&reason=` | **Phase 0** table; the "why" view |
| GET | `/api/users/:id/orders?from=&to=` | `orders` + joined `execution_events` timeline |
| GET | `/api/users/:id/positions?status=OPEN` | `positions` + live LTP where available |
| GET | `/api/users/:id/watches` | live price-monitor watches (proxied, see Phase 3) |
| GET | `/api/users/:id/credentials` | **masked** broker/session health, see Phase 4 |
| GET | `/api/users/:id/timeline/:orderId` | one order end-to-end: decision → signal → order → fills → position → exit |

`/timeline/:orderId` is the highest-value endpoint. It replaces exactly the manual log
trace done during incident review: news event → match/reject → sizing → compliance →
publish → watch → trigger → broker submit → fill → OCO adoption → SL/TP → exit + P&L.
Sources: `signal_decisions`, `trade_signals`, `orders`, `execution_events`,
`order_status_history`, `fills`, `order_groups`, `positions`, `signal_metrics`.

**Guardrails:** every query parameterised (`$1`), `statement_timeout` already set to
5000 ms on the pools, all date ranges bounded server-side (default today, max 90 days),
`LIMIT` capped. The existing DB-explorer allowlist pattern
([dbExplorer.js](../../admin-aggregator/src/collectors/dbExplorer.js)) should be reused —
do not add a raw-SQL endpoint.

---

## 5. Phase 2 — UI: Users list + user detail

New pages under `admin-panel/src/pages/`, routes added in
[App.tsx](../../admin-panel/src/App.tsx) and a `Users` nav entry.

- `Users.tsx` — searchable table; click-through to detail.
- `UserDetail.tsx` — KPI header + tabs:
  - **Overview** — today at a glance; cap usage `5/5`; credential/session badge
  - **Strategies** — config cards. Render conditions in plain language
    ("Financial Results, impact 6-10, NSE, move 4-15%") next to raw values, so config
    mistakes are obvious without reading JSON.
  - **Signals** — selected signals, filterable
  - **Decisions** — selected *and* rejected, grouped by reason (Phase 0)
  - **Orders** — with expandable per-order timeline
  - **Positions** — open + closed, P&L
  - **Watches** — live price monitor: target, max, LTP, distance-to-target %
  - **Credentials** — masked, with session health

Reuse existing `Kpi.tsx` and `Spark.tsx`. Charts go through `recharts`, already a
dependency.

---

## 6. Phase 3 — Live price-monitor watches

Watches are in-memory in trade-execution, so they cannot be read from Postgres.
`GetWatchSnapshot(userID)` and `GET /ws/live-orders/price-watches` already exist —
the aggregator proxies that endpoint rather than duplicating state.

Two additions worth making to `WatchSnapshot`
([price_monitor.go:1014](../services/trade-execution/internal/scheduler/price_monitor.go#L1014)),
both cheap and both directly useful to an admin:
- `current_ltp` — the last price seen for the symbol
- `distance_pct` — how far the stock is from its trigger

Push over the existing WebSocket hub as a `watches_update` message on the poller tick,
matching the current `services_update` / `metrics_update` pattern in
[poller.js](../../admin-aggregator/src/poller.js).

Also surface the tick-feed health already visible in the logs
(`WSS healthy: false`, `NO_TICK`) — a stale price feed silently degrades every watch and
every trailing stop, and today nothing outside the log stream shows it.

---

## 7. Phase 4 — Credentials & security

This phase changes the panel's risk profile: it is currently ops telemetry, and this
adds per-user trading data plus credential material.

**Never send a bearer token to the browser.** `/api/users/:id/credentials` returns only:

```json
{
  "broker_name": "INDIRA",
  "broker_user_id": "IS19094",
  "app_id": "AB1234",
  "source": "WEB",
  "is_active": true,
  "token_present": true,
  "token_last4": "…7f2a",
  "token_updated_at": "2026-07-23T08:00:06+05:30",
  "token_age_hours": 7.1,
  "session_status": "HEALTHY"
}
```

`session_status` derives from recent `HTTP 401 / Session expired` occurrences for that
user — the failure mode seen twice today. That turns an invisible, retry-masked problem
into a visible badge.

Hardening required before this ships:
1. **Admin audit log** — record which admin viewed which user's data and when.
   Non-negotiable once credentials and P&L are exposed.
2. **Role separation** — at minimum `viewer` vs `admin`; credential tab gated to `admin`.
   Auth today is a single shared `ADMIN_USER`/`ADMIN_PASS`
   ([auth.js](../../admin-aggregator/src/auth.js)).
3. **JWT expiry + refresh** — verify current expiry; sessions must not be indefinite.
4. **Bind the aggregator to localhost / VPN only**, fronted by the existing reverse proxy.
   It must not be internet-reachable on `:4500`.
5. Keep the aggregator **strictly read-only** — no square-off, no cancel, no token reset.
   Any write action belongs in the trading services behind their own auth.

---

## 8. Phase 5 — Decisions explorer

Cross-user view answering fleet-level questions:
- reason-code histogram for a date range ("what is rejecting the most, system-wide?")
- funnel: events ingested → strategies evaluated → matched → published → triggered →
  filled, with drop-off at each stage
- per-strategy match rate, to spot configs that can never fire

This is where a config bug like the market-cap format mismatch (`"SMALL"` vs
`"Small Cap"`, fixed in
[evaluator.go](../services/rules-engine/internal/matcher/evaluator.go)) would have shown
up immediately as *"strategy X: 0% match rate, 100% rejected on `market_cap`"* instead of
going unnoticed.

It would equally have surfaced today's finding that **`pct_change` arrives as `0` on
100% of news events**, so the 4-15% filter never discriminates and every signal falls
through to the `below_min` breakout path.

---

## 9. Sequencing

| Order | Phase | Depends on | Risk |
|---|---|---|---|
| 1 | Phase 1 + 2 (users API + UI, existing data) | — | none, read-only |
| 2 | Phase 3 (live watches) | Phase 1 | low, proxy only |
| 3 | Phase 4 (credentials + hardening) | Phase 1 | **security review required** |
| 4 | Phase 0 (decision persistence) | — | **hot path — flag-gated rollout** |
| 5 | Phase 5 (decisions explorer) | Phase 0 | none |

Phases 1-3 deliver most of the day-to-day value and touch no trading code. Phase 0 is
sequenced after them deliberately: it is the only change that can affect live execution
latency, so it should land on its own, behind a flag, with `e2e_ms` watched.

---

## 10. Open questions

1. **Retention** — how long should `signal_decisions` be kept? 90 days assumed above.
2. **Historical backfill** — logs go back months. Worth a one-off parser to seed
   `signal_decisions` from `rules-engine-out.log`? Note the log masking strips
   `strategy_id`, so backfill can only be partial.
3. **Multi-tenancy** — should an admin see all users, or only those in their org?
   Current model assumes a single trusted operator.
4. **Paper vs live** — separate views or one view with a mode filter?
5. **`orders.status = CANCELLED` on executed entries** — fix the writer, or have the
   admin panel treat `trade_signals` as authoritative? Recommend fixing the writer;
   the panel should not encode a workaround for a data bug.

---

## 11. What changed during implementation

Three things were wrong in the plan above and were corrected while building. They
are recorded here because each one would silently produce a confidently wrong
number in the panel.

### 11.1 The positions / fills / P&L tables are empty

The plan sourced open positions and P&L from `trading_execution.positions` and
`fills`. Those tables have **zero rows** — for every user, not just recently:

```
positions: 0     fills: 0            position_fills: 0
order_groups: 0  daily_pnl_summary: 0  signal_metrics: 0
orders: 8243     execution_events: 16140
```

The live path never populates them. Entry, exit, reason and P&L all live on the
`orders` row itself (`filled_price`, `live_exit_price`, `live_pnl`,
`live_exit_reason`, and the `paper_*` equivalents), which is also where the
schema's real indexes point (`idx_orders_live_closed`, `idx_orders_live_exit_time`).

A single `TRADES_VIEW` projection in
[users.js](../../admin-aggregator/src/collectors/users.js) is now the one place
that defines "a trade", and every endpoint reads through it. `signal_metrics`
being empty also means the latency-timeline idea in §4 has no data behind it yet;
the timeline endpoint uses `execution_events` instead, which is populated.

### 11.2 A trade is `filled_quantity > 0`, and OCO legs are not positions

Two filters are load-bearing:

- **`filled_quantity > 0`** — not `status`, for the reason in §2.
- **`parent_order_id IS NULL`** — excludes OCO `SL_LEG` / `TP_LEG` rows.

The second was found by review: a bracket trade writes three rows (entry + SL leg
+ TP leg), and the filled leg has `filled_quantity > 0` with no exit timestamp of
its own. Counting legs made every closed trade *also* appear as an open one —
IS19094 showed **61 open positions instead of 7** until the filter was added.

### 11.3 Fail-soft query wrappers must still log

`qSafe` originally swallowed errors so a missing table degraded one tab instead
of the page. That also made a renamed column (`broker_order_id`, which is
actually `indira_order_id`) look exactly like "no data" — the summary reported
₹0.00 P&L on a day that made ₹45.06. It now logs every failure while still
returning `[]`.

### 11.4 Verified against live data

The endpoints were checked against 2026-07-23 for IS19094 and reproduce the
figures derived independently from the pm2 logs:

| Check | Panel | Logs |
|---|---|---|
| Signals today | 10 (5 executed, 5 pending) | 10 |
| Net P&L | ₹45.06 (2W/3L) | ₹45.06 |
| Trade-cap usage, "July 22 Fin" | 5 / 5 | 5 / 5 |
| Credentials | masked, `HEALTHY`, 5.6 h old | — |
| Role gating | viewer → 403 on credentials, 200 on signals | — |

### 11.5 Deviations worth knowing

- **`pkg/decisions`, not `services/rules-engine/internal/decisions`.** The price
  monitor in trade-execution also records cap rejections, and an `internal/`
  package cannot cross module boundaries.
- **The engine records only match-stage rejections; the handler records the
  terminal outcome.** Otherwise every published order would produce two rows and
  the funnel would double-count.
- **`RISK_BYPASSED` is recorded on every published order** while
  [handler.go:909](../services/rules-engine/internal/consumer/handler.go#L909)
  still reads `if false && …`. A bypass visible on every row in the UI is much
  harder to leave switched on than a `TODO`.
- **Role separation is username-based but there is still one credential pair.**
  `ADMIN_ROLE_USERS` decides who is an admin, yet `checkCredentials` only accepts
  the single `ADMIN_USER`/`ADMIN_PASS`. Real multi-user auth is still outstanding;
  the gate is enforced and tested, but today it can only ever apply to one login.
