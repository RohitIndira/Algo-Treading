# AMN Persisted Selections & Day-Wise Reactivation — Knowledge Transfer

## 📋 Table of Contents
1. [Overview](#overview)
2. [What changed & why](#what-changed--why)
3. [End-to-end flows](#end-to-end-flows)
4. [Data model](#data-model)
5. [Event contract](#event-contract)
6. [Service-by-service changes](#service-by-service-changes)
7. [Public API contract (for frontend)](#public-api-contract-for-frontend)
8. [Idempotency & duplicate-order guard](#idempotency--duplicate-order-guard)
9. [Operational notes](#operational-notes)
10. [Testing](#testing)
11. [Known gaps & follow-ups](#known-gaps--follow-ups)

---

## Overview

The **After-Market-News (AMN)** feature lets a user create a strategy that, once
created, scans the previous NSE_EQ trading day's news and places orders for
matching stocks. Previously the user's **preview selection was never persisted**:
it rode a single Kafka event and was consumed once by the create-time backfill,
then lost.

This change makes the selection a **first-class, day-wise, persisted record** and
turns **reactivation** into a repeatable daily flow: each time a user reactivates
an AMN strategy they submit a **fresh preview pick**, which is stored and used to
**re-run the backfill for that day**. A pick is not mandatory — an empty selection
activates the strategy for live news with no backfill.

---

## What changed & why

| Before | After |
|---|---|
| `process_after_market_news` was create-time-only (`db:"-"`), not stored | Persisted column on `strategies` — reactivation & UI can identify AMN strategies |
| Selected stocks (ISINs) rode the Kafka event once, never stored | Stored in normalized `amn_activations` + `amn_activation_stocks` tables, one record per strategy per trading day |
| Only `place`-bucket picks were meaningful | `monitor` (price-watch) picks are persisted too, with their `target_price` |
| Backfill fired only on `CONFIG_CREATED` | Fires on create **and** reactivation (`CONFIG_ACTIVATED`); never on a plain edit |
| Reactivation carried no selection | Reactivation accepts a fresh pick and stores it day-wise. An **empty** pick is allowed (no matching news in the AMN window): the strategy still activates for live news, and no backfill runs |
| Re-running could double-place orders | Cross-run guard skips stocks already ordered today |

---

## End-to-end flows

### Create (AMN strategy)
```
UI preview → user picks stocks
  → POST /api/v1/strategies { process_after_market_news:true, amn_selection:[...] }
    → user-config: INSERT strategies(process_after_market_news=true)
                   + upsert amn_activations (source=CREATE, today)
                   + amn_activation_stocks (one row per pick)   [same TX]
                   + outbox STRATEGY_CREATED (payload carries ISINs)
      → outbox worker → Kafka CONFIG_CREATED
        → rules-engine consumer: upsert config + trigger AMN backfill (once)
```

### Reactivate (next day)
```
UI reads strategy.process_after_market_news == true
  → shows AMN preview popup driven by the strategy's stored conditions
  → user re-picks stocks
    → POST /api/v1/strategies/{id}/activate { amn_selection:[...] }
      → user-config: UPDATE strategies SET active=true, version=version+1
                     + REJECT if AMN strategy and selection empty
                     + upsert amn_activations (source=REACTIVATE, today) + child stocks  [same TX]
                     + outbox STRATEGY_ACTIVATED (thin)
        → outbox worker: re-read strategy + latest activation ISINs
                       → Kafka CONFIG_ACTIVATED (full payload + ISINs)
          → rules-engine consumer: upsert config + RE-RUN AMN backfill
                                   (scoped to the fresh pick; duplicate guard applies)
```

---

## Data model

Migration: [`migrations/002_amn_selections.sql`](../../services/user-config/migrations/002_amn_selections.sql) (idempotent, `CREATE ... IF NOT EXISTS` style, matching `001_init.sql`).

**`strategies.process_after_market_news`** `BOOLEAN NOT NULL DEFAULT false`
— persisted AMN flag.

**`amn_activations`** — parent, one row per strategy per trading day:

| column | notes |
|---|---|
| `activation_id` | UUID PK |
| `strategy_id` | FK → `strategies` ON DELETE CASCADE |
| `user_id` | |
| `trading_date` | DATE (IST) |
| `source` | `CREATE` \| `REACTIVATE` |
| `strategy_version` | version at activation |
| `created_at` / `updated_at` | |

`UNIQUE(strategy_id, trading_date)` — one record per day; same-day re-activation
**upserts** the parent and **replaces** the children.

**`amn_activation_stocks`** — child, one row per selected stock:

| column | notes |
|---|---|
| `id` | BIGSERIAL PK |
| `activation_id` | FK → `amn_activations` ON DELETE CASCADE |
| `isin`, `symbol`, `nse_code` | stock identity |
| `bucket` | `place` \| `monitor` |
| `target_price` | monitor trigger level (0 for `place`) |
| `entry_price`, `quantity`, `invested_amount` | preview-time snapshot |

`UNIQUE(activation_id, isin)`.

> The child table is the "monitoring" persistence: `monitor`-bucket picks are the
> price-watch selections that trigger at `target_price`.

---

## Event contract

New Kafka `ConfigEventType`: **`CONFIG_ACTIVATED`** (both sides):
- User-config: [`internal/events/config_event.go`](../../services/user-config/internal/events/config_event.go)
- Rules-engine: [`internal/configsync/event.go`](../../services/rules-engine/internal/configsync/event.go)

Semantics: **upsert the strategy (like `CONFIG_UPDATED`) AND re-run the AMN backfill
for AMN strategies.** A plain `CONFIG_UPDATED` (edit) never re-runs the backfill —
this separation is the whole reason for the distinct type.

The `StrategyPayload` already carries `process_after_market_news` and
`amn_selected_stocks` (ISINs); the reactivation backfill filters on the latter.

---

## Service-by-service changes

**Proto** — [`api/proto/user_config/user_config.proto`](../../api/proto/user_config/user_config.proto)
- New message `AMNSelectedStock { isin, symbol, nse_code, bucket, target_price, entry_price, quantity, invested_amount }`.
- `repeated AMNSelectedStock amn_selection` added to `CreateStrategyRequest` (field 12) and `ActivateStrategyRequest` (field 3).
- `.pb.go` is **gitignored**; regenerate with `make -C api/proto generate-user-config`.

**user-config**
- Model: `ProcessAfterMarketNews` now `db:"process_after_market_news"`; new `AMNSelectedStock` / `AMNActivation` / `AMNActivationStock` types — [`internal/models/strategy.go`](../../services/user-config/internal/models/strategy.go).
- Repo — [`internal/repository/strategy_repository.go`](../../services/user-config/internal/repository/strategy_repository.go): `Create` persists the flag + day-1 activation; `Activate(…, selection)` enforces the mandatory pick and writes the `REACTIVATE` record; helpers `upsertAMNActivation`, `GetLatestActivationISINs`, `normalizeAMNSelection`, `isinsOf`, `todayISTDate`.
- Service/gRPC thread the selection through `ActivateStrategy` — [`internal/service/strategy_service.go`](../../services/user-config/internal/service/strategy_service.go), [`internal/server/grpc_server.go`](../../services/user-config/internal/server/grpc_server.go).
- Outbox worker emits `CONFIG_ACTIVATED` and loads the fresh ISINs — [`internal/worker/outbox_worker.go`](../../services/user-config/internal/worker/outbox_worker.go).

**rules-engine**
- Consumer handles `CONFIG_ACTIVATED` = upsert + trigger backfill on create **or** reactivation — [`internal/kafka/config_consumer.go`](../../services/rules-engine/internal/kafka/config_consumer.go).
- Backfill cross-run duplicate guard — [`internal/backfill/amn_runner.go`](../../services/rules-engine/internal/backfill/amn_runner.go); `startOfTodayIST` in [`internal/backfill/trading_day.go`](../../services/rules-engine/internal/backfill/trading_day.go).

**gateway** — [`internal/handlers/user_config_handler.go`](../../api/gateway/internal/handlers/user_config_handler.go), [`internal/handlers/converters.go`](../../api/gateway/internal/handlers/converters.go), [`internal/dto/strategy.go`](../../api/gateway/internal/dto/strategy.go): Create + Activate carry `amn_selection`.

---

## Public API contract (for frontend)

**Read strategy** (`GET /api/v1/strategies/{id}?user_id=...`) returns
`process_after_market_news` — use it to decide whether to show the AMN preview
popup on reactivate — plus `amn_activations`: the day-wise history of what the
user picked, newest trading day first.

Notes for the frontend:
- `amn_activations` is populated **only** on this single-strategy read. The list
  endpoint (`GET /api/v1/users/{user_id}/strategies`) always returns it as `[]`.
- It is `[]` for non-AMN strategies, and also for AMN strategies created before
  the day-wise selection tables existed — those only gain history from their next
  activation. An empty list is normal, not an error.
- One entry per trading day. `source` is `CREATE` (the day the strategy was made)
  or `REACTIVATE` (a later day the user re-picked).
- A day can legitimately have `"stocks": []` — the user reactivated on a day when
  the AMN window had no matching news, so nothing was picked and no backfill ran.
- `bucket` is `place` (ordered immediately at live LTP) or `monitor` (price-watch
  that fires at `target_price`). `target_price` is `0` for `place` picks.
- Prices/quantities are the **preview-time snapshot** the user saw when choosing.
  The backfill recomputes live pricing at placement, so these will not always
  equal the executed order.
- `nse_code` is an int64 and therefore serialized as a **string** by protojson
  (`"1333"`), while `quantity` and `strategy_version` are int32 and serialize as
  numbers. This is protojson's standard behaviour — not a bug.

<details>
<summary><b>Sample response</b> — <code>GET /api/v1/strategies/ed756418-f4b8-4c35-b328-d48d7424ce25?user_id=ISPL19027</code></summary>

```json
{
  "success": true,
  "strategy": {
    "strategy_id": "ed756418-f4b8-4c35-b328-d48d7424ce25",
    "user_id": "ISPL19027",
    "strategy_name": "Contact 3",
    "description": "",
    "active": false,
    "trading_mode": "LIVE",
    "conditions": {
      "match_all_news": false,
      "impact_score_min": 1,
      "impact_score_max": 10,
      "sentiments": ["SENTIMENT_POSITIVE", "SENTIMENT_NEGATIVE", "SENTIMENT_NEUTRAL"],
      "categories": [],
      "price_range": null,
      "market_cap_types": [],
      "market_cap_range": { "min_mcap": 0, "max_mcap": 0 },
      "pct_change_range": { "min_pct_change": 0, "max_pct_change": 0 },
      "exchanges": ["EXCHANGE_NSE"]
    },
    "trade_config": {
      "order_type": "ORDER_TYPE_MARKET",
      "product_type": "PRODUCT_TYPE_BRACKET",
      "validity": "DAY",
      "quantity": 1,
      "exchange": "EXCHANGE_NSE",
      "order_side": "ORDER_SIDE_BUY",
      "stop_loss_pct": 2,
      "take_profit_pct": 5,
      "stop_loss_type": "FIXED",
      "trailing_sl_pct": 0,
      "limit_price": 0,
      "take_profit_type": "TAKE_PROFIT_FIXED",
      "multi_level_sl": [],
      "multi_level_tp": [],
      "trade_window_start": "09:15",
      "trade_window_end": "15:00",
      "instrument": "",
      "lot_size": 0,
      "disclosed_qty": 0,
      "amo": false,
      "bracket_order_stop_loss": 0,
      "bracket_order_target": 0,
      "max_position_size": 0
    },
    "risk_limits": {
      "strategy_id": "",
      "max_daily_trades": 10,
      "max_per_trade_risk": 1000,
      "max_portfolio_exposure_pct": 25,
      "max_loss_per_day": 10000,
      "enable_risk_checks": true,
      "enable_auto_square_off": false,
      "auto_square_off_time": "",
      "position_sizing": "POSITION_SIZING_UNSPECIFIED",
      "max_amount_per_stock": 1000,
      "max_trades_per_strategy": 2
    },
    "created_at": { "seconds": "1783585884", "nanos": 0 },
    "updated_at": { "seconds": "1783589740", "nanos": 0 },
    "version": 2,
    "process_after_market_news": true,
    "amn_activations": [
      {
        "trading_date": "2026-07-16",
        "source": "REACTIVATE",
        "strategy_version": 2,
        "stocks": [
          {
            "isin": "INE040A01034",
            "symbol": "HDFCBANK",
            "nse_code": "1333",
            "bucket": "place",
            "target_price": 0,
            "entry_price": 1650.25,
            "quantity": 3,
            "invested_amount": 4950.75
          }
        ]
      },
      {
        "trading_date": "2026-07-15",
        "source": "CREATE",
        "strategy_version": 1,
        "stocks": [
          {
            "isin": "INE467B01029",
            "symbol": "TCS",
            "nse_code": "11536",
            "bucket": "place",
            "target_price": 0,
            "entry_price": 3120.5,
            "quantity": 2,
            "invested_amount": 6241
          },
          {
            "isin": "INE002A01018",
            "symbol": "RELIANCE",
            "nse_code": "2885",
            "bucket": "monitor",
            "target_price": 1400,
            "entry_price": 1425,
            "quantity": 1,
            "invested_amount": 1425
          }
        ]
      }
    ]
  },
  "error": null
}
```
</details>

A non-AMN strategy (or an AMN strategy with no recorded history) returns the same
shape with `"process_after_market_news": false` and `"amn_activations": []`.

**Create** (`POST /api/v1/strategies`):
```json
{
  "process_after_market_news": true,
  "amn_selection": [
    { "isin": "INE...", "symbol": "ABC", "nse_code": 12345,
      "bucket": "place",   "entry_price": 101.5, "quantity": 9, "invested_amount": 913.5 },
    { "isin": "INE...", "symbol": "XYZ", "nse_code": 67890,
      "bucket": "monitor", "target_price": 200.0, "entry_price": 201.0, "quantity": 4, "invested_amount": 804.0 }
  ]
}
```
`amn_selected_stocks` (plain ISIN array) is still accepted for backward compatibility.

**Reactivate** (`POST /api/v1/strategies/{id}/activate`):
```json
{ "user_id": "IS...", "amn_selection": [ /* same shape as create */ ] }
```
An empty `amn_selection` is **accepted**, not rejected: when the AMN window has no
matching news there is nothing to pick, and the strategy must still be able to go
live for news going forward. The activation is recorded for the day with no stocks
and no backfill runs.

---

## Idempotency & duplicate-order guard

- **Event redelivery:** the consumer's stale-version skip (`existing.Version >= ev.Version`) means a redelivered `CONFIG_ACTIVATED` is a no-op → backfill fires **once per activation**.
- **Same-day double placement:** before placing, the backfill calls `HasSignalToday(strategyID, nse_code, startOfTodayIST())` and **skips stocks already ordered today** (non-FAILED/CANCELLED). This protects against a second same-day reactivation with an overlapping pick. On a check error it **proceeds** (best-effort dedup) rather than block a legitimate order. Within-run dedup is still handled by the `placedISIN` map.
- **DB atomicity:** activation state + activation record + outbox row are written in **one transaction**; a missing selection rolls the whole thing back (strategy stays inactive).

---

## Operational notes

1. **Apply the migration before deploy:**
   `psql -h <host> -U <user> -d <db> -f services/user-config/migrations/002_amn_selections.sql`
2. **Regenerate proto** after pulling: `make -C api/proto generate-user-config`
   (toolchain: protoc v6.33.0, protoc-gen-go v1.36.10 — regen is byte-stable).
3. **Trading date** is computed in IST (`Asia/Kolkata`, with a fixed +5:30 fallback if tzdata is absent).

---

## Testing

- **Build:** all 7 modules compile (incl. downstream trade-execution/risk-management — the proto additions are backward-compatible).
- **Unit:** [`config_consumer_test.go`](../../services/rules-engine/internal/kafka/config_consumer_test.go) asserts `CONFIG_ACTIVATED` re-runs the backfill with the forwarded selection, and a plain `CONFIG_UPDATED` does **not**.
- **Not unit-tested:** the outbox worker's activation path (needs a live DB — it re-reads the strategy + latest ISINs via the concrete repo). Cover with an integration test.

---

## Known gaps & follow-ups

- **Stock-code / volume filtering** exists in the `strategy_conditions` schema (`stock_codes`, `min_volume`) but is **not implemented** in the rules-engine matcher; the stale unit tests that assumed it were removed. Re-add tests with the feature if built.
- **Outbox worker testability:** the worker holds a concrete `*StrategyRepository`. Extracting a small interface (for `GetByID` + `GetLatestActivationISINs`) would let the activation path be unit-tested with a mock.
- **Frontend popup** (separate repo): read `process_after_market_news`, drive the preview from stored conditions, submit `amn_selection` on activate.
