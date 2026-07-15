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
an AMN strategy they must submit a **fresh preview pick**, which is stored and used
to **re-run the backfill for that day**.

---

## What changed & why

| Before | After |
|---|---|
| `process_after_market_news` was create-time-only (`db:"-"`), not stored | Persisted column on `strategies` — reactivation & UI can identify AMN strategies |
| Selected stocks (ISINs) rode the Kafka event once, never stored | Stored in normalized `amn_activations` + `amn_activation_stocks` tables, one record per strategy per trading day |
| Only `place`-bucket picks were meaningful | `monitor` (price-watch) picks are persisted too, with their `target_price` |
| Backfill fired only on `CONFIG_CREATED` | Fires on create **and** reactivation (`CONFIG_ACTIVATED`); never on a plain edit |
| Reactivation carried no selection | Reactivation **requires** a fresh pick (rejected if empty for AMN strategies) |
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

**Read strategy** (`GET /api/v1/strategies/{id}`) now returns
`process_after_market_news` — use it to decide whether to show the AMN preview
popup on reactivate.

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
{ "user_id": "IS...", "amn_selection": [ /* same shape; REQUIRED for AMN strategies */ ] }
```
An AMN strategy activated with an empty selection returns
`400 "AMN strategy requires a stock selection to reactivate"`.

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
