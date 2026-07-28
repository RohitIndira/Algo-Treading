# Trade Value Filter — Implementation Plan

Add a **trade value** (turnover) filter: `volume × LTP`, sourced from the Redis market-data
snapshot. Flexible operator — user picks *above*, *below*, or *a range*.

---

## ⚠️ Lead finding: this CANNOT be an evaluator condition

Market cap worked as a `matcher.Evaluator` condition because `mcap` rides on the news event.
**Trade value does not.** `event.MarketData` is empty in every production path:

| Path | Why `MarketData` is empty | Evidence |
|---|---|---|
| **Live news** | `NewsPayload` — what data-ingestion publishes to Kafka — has **no** `pricemap` and **no** `LastTradedPrice`. Only `mcap`/`mcaptype`/`exchange`/news fields. | `data-ingestion/internal/models/news.go:26-40` |
| **AMN preview** | `previewEvent` sets `StockData`, `NewsData`, `Analysis` — never `MarketData`. | `backfill/preview.go:459-476` |
| **AMN backfill** | `buildMarketEvent` likewise sets no `MarketData`. | `backfill/amn_runner.go` |

`MongoDBEvent` *does* declare `pricemap`/`LastTradedPrice` (`mongodb_event.go:35,41`) and
`mapMarketData` reads `PriceMap["Volume"]` (`:292`) — but nothing on the live topic populates them,
so both decode to `0`.

**Consequence:** an `evaluateTradeValue` in the matcher would see `volume=0, ltp=0` → `0`, and every
strategy with an active filter would match **nothing**. Silently. This is exactly the class of bug
`market_cap_types` had before (evaluator compared `"SMALL"` to `"Small Cap"` → zero signals).

### Where it must go instead

The codebase already has the right pattern for live-data conditions — `processMatch` re-validates
pct_change against Redis *after* evaluation:

```go
// handler.go:781-793
// ── Re-validate pct_change against live Redis data ───────────────────
// The evaluator used the event's (potentially stale) pct_change.
// Re-check here with real-time Redis percent_change before placing.
if maxPct := strategy.Conditions.MaxPctChange; maxPct > 0 { ... return nil }
```

Trade value follows the same shape, at the **three** places live market data exists. One shared
helper so semantics cannot drift between them.

> Deliberate divergence from pct_change: that filter lives in *both* the evaluator (on stale/zero
> data) and the handler (authoritative). That split is a wart — the evaluator half does nothing
> useful. Trade value gets **one** authoritative check, not two.

---

## 1. Data source — already in the same Redis GET

The payload at `market:<exch>:<token>` already contains what we need:

```json
{"symbol":"ACC","token":"22","ltp":1359.1,"volume":167223,"percent_change":1.357,"tick_size":0.1, ...}
```

`getMarketDataFromRedis` (`consumer/market_data.go:66-151`) already fetches this in **one GET** and
decodes 6 fields into an anonymous struct (`:104-111`) — but drops `volume`.

**So the optimization is free:** add one field to the decode struct and one to `MarketDataResult`.
Zero extra Redis round-trips, zero extra latency, no new cache, no schema change to the feed.

```go
// market_data.go:104 — add to the anonymous decode struct
Volume int64 `json:"volume"`

// market_data.go:19 — add to MarketDataResult
Volume int64 // day-cumulative traded quantity (for trade-value filter)
```

Plus one line in the `MarketDataResult{...}` literal at `:133`.

### Formula and units

```
tradeValueCr = volume × ltp / 1e7        // ₹ crore
```

ACC worked example: `167223 × 1359.1 = ₹227,272,779` → **₹22.73 Cr**.

Store the user's thresholds in **₹ crore**, matching the market-cap filter, so the UI never shows
₹227254977. Convert once, inside the shared helper — never at a call site.

> Use `volume`, not `avg_volume_5d` — the request is "volume * current ltp". `avg_volume_5d` is
> available in the same payload if a smoothed variant is ever wanted.

---

## 2. Flexible operator design

You asked for range **or** above **or** min — so the mode must be explicit. Note this is the
opposite of the market-cap decision, where you rejected open-ended bounds; encoding mode implicitly
in `max == 0` is exactly the ambiguity that caused trouble there. An explicit mode removes it.

```go
// TradeValueFilter — liquidity filter on volume × LTP, in ₹ crore.
type TradeValueFilter struct {
    Mode string  `json:"mode"`            // "" | "ABOVE" | "BELOW" | "RANGE"
    Min  float64 `json:"min_trade_value"`  // ₹ crore; used by ABOVE and RANGE
    Max  float64 `json:"max_trade_value"`  // ₹ crore; used by BELOW and RANGE
}
```

| Mode | Meaning | Uses | Example |
|---|---|---|---|
| `""` (or absent) | Filter off | — | no liquidity filter |
| `ABOVE` | `tradeValue >= Min` | `Min` | "at least ₹10 Cr turnover" |
| `BELOW` | `tradeValue <= Max` | `Max` | "under ₹5 Cr turnover" |
| `RANGE` | `Min <= tradeValue <= Max` | both | "₹10 Cr to ₹100 Cr" |

**Mode is a string, not a proto enum** — consistent with `stop_loss_type` / `take_profit_type` /
`market_cap_types`, which are all plain strings in this codebase. Avoids the `stringToExchange`
style mapping boilerplate an enum would require on both sides.

### The single shared helper

```go
// pkg or consumer-level, imported by handler + preview + amn_runner
//
// TradeValueCr returns volume × ltp expressed in ₹ crore.
func TradeValueCr(volume int64, ltp float64) float64 {
    return float64(volume) * ltp / 1e7
}

// PassesTradeValue reports whether the given turnover satisfies the filter.
// An inactive filter (empty mode) always passes.
//
// volume == 0 (pre-open, or a genuinely untraded stock) yields tradeValue 0,
// which fails ABOVE and RANGE — correct, since zero turnover cannot satisfy a
// liquidity floor — and passes BELOW.
func PassesTradeValue(f models.TradeValueFilter, volume int64, ltp float64) (bool, float64) {
    tv := TradeValueCr(volume, ltp)
    switch f.Mode {
    case "ABOVE":
        return tv >= f.Min, tv
    case "BELOW":
        return tv <= f.Max, tv
    case "RANGE":
        return tv >= f.Min && tv <= f.Max, tv
    default: // "" / unknown → filter off (fail-open, never blocks a trade on bad config)
        return true, tv
    }
}
```

Returning `tv` alongside the verdict lets every call site log the actual value without recomputing.

---

## 3. Backend changes

### 3.1 Migration — **required** (unlike market cap)

New file `services/user-config/migrations/004_add_trade_value_filter.sql`:

```sql
ALTER TABLE strategy_conditions
    ADD COLUMN IF NOT EXISTS trade_value_mode  TEXT,
    ADD COLUMN IF NOT EXISTS min_trade_value   DECIMAL,
    ADD COLUMN IF NOT EXISTS max_trade_value   DECIMAL;
```

Also add the three columns to `deployments/docker/init_all_schemas.sql` (`strategy_conditions`,
around `:71-76`) so fresh environments match.

> **Do not reuse the existing dead `min_volume BIGINT` column** (`001_init.sql:50`). It means raw
> share count, not rupee turnover — different unit, different semantics. Leave it alone.

### 3.2 Proto — field 13

`api/proto/user_config/user_config.proto`, inside `StrategyConditions` (12 = `exchanges` is the
current highest; 6 and 10 are reserved):

```protobuf
  // Trade value (turnover = volume × LTP) filter, in ₹ crore.
  // mode: "" (off) | "ABOVE" | "BELOW" | "RANGE"
  message TradeValueFilter {
    string mode = 1;
    double min_trade_value = 2; // ₹ crore; ABOVE and RANGE
    double max_trade_value = 3; // ₹ crore; BELOW and RANGE
  }
  TradeValueFilter trade_value_filter = 13;
```

**Requires `protoc` regeneration** — the market-cap work did not. Check how the repo generates
(`Makefile` / `scripts/`) and regenerate `user_config.pb.go` rather than hand-editing.

### 3.3 Persistence — mirror the market-cap chain exactly

Every hop that carries `min_market_cap` needs the three new fields. Verified list:

| File | Change |
|---|---|
| `user-config/internal/models/strategy.go:133` | `TradeValueMode *string`, `MinTradeValue *float64`, `MaxTradeValue *float64` with `db:` + `json:` tags |
| `user-config/internal/repository/strategy_repository.go` | INSERT (`:163-177`), UPDATE (`:445-460`), and **all five** SELECT column lists (`:67, 302, 379, 528, 879`) |
| `user-config/internal/server/grpc_server.go:482` | proto → model (used by both create `:36` and update `:100`) |
| `user-config/internal/server/grpc_server.go:835` | model → proto |
| `user-config/internal/events/config_event.go:61` | Kafka `ConditionsPayload` |
| `user-config/internal/events/mapper.go:74` | populate it |
| `api/gateway/internal/dto/strategy.go:71` | DTO |
| `api/gateway/internal/handlers/converters.go:156` | DTO → proto |
| `rules-engine/internal/models/strategy.go:58` | `Conditions.TradeValueFilter` |
| `rules-engine/internal/configsync/event.go:58` + `mapper.go:45` | Kafka → model |
| `rules-engine/internal/startup/userconfig_client.go:142` | gRPC bootstrap → model |

> ⚠️ `:528` is the **update path's in-transaction re-read** that builds the outbox payload. Miss it
> and the Kafka event silently carries a stale/empty filter on every update — the exact bug class
> the market-cap audit was looking for.

### 3.4 Enforcement — three call sites

**(a) Live path — `consumer/handler.go`, in `processMatch`**

Place directly after the existing pct_change re-validation (`:793`), before order construction:

```go
// ── Trade value (liquidity) gate against live Redis data ─────────────
// volume × LTP is not on the news event, so this is the only place the
// live filter can be applied on the streaming path.
if ok, tv := consumer.PassesTradeValue(strategy.Conditions.TradeValueFilter, md.Volume, ltp); !ok {
    h.logger.Info("Skipping order — trade value outside configured filter",
        zap.String("strategy_id", strategy.StrategyID),
        zap.String("symbol", event.StockData.Symbol),
        zap.Float64("trade_value_cr", tv),
        zap.String("mode", strategy.Conditions.TradeValueFilter.Mode))
    return nil
}
```

Consider routing this through `recordEventDecision` / `rejectForCompliance` (`:206`, `:356`) so the
skip lands in the decisions audit trail like other post-match rejections, rather than only a log
line. Check which reason-code enum the audit package expects and add one
(e.g. `ReasonTradeValueFilter`).

**(b) AMN preview — `backfill/preview.go`**

In the candidate loop where `md` is already resolved from the snapshot (near `classifyPctChange`,
`:355-360`). **Filter before** the affordability/quantity math at `:370-378` — it is a cheap
multiply-and-compare, so rejecting early saves the expensive per-row work.

Add `MinTradeValue`/`MaxTradeValue`/`TradeValueMode` to `PreviewConditions` (`:61-80`) and
`TradeValue float64 \`json:"trade_value"\`` to `PreviewItem` (`:94-117`) for display.

**(c) AMN backfill — `backfill/amn_runner.go`**

Same gate in the run loop where `md` is resolved (near `:296-303`), so the real backfill agrees with
its own preview. The runner already copies the full `*models.Strategy` (`:260`), so the filter
arrives automatically.

### 3.5 Validation — gateway, extend the existing helper

Add to `validateConditions` in `api/gateway/internal/handlers/user_config_handler.go` (already
called from both create and update):

| Condition | HTTP | Message |
|---|---|---|
| mode not in `{"", "ABOVE", "BELOW", "RANGE"}` | 400 | `Trade value mode must be one of: ABOVE, BELOW, RANGE` |
| any value `< 0` | 400 | `Trade value cannot be negative` |
| `ABOVE` with `Min <= 0` | 400 | `Minimum trade value is required when mode is ABOVE` |
| `BELOW` with `Max <= 0` | 400 | `Maximum trade value is required when mode is BELOW` |
| `RANGE` with `Min <= 0` or `Max <= 0` | 400 | `Both minimum and maximum trade value are required when mode is RANGE` |
| `RANGE` with `Max < Min` | 400 | `Maximum trade value must be greater than or equal to minimum trade value` |

Mode empty → ignore the values entirely (filter off).

### 3.6 Tests

- **`consumer/market_data_test.go`** — decode a literal Redis payload (the ACC JSON above) and
  assert `Volume == 167223`. Pins the `volume` JSON key.
- **New `PassesTradeValue` table test** — every mode × below/at/above boundary, plus `volume=0`,
  `ltp=0`, unknown mode (must fail open), and `TradeValueCr` unit conversion against the ACC number
  (`22.7254977`).
- **`configsync/mapper_test.go`** — extend the existing wire-format test with `trade_value_mode` /
  `min_trade_value` / `max_trade_value` in the raw JSON, same rationale as market cap.
- **`user_config_handler_test.go`** — every row of the §3.5 matrix.

---

## 4. Frontend changes

### 4.1 Form — `CreateStrategyModal.tsx`

Add below the Market Cap Range block. Mode selector drives which inputs render — this is what makes
it flexible without being ambiguous:

- **Mode**: a 4-way selector (`Off` / `Above` / `Below` / `Range`), styled like the existing
  `stop_loss_type` control.
- `Above` → one input (Min). `Below` → one input (Max). `Range` → both. `Off` → none.
- Label the unit explicitly: **₹ Crore**.
- Hint that it is a liquidity filter and that it moves intraday (see §5).

Add `validateTradeValue(mode, min, max)` mirroring §3.5 messages verbatim, wired to inline feedback
+ step gate + submit gate — same three points as `validateMarketCapRange`.

### 4.2 Types + preview API

- `types/algo-trading/strategies.ts` — add `trade_value_mode`, `min_trade_value`, `max_trade_value`
  to `StrategyConditions`.
- `utils/algo-trading/strategiesApi.ts` — add all three to `AMNPreviewConditions` and map them in
  `strategyConditionsToPreview`; add `trade_value?: number` to `AMNPreviewItem`.
  `fetchAMNPreview` now takes `AMNPreviewConditions`, so TS will flag a missed field.

### 4.3 Display

Add a **"Trade Val ₹Cr"** column to the AMN preview table next to the M-Cap column, and to the
`AmnReactivateModal` row subtitle — same treatment as mcap.

---

## 5. Behaviour worth calling out

| Topic | Note |
|---|---|
| **Intraday drift** | Volume is *day-cumulative*, so trade value only ever grows during a session. A stock failing `ABOVE ₹10 Cr` at 09:20 may pass at 14:00. This makes the filter time-dependent in a way no other condition is — surface it in the UI hint. |
| **Pre-open / halted** | `volume = 0` → trade value 0 → fails `ABOVE`/`RANGE`, passes `BELOW`. Fails closed on the liquidity floor, which is the safe direction. |
| **`BELOW` is a real ask but sharp** | Combined with the intraday growth above, a `BELOW` filter gets *harder* to satisfy as the day progresses. Worth a UI note. |
| **Unknown mode** | Fails **open** (filter ignored). A corrupt/unrecognised mode must not silently block every trade. Contrast with market-cap `max < min`, which fails closed — there the intent was unambiguous; here it is not. |
| **Not the same as `min_volume`** | The dead `min_volume BIGINT` column is raw share count. Trade value is rupees. Do not conflate or reuse. |
| **Interaction** | ANDed with every other condition, including the two market-cap filters. Note that market cap and trade value are correlated but not equivalent — a large-cap can have a thin day. |

---

## 6. Ordering

1. **§3.1 migration + §3.2 proto regen** — everything else depends on the field existing.
2. **§3.3 persistence chain** — verify round-trip before wiring behaviour.
3. **§1 `MarketDataResult.Volume`** — one-line, unblocks all three gates.
4. **§3.4 enforcement** (helper first, then the three call sites) + **§3.6 tests**.
5. **§3.5 validation.**
6. **§4 frontend** — after backend deploys, or users get a control that does nothing.

---

## 7. Files to touch

**Backend (~14)**
- `services/user-config/migrations/004_add_trade_value_filter.sql` *(new)*
- `deployments/docker/init_all_schemas.sql`
- `api/proto/user_config/user_config.proto` + regenerated `user_config.pb.go`
- `services/user-config/internal/models/strategy.go`
- `services/user-config/internal/repository/strategy_repository.go` *(INSERT, UPDATE, 5 SELECTs)*
- `services/user-config/internal/server/grpc_server.go` *(both directions)*
- `services/user-config/internal/events/config_event.go`, `mapper.go`
- `api/gateway/internal/dto/strategy.go`, `handlers/converters.go`, `handlers/user_config_handler.go`
- `services/rules-engine/internal/models/strategy.go`
- `services/rules-engine/internal/configsync/event.go`, `mapper.go`
- `services/rules-engine/internal/startup/userconfig_client.go`
- `services/rules-engine/internal/consumer/market_data.go` *(Volume + helper)*
- `services/rules-engine/internal/consumer/handler.go` *(live gate)*
- `services/rules-engine/internal/backfill/preview.go`, `amn_runner.go`
- Tests: `market_data_test.go` *(new)*, `configsync/mapper_test.go`, `user_config_handler_test.go`

**Frontend (4)**
- `CreateStrategyModal.tsx`, `AmnReactivateModal.tsx`
- `types/algo-trading/strategies.ts`, `utils/algo-trading/strategiesApi.ts`

**Explicitly NOT touched:** `data-ingestion` (nothing new needed on the news event — the filter
reads Redis directly), `matcher/evaluator.go` (see lead finding).

---

## 8. Verification

1. Create a strategy with `ABOVE ₹10 Cr`, confirm round-trip via `GET` and in Postgres.
2. Confirm the Kafka config event carries all three fields (decode a topic message).
3. ACC (`volume 167223`, `ltp 1359.1` → **₹22.73 Cr**) must **pass** `ABOVE ₹10 Cr` and **fail**
   `ABOVE ₹50 Cr`.
4. `BELOW ₹10 Cr` must **reject** ACC; `RANGE ₹10–100 Cr` must **accept** it.
5. Confirm the skip is logged with `trade_value_cr` and appears in the decisions audit.
6. AMN preview with the same filter returns the same set the live engine accepts, and the new
   Trade Val column renders.
7. Set an invalid combination (`RANGE` with max < min, `ABOVE` with no min) → **400** with the exact
   §3.5 message, on **both** create and update, and blocked in the UI first.
8. Pre-open (volume 0) → `ABOVE` rejects, `BELOW` accepts.
