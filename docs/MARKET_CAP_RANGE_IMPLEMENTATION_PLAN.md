# Market Cap Value Range — Implementation Plan

Add a numeric market-cap range filter (e.g. `1000` – `10000` ₹ Cr) as a strategy condition,
sourced from `OdinMasterData.CompanyMaster.mcap`, evaluated in the rules engine and
configurable/visible in the frontend.

---

## TL;DR — most of this is already built

The `min_market_cap` / `max_market_cap` field already exists **end to end**: Postgres column →
Go model → proto → gRPC → Kafka config event → rules-engine `models.MarketCapRange`, and on the
frontend it exists in both the TS type and the form state.

**It is dead config.** Nothing reads it. There are five real gaps:

| # | Gap | Where |
|---|-----|-------|
| 1 | Evaluator never checks the range | `services/rules-engine/internal/matcher/evaluator.go` |
| 2 | No UI control to set it | `StockkAsk/src/components/algo-trading/CreateStrategyModal.tsx` |
| 3 | Validation is partial on create and **absent on update** | `api/gateway/internal/handlers/user_config_handler.go` |
| 4 | AMN preview drops the range → preview disagrees with live engine | `services/rules-engine/internal/backfill/preview.go` |
| 5 | No display of the filter or of a stock's mcap | `StrategyCard.tsx`, `preview.go` `PreviewItem` |

### Agreed semantics

- **Units:** ₹ crore, matching `CompanyMaster.mcap`. No conversion anywhere.
- **Strict bounded range.** No open-ended form — `min=1000, max=0` is a **validation error**, not
  "₹1,000 Cr and above". `max >= min` is enforced on both frontend and backend with an explicit
  message and a **400**.
- **Optional.** Both blank (`0/0`) = no market-cap filter, consistent with every other condition.
- **Coexists with `market_cap_types`.** Both filters stay; a stock must satisfy **both** (AND).

**`mcap` needs no new data plumbing.** It is already loaded into Redis at startup and already
rides on every news event (see [Data source](#1-data-source--mcap-is-already-in-memory)).

---

## 1. Data source — `mcap` is already in memory

The user's instinct is right: this data is already cached, so **no new Mongo read is needed at
evaluation time**.

### Existing flow

```
OdinMasterData.CompanyMaster
  │  (bulk load at data-ingestion startup)
  ▼
RedisManager.LoadCompanyMasterData        redis_manager.go:159
  → Redis key  isin:<ISIN>  =  models.CompanyDetails{ISIN, BSECode, NSECode, MCap, MCapType, Exchange}
  │
  │  (cache miss → fetchAndCacheCompanyDetails, redis_manager.go:74)
  ▼
MongoWatcher.enrichAndPublish             mongo_watcher.go:59,81
  → NewsMessage.MCap                      models/news.go:36
  ▼
Kafka news topic  { ..., "mcap": 88783.31, "mcaptype": "Mid Cap" }
  ▼
rules-engine  MongoDBEvent.mapStockData   mongodb_event.go:128
  → event.StockData.MCap (float64)        models/event.go:64
```

`CompanyDetails.MCap` (`data-ingestion/internal/models/company.go:8`) already carries the value,
and `redis_manager.go` already coerces the mixed BSON numeric types (`float64`/`int32`/`int64`).

**AMN backfill path** has its own source and is also already covered:
`backfill/company_lookup.go` projects `mcap` from CompanyMaster into `CompanyInfo.MCap`
(`company_lookup.go:42,103,141`), and `previewEvent` puts it on `StockData.MCap`
(`preview.go:448`).

### Verdict

- ✅ No schema change to `CompanyDetails`.
- ✅ No new Redis key or cache warm step.
- ✅ No change to `mongo_watcher.go` or the Kafka message.
- ⚠️ **One thing to verify at runtime:** that `mcap` is non-zero on live events for the symbols
  you test with. A company whose CompanyMaster doc has `mcap: 0` or a missing field will decode
  to `0.0` and be silently excluded by a `min > 0` filter. See
  [§6 Zero-mcap handling](#6-edge-cases--decisions).

### Units

`CompanyMaster.mcap` is in **₹ crore** (Ashok Leyland = `88783.31` ≈ ₹88,783 Cr). The user's
example `1000` → `10000` therefore means ₹1,000 Cr – ₹10,000 Cr (a small-cap band). All layers
already document "in crores" — keep that unit everywhere and label the UI accordingly. **Do not
convert.**

---

## 2. What already exists (verify only — no code changes)

| Layer | Artifact | Location |
|---|---|---|
| Postgres | `min_market_cap DECIMAL`, `max_market_cap DECIMAL` | `services/user-config/migrations/001_init.sql:45-46`; `deployments/docker/init_all_schemas.sql:71-72` |
| user-config model | `MinMarketCap *float64`, `MaxMarketCap *float64` | `services/user-config/internal/models/strategy.go:133-134` |
| Repository | columns in SELECT / INSERT / UPDATE | `strategy_repository.go:67,165,174,449,457` (+ read paths at `302,379,528,879`) |
| Proto | `StrategyConditions.MarketCapRange { min_mcap, max_mcap }` = field 9 | `api/proto/user_config/user_config.proto:94-98` (generated: `user_config.pb.go:315,2517`) |
| gRPC in | proto → model | `services/user-config/internal/server/grpc_server.go:482-484` |
| gRPC out | model → proto | `grpc_server.go:835-838` |
| Kafka config event | `min_market_cap` / `max_market_cap` | `internal/events/config_event.go:61-62`; `mapper.go:74-75` |
| Gateway DTO | `min_market_cap` / `max_market_cap` | `api/gateway/internal/dto/strategy.go:71-72` |
| Gateway converter | DTO → proto | `handlers/converters.go:156-158` |
| rules-engine (hot reload) | config event → `MarketCapRange` | `internal/configsync/mapper.go:45-47` |
| rules-engine (bootstrap) | gRPC → `MarketCapRange` | `internal/startup/userconfig_client.go:142-143` |
| rules-engine model | `Conditions.MarketCapRange{MinMcap, MaxMcap}` | `internal/models/strategy.go:58,67-71` |
| Frontend type | `min_market_cap` / `max_market_cap` | `src/types/algo-trading/strategies.ts:28-29` |
| Frontend form state | present, defaults `0` | `CreateStrategyModal.tsx:55-56,109-110` |

> Because the persistence and transport layers are done, **no migration, no `.proto` change, and
> no `protoc` regeneration are required.**

---

## 3. Backend changes

### 3.1 Evaluator — the core gap

**File:** `services/rules-engine/internal/matcher/evaluator.go`

Add `market_cap_range` as its own condition, parallel to the existing `market_cap` (bucket-type)
check. Keeping them separate preserves the existing per-condition scoring/reporting model and lets
a user set either or both.

Add a new method mirroring `evaluatePriceRange` (`evaluator.go:212`):

```go
// evaluateMarketCapRange evaluates the numeric market-cap range filter (₹ crore),
// sourced from OdinMasterData.CompanyMaster.mcap via StockData.MCap.
//
// This is a STRICT bounded range — there is no open-ended form:
//   - min == 0 && max == 0  → filter not set, auto-pass
//   - otherwise             → mcap must satisfy min <= mcap <= max (both inclusive)
//
// The API layer guarantees max >= min whenever the filter is active (see the
// gateway validation), so no min/max swap or +Inf fallback is done here.
//
// A stock whose CompanyMaster doc has no mcap decodes to 0.0 and fails any active
// filter — an unknown cap cannot be proven to be inside the requested band.
func (e *Evaluator) evaluateMarketCapRange(event *models.MarketEvent, strategy *models.Strategy, result *EvaluationResult) {
    condition := "market_cap_range"

    mcap := event.StockData.MCap
    min := strategy.Conditions.MarketCapRange.MinMcap
    max := strategy.Conditions.MarketCapRange.MaxMcap

    // Both zero → filter not configured.
    if min == 0 && max == 0 {
        result.MatchedConditions = append(result.MatchedConditions, condition)
        result.ConditionScores[condition] = 100.0
        return
    }

    // Defensive: validation rejects max < min at the API boundary, so this state
    // should be unreachable. Fail closed and log loudly rather than trade on it.
    if max < min {
        result.FailedConditions = append(result.FailedConditions, condition)
        result.ConditionScores[condition] = 0
        e.logger.Warn("market_cap_range has max < min — rejecting; strategy config is corrupt",
            zap.Float64("min_mcap", min),
            zap.Float64("max_mcap", max),
            zap.String("strategy_id", strategy.StrategyID))
        return
    }

    if mcap >= min && mcap <= max {
        result.MatchedConditions = append(result.MatchedConditions, condition)
        result.ConditionScores[condition] = 100.0
        return
    }

    result.FailedConditions = append(result.FailedConditions, condition)
    result.ConditionScores[condition] = 0
    e.logger.Debug("market_cap_range filter excluded stock",
        zap.String("isin", event.StockData.ISIN),
        zap.Float64("mcap", mcap),
        zap.Float64("min_mcap", min),
        zap.Float64("max_mcap", max),
        zap.String("strategy_id", strategy.StrategyID))
}
```

Then wire it in **two** places:

1. `Evaluate` (`evaluator.go:61-67`) — add `e.evaluateMarketCapRange(event, strategy, result)`
   after the existing `e.evaluateMarketCap(...)` call.
2. `evaluateMatchAllStrategy` (`evaluator.go:83-100`) — add `"market_cap_range"` to the
   auto-matched condition list **and** to `result.ConditionScores`. Missing this means a
   `match_all_news` strategy reports an inconsistent condition set.

> **No `math` import needed** — unlike `evaluatePctChange`, this filter has no `+Inf` branch.
> `min=1000, max=0` is **not** "₹1,000 Cr and above"; it is a validation error rejected with 400
> before it can be stored (see §3.3).

### Relationship to `market_cap_types` — both apply, ANDed

`market_cap` (bucket types: SMALL/MID/LARGE) and `market_cap_range` (numeric ₹ Cr band) are two
**independent conditions**. Both stay in the product and both remain settable. Because
`IsFullMatch()` requires every condition to pass, a strategy that sets `MID` **and** `1000–10000`
gets the **intersection** — only mid-cap stocks whose mcap also falls in that band.

This needs no special code: registering `market_cap_range` as its own condition in `Evaluate`
produces AND semantics automatically. It does need a **UI hint** so users are not surprised when a
narrow combination matches nothing (see §4.1).

### 3.2 AMN preview — keep preview consistent with the live engine

**File:** `services/rules-engine/internal/backfill/preview.go`

Without this, the AMN preview will list stocks that the real backfill/live engine then rejects.

1. `PreviewConditions` (`preview.go:61-74`) — add:
   ```go
   MinMarketCap float64 `json:"min_market_cap"`
   MaxMarketCap float64 `json:"max_market_cap"`
   ```
2. `buildEvalStrategy` (`preview.go:421-433`) — populate the range so the shared evaluator applies it:
   ```go
   MarketCapRange: models.MarketCapRange{MinMcap: c.MinMarketCap, MaxMcap: c.MaxMarketCap},
   ```
3. `PreviewItem` (`preview.go:88-107`) — add `MCap float64 \`json:"mcap"\`` (and optionally
   `MCapType string \`json:"mcaptype,omitempty"\``) so the UI can show why a stock qualified.
   Populate it from `CompanyInfo.MCap` where `PreviewItem`s are constructed.

`previewEvent` (`preview.go:448`) already sets `StockData.MCap`, and `company_lookup.go` already
projects `mcap` — **no change needed there.**

**AMN runner** (`backfill/amn_runner.go:260-264`) copies the real `*models.Strategy`, so the range
flows automatically once §3.1 lands. **No change needed.**

**Gateway AMN preview handler** (`api/gateway/internal/handlers/amn_preview_handler.go`) is a plain
reverse proxy that forwards the raw JSON body. **No change needed.**

### 3.3 Gateway validation — authoritative, mirrored on the frontend

**File:** `api/gateway/internal/handlers/user_config_handler.go`

The backend is the source of truth. Frontend validation (§4.1) is a UX convenience only and must
never be the sole gate.

#### Validation matrix

Let `min = MinMarketCap`, `max = MaxMarketCap`. The filter is **inactive** only when both are `0`.

| `min` | `max` | Result | HTTP | `{"error": ...}` message |
|---|---|---|---|---|
| `0` | `0` | ✅ Valid — no filter | 200/201 | — |
| `1000` | `10000` | ✅ Valid range | 200/201 | — |
| `0` | `10000` | ✅ Valid — up to ₹10,000 Cr | 200/201 | — |
| `5000` | `5000` | ✅ Valid — exact cap | 200/201 | — |
| `10000` | `1000` | ❌ max < min | **400** | `Maximum market cap must be greater than or equal to minimum market cap` |
| `1000` | `0` | ❌ max < min (no open-ended range) | **400** | `Maximum market cap is required when a minimum is set` |
| `-1` | `10000` | ❌ negative | **400** | `Market cap values cannot be negative` |
| `1000` | `-5` | ❌ negative | **400** | `Market cap values cannot be negative` |

Check order: **negatives first**, then the `min>0 && max==0` case (so the user gets the specific
"maximum is required" message rather than the generic comparison one), then `max < min`.

#### Shared helper

`CreateStrategy` has a partial check at `:72-78`; **`UpdateStrategy` (`:118`) has none at all** — a
user can bypass validation entirely via update today. Extract one helper and call it from both:

```go
// validateConditions enforces cross-field rules the proto/DB layers cannot express.
// Returns a user-facing message; empty string means valid.
func validateConditions(c *dto.StrategyConditions) string {
    if c == nil {
        return ""
    }
    if c.MinMarketCap < 0 || c.MaxMarketCap < 0 {
        return "Market cap values cannot be negative"
    }
    // Both zero = filter not set. Otherwise a complete bounded range is required.
    if c.MinMarketCap > 0 && c.MaxMarketCap == 0 {
        return "Maximum market cap is required when a minimum is set"
    }
    if c.MaxMarketCap < c.MinMarketCap {
        return "Maximum market cap must be greater than or equal to minimum market cap"
    }
    return ""
}
```

Call site in **both** `CreateStrategy` and `UpdateStrategy`, replacing the existing `:72-78` block:

```go
if msg := validateConditions(reqDTO.Conditions); msg != "" {
    respondWithError(w, http.StatusBadRequest, msg)
    return
}
```

`respondWithError` (`:444`) emits `{"error": "<message>"}` with the given status — which the
frontend's `checkResponse` (`strategiesApi.ts:49-56`) already unwraps via `err.error` and rethrows
as a plain `Error`. **So a 400 from this helper surfaces its exact message in the UI with no
frontend changes.**

> Put the helper in `handlers/` next to `respondWithError` so both handlers can reach it, and add
> a `validateConditions` unit test covering every row of the matrix above.

#### AMN preview endpoint

`amn_preview_handler.go` is a pass-through proxy with no validation. Since the preview only reads
(it places no orders), an invalid range there is harmless — the strict evaluator simply matches
nothing. Optionally run the same helper for a cleaner error, but it is not required for correctness.

### 3.4 Tests

**File:** `services/rules-engine/internal/matcher/evaluator_test.go` (exists)

Add table-driven cases for `evaluateMarketCapRange`:

| mcap | min | max | expect |
|---|---|---|---|
| 5000 | 0 | 0 | pass (filter not set) |
| 5000 | 1000 | 10000 | pass (in range) |
| 500 | 1000 | 10000 | fail (below) |
| 88783.31 | 1000 | 10000 | fail (above — the Ashok Leyland doc) |
| 1000 | 1000 | 10000 | pass (inclusive lower boundary) |
| 10000 | 1000 | 10000 | pass (inclusive upper boundary) |
| 5000 | 0 | 10000 | pass (zero min is a real lower bound) |
| 5000 | 5000 | 5000 | pass (exact-cap range) |
| 0 | 1000 | 10000 | fail (missing mcap) |
| 0 | 0 | 0 | pass (missing mcap, filter not set) |
| 5000 | 10000 | 1000 | fail + Warn log (corrupt config, unreachable via API) |

Assert `evaluateMatchAllStrategy` includes `"market_cap_range"` in both `MatchedConditions` and
`ConditionScores`.

Add an **AND-interaction** test: `MarketCapTypes: ["MID"]` + range `1000–10000` against a
`Mid Cap` / `88783.31` event → `IsFullMatch()` is false, with `market_cap` in `MatchedConditions`
and `market_cap_range` in `FailedConditions`.

**Also add `validateConditions` tests** in the gateway covering every row of the §3.3 matrix.

---

## 4. Frontend changes

### 4.1 Create/Edit strategy form — the missing control

**File:** `src/components/algo-trading/CreateStrategyModal.tsx`

`min_market_cap` / `max_market_cap` are already in the form type (`:55-56`) and defaults (`:109-110`)
and are already submitted. **Only the input is missing.** Add it to the conditions step directly
below the existing `Market Cap Types` `SelectMulti` (`:794-796`) — **keep that selector**; the two
filters coexist — following the two-input pattern already used for the pct-change range in that step:

```tsx
<div>
    <label className={labelCls}>Market Cap Range (₹ Crore, optional)</label>
    <div className="flex gap-2">
        <input type="number" min={0} step="any" className={inputCls} placeholder="Min (e.g. 1000)"
            value={form.conditions.min_market_cap || ''}
            onChange={e => patch('conditions.min_market_cap', Number(e.target.value) || 0)} />
        <input type="number" min={0} step="any" className={inputCls} placeholder="Max (e.g. 10000)"
            value={form.conditions.max_market_cap || ''}
            onChange={e => patch('conditions.max_market_cap', Number(e.target.value) || 0)} />
    </div>
    {mcapError && <p className="text-xs text-red-500 mt-1">{mcapError}</p>}
    <p className="text-xs opacity-60 mt-1">
        Leave both blank for no market-cap limit. If you set one, set both.
        Combined with Market Cap Types above — a stock must satisfy both.
    </p>
</div>
```

> Reuse the `||''` / `Number(...)||0` idiom already used by the other numeric inputs in this file so
> `0` renders as an empty box (meaning "unset") rather than a literal `0`.

#### Client-side validation — mirror §3.3 exactly

Same rules, same messages, so the inline hint and any server 400 read identically:

```ts
// Returns an error message, or '' when valid. Mirrors gateway validateConditions().
function validateMarketCapRange(min: number, max: number): string {
    if (min < 0 || max < 0) return 'Market cap values cannot be negative';
    if (min > 0 && max === 0) return 'Maximum market cap is required when a minimum is set';
    if (max < min) return 'Maximum market cap must be greater than or equal to minimum market cap';
    return '';
}
```

Wire it in three places:
1. **Inline**, under the inputs, as the user types (`mcapError` above).
2. **Step gate** — block advancing past the conditions step while non-empty, matching how the other
   steps already gate.
3. **Submit gate** — re-check before the create/update call so a step skipped by an "edit" flow
   cannot slip through.

Because `checkResponse` (`strategiesApi.ts:49-56`) already rethrows the gateway's `{"error": ...}`
message verbatim, a server-side 400 lands in the existing error toast with **no extra wiring** —
this is purely defence in depth.

### 4.2 AMN preview conditions

**File:** `src/utils/algo-trading/strategiesApi.ts`

1. `AMNPreviewConditions` (`:107-117`) — add `min_market_cap: number;` and `max_market_cap: number;`.
2. `strategyConditionsToPreview` (`:129`) — map them through:
   ```ts
   min_market_cap: c.min_market_cap ?? 0,
   max_market_cap: c.max_market_cap ?? 0,
   ```
3. `AMNPreviewItem` (`:3`) — add `mcap?: number;` to match §3.2.
4. Find every caller that builds an `AMNPreviewConditions` from the create form (not just from a
   saved strategy) and pass the two new fields — otherwise the preview shown during **creation**
   still ignores the filter.

### 4.3 Display

- **`StrategyCard.tsx`** currently renders no condition summary. If a conditions summary is added
  (or exists elsewhere), show `₹1,000 Cr – ₹10,000 Cr` when set. Format with the Indian grouping
  helper in `src/utils/algo-trading/format.ts`.
- **AMN preview table** — add an "M-Cap" column fed by the new `mcap` field so the user can see why
  each stock qualified.
- **`AmnReactivateModal.tsx`** — inherits `strategyConditionsToPreview`, so it picks the filter up
  automatically once §4.2 lands. Worth an explicit test.

---

## 5. Ordering / dependencies

Backend §3.1 is the only strictly blocking item — everything else can land in parallel.

1. **§3.1 evaluator + §3.4 tests** — makes the existing stored config actually take effect.
2. **§3.3 validation helper** — small, independent.
3. **§3.2 preview** — depends on §3.1 (shares the evaluator).
4. **§4.1 UI input** — depends on §3.1 being deployed, or users will set a filter that does nothing.
5. **§4.2 preview wiring** — depends on §3.2.
6. **§4.3 display** — depends on §3.2 for the `mcap` field.

⚠️ **Deploy order matters.** Shipping the UI (§4.1) before the evaluator (§3.1) gives users a
control that silently does nothing. Ship backend first.

---

## 6. Edge cases & decisions

| Case | Decision |
|---|---|
| **Units** | ₹ crore throughout, matching `CompanyMaster.mcap`. No conversion at any layer. Label the UI "₹ Crore". |
| **`min=0, max=0`** | Filter not set — auto-pass. Consistent with `price_range` and `pct_change`. |
| **`min>0, max=0`** | **Rejected with 400.** There is no open-ended range; if a minimum is set, a maximum is required. |
| **`max < min`** | **Rejected with 400** on both create and update, frontend and backend. Evaluator additionally fails closed with a Warn log if it ever sees this state. |
| **`min == max`** | Valid — an exact-cap filter. |
| **Negative values** | **Rejected with 400.** |
| **Missing / zero `mcap`** | Fails any active filter. An unknown cap cannot be proven in-band. Log at Debug so these are diagnosable. |
| **Interaction with `market_cap_types`** | **Both filters stay and both apply, ANDed.** They are separate conditions and `IsFullMatch()` requires all to pass, so `MID` + `1000–10000` yields the intersection. UI hint required (§4.1). |
| **`match_all_news`** | Bypasses the range filter, exactly as it bypasses every other non-impact condition today. Preserves existing semantics. |
| **Legacy rows** | The field was never settable from the UI, so every existing `strategy_conditions` row should hold `0/0` (the form default) and be unaffected. Confirm with `SELECT count(*) FROM strategy_conditions WHERE min_market_cap <> 0 OR max_market_cap <> 0;` before deploying §3.1 — a non-zero count means live strategies will start filtering the moment the evaluator ships. |
| **Staleness** | `mcap` refreshes only when `LoadCompanyMasterData` runs (data-ingestion startup) or on a Redis cache miss. A range filter near a boundary can act on a stale cap. Acceptable for v1 — mcap moves slowly — but worth confirming how often that bulk load runs, and consider a TTL on the `isin:*` keys if it is startup-only. |

---

## 7. Files to touch

**Backend (5 files)**
- `services/rules-engine/internal/matcher/evaluator.go` — new `evaluateMarketCapRange` + 2 wire-ups
- `services/rules-engine/internal/matcher/evaluator_test.go` — range table tests + AND-interaction test
- `services/rules-engine/internal/backfill/preview.go` — `PreviewConditions`, `buildEvalStrategy`, `PreviewItem`
- `api/gateway/internal/handlers/user_config_handler.go` — `validateConditions` helper, called from create **and** update
- `api/gateway/internal/handlers/user_config_handler_test.go` — `validateConditions` matrix tests (create if absent)

**Frontend (3 files)**
- `src/components/algo-trading/CreateStrategyModal.tsx` — range inputs, `validateMarketCapRange`, inline + step + submit gates
- `src/utils/algo-trading/strategiesApi.ts` — `AMNPreviewConditions`, `strategyConditionsToPreview`, `AMNPreviewItem`
- `src/components/algo-trading/StrategyCard.tsx` — display (optional)

**Zero changes:** DB migrations, `.proto` / generated pb.go, user-config service, `redis_manager.go`,
`mongo_watcher.go`, `company_lookup.go`, `amn_runner.go`, `configsync/`, `startup/`, AMN preview gateway handler.

---

## 8. Manual verification

### Happy path
1. Create a strategy with market cap `1000` – `10000` and no other narrowing filters.
2. Confirm the value round-trips: `GET /api/v1/strategies/:id` returns
   `conditions.min_market_cap: 1000`, `max_market_cap: 10000`.
3. Check Postgres directly: `SELECT min_market_cap, max_market_cap FROM strategy_conditions WHERE strategy_id = '...'`.
4. Confirm the rules-engine loaded it (config-sync log / debug dump of `Conditions.MarketCapRange`).
5. Feed a news event for **Ashok Leyland** (`ISIN INE208A01029`, `mcap 88783.31`) — must be
   **excluded**, with `market_cap_range` in `FailedConditions`.
6. Feed a news event for a stock with `mcap` between 1000 and 10000 — must **match**.
7. Leave both fields blank → confirm no market-cap filtering occurs (Ashok Leyland matches again).

### Validation (each must return **400** with the exact §3.3 message, and be blocked in the UI first)
8. `min=10000, max=1000` → "Maximum market cap must be greater than or equal to minimum market cap"
9. `min=1000, max=0` (blank max) → "Maximum market cap is required when a minimum is set"
10. `min=-1` → "Market cap values cannot be negative"
11. Repeat 8–10 against **`PUT` update** on an existing strategy — this path had no validation before.
12. Bypass the UI (curl the gateway directly) with an invalid range → confirm the 400 still fires.

### Combined filter
13. Set Market Cap Types = `MID` **and** range `1000–10000`. Confirm a mid-cap stock at
    `mcap 88783.31` is **rejected** (types passes, range fails) — proving the AND.

### AMN
14. Run the AMN preview with the same conditions and confirm the returned set matches what the live
    engine accepts (no stock outside the band), and that the new `mcap` column renders.
15. Reactivate an existing AMN strategy via `AmnReactivateModal` and confirm the filter is applied.