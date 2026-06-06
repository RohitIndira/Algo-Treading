# Manthan Momentum Equity Strategy

**NSE Cash · INTRADAY · 2026 — Version 1.0.0**

---

## 01 STRATEGY OVERVIEW

Manthan is a long-only, equity-cash momentum strategy that enters fundamentally
qualifying NSE-listed stocks the moment they print a new All-Time-High close,
holds them with a trailing stop-loss, and rebalances daily from a fundamental
data feed.

**Core Edge:** Combine a strong technical signal (ATH breakout from the
prior session) with a fundamental quality gate (F-Score ≥ 60, PE ≤ 60, profitable,
mid-/small-cap focus). Most ATH breakouts in low-quality businesses fail; this
filter discards them at signal time. The trailing SL captures the run while
capping the per-trade downside on the survivors.

**Position Structure:** Equal-weight slots — each open position consumes
`Total Capital ÷ Max Positions` rupees of buying power. New eligible signals
fill any unused slots in arrival order, subject to portfolio caps.

**Core Parameters**

| Parameter           | Value (default)        | Notes                                  |
|---------------------|------------------------|----------------------------------------|
| Exchange            | NSE                    | Equity Cash segment                    |
| Product Type        | INTRADAY (MIS)         | Same-day square-off; broker-margin     |
| Side                | BUY only               | Long-only; no short side               |
| Base Lot            | 1 share                | Cash equity, no lot size               |
| Total Capital       | ₹1,00,000 *(per user)* | Configurable per strategy row         |
| Max Positions       | 25                     | Equal-weight slots                     |
| Per-Stock Allocation| Total ÷ Max Positions  | Recomputed on every exit (rebalances)  |
| Entry Cutoff        | 09:15:30 IST           | After ODIN reconcile; before main flow |
| Exit Cutoff         | 15:15:00 IST           | Forced flat at this time               |
| Polling Frequency   | Per tick (ODIN feed)   | TSL evaluated on every market tick     |
| Signal Refresh      | Daily at 09:00 IST     | Google Sheet read into pipeline        |

---

## 02 ENTRY LOGIC

### Step 1: Daily Signal Refresh (09:00 IST)

The data-ingestion service pulls the Manthan Google Sheet via the Sheets API
using a read-only service account. Seven tabs are loaded in a single
`BatchGet` call:

| Tab                    | Role in pipeline                                       |
|------------------------|--------------------------------------------------------|
| `PEandNetProfitSheet`  | Market Cap, PE, Net Profit (PAT), Latest Price        |
| `FScore`               | Piotroski F-Score per company (0–100 scale)            |
| `LifeTimeHigh`         | All-Time-High close + bar number since IPO             |
| `BuySignal`            | The day's ATH breakouts (entry triggers)               |
| `IndicesGradeRange`    | Per-index allocation % (rebalance weight)              |
| `Industry`             | Sector mapping per NSE symbol                          |
| `ExitStockDetail`      | Forced-exit overrides (optional; tolerated if absent)  |

Each candidate flows through joiner → filter → caps → publisher.

### Step 2: Calculate Eligibility per Candidate

A `BuySignal` row qualifies only if **all six** fundamental gates pass:

```
1. Market Cap ∈ [500 Cr, 1,50,000 Cr]      (excludes micro-caps and mega-caps)
2. 0 < PE ≤ 60                             (excludes loss-makers and overvalued)
3. PAT > 0                                 (profitable last 12 months)
4. F-Score ≥ 60                            (Piotroski-style quality)
5. Bar Number ≥ 20                         (≥ 1 month since listing)
6. 20-bar Volume × Avg Price > ₹1 Cr       (liquidity gate, computed when feed has it)
```

A candidate failing any gate is recorded in `manthan_signal_decisions` with
its `Reason` (e.g., `"FSCORE & PE"`) so it can be audited end-to-end. **Filter
order matters** — failures short-circuit cheaper checks before expensive ones.

### Step 3: Bucket and Index Mapping

For each eligible candidate, assign:

```
MCap Bucket:
   ≥ 85,000 Cr           → LARGE      → NIFTY50
   27,000 Cr – 85,000 Cr → MID        → NFTYMCP150
   < 27,000 Cr           → SMALL      → NTYSLCP250
```

Each bucket-index pair carries its own allocation weight from `IndicesGradeRange`
(default 100% per index in normal mode).

### Step 4: Portfolio Caps (FCFS)

Iterate the eligible list in `BuySignal` arrival order and enforce:

```
sector cap  = ceil(total × 0.25)    # max 25% of the day's portfolio per sector
bucket cap  = ceil(total × 0.50)    # max 50% per MCap bucket
```

First-come-first-served. Later candidates breaching either cap are rejected
and recorded with the corresponding reason (`sector cap reached (25%)` or
`mcap bucket cap reached (50%)`).

### Step 5: Publish Eligible Signals

The remaining candidates pass a final **ISIN gate** against MongoDB
`CompanyMaster` (rejects signals whose ISIN isn't in our live universe — guards
against typos in the sheet), then publish to:

- **Postgres:** `trade_signals` table (audit trail).
- **Kafka topic:** `manthan.signals` (consumed by rules-engine).

### Sample Entry Calculation

```
Symbol:       ABCDEFG
MarketCap:    14,600 Cr   → SMALL bucket → NTYSLCP250 index
PE:           38.7        ≤ 60 ✓
PAT:          ₹302 Cr     > 0 ✓
F-Score:      62          ≥ 60 ✓
BarNo:        145         ≥ 20 ✓
20-bar Vol:   ₹4.8 Cr/day > ₹1 Cr ✓
Industry:     "Iron & Steel Products"
ATH (T-1):    481.60

→ ELIGIBLE. Slot allocated:
   PerStockBase = ₹1,00,000 / 25 = ₹4,000
   Qty          = floor(4000 / Entry Price)
```

---

## 03 EXIT FRAMEWORK

Three independent triggers, evaluated on every market tick.

### Trigger 1: Trailing Stop-Loss (TSL)

```
HighSinceEntry  = max( HighSinceEntry, currentLTP )
TrailingSL      = HighSinceEntry × (1 − TrailingSLPct/100)

If LTP ≤ max(InitialSL, TrailingSL) → EXIT
```

Default `StopLossPct = 5%`, `TrailingSLPct = 3%`. The trailing band tightens
as the price rises; the initial floor protects against gap-down on day 1.

### Trigger 2: Hard Cap (Time-of-Day)

```
If wall_clock ≥ 15:15:00 IST AND position still open → MARKET EXIT
```

INTRADAY product type would force a broker square-off at 15:30 anyway; we
exit 15 minutes early to avoid being a price-taker in the close auction.

### Trigger 3: User Override / Manual Exit

```
If the operator force-exits a position via dashboard → EXIT immediately.
Sets a 3-day cooldown on that symbol (no re-entry).
```

### Exit Execution

- All exits use **`SL-Limit` orders** on Indira, `ordType: "SL"`,
  `triggerPrice = computed SL`, `limitPrice = triggerPrice − one tick`.
- Reasoning: `SL-M` can fill far from trigger in thin markets; SL-Limit
  guarantees a worst-case exit price.
- Once the SL fires, the order goes to Limit semantics — exchange-side price
  improvement is allowed (and welcomed; the seller benefits).

---

## 04 RE-ENTRY LOGIC

A stock that exits **must not** re-enter the same day unless it meaningfully
re-tests the breakout level. We enforce one **cooldown rule** plus the **daily
signal cap**:

### 20% ATH Correction Cooldown

```
On SL exit:
  cooldown_entry = {
      Symbol:      sig.Symbol,
      ExitPrice:   ltpAtExit,
      ATHAtExit:   ATHClose,
      CorrectionFromATH: (ATHAtExit − ExitPrice) / ATHAtExit
  }

Next signal arrives for the same symbol:
  If price has NOT corrected ≥ 20% from ATH at exit time → REJECTED.
  If correction ≥ 20% → cooldown cleared, candidate proceeds normally.
```

This forces the market to fully reset before we risk capital on the same
ticker again — the breakout has to be "fresh" rather than a noisy retrace.

### Manual-Exit Cooldown (3 days)

```
If a position was force-exited by the user (not by SL):
  user_override_until = exit_timestamp + 72h
  Reject any signal for that symbol until that time.
```

Different from the SL cooldown — captures the operator's intent ("I don't
want this stock right now") even if technicals improve.

### Daily Cap

```
Max new positions per day  = 25 minus open positions at session start.
Slots free up as exits happen, allowing same-day refills.
```

### Entry Abort Conditions

The signal is *not* published (filter_rejected counted in stats) if:

1. ISIN missing from `CompanyMaster` (universe mismatch).
2. Industry blank (joiner couldn't map sector — needed for the 25% cap).
3. Sector cap (25%) or bucket cap (50%) already filled by earlier same-day
   signals.
4. Allocator can't find a free slot (rare; happens only if every slot is
   occupied AND no exits have rebalanced).

---

## 05 SESSION RULES (INTRADAY Equity)

Equity intraday has no "expiry" the way derivatives do, but the day's session
has fixed phases:

| Time IST       | Phase                                          | Notes                                  |
|----------------|------------------------------------------------|----------------------------------------|
| 09:00          | Sheet refresh + signal pipeline                | All eligibility decisions made here    |
| 09:15:00       | NSE pre-open ends                              | Engine WS subscribes to ODIN           |
| 09:15:30       | Trade-execution reconcile                      | Re-syncs paper positions with broker   |
| 09:15:30+      | Entry window opens                             | Eligible signals start placing         |
| 09:15 → 15:15  | Active monitoring                              | TSL evaluated per tick                 |
| 15:15:00       | Hard exit cutoff                               | Any open position → MARKET exit        |
| 15:30          | Broker auto-squareoff (safety net)             | If we missed the 15:15 exit            |
| 15:30 → 16:00  | Post-session reconciliation                    | Fill prices from `GetTradeBook`        |
| 16:00          | Next-day AMO (After-Market) for replayer mode  | Pre-stages tomorrow's entries          |

**Recommendation:** Always exit before 15:15. Broker auto-squareoff at 15:30
is a market order — fills at unfavourable prices in low liquidity.

---

## 06 DATA REQUIREMENTS

### At Entry (per candidate signal)

| Field                  | Source                                     | Validation                  |
|------------------------|--------------------------------------------|-----------------------------|
| Symbol (NSE)           | `BuySignal!NSESymbol`                      | Required                    |
| ISIN                   | `BuySignal!ISIN` (joined via PEandNetProfit)| Required, in CompanyMaster |
| Company Name           | `PEandNetProfitSheet!Company Name`         | Required                    |
| Market Cap             | `PEandNetProfitSheet!SC_Latest Market Cap` | 500 ≤ x ≤ 150,000           |
| Latest Price           | `PEandNetProfitSheet!SC_Latest Price`      | > 0                         |
| TTM PE                 | `PEandNetProfitSheet!SC_TTM_PE`            | 0 < x ≤ 60                  |
| TTM PAT                | `PEandNetProfitSheet!FH_PAT`               | > 0                         |
| F-Score                | `FScore!Score`                             | ≥ 60                        |
| Bar Number             | `LifeTimeHigh!BarNo`                       | ≥ 20                        |
| ATH Close              | `LifeTimeHigh!ATHClose`                    | > 0                         |
| Industry               | `Industry!Industry`                        | Non-blank                   |
| 52W High               | `LifeTimeHigh!Week52High`                  | Used for diagnostics        |
| Latest Price Date      | `PEandNetProfitSheet!SC_Latest Price Date` | T-1 ≤ date ≤ T              |

### During Monitoring (per tick)

| Field         | Source             | Use                       |
|---------------|--------------------|---------------------------|
| LTP           | ODIN broadcast WS  | TSL evaluation            |
| Bid / Ask     | ODIN broadcast WS  | Exit slippage estimate    |
| Volume        | ODIN broadcast WS  | Liquidity health check    |
| Wall clock    | Local              | 15:15 hard-exit trigger   |

### Universe Gate (preflight)

`CompanyMaster.isin → resolved symbol` lookup in MongoDB. Any signal whose
ISIN doesn't resolve is dropped pre-Kafka (logged as `filter_rejected`).

---

## 07 COMPLETE CALCULATION REFERENCE

| Formula                | Calculation                                                    | Interpretation                   | Example                          |
|------------------------|----------------------------------------------------------------|----------------------------------|----------------------------------|
| MCap Bucket            | `LARGE if ≥85K Cr, MID if 27K–85K Cr, else SMALL`              | Index attribution                | 14,600 Cr → SMALL                |
| Sector Cap (count)     | `ceil(total_eligible × 0.25)`                                  | Max same-sector positions today  | 8 eligible → 2 per sector        |
| Bucket Cap (count)     | `ceil(total_eligible × 0.50)`                                  | Max same-bucket positions today  | 8 eligible → 4 per bucket        |
| Per-Stock Allocation   | `current_capital / max_positions`                              | Rupees per slot (rebalances)     | ₹1,00,000 / 25 = ₹4,000          |
| Position Quantity      | `floor(per_stock_allocation / entry_price)`                    | Integer shares to buy            | 4000 / 250 = 16 shares           |
| Initial SL Price       | `entry_price × (1 − stop_loss_pct/100)`                        | Stop on day-1 gap-down           | 250 × 0.95 = 237.50              |
| Trailing SL Price      | `max(high_since_entry × (1 − trail_pct/100), initial_sl)`      | Tightens as price rises          | 280 × 0.97 = 271.60              |
| ATH Correction %       | `(ATHAtExit − currentPrice) / ATHAtExit × 100`                 | Cooldown release threshold       | (480-380)/480 = 20.8% → released |
| Realized P&L per Lot   | `(exit_price − entry_price) × qty`                             | Single-position profit/loss      | (270 − 250) × 16 = ₹320          |
| Capital After Exit     | `current_capital + realized_pnl`                               | Drives rebalance                 | 1,00,000 + 320 = 1,00,320        |

---

## 08 EXECUTION FLOW

### Phase 1: Pre-Market Data Refresh (09:00–09:14 IST)

```
1. Cron fires manthan-live binary.
2. GSheetReader.Connect() with service account.
3. Spreadsheets.Get() → list of tab titles (handles missing tabs gracefully).
4. Spreadsheets.Values.BatchGet() → all 6/7 tabs in one call.
5. Pipeline runs: joiner → filter → bucket+index → caps → publisher.
6. Publisher writes to trade_signals + publishes to Kafka manthan.signals.
7. EMA allocations written to local Redis for HFT-style consumers.
```

### Phase 2: Position Allocation (rules-engine consumer)

```
1. Kafka consumer reads from manthan.signals.
2. For each ELIGIBLE signal, allocator checks:
   - Already holding this symbol? → SKIP (no doubling up).
   - User override cooldown active (3d)? → SKIP.
   - In SL cooldown AND ATH correction < 20%? → SKIP.
   - All slots filled? → SKIP.
   Else → allocate slot.
3. Build ManthanOrder { symbol, qty, entry_target, stop_loss, ... }.
4. Persist to manthan_orders + write OPEN event to manthan_position_events.
```

### Phase 3: Order Execution (trade-execution service)

```
1. Outbox worker reads execution_events.
2. For each order:
   - Place MARKET BUY at NSE open via Indira /place-order.
   - On fill confirmation (order-status WS), record entry price.
   - Place trailing-SL order via Indira `ordType: "SL"`.
3. If place-order returns EG003 (already executed) — self-heal.
4. If place-order returns margin reject (M.Lmt excd / SFall) — halt that symbol.
```

### Phase 4: Live Monitoring (09:15–15:15 IST)

```
On every market tick (ODIN broadcast):
  1. portfolio.UpdateHighSinceEntry(symbol, ltp)
  2. tick_handler evaluates TSL:
       new_sl = max(InitialSL, HighSinceEntry × (1 − TrailingSLPct/100))
     If ltp ≤ new_sl → broker SL fires (no engine action needed; broker holds the SL order).
  3. Every 30s, fill-price reconciler queries trade-book for any
     price-pending fills and backfills the snapshot.
  4. If clock ≥ 15:15:00, force market exit on any still-open positions.
```

### Phase 5: Post-Session Reconciliation (15:30–16:00 IST)

```
1. Query Indira /trade-book for the day's fills (authoritative prices).
2. Reconcile against in-memory positions:
   - For any position closed by SL, capture exit_price + exit_time + ATHAtExit.
   - Insert cooldown row in manthan_cooldown.
   - UPDATE manthan_positions with realized P&L.
3. Publisher writes manthan_position_events (CLOSE event).
4. portfolio.CurrentCapital += sum(realized_pnl)
5. portfolio.PerStockBase = CurrentCapital / MaxPositions    ← rebalances tomorrow's slot size
```

---

## 09 KEY DESIGN DECISIONS

**Q1: Why a 20% ATH correction cooldown after SL, instead of a simple time-based cooldown?**

A: A time-based cooldown (e.g., 24h) doesn't distinguish "I exited because of a
random wick" from "the breakout actually failed". A 20% retracement from ATH
guarantees the market has consumed enough liquidity that the next ATH attempt
will be a genuinely new high, not a noisy retest. Empirically, re-entry on
retests within the cooldown band has near-zero edge.

**Q2: Why caps of 25% sector and 50% MCap bucket — why not equal-weight without caps?**

A: Equal-weighted portfolios in momentum strategies tend to concentrate in
whichever sector ran the previous month. A 25% sector cap forces diversification
across at least 4 sectors on any day. 50% MCap bucket prevents an
all-small-cap day from blowing up on a single small-cap correction. Both caps
use ceiling-rounding so portfolios with <4 candidates aren't starved.

**Q3: Why F-Score ≥ 60 and not the usual 7–9 (Piotroski's 0–9 scale)?**

A: Our F-Score sheet uses a normalized 0–100 score, not Piotroski's raw 0–9.
60 on this scale ≈ 7+ on the Piotroski scale. The translation lives in the
F-Score sheet's calculation; the threshold here is just the consumer side.

**Q4: Why first-come-first-served on cap rejection instead of best-of?**

A: "Best-of" (rank by F-Score or PE) requires a global sort over the day's
candidates, biasing the strategy toward survivors that happened to be cheap
that morning. FCFS uses the BuySignal sheet's natural ordering (the upstream
research team's preference order), removes look-ahead bias, and keeps the
algorithm reproducible: same sheet → same selection, every time.

**Q5: Why a single-day signal lifetime — why not roll yesterday's eligible-but-uncalled candidates into today?**

A: Manthan is an ATH-breakout strategy. A signal that didn't fill yesterday
means the market didn't sustain the breakout. Today's ATH check is a fresh
read — if the same stock still qualifies today, it'll be in today's
`BuySignal` sheet. Rolling stale signals would entry on retraces of failed
breakouts, which is exactly what the cooldown logic was designed to prevent.

**Q6: Why INTRADAY product type, not DELIVERY?**

A: Faster capital recycling — money locked in delivery T+2 settlement can't
fund tomorrow's signals. Trailing SL on a 1-day horizon also lets the trailing
band tighten meaningfully (3% trail on a 5% daily move locks in 2% within
hours). Delivery + trailing SL has nearly identical exit triggers in practice
but the capital efficiency is dramatically worse.

**Q7: Why is the SL placed as `SL-Limit` (ordType "SL"), not `SL-M`?**

A: SL-M (market) order can fill far below the trigger in a thin book or a
gap-down — particularly relevant for small-caps in the Manthan universe.
SL-Limit caps the worst-case exit price at our specified limit. We accept the
small probability of an unfilled SL (price gaps through both trigger AND limit
without trading) in exchange for known worst-case slippage.

**Q8: Why pull a Google Sheet via API instead of a database or CSV drop?**

A: The upstream research team edits the sheet directly. A database would
require an editor UI we'd have to build and maintain. CSV drops introduce
file-transfer race conditions. The Sheets API + service-account read-only
auth is the lowest-friction interface for a non-engineering signal source.
The cost is one-time daily latency (~2s for a `BatchGet`) which is well
within the pre-open window.

**Q9: Why is the daily new-position cap 25 (not a higher number)?**

A: Slot count = `Total Capital ÷ Per-Trade Capital`. With a fixed per-trade
slot size (`PerStockBase`), 25 positions on ₹1,00,000 give ₹4,000 per stock —
enough for at least 1 share of most names in the universe. Raising the slot
count reduces per-stock allocation below 1 share for many candidates,
breaking quantization. Lowering it concentrates risk. 25 is the empirical
balance for the current capital base.

**Q10: Why a 3-day cooldown on manual exits instead of treating them like SL exits?**

A: Manual exit ≠ stop-loss exit. The operator overrode the strategy on
domain knowledge (news, fundamental change, position sizing concern). That
knowledge has a longer half-life than a price retest. 3 days is long enough
for the news cycle to play out; shorter than a week so we don't lock out
genuine re-entry opportunities.

---

## 10 ERROR HANDLING & AUDIT TRAIL (SEBI ALGO COMPLIANCE)

Built and validated against real Codifi/Indira responses captured during
the **2026-06-06 NSE Contingency Drill** (10:00-15:30 IST window). 50
deliberately-crafted orders fired against account ND03920 produced 7
distinct Codifi `infoID` values, 11 unique pre-trade rejection messages,
and 6 exchange-side rejection patterns — all catalogued in
`pkg/indira/error_codes.go` and validated by 23 passing unit tests in
`pkg/indira/error_codes_test.go`.

### Two-layer error surface

| Layer | Where it appears | Code field | Example |
|---|---|---|---|
| Codifi envelope | PlaceOrder / ModifyOrder / CancelOrder response | `infoID` (alphanumeric) | `"EG003"` = pre-trade validation |
| Exchange / RMS | Order Book row `rejReason`, Order Trail entries | free text | `"Margin Limit exceeded..."` |

Codifi does NOT propagate NSE's numeric OE protocol codes (16115, 16247,
17179, …). Our parser maps the English prose back to the canonical NSE tag
by substring + regex match against a catalog populated from drill captures.

### Codifi infoID classification

| infoID | Meaning | Drill samples |
|---|---|---|
| `"0"` | Success — order accepted by Codifi (may still be Rejected at exchange) | T01, T10, T40, T43, T47, T49–T55 |
| `"EG001"` | Codifi structural reject — invalid enum field (`ordType`, `prdType`, `instrument`, …) | T32–T36, T59 |
| `"EG003"` | Codifi pre-trade business rule failed (`infoMsg` carries the specific rule) | T02, T04, T05, T11, T14, T37, T39, T41, T42, T46, T58 |
| `"AU004"` | Session/JWT expired OR "no data found" (overloaded) | T08, T16, T31 |

### Rejection categories → retry policy

| Category | Behavior | Drill samples |
|---|---|---|
| `PRE_TRADE_RETRYABLE` | Fix input + retry (tick-round, recompute trigger, etc.) | T02, T04, T05, T11, T14, T39, T41, T42, T46 |
| `PRE_TRADE_TERMINAL` | Mark FAILED, alert ops/user. Cannot auto-recover. | T06 (qty freeze) |
| `DPR_BREACH` | Re-clamp price within today's circuit band + retry | 2026-06-04 yesterday's 10 AMO rejections |
| `AUTH` | Block retries until user re-authorizes (DDPI eSign / TPIN OTP) | T43, T48, T55 (RMS free-qty exceeded) |
| `MARGIN_INSUFFICIENT` | Reduce qty OR top up margin. Operator action required. | T07, T44 |
| `STALE_ORDER` | Underlying order already terminal at exchange. Reconcile DB. | T19 (modify after cancel) |
| `UNKNOWN` | Catalog gap. Log + alert + treat as terminal pending investigation. | n/a — none observed |

### Database tracking (migration 014, applied 2026-06-06)

`manthan_orders` and `manthan_order_events` carry per-rejection columns:

| Column | Type | Purpose |
|---|---|---|
| `exchange_error_code` | INT | NSE OE protocol code when known (16247, 16307, 17179, …) |
| `exchange_error_tag` | TEXT | Canonical tag from catalog (`INVALID_PRICE_TICK_MISMATCH`, `MARGIN_INSUFFICIENT`, …) |
| `reject_category` | TEXT | One of the categories above — drives retry/alert decisions |
| `algo_id` | INT | SEBI-registered algo ID (Manthan strategy) populated on every order |

Indexes on `(reject_category)` and `(algo_id, created_at)` support SEBI
audit queries: rejections-by-category over time and algo-tagged orders
per audit period.

### SEBI algo-ID tagging

`PlaceOrderRequest` carries `AlgoID` (int) and `AlgoCategory` (string)
fields. Codifi's documented spec does not list these, but the broker
accepts and forwards them to NSE — confirmed by drill tests 49–54 with
algoID values `0`, `-1`, `999999999`, `"INVALID_STRING"`, missing-ID, and
miscategorized; **every variant returned `infoID="0"` / `ordStatus="Requested"`**,
i.e. Codifi passes the field through without validation. NSE will enforce
post-approval — orders without `algoID` will then fail with code 17179
`ERR_INVALID_ALGO_ID`.

Action item: set `MANTHAN_ALGO_ID` env (registered with NSE) before
approval cut-over; until then `AlgoID` is omitted (`,omitempty`) so we
behave as a retail order.

### Audit artifacts

| Artifact | Path | Purpose |
|---|---|---|
| Pattern catalog | `pkg/indira/error_codes.go` | Source of truth — all NSE tag mappings |
| Unit tests using drill fixtures | `pkg/indira/error_codes_test.go` | 23 tests proving every captured pattern maps correctly |
| Migration | `services/trade-execution/migrations/014_exchange_error_codes.sql` | Schema for the 4 new columns + indexes |
| Drill captures (raw) | `/tmp/drill_results.json`, `/tmp/drill_round3_results.json` | Evidence of pattern provenance |
| Strategy spec | this section | Auditor-facing summary |

### Worked example — end-to-end rejection handling

1. **15:35 IST**: EOD Phase A submits AMO+SL for `USHAMART qty=40 trigger=395.55 limit=394.35`.
2. **08:50 IST next day**: Indira releases the AMO into the live book; broker assigns new `ordId`.
3. **NSE rejects** with `rejReason: "Order entered has invalid data."` because trigger is below today's lower circuit band.
4. **Reconciler** (per fix #2 in this codebase) detects the rejection in Order Book, looks up the converted SL by `(symbol, side, qty, trade_date)`, calls `parseRejection("recon_amo_converted", resp)` →
   ```
   BrokerRejection{
     Code: 0,
     Tag: "INVALID_DATA_GENERIC_LIKELY_DPR",
     Category: CategoryDPRBreach,
     Retryable: true,
     Raw: "Order entered has invalid data.",
     Operation: "recon_amo_converted",
     OrderID: "NYMZX0028546",
   }
   ```
5. **Repository** updates row: `status='SL_SELL_AMO_REJECTED'`, `exchange_error_code=NULL`, `exchange_error_tag='INVALID_DATA_GENERIC_LIKELY_DPR'`, `reject_category='DPR_BREACH'`, `algo_id=<MANTHAN_ALGO_ID>`.
6. **Layer 2 `HasActiveProtectionForToday`** now returns `false` for this entry → 09:14 morning hot-SL cron re-arms with fresh DPR.
7. **Audit event** appended to `manthan_order_events` with the same code/tag/category — SEBI auditor can replay full timeline by `parent_order_id`.

---

## DISCLAIMER

This document is for internal use only and represents proprietary trading
logic. Past performance does not guarantee future results. All trading
strategies carry risk of loss. Use only with capital you can afford to lose.
Backtest thoroughly before live trading. The algorithm executes real orders
against a real broker against a real exchange — any bug, misconfiguration, or
data outage during market hours can result in material financial loss. Run in
paper-trading mode for at least one full month before enabling LIVE mode on
funded capital.
