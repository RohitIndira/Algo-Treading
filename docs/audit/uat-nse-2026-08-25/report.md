# UAT Test Report — Manthan Strategy, NSE (Auditor's Checklist — UAT Market Session 2026 v1.0)

**Date:** 2026-08-25, 13:24–13:40 IST (live NSE market hours)
**Environment:** Full Manthan pipeline (api-gateway → user-config → rules-engine → trade-execution) running locally against UAT broker `https://trade.indiratrade.com`; market data from production feed Redis (15.207.203.46, db0, live ticks).
**Account:** ND03920 (UAT session, `loginSource: APP`), ledger ₹3,00,00,000. Strategy `b5aca8c5-b981-4789-ab90-bb0a539de466` "Manthan UAT" (capital ₹5,00,000, max 25 positions, 20% SL, 2% trail).
**Evidence:** service-log screenshots (this folder's `logs/`), DB CSVs (`db/`), all order IDs in UAT series `NZVAH…`. Complements the broker-level UAT report of 2026-08-24 (`../uat-nse-2026-08-24/`).

## Section A — Checklist items

| Item | Result | Evidence (log source) |
|---|---|---|
| Strategy Initialization | ✅ | gateway `POST /api/v1/strategies` (24th) → user-config outbox → RE `Manthan strategy loaded — creation-time signal gate armed`; lifecycle `DEPLOYED {"capital":500000}` |
| New Order Request/Response | ✅ ×12 | RE `Using live LTP → Entry order generated → entry order published` per symbol; TE `[indira] → POST place-order body={full order}` → `← 200 ordId NZVAH… Requested`; `manthan_orders`/`manthan_order_events` (13 orders, 84 events) |
| Price Check / DPR | ✅ | Entry priced LTP+2 ticks (always in band); protective stop: `SL deferred — intended 20% below DPR band; will place when band re-centers` (RELIANCE 1124.64 < floor) — stop **not released** in breach, never clamped |
| Quantity / Order Value | ✅ | allocator caps every order to per-stock capital (`invested` ₹3.2k–7.9k ≤ ₹20,000 across all 12); UAT exchange qty cap separately evidenced 24th (`Maximum Quantity 47035`) |
| Trade Price Protection | ✅ | `Entry timeout — modifying price escalation_pct 0.2/0.4/0.6 retry 1..3` → `LIMIT retries exhausted` → `MARKET fallback proceeding — LIMIT confirmed unfilled by safety poll` (LGEINDIA, FEDERALBNK, ACUTAAS, RELIANCE) |
| Exposure Limit | ✅ | fail-closed margin block evidenced 24th (`insufficient margin available=₹0.00 required=₹6887.16` → MARGIN_INSUFFICIENT → DLQ) |
| Trade Confirmation | ✅ | RELIANCE: `MARKET fallback poll result Executed filled_qty 6 avg_price 1405.8` → `LIVE BUY filled` → order 855 FILLED with broker id + timestamps |
| User can Pause | ✅ | `/deactivate` → lifecycle `PAUSED` 13:34:14 → TCS signal published while paused → **0 rules-engine lines, 0 orders** |
| User can Resume | ✅ | `/activate` → lifecycle `RESUMED` 13:37:40 → INFY signal allocated + placed (`NZVAH00017I8`) |
| Stop (square-off) | ⚠️ | lifecycle `DELETED {"positions_exited":0}` recorded; live-ws `Force-exiting … 0 exitable positions` — Manthan positions not visible to the force-exit path (defect D2) |
| Strategy-specific controls | see Section B | |

## Section B — Manthan strategy controls

| # | Control | Result | Evidence |
|---|---|---|---|
| B1 | Capital-bound sizing | ✅ | 12 × `Entry order generated … invested ≤ per-stock cap` |
| B3 | Creation-time signal gate | ✅ | `Signal skipped … signal predates strategy (first seen 2026-08-24T04:00Z, strategy created …)` (INFY predate publish) |
| B4 | Duplicate/idempotency | ✅ | `Inbox MANTHAN_ENTRY already placed — dedup OK` ×4 (LGEINDIA republish + retry paths) |
| B5 | Hard 20% stop + band-defer | ✅ | RELIANCE fill 1405.8 → intended stop 1124.64 (exactly ×0.80) → `SL_DEFERRED_BAND`; resting-SL outcome evidenced 24th (NZVAH00009H8 Pending) |
| B7 | Price-protection ladder + market fallback | ✅ | bounded 0.2%-step chase ×3 → cancel → market; RELIANCE fallback filled |
| B8 | Market-session gate | ✅ (24th) | `Entry pre-check failed — too close to market close (after 15:20)` → DLQ |
| B9 | Exposure fail-safe | ✅ (24th) | MARGIN_INSUFFICIENT, order never released |
| B10 | Broker-session failure handling | ✅ (24th) | AU004 → `class=AUTH_EXPIRED retry` → `Inbox row → DLQ — operator attention required` |
| B11 | Full audit trail | ✅ | 13 orders / 84 events / 12 decisions / 4 lifecycle rows (CSVs in `db/`) |
| B12 | Reconciler broker↔DB sync | ✅ | UAT sim cancelled 9 unfilled orders → `Reconciler fixed order → CANCELLED` ×9 → `Reconciler pass complete drifts_fixed=9` within one 5-min pass |
| B13 | Pause/Resume/Stop lifecycle | ✅ | Section A rows |
| B2/B6/B14 | Portfolio caps / trailing-modify / EOD arming | not exercised this session | caps not reachable with 12 positions; no position rose 2% before stop; EOD arming occurs 16:35 (stack can be left running to capture) |

## Observations / defects for engineering

- **D1 (environment):** `portfolio.allocations` Kafka topic missing locally → RE `POSITION_OPENED` publish failed → positions never flipped PENDING_ENTRY→ACTIVE this session. Create the topic for future runs; production topic exists.
- **D2 (product, known):** Stop with SQUARE_OFF_AT_MARKET does not exit Manthan positions — `/ws/live-orders/force-exit-strategy` reads the non-Manthan book (same defect recorded in production audit 2026-08-24).
- **UAT sim liquidity:** only large-cap instruments fill (RELIANCE, TCS); the 10 small/mid-cap orders were accepted then cancelled/rejected by the sim — order life-cycle and reconciliation still fully exercised.
- GTC→DAY validity downgrade reproduced on UAT (24th).

## Residual UAT state
RELIANCE 6 @ 1405.80 held on the UAT book (SL deferred, software-watched until stack stopped); FEDERALBNK limit possibly resting; strategy deleted; 12 local PENDING_ENTRY rows expire via 8h TTL.
