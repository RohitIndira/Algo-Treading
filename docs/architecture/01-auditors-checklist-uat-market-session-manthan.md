# Auditor's Checklist

## UAT Market Session

### 2026 - Version 1.0

**Algo / Strategy:** MANTHAN (52-week breakout, delivery, long-only)
**Member:** Indira Securities Pvt. Ltd.
**Test account:** ND03920 (UAT environment — `https://trade.indiratrade.com`)
**Test dates:** 2026-08-24 (exchange-limit validation) and 2026-08-25 (full strategy pipeline, market hours 13:24–13:40 IST)
**System under test:** api-gateway → user-config → rules-engine → trade-execution (production build), broker UAT middleware, real-time NSE market-data feed.

---

At Individual Level

### ● Price Check

Algo orders shall not be released in breach of the price bands /dummy filters as defined by the Exchange in respective segments.

> **[SCREENSHOT PC-1]** — trade-execution service log: protective stop-loss computed exactly 20% below fill (RELIANCE ₹1124.64) falls below the day's DPR floor; the algo **defers** the order instead of releasing it — `"SL deferred — intended 20% below DPR band; will place when band re-centers"`.
>
> **[SCREENSHOT PC-2]** — UAT exchange verdicts confirming the band enforcement end-to-end: orders priced +25% / −30% of LTP rejected with *"The order has been cancelled due to price freeze."* (ordIds NZVAH00002H8 / NZVAH00003H8, exchOrdIds 1300000000047689 / …690); tick-size validation *"Price Not in multiple of PriceTick"* (NZVAH00007H8).

All entry orders are priced at LTP + 2 ticks from the real-time feed and are therefore always within the band; the only order type that can breach a band (the fixed 20% protective stop) is held by the system and re-evaluated daily, never clamped and never released in breach.

### ● Quantity Check

Algo Orders shall not be released in breach of order quantity limit per order as defined by the Exchange in respective segments. Quantity Limit check is also applicable for Spread Order being placed.

> **[SCREENSHOT QC-1]** — rules-engine service log: every order quantity is derived from per-stock capital allocation (`Entry order generated … qty … invested`), capping single-order size structurally (all 12 test orders ≤ ₹20,000 per stock).
>
> **[SCREENSHOT QC-2]** — UAT exchange verdict for a deliberately oversized order (5,000,000 TCS): *"Quantity is more than Maximum Quantity (47035) allowed by the exchange"* (NZVAH00004H8) — demonstrating the exchange-side limit and that normal strategy sizing operates far below it.

*(Spread orders: not applicable — the strategy places only cash-segment delivery orders.)*

### ● Order Value Check

Algo Orders should not exceed the limit specified by the Exchange. The order value check should be within the ranges as prescribed by Exchange circulars. Order value check is also applicable for Spread Order being placed.

> **[SCREENSHOT OV-1]** — rules-engine service log: order value bounded by strategy capital allocation — `invested` field per generated order (₹3,248 – ₹7,852 in the test run against the ₹20,000 per-stock cap).
>
> **[SCREENSHOT OV-2]** — UAT exchange verdict for a ≈ ₹12 crore order (52,539 × ₹2,284): rejected by the exchange value/quantity cap (NZVAH00005H8).

### ● Trade Price Protection Check

(already in price check)

> **[SCREENSHOT TP-1]** — trade-execution service log: bounded price-chase ladder — `"Entry timeout — modifying price … escalation_pct 0.2 / 0.4 / 0.6, retry 1..3"` → `"LIMIT retries exhausted — verifying state before MARKET fallback"` → `"MARKET fallback proceeding — LIMIT confirmed unfilled by safety poll"`. Price escalation is hard-capped at 0.6% above signal LTP before falling back to a market order.

### ● Exposure Limit

> **[SCREENSHOT EL-1]** — trade-execution service log: pre-trade margin verification against the broker ledger — `"Entry skipped — margin pre-check failed: insufficient margin: available=₹0.00 required=₹6887.16 (qty=3 × price=2284.30 × 1.005)"` followed by `"Inbox row → DLQ — operator attention required"`. The order was **not released**; the row was parked for operator attention (fail-closed).

### ● Strategy Initialization & New Order Request/Response Confirmation

All new order of the strategy requests received shall be captured in the complete order details including timestamp, user identifier, security information, quantity, price, order type, and product type.

All responses received from the OMS/exchange against new order requests shall be recorded along with order identifier, response code, status, and timestamp.

> **[SCREENSHOT SI-1]** — strategy initialization across services: api-gateway `POST /api/v1/strategies` → user-config outbox event → rules-engine `"Manthan strategy loaded — creation-time signal gate armed" (user ND03920, strategy b5aca8c5-…)`; lifecycle record `DEPLOYED {"capital": 500000}`.
>
> **[SCREENSHOT SI-2]** — new order request/response capture, trade-execution log: full request body (`symbol, excToken, exc, ordAction, ordValidity, ordType, prdType, limitPrice, qty`) and the broker response (`ordId NZVAH…, ordStatus Requested, timestamp`) for each of the 12 strategy orders; persisted to the order-audit tables (13 orders / 84 events with timestamps).

### ● Manthan Strategy Controls

*(Strategy-specific risk and integrity controls of the MANTHAN algo.)*

> **[SCREENSHOT MS-1]** — creation-time signal gate: `"Signal skipped … signal predates strategy (first seen 2026-08-24T04:00:00Z, strategy created 2026-08-24T11:52:16Z)"` — the algo never acts on signals older than the strategy.
>
> **[SCREENSHOT MS-2]** — duplicate/idempotency guard: `"Inbox MANTHAN_ENTRY already placed — dedup OK"` — a republished signal can never produce a second order.
>
> **[SCREENSHOT MS-3]** — broker↔system reconciliation: `"Reconciler fixed order → CANCELLED"` ×9 → `"Reconciler pass complete … drifts_fixed=9"` — order-state drift against the exchange book self-heals within one 5-minute cycle.
>
> **[SCREENSHOT MS-4]** — session-hours control: `"Entry pre-check failed — too close to market close (after 15:20)"` → order parked, not released.

### ● User can Pause Strategy

> **[SCREENSHOT PS-1]** — pause via API at 13:34:14 IST: gateway `POST …/deactivate` → lifecycle `PAUSED`; a signal (TCS) published while paused produced **zero** rules-engine processing and **zero** orders (query evidence included in screenshot).

### ● User can resume Strategy

> **[SCREENSHOT RS-1]** — resume via API at 13:37:40 IST: gateway `POST …/activate` → lifecycle `RESUMED`; the next signal (INFY) was allocated and placed (`NZVAH00017I8`) — order flow restored.

### ● Order Place Request, Response and Trade Confirmation

> **[SCREENSHOT TC-1]** — complete order lifecycle for RELIANCE in the trade-execution log: place-order request → `Requested NZVAH00015I8` → bounded modifies → market fallback `NZVAH00016I8` → `"MARKET fallback poll result … Executed, filled_qty 6, avg_price 1405.8"` → `"LIVE BUY filled"` → protective stop computed (fill × 0.80 = 1124.64) and correctly deferred below the DPR floor; order table row FILLED with broker order id and fill timestamp.

---

**Stop / square-off:** strategy deletion via API records lifecycle `DELETED`; the position square-off leg is under an engineering fix (tracked internally) and positions remain protected by the stop-loss watcher until flat.

**Annexure:** see *03-screenshot-appendix* for the exact command, log file, and order-ID registry behind every screenshot above.
