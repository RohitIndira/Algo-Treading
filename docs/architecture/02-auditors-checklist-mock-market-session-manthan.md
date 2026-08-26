# Auditor's Checklist

## Mock Market Session

### 2026 - Version 1.0

**Algo / Strategy:** MANTHAN (52-week breakout, delivery, long-only)
**Member:** Indira Securities Pvt. Ltd.
**Test session:** NSE/BSE Mock Trading Session, 2026-08-22 (Saturday), production stack (manthan-prod) with broker middleware.
**Test accounts:** S4450 (strategy pipeline) and ND03920 (fresh strategy lifecycle).

---

At Individual Level

### ● Price Check

Algo orders shall not be released in breach of the price bands /dummy filters as defined by the Exchange in respective segments.

> **[SCREENSHOT M-PC-1]** — trade-execution log: stock pinned at its upper price band (AXISBANK at dpr_upper) — `"Entry on hold — stock at upper circuit, will retry until relieved"`. **No order row created, no broker call made** (DB query in screenshot shows zero orders).
>
> **[SCREENSHOT M-PC-2]** — protective stop 20% below fill outside the day's band — `"SL deferred — intended 20% below DPR band"` / order status `SL_DEFERRED_BAND` (`intended=1262.56 < dpr_lower=1381.80`) — deferred, never clamped, never released in breach.
>
> **[SCREENSHOT M-PC-3]** — exchange-side confirmation from the mock session order book: BSE *"LIMIT PRICE: 400 NOT EQUAL TO CLOSING PRICE: 292"* and NSE *"The order has been cancelled due to price freeze."*

### ● Quantity Check

Algo Orders shall not be released in breach of order quantity limit per order as defined by the Exchange in respective segments.

> **[SCREENSHOT M-QC-1]** — trade-execution log, forced oversized order (5,000,000 CUB): margin pre-check computed ₹111.93 crore requirement, order **not released** — `"Entry skipped — margin pre-check failed: insufficient margin: available=₹2011973.00 required=₹1119318750.00"` → `MARGIN_INSUFFICIENT → CANCELLED` event, inbox row → DLQ.
>
> **[SCREENSHOT M-QC-2]** — exchange verdict for a direct oversized order: *"Quantity is more than Maximum Quantity (43417) allowed by the exchange"* (NYMZX002A6F8).

### ● Order Value Check

> **[SCREENSHOT M-OV-1]** — allocator sizing rows (`manthan_signal_decisions`: intended_invested ≤ capital/25 for every dispatched signal) and the ≈ ₹12 crore direct order rejected by the exchange cap (NYMZX002A7F8).

### ● Trade Price Protection Check

(already in price check)

> **[SCREENSHOT M-TP-1]** — bounded price escalation on the entry: `"Entry timeout — modifying price … escalation 0.20% retry 1"` → `MODIFIED` event `"retry 1, drift 0.45%, escalation 0.20%"` → fill at the protected price (RELIANCE 1578.20).

### ● Exposure Limit

> **[SCREENSHOT M-EL-1]** — margin pre-check via broker fund-limit API before every entry (`GET /payments/api/v1/get-fund-limit → availableMargin`) and the insufficient-margin rejection of M-QC-1.

### ● Strategy Initialization & New Order Request/Response Confirmation

> **[SCREENSHOT M-SI-1]** — creation at 12:29:11 IST across four layers: gateway `POST /api/v1/strategies` · user-config `stored Indira credentials` + outbox processed · `strategies` row (ND03920, LIVE, MANTHAN) · lifecycle `DEPLOYED {"capital":500000}` · rules-engine `"MANTHAN strategy created — triggering catch-up"`.
>
> **[SCREENSHOT M-SI-2]** — order request/response capture: trade-execution `place-order` request body → broker `{"ordId":"NZWKE00001F8","ordStatus":"Requested"}` → order/event tables with timestamps, user, security, qty, price, order type, product type.

### ● Manthan Strategy Controls

> **[SCREENSHOT M-MS-1]** — session-recovery re-arm: after credential refresh, the 5-minute loop placed 12 protective stop-loss orders within one cycle (broker ids NYMZX0028CH8…297H8 on 2026-08-24 12:23–12:24 IST); all Pending on the exchange book.
>
> **[SCREENSHOT M-MS-2]** — reconciliation: `"Reconciler fixed order → CANCELLED"` (broker-cancelled SL detected and re-armed) and `"Reconciler pass complete … drifts_fixed"`.
>
> **[SCREENSHOT M-MS-3]** — market-hours gate: rules-engine `"Strategy created outside market hours — deferring entries to next open"` (Saturday).
>
> **[SCREENSHOT M-MS-4]** — trailing stop: `"Trailing SL updated … old_sl → new_sl, high"` with the band-aware placement (`SL_DEFERRED_TRAIL` when the new level remains outside the band).

### ● User can Pause Strategy

> **[SCREENSHOT M-PS-1]** — `POST …/deactivate` → lifecycle `PAUSED {"positions_exited": 0}` → a signal (CUB) published while paused produced no order for the paused strategy (rules-engine skip lines + zero-order query).

### ● User can resume Strategy

> **[SCREENSHOT M-RS-1]** — `POST …/activate` → lifecycle `RESUMED` → the next signal (NRBBEARING) generated and placed an order (NZWKE0001AF8).

### ● Order Place Request, Response and Trade Confirmation

> **[SCREENSHOT M-TC-1]** — full chain for RELIANCE (12:59:26–12:59:42 IST): signal → allocation (`Using live LTP … Entry order generated qty 12`) → pre-checks (mock-session gates logged, margin OK ₹20.1 L) → `place-order` → `Requested NZWKE00001F8` (exchOrdId 1310000014940827) → WSS `PENDING` → price-protect modify to 1578.20 → `"LIVE BUY filled"` → `FILL — WSS fill confirmed` event → position ACTIVE.

---

**Annexure:** see *03-screenshot-appendix* for commands, log files, and the order-ID registry (NZWKE / NYMZX series with exchange order numbers).
