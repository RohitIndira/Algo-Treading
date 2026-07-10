# Codify (Indira) Order-Notify WSS State Machine

Live-verified reference for the `wss://livemiddleware.indiratrade.com/order-notify/websocket`
event surface. Every state and transition below comes from real events captured
against a live account — no docs, no assumptions.

**Last verification:** 2026-07-09 — S4450 during market hours + edge tests.
**Capture logs:** `/tmp/wss_capture_20260709_113242/` (events.jsonl, actions.jsonl, summary.json).
**Capture tool:** `scripts/codify_learn/03_wss_full_capture.py`.

## MessageType (verified — 2 values)

| Value | When emitted |
|---|---|
| `ORD_NRML` | Any order state change: PENDING, CANCELLED, A.REJECTED, ORDER ERROR, ADMIN PENDING |
| `TRD_MSG` | Fill / trade confirmation. Comes ALONGSIDE (not instead of) an ORD_NRML EXECUTED |

Rule: route on MessageType FIRST. TRD_MSG events carry the authoritative `TradedPrice`
for realized-PnL computation; ORD_NRML EXECUTED events may also have `TradedPrice`
populated but treat TRD_MSG as canonical.

## OrderStatus (verified — 6 values, all case-sensitive; **watch trailing whitespace**)

Codify sometimes emits `"ORDER ERROR "` with a trailing space and `"ADMIN PENDING "`
with a trailing space. Always `strings.TrimSpace(status)` before comparing.
`statusservice/service.go:303` already does this correctly.

| Status | Meaning | Terminal? | Follow-up? |
|---|---|---|---|
| `PENDING` | Order accepted, sitting at exchange (Codify's "OPEN" — the state Codify never actually emits as OPEN) | No | Modify emits another PENDING with new OrderPrice; Cancel emits CANCELLED; Fill emits EXECUTED (TRD_MSG) |
| `ADMIN PENDING` | Transitional pre-rejection state (OMSOrderStatus=8) | No | Always followed by A.REJECTED within ~200ms |
| `A.REJECTED` | Exchange/broker rejection | **Yes** | None |
| `CANCELLED` | User cancel succeeded | **Yes** | None |
| `EXECUTED` | Order filled | **Yes** | Paired TRD_MSG event |
| `ORDER ERROR` | Instant broker reject (price freeze etc) | **Yes** | None |

**Never observed** despite our checks in `wss_bridge.go`:
`OPEN`, `TRADED`, `COMPLETE`, `FILLED`, `REJECTED`, `PARTIALLY TRADED`, `PARTIALLY EXECUTED`,
`MODIFIED`, `AMEND CONFIRMED`, `TRIGGERED`.

We keep these in the `Is*` helpers for defensive coverage but should not rely on them.

## OMSOrderStatus numeric codes (verified — 3 values)

Distinguishes semantically-different `A.REJECTED` reasons:

| Code | OrderStatus pairing | Meaning |
|---|---|---|
| `8` | `ADMIN PENDING` (transitional) | Broker is validating the order, likely to reject soon |
| `10` | `A.REJECTED` | **Cancel-of-dead-order rejection** — order was already terminal, cancel had nothing to act on. Do NOT re-mark the underlying order as rejected. |
| `15` | `A.REJECTED` | **Fresh order rejection** — order failed validation (insufficient qty, limit exceeded, etc). Terminal. |

Other codes exist in Codify's system (docs mention 1-20+) but we haven't seen
them. Log unknowns with `zap.Warn("unknown_oms_code")` rather than switching on them.

## Transition graph

```
                     REST place-order (BUY or SELL)
                          │
              ┌───────────┼────────────┐
              │           │            │
              ▼           ▼            ▼
        ORD_NRML:    ORD_NRML:    ORD_NRML:
        PENDING      ADMIN PEND   ORDER ERROR
        (code —)     (code 8)     (terminal, instant reject
        active       transitional  — Reason has "price freeze")
        │            │
        │            ▼
        │      ORD_NRML: A.REJECTED
        │      (code 15, terminal — FRESH REJECT)
        │
        ├────────┬────────────┐
        │        │            │
        │        │            │  Modify emits:
        │        │            │  ORD_NRML: PENDING (same UniqueCode,
        │        │            │      OrderPrice changed to new value)
        │        │            │  (loops back into PENDING)
        │        │            │
        │        ▼            ▼
        │   ORD_NRML:     TRD_MSG: EXECUTED
        │   CANCELLED     (TradedPrice = fill,
        │   (terminal)    TradedQTY = fill qty)
        │                 + concurrent ORD_NRML: EXECUTED
        │                 (both terminal)
        │
        ▼
  Cancel of dead order:
    REST cancel-order → ORD_NRML: A.REJECTED (code 10, terminal — CANCEL FAILED)
                     → ORD_NRML: ADMIN PENDING (code 8, transitional)
                        (order was already terminal — this is broker being confused)
```

## Field-level notes (verified)

| Field | Notes |
|---|---|
| `UniqueCode` | Same as REST `ordId` and our DB `broker_order_id`. THE join key. |
| `OrderNumber` | NSE exchange order number (different from UniqueCode). Only populated after order accepted at exchange. `"0"` while pending broker-side. |
| `Buy_Sell` | String `"1"` = BUY, `"2"` = SELL. |
| `OrderType` | REST vocabulary translated: `"Limit"`→`"REGULAR LIMIT"`, `"Market"`→`"REGULAR MARKET"`, `"SL"`→`"SL LIMIT"`, `"SL-M"`→`"STOP LOSS MARKET"`. |
| `TradedPrice` | On TRD_MSG EXECUTED: real fill price. On ORD_NRML PENDING for MARKET orders: broker's estimated market price. Zero otherwise. |
| `OrderPrice` | On modify: shows the NEW price (order stays PENDING but this field changes — how we detect a successful modify). |
| `TriggerPrice` | Raw exchange value — divide by `DecimalLocator` for actual rupees. `statusservice/service.go:423` handles this. |
| `PendingQty` | On CANCELLED, this DOES NOT drop to 0 — stays at original qty. Do not use `PendingQty==0` to detect terminal state; use `OrderStatus`. |
| `Reason` | Free-text. Price freeze pattern: `" The order has been cancelled due to price freeze."` (note leading space + "price freeze" substring). |

## Detecting a successful modify

**There is no `MODIFIED` OrderStatus.** The broker re-emits a PENDING event with
the new OrderPrice. Detection:

```go
// pseudocode
if event.OrderStatus == "PENDING" && event.UniqueCode == existingOrder.UniqueCode {
    if event.OrderPrice != existingOrder.OrderPrice {
        // successful modify — new price accepted
    }
}
```

Currently our code (see `manthan/broker_adapter.go:442`) checks only the REST
response, which returns `infoID:"0"` even when the modify silently failed. This
is the Bug 3 vulnerability against AEGISLOG-type stale trailing SL.

## Log-spam suppression: price freeze

Production sees ~100/min of ORDER ERROR "price freeze" events (313 in a 3-min
log window on 2026-07-08). Suppression key = `(user_id, broker_order_id)` → log
once, dedup for 10 min. Use `IsPriceFreezeReject(status, reason)` helper
(added to `wss_bridge.go` 2026-07-09).

## Two-event patterns to expect

1. **Fresh reject:** `ADMIN PENDING` (code 8) → `A.REJECTED` (code 15). Both same UniqueCode, ~50ms apart. Treat as atomic — don't fire two separate handlers.

2. **Cancel of dead order:** `A.REJECTED` (code 10) → `ADMIN PENDING` (code 8). Second event does NOT re-active the order — broker is just confused about the cancel target.

3. **Fill:** `ORD_NRML: PENDING` → `TRD_MSG: EXECUTED` (and sometimes also `ORD_NRML: EXECUTED`). The TRD_MSG event is canonical for fill price/qty.

## Missing from this reference (need future capture)

Requires specific broker scenarios we didn't reach on 2026-07-09:

- Real `TRIGGERED` event when broker-side SL fires (needs SL to actually fire during market)
- Partial fill events (needs large qty on illiquid symbol)
- AMO acceptance path (needs market closed — after 15:30 IST)
- SL trigger being clamped by DPR (needs stock at circuit)
- Session invalidation shape when the WSS token expires mid-session
- Reconnection behavior — does WSS replay missed events?

Run `scripts/codify_learn/03_wss_full_capture.py` in each of those windows to fill
the gaps. Each run appends to `/tmp/wss_capture_<ts>/`.

## Existing code that DEPENDS on this state machine

Callers to double-check when adding a new state:

- `services/trade-execution/internal/statusservice/service.go` — main WSS event dispatcher
- `services/trade-execution/internal/manthan/wss_bridge.go` — Is* helpers (source of truth)
- `services/trade-execution/internal/manthan/entry_handler.go:429,490,500,512` — entry order lifecycle
- `services/trade-execution/internal/manthan/safety_monitor.go:151,174,265` — 15s fallback polling
- `services/trade-execution/internal/manthan/recovery.go:123,137` — startup recovery
- `services/trade-execution/cmd/manthan-e2e-test/main.go:302,308` — e2e test

## Known gaps in the current implementation

1. **SL-SELL EXECUTED events do not publish `SL_FILLED` in real time.** `recovery.go`
   registers WSS channels for active SLs but no goroutine listens on them. Fills
   only get processed via `safety_monitor.go`'s 15s polling. Symptom in
   production: `manthan_positions.realized_pnl = 0` on SL-triggered exits for the
   window between fill and next poll.

   Fix path: add a listener goroutine per SL (symmetric to entry_handler's fill wait)
   OR add a new callback in statusservice that fires `PublishSLFilled` when
   EXECUTED SELL arrives for a broker_order_id that matches an active manthan SL
   row. Needs new repo method `GetSLOrderByBrokerID` + verification that
   rules-engine projector is idempotent to double-publish from safety_monitor.

2. **Modify silent failure** (Bug 3): `broker_adapter.go:442` treats REST `infoID:"0"`
   as success. Real success = follow-up WSS PENDING with new OrderPrice. Symptom
   in production: trailing SL updates persist in our DB but never reach broker.
