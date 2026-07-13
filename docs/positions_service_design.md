# positions svc — Design Doc

**Status:** design, not yet implementing.
**Author of decision:** rohitt (2026-07-13) — chose gRPC lookup + backfill + full 8-chunk plan.
**Related:**
- [orderstatus_service_design.md](./orderstatus_service_design.md) — CQRS split target
- [rules_engine_refactor.md](./rules_engine_refactor.md) — the signal-engine-only end state
- Memory: [project_manthan_exit_order_gap.md] — safety-monitor writes exits w/o filling manthan_orders

## 1. Purpose

Two responsibilities, one binary:

1. **Fix `realized_pnl=0`** — the original bug that kicked off this whole refactor. Rules-engine's projector was deleted; nobody has computed realized PnL since. Positions svc closes that loop.

2. **Track Manthan positions AND user manual positions with CLEAR segregation** — new requirement raised 2026-07-13. Same broker account, two logical books, cleanly separated in our DB so:
   - Manthan cap-check math (25% sector, 50% MCap) counts only Manthan positions
   - PnL reports per strategy exclude user's manual trades
   - SL / TSL logic only applies to Manthan positions
   - Frontend shows "my strategy" separately from "my manual trades"

## 2. The segregation problem

### Broker's view: fungible

Broker (Codify) sees ONE holding per symbol per user. If user's Manthan bot bought 100 IDEA @ 14 and user later manually bought 50 more @ 15, broker shows: **150 IDEA @ weighted avg**. User can't tell the broker "sell my Manthan shares specifically."

### Our view: two logical books

We split the same broker holding into logical origins:

| origin | Owned by |
|---|---|
| `MANTHAN` | The strategy — automated entry via signal, automated SL/TSL exit |
| `USER_MANUAL` | The user — manual buy/sell via broker app or web |

We track each independently. Broker balance = SUM of all origins' `net_qty`.

### Where segregation gets tricky

Three ambiguous cases:

1. **Manual sell that exceeds manual holdings.** User has 50 MANUAL IDEA + 100 MANTHAN IDEA. Sells 80 via broker app. Where do the extra 30 come from?

2. **Manual buy on a symbol Manthan already holds.** User buys 20 IDEA manually while Manthan holds 100. Is that one 120-qty position or two separate rows?

3. **Broker-side liquidation** (margin call, corporate action). No signal_id, no user click. Which origin does it hit?

**Proposed rules** (see §7 for full state machine):

- **Buy fill with no signal_id → USER_MANUAL position** (separate row from any Manthan position on same symbol)
- **Sell fill with no signal_id → decrements USER_MANUAL first (FIFO)**. If sell_qty > user_manual_qty, remaining hits MANTHAN → mark MANTHAN as `MANUAL_EXIT` with pro-rated realized_pnl.
- **Sell fill with signal_id → touches only the linked MANTHAN position.**

## 3. Architecture

```
                    ┌──────────────────────────────────────┐
                    │  BROKER (Codify)                     │
                    └──────────────────────────────────────┘
                                    │
                                    ▼ WSS + REST
                    ┌──────────────────────────────────────┐
                    │  orderstatus svc                     │
                    │  → order.events Kafka topic          │
                    └──────────────────┬───────────────────┘
                                       │
                                       ▼
       ┌───────────────────────────────────────────────────────┐
       │  positions svc (NEW)                                  │
       │                                                       │
       │  Consumes: order.events                               │
       │  Calls:    trade-execution.LookupOrderMeta(gRPC)      │
       │            (returns signal_id + order_type + origin)  │
       │                                                       │
       │  Owns positions_db:                                   │
       │    positions          — one row per logical position  │
       │                         with origin (MANTHAN | USER_  │
       │                         MANUAL) discriminator         │
       │    position_events    — append-only audit log         │
       │                                                       │
       │  Emits: position.events Kafka topic                   │
       └──────────────────────────┬────────────────────────────┘
                                  │
              ┌───────────────────┴───────────────────┐
              │                                       │
              ▼                                       ▼
        rules-engine                          api-gateway
        (position.events →                    (position.events →
         cooldown INSERT for                   frontend push,
         MANTHAN exits only)                   filtered by origin)
```

## 4. DB schema

### 4.1 `positions` — logical position rows

```sql
CREATE TABLE positions (
    position_id       UUID PRIMARY KEY,

    origin            VARCHAR(16) NOT NULL,  -- 'MANTHAN' | 'USER_MANUAL'
    user_id           VARCHAR(64) NOT NULL,
    strategy_id       UUID,                   -- NULL for USER_MANUAL
    signal_id         UUID,                   -- NULL for USER_MANUAL, NOT NULL for MANTHAN
    symbol            VARCHAR(32) NOT NULL,
    exchange          VARCHAR(8)  NOT NULL,

    -- Lifecycle
    status            VARCHAR(16) NOT NULL DEFAULT 'ACTIVE',
                                              -- ACTIVE | EXITED
    entry_price       NUMERIC(14,4) NOT NULL,
    entry_time        TIMESTAMPTZ   NOT NULL,
    quantity          INTEGER       NOT NULL,  -- current net qty (may go down on partial exits)
    invested_amount   NUMERIC(14,2) NOT NULL,  -- entry_price × quantity (frozen at entry)

    -- Exit fields — populated when status flips to EXITED
    exit_price        NUMERIC(14,4),
    exit_time         TIMESTAMPTZ,
    exit_reason       VARCHAR(32),             -- SL_TRIGGER | MANUAL_EXIT | STRATEGY_EXIT | LIQUIDATION
    realized_pnl      NUMERIC(14,2),           -- (exit_price - entry_price) × quantity_exited

    -- Broker linkage (nullable — USER_MANUAL positions link to the manual BUY order)
    entry_broker_order_id VARCHAR(50),
    exit_broker_order_id  VARCHAR(50),

    -- Manthan trail state (unused for USER_MANUAL)
    current_sl        NUMERIC(14,4),
    high_since_entry  NUMERIC(14,4),
    last_trail_level  NUMERIC(14,4),

    -- Audit
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_positions_origin CHECK (origin IN ('MANTHAN','USER_MANUAL')),
    CONSTRAINT chk_positions_status CHECK (status IN ('ACTIVE','EXITED')),
    CONSTRAINT chk_positions_manthan_has_signal
        CHECK (origin != 'MANTHAN' OR signal_id IS NOT NULL),
    CONSTRAINT chk_positions_manual_no_signal
        CHECK (origin != 'USER_MANUAL' OR signal_id IS NULL)
);

CREATE INDEX idx_positions_user_symbol_active
    ON positions (user_id, symbol) WHERE status = 'ACTIVE';
CREATE INDEX idx_positions_manthan_signal
    ON positions (signal_id) WHERE origin = 'MANTHAN';
CREATE INDEX idx_positions_user_origin
    ON positions (user_id, origin, status);
```

### 4.2 `position_events` — append-only audit log

```sql
CREATE TABLE position_events (
    id                BIGSERIAL PRIMARY KEY,
    position_id       UUID NOT NULL REFERENCES positions(position_id),

    event_type        VARCHAR(32) NOT NULL,
        -- ENTRY_FILLED | SL_MODIFIED | SL_FILLED | MANUAL_SELL_APPLIED |
        -- USER_MANUAL_ENTRY | LIQUIDATION | RECONCILER_DRIFT_FIX

    broker_order_id   VARCHAR(50),
    signal_id         UUID,
    delta_qty         INTEGER,                 -- +N on entry/top-up, -N on exit
    fill_price        NUMERIC(14,4),
    realized_pnl_delta NUMERIC(14,2),          -- pro-rata for partial exits
    reason            TEXT,

    raw_source_event  JSONB NOT NULL,          -- full order.events envelope for forensics
    source_topic      VARCHAR(32) NOT NULL,    -- 'order.events' etc.
    source_offset     BIGINT,
    observed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Same idempotency pattern as broker_events: (source_event_id, position_id)
    source_event_id   VARCHAR(80) NOT NULL,
    CONSTRAINT uq_pe_source_event UNIQUE (position_id, source_event_id)
);

CREATE INDEX idx_pe_position_time
    ON position_events (position_id, observed_at DESC);
```

### 4.3 Why not link `signal_id` to `manthan_signal_decisions.signal_id` as FK?

Cross-DB FK isn't enforceable (rules-engine's `trading_db` vs positions svc's `positions_db`). We use `signal_id` as a soft-link — application code + JOINs at read time.

## 5. Kafka events

### 5.1 Topics consumed

| Topic | Producer | positions svc uses for |
|---|---|---|
| `order.events` | orderstatus svc | Every observed broker order event — FILLED / CANCELLED / REJECTED / etc. |

### 5.2 Topics produced

**New topic:** `position.events`, partition key = `position_id`

```json
{
  "event_id":        "<position_id>-<event_seq>",
  "event_type":      "POSITION_OPENED",       // or POSITION_EXITED / SL_TRAILED / MANUAL_INTERRUPT
  "produced_at_ms":  1783576973805,

  "position_id":     "<uuid>",
  "origin":          "MANTHAN",               // or USER_MANUAL
  "user_id":         "S4450",
  "strategy_id":     "<uuid>",                // null for USER_MANUAL
  "signal_id":       "<uuid>",                // null for USER_MANUAL
  "symbol":          "IDEA",

  "action":          "ENTRY",                 // or EXIT / TRAIL / TOP_UP
  "price":           14.09,
  "quantity":        100,

  "exit_reason":     "SL_TRIGGER",            // only on POSITION_EXITED
  "realized_pnl":    -60.00                   // only on POSITION_EXITED
}
```

### 5.3 Downstream consumers

| Consumer | Filter | What it does |
|---|---|---|
| rules-engine | `origin=MANTHAN` AND `event_type=POSITION_EXITED` AND `exit_reason=SL_TRIGGER` | INSERT into `manthan_cooldown` |
| api-gateway | `user_id` | Push to `/ws/live-orders` |
| notification svc (future) | `event_type=POSITION_EXITED` | Send user message |

## 6. gRPC dependency: `LookupOrderMeta`

New RPC exposed by trade-execution:

```proto
service TradeExecutionService {
  rpc LookupOrderMeta(LookupOrderMetaRequest) returns (LookupOrderMetaResponse);
}

message LookupOrderMetaRequest {
  string broker_order_id = 1;
}

message LookupOrderMetaResponse {
  bool    found            = 1;   // false if broker_order_id not in manthan_orders
  string  signal_id        = 2;   // populated iff this was a MANTHAN-placed order
  string  order_type       = 3;   // ENTRY | SL_SELL | EXIT | AMO
  string  strategy_id      = 4;
  string  user_id          = 5;
}
```

Backed by a single SQL query against `trading_execution.manthan_orders`:
```sql
SELECT signal_id, order_type, strategy_id, user_id
FROM manthan_orders
WHERE broker_order_id = $1;
```

**Cache:** positions svc holds a 24h TTL in-memory LRU (10k entries) so repeat lookups don't hammer trade-execution. Cache misses go via gRPC; NOT_FOUND responses are cached with a short TTL (60s) — could be racing an in-flight INSERT.

## 7. State machine — event handling per case

### 7.1 `order.events` with `event_type=FILLED` and `buy_sell="1"` (BUY fill)

```
Call trade-execution.LookupOrderMeta(broker_order_id)
    │
    ├── found=true, order_type=ENTRY (MANTHAN)
    │     → INSERT positions row: origin='MANTHAN', signal_id, entry_price, qty
    │     → INSERT position_events: ENTRY_FILLED
    │     → PUBLISH position.events: POSITION_OPENED (MANTHAN)
    │
    ├── found=true, order_type=SL_SELL — should be BUY, this is inconsistent
    │     → LOG warning, skip (broker returned inconsistent data)
    │
    └── found=false  → USER_MANUAL entry
          → INSERT positions row: origin='USER_MANUAL', signal_id=NULL, entry_price, qty
          → INSERT position_events: USER_MANUAL_ENTRY
          → PUBLISH position.events: POSITION_OPENED (USER_MANUAL)
```

### 7.2 `order.events` with `event_type=FILLED` and `buy_sell="2"` (SELL fill)

```
Call trade-execution.LookupOrderMeta(broker_order_id)
    │
    ├── found=true, order_type IN (SL_SELL, EXIT)
    │     → look up MANTHAN position by signal_id (parent link)
    │     → UPDATE position: status='EXITED', exit_price, exit_reason
    │     → realized_pnl = (exit_price - entry_price) × quantity_exited
    │     → INSERT position_events: SL_FILLED
    │     → PUBLISH position.events: POSITION_EXITED (SL_TRIGGER)
    │
    └── found=false — MANUAL SELL
          → For (user_id, symbol), get ACTIVE positions ordered: USER_MANUAL first, then MANTHAN oldest-first
          → Deduct filled_qty across them:
              consume USER_MANUAL positions first (mark EXITED as we go)
              spillover consumes MANTHAN positions (mark EXITED with reason=MANUAL_EXIT)
          → INSERT position_events: MANUAL_SELL_APPLIED, one row per touched position
          → PUBLISH position.events: POSITION_EXITED for each position touched
```

### 7.3 Other `event_type`s

| Event | Positions svc reaction |
|---|---|
| `PARTIALLY_FILLED` | Same as FILLED but keep position ACTIVE, decrement quantity by delta only |
| `CANCELLED` | Log, no position mutation (position never existed if this was pre-fill) |
| `REJECTED` | Log, no position mutation |
| `MODIFIED` | Broker's SL trigger changed. UPDATE positions.current_sl if it's a MANTHAN SL. |
| `STATUS_CHANGED` | Ignore for position lifecycle. |
| `TRIGGERED` | Ignore — the following FILLED event carries the actual state change. |

## 8. Recovery / boot

On startup, positions svc:
1. Reads `positions_db.positions WHERE status='ACTIVE'` into memory (for allocator cap-check queries, if we add a read API).
2. Kafka consumer group `positions-order-events-consumer` resumes from committed offset.
3. Any events published to `order.events` while positions svc was down get replayed in order.
4. Idempotency (position_events UNIQUE constraint) makes re-play safe.

## 9. Chunk plan — 8 chunks

Same pattern as orderstatus svc. Each independently shippable + rollback-able.

| # | Chunk | Where | Est |
|---|---|---|---|
| **P.A** | Skeleton — `services/positions/`, go.mod, migration 001, boot main.go, PM2 config | positions svc | 1 hr |
| **T.gRPC** | Trade-execution: expose `LookupOrderMeta` RPC (proto + handler + repo query) | trade-exec | 2 hrs |
| **P.B** | `order.events` Kafka consumer + `position_events` audit writer (no lifecycle yet) | positions svc | 3 hrs |
| **P.B.5** | gRPC client to trade-exec + in-memory LRU cache | positions svc | 2 hrs |
| **P.C** | State machine — BUY / SELL / MANUAL_SELL / partial. **`realized_pnl` fix ships here.** | positions svc | 4 hrs |
| **P.D** | Publish `position.events` topic on every state transition | positions svc | 1 hr |
| **P.E** | Rules-engine consumes `position.events` → INSERT `manthan_cooldown` on SL_TRIGGER exits only | rules-engine | 2 hrs |
| **P.F** | Backfill — one-time migration to copy `trading_db.manthan_positions` → `positions_db.positions` with `origin=MANTHAN` | positions svc migration | 2 hrs |

**Total: ~17 hrs. 3-4 focused sessions.**

## 10. What ships at end of each chunk

| After | State |
|---|---|
| **P.A** | Binary boots, own DB + Kafka consumer group registered. No processing. |
| **T.gRPC** | Trade-execution serves order-metadata lookups. Positions svc can't use it yet. |
| **P.B** | Every `order.events` message lands in `position_events` audit table (raw, no interpretation). |
| **P.B.5** | Every event enriched with signal_id / order_type / origin. |
| **P.C** | 🎯 **`realized_pnl` computed correctly. Original bug fixed.** MANTHAN + USER_MANUAL segregation working. |
| **P.D** | Frontend + downstream services can subscribe to `position.events`. |
| **P.E** | Cooldown lifecycle end-to-end for MANTHAN exits (excludes user manual exits — correct). |
| **P.F** | Historical positions migrated. Existing data queryable via positions_db. |

## 11. Open questions before we start

### Q1 — Manual SELL priority rule

Default in this doc: USER_MANUAL positions get sold FIRST (FIFO), spillover hits MANTHAN.

**Alternative:** MANTHAN gets sold first (protects user's manual holdings).
**Alternative:** Prompt user via app when ambiguous.

Recommend keeping default. User-friendly for accounting: your manual trades sell out first, your algo positions stay intact unless you sell "into" them.

### Q2 — Same broker holding, split origins — how do we survive a broker restart / re-auth?

If broker resets holdings view on relogin and we get a `GetHoldings` response showing only NET qty, we can't tell which origin is which. Our DB is source of truth for the split.

**Reconciler behavior:** if DB positions sum ≠ broker holdings, log alert but DON'T auto-adjust. Human intervention needed. (Same as safety_monitor's "don't liquidate on drift" lesson.)

### Q3 — Top-up: same user BUYs more IDEA via Manthan (a second signal for same symbol)

Two options:
- **Merge:** UPDATE existing MANTHAN position, weighted-avg entry_price, add qty
- **New row:** INSERT a second MANTHAN position with new signal_id

Existing behavior in the deleted projector was **merge** (per parent_signal_id link). Preserving that keeps rules-engine's cap-check math consistent.

Recommend: **merge**. Requires `parent_signal_id` support in `order.events`.

### Q4 — Reject rules-engine's UPDATE of `manthan_positions` — anywhere still doing it?

Grepped: after e467fd2, rules-engine's `publisher.go` no longer writes `manthan_positions`. Only `rehydrate.go` still SELECTs from it (read-only). So the CQRS boundary is intact — no cross-service write to worry about.

### Q5 — When does positions svc become the AUTHORITATIVE writer for `manthan_positions`?

Suggested cutover:
1. Deploy positions svc reading `order.events`
2. Verify positions_db.positions matches trading_db.manthan_positions for 1 trading day
3. Flip rules-engine's `PortfolioManager.RehydrateActivePositions` to read from positions_db instead
4. Retire trading_db.manthan_positions (or keep as archive)

## 12. Sign-off checklist before P.A starts

- [ ] Reviewed §2 segregation problem — agree on origin=MANTHAN / USER_MANUAL split.
- [ ] Confirmed §7.2 MANUAL_SELL rule: USER_MANUAL first, spillover to MANTHAN.
- [ ] Confirmed §11 Q3: top-up merges into existing MANTHAN row.
- [ ] Confirmed backfill (§P.F) copies historic manthan_positions with origin=MANTHAN.
- [ ] `LookupOrderMeta` gRPC signature (§6) — request/response shape OK.
- [ ] Deploy target: same host as orderstatus svc.
- [ ] No design doc revisions needed before starting P.A.

## 13. What this UNBLOCKS

- **`realized_pnl` bug fix** — end of P.C.
- **Clean per-strategy PnL reporting** — MANTHAN vs USER_MANUAL split queryable.
- **Frontend "my strategy" tab** — filtered on `origin=MANTHAN`.
- **Notification svc** later — can subscribe to `position.events` for user messages ("Your IDEA position exited at ₹12.80, loss ₹60").
- **Multi-strategy** — MANTHAN, HFT, future strategies each get their own strategy_id but share origin='STRATEGY' (or split origin further).
