# rules-engine Refactor — Signal-Engine-Only Target

**Status:** design, not yet implementing.
**Author of decision:** rohitt (2026-07-10) — declared rules-engine mixing signals + positions + PnL is wrong architecture; wants rules-engine reduced to pure signal generation.
**Related:** [orderstatus_service_design.md](./orderstatus_service_design.md) — the CQRS work this refactor enables downstream.

## 1. Goal

**Reduce `services/rules-engine` to a single responsibility: signal generation.**

After this refactor, rules-engine answers ONE question: *"Given eligible stocks + current portfolio caps, should we trade IDEA?"* It emits trade signals to Kafka and stops caring what happens next.

Position management (fills, trailing SL, PnL, portfolio state) moves out — first into a clearly-bounded sub-package inside rules-engine, then in a future refactor into a separate `positions` service.

## 2. Non-goals for this refactor

- **Not creating the `positions` service in this refactor.** That's a follow-up. This refactor only sets the boundary INSIDE rules-engine so the extraction is trivial later.
- **Not changing Kafka topics.** Topics stay the same during and after.
- **Not changing DB schema.** `manthan_positions`, `manthan_orders` etc. keep their columns.
- **Not deleting the CQRS design.** [orderstatus_service_design.md](./orderstatus_service_design.md) still stands; this refactor is complementary — it cleans up rules-engine while the CQRS work handles trade-execution's WSS ownership.
- **Not changing behavior.** Every step ships as a pure refactor. `go build ./...` clean + `go test ./...` clean = ship criterion.

## 3. Current state — what's mixed

rules-engine has 5 responsibilities crammed into `internal/manthan/`:

| Responsibility | Files | Belongs in |
|---|---|---|
| **Signal generation** — react to 52W breakouts, allocate capital, publish entry orders | `consumer.go`, `allocator.go`, `order.go`, part of `publisher.go` | rules-engine (stays) |
| **Portfolio-cap rules** — 25% sector cap, 50% mcap bucket cap, FCFS tie-break | `allocator.go` (rules), `portfolio.go` (state) | Split: rules in signals, state in positions |
| **Position projection** — turn fill events into `manthan_positions` rows + realized_pnl | `projector/`, `fill_consumer.go` | Positions |
| **Trailing SL** — react to LTP ticks, compute new trigger, publish SL modify | `tick_handler.go`, `trailing_sl.go`, `ltp_feed.go` | Positions |
| **Startup / rehydrate** — rebuild in-memory portfolio from DB on boot | `rehydrate.go`, part of `wire.go` | Split by owner |
| **Notification publisher** — user-facing messages | `notification_publisher.go` | Undecided — probably positions |

**Two files hold everything together and must be split before anything else moves:**

- `publisher.go` (591 lines) — has SIGNAL publish methods (`PublishEntryOrder`, `PublishSLModify`, `PublishSLExit` → Kafka `trade-signals` topic) AND POSITION DB writes (`UpdatePositionFill`, `UpdatePositionSL`, `UpdatePortfolioState`, direct SQL on `manthan_positions`).
- `models.go` (189 lines) — has SIGNAL types (`ManthanSignal`, `UserStrategy`) AND POSITION types (`Position`, `Portfolio`, `PositionState`, `CooldownEntry`, `CapCheck`) AND SHARED type (`AllocationResult`).

## 4. Target state

```
services/rules-engine/internal/
├── configstore/            ← unchanged (infra)
├── configsync/             ← unchanged (infra)
├── startup/                ← unchanged (infra)
├── kafka/                  ← unchanged (infra)
├── cache/                  ← unchanged (infra)
├── models/                 ← unchanged (strategy config types)
└── manthan/
    ├── types/              ← NEW — shared type definitions, no logic
    │   ├── signal.go
    │   ├── position.go
    │   └── allocation.go
    ├── signals/            ← NEW — pure signal engine
    │   ├── consumer.go
    │   ├── allocator.go
    │   ├── order.go
    │   ├── publisher.go    ← signals-only methods
    │   └── doc.go
    ├── positions/          ← NEW — position management
    │   ├── projector/
    │   ├── fill_consumer.go
    │   ├── tick_handler.go
    │   ├── trailing_sl.go
    │   ├── ltp_feed.go
    │   ├── portfolio.go
    │   ├── rehydrate.go
    │   ├── publisher.go    ← position-only methods
    │   └── doc.go
    ├── notifications/      ← NEW — user-facing messages (moved from notification_publisher.go)
    │   └── publisher.go
    └── wire.go             ← updated to construct signals + positions + notifications
```

**Final state after this refactor + future positions service extraction:**

```
services/rules-engine/internal/
├── configstore/, configsync/, startup/, kafka/, cache/, models/
└── signals/                ← renamed from manthan/signals/
    ├── consumer.go
    ├── allocator.go
    ├── order.go
    ├── publisher.go
    └── positions_client.go ← gRPC or Kafka client to future positions svc
```

positions/ code lifts out to `services/positions/` — that's a separate future refactor.

## 4.5 What rules-engine WRITES after the refactor — ratified 2026-07-10

Approved by rohitt after PM+senior-dev review. This is the final target the
5-step extraction below is aiming at.

### Kafka publishes (2 topics)

| Topic | Payload | Consumer(s) |
|---|---|---|
| `trade-signals` | Trade action envelope (entry / SL modify / exit) | trade-execution |
| `portfolio.allocations` | Portfolio allocation state change | frontend, monitoring |

### Postgres writes (2 tables, `trading_db`)

**Table 1: `manthan_signal_decisions`** — durable signal audit log.
Every signal rules-engine fires gets ONE INSERT. One follow-up UPDATE
sets `published_at` after Kafka ACK (transactional-outbox pattern —
crash-safe publishing).

```sql
CREATE TABLE manthan_signal_decisions (
  signal_id         UUID PRIMARY KEY,
  parent_signal_id  UUID REFERENCES manthan_signal_decisions(signal_id) NULL,
  signal_type       VARCHAR(32) NOT NULL,  -- ENTRY_BUY, SL_MODIFY, EXIT_TSL, EXIT_MANUAL, SL_CANCEL
  strategy_id       VARCHAR(64) NOT NULL,
  user_id           VARCHAR(64) NOT NULL,
  symbol            VARCHAR(32) NOT NULL,
  payload           JSONB NOT NULL,        -- type-specific fields
  fired_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  published_at      TIMESTAMPTZ NULL       -- NULL = INSERT-ed but Kafka publish pending
);

CREATE INDEX idx_msd_type_user_symbol ON manthan_signal_decisions(signal_type, user_id, symbol);
CREATE INDEX idx_msd_unpublished ON manthan_signal_decisions(fired_at) WHERE published_at IS NULL;
```

Write pattern (transactional outbox — industry standard):
```
BEGIN;
  INSERT INTO manthan_signal_decisions (…, published_at=NULL);
COMMIT;

publish to Kafka trade-signals
  → on ACK:
UPDATE manthan_signal_decisions SET published_at=NOW() WHERE signal_id=?
```

**On startup**, republish any row where `published_at IS NULL` — recovers
from crashes between INSERT and Kafka ACK. Zero signal loss, zero
duplicate re-publishes (Kafka key = signal_id gives idempotency at consumer).

**Table 2: `manthan_cooldown`** — re-entry cooldown state (Manthan spec:
after SL exit, don't re-enter until price drops below `ATH × 0.80`).

```sql
CREATE TABLE manthan_cooldown (
  user_id           VARCHAR(64) NOT NULL,
  strategy_id       VARCHAR(64) NOT NULL,
  symbol            VARCHAR(32) NOT NULL,
  ath_at_exit       NUMERIC(14,4) NOT NULL,
  exit_price        NUMERIC(14,4) NOT NULL,
  exit_time         TIMESTAMPTZ NOT NULL,
  reentry_below     NUMERIC(14,4) NOT NULL,  -- ATH × 0.80
  PRIMARY KEY (user_id, strategy_id, symbol)
);
```

Write pattern: INSERT on EXIT_TSL / EXIT_MANUAL signal. DELETE (or expire)
when re-entry conditions are met. Allocator reads this on every entry
decision.

### Signal types rules-engine fires (enum for `signal_type` column)

| `signal_type` | When fired | Payload fields |
|---|---|---|
| `ENTRY_BUY` | Allocator picks stock → entry order | qty, entry_price, initial_sl_trigger, initial_sl_limit |
| `SL_MODIFY` | Trailing SL ratchet-up (LTP moved 2%+) | new_trigger, new_limit, old_trigger, ltp_when_fired |
| `EXIT_TSL` | LTP crossed trailing SL — rules-engine emits sell | ltp_at_fire, sl_at_fire |
| `EXIT_MANUAL` | User-triggered exit | reason |
| `SL_CANCEL` | Cancel a stuck / misplaced SL | reason |

Extending the enum later is one migration + one Go const.

### What rules-engine will NOT write (moved out)

| Was writing | New owner |
|---|---|
| `manthan_positions` (16 writes) | positions svc |
| `manthan_position_events` (1 write) | positions svc |
| `manthan_portfolio_state` (1 write) | portfolio svc |
| `manthan_signal_decisions` outcome UPDATEs (10 writes) | positions svc updates via its own table; JOIN at read time. **rules-engine never UPDATEs outcome columns.** |
| Kafka `manthan.notifications` | notification svc (subscribes to positions svc + rules-engine events; owns user messaging) |

### Reads (input side)

| Source | What |
|---|---|
| Postgres `MANTHAN_SIGNALS_DB.manthan_signals` (read-only) | Eligible 52W breakouts |
| Kafka `manthan.signals` | Real-time breakout events |
| Kafka `user-config-events` | User strategy configs |
| Kafka `position.updates` (from positions svc) | Live portfolio snapshot for allocator cap checks + trail SL LTP context |

### Numbers — before vs after

| Metric | Today | After |
|---|---|---|
| Tables written to | 4 | 2 |
| SQL statements per signal cycle | ~30 | 2 (INSERT + UPDATE published_at) |
| Cross-service writes | Yes (writes tables that belong to positions/portfolio concerns) | No |
| Recovery-safe publishing | No | Yes (outbox pattern) |
| Signal audit horizon | Kafka only (30 days) | Postgres forever |
| Kafka topics published | 3 (trade-signals, portfolio.allocations, manthan.notifications) | 2 (drops notifications) |

### Read-time JOIN for "was signal profitable?"

Signal outcomes live in positions svc — no cross-service writes needed.
Reader combines via JOIN:

```sql
SELECT d.signal_id, d.symbol, d.fired_at,
       p.state, p.realized_pnl, p.exit_price
FROM manthan_signal_decisions d
LEFT JOIN manthan_positions p ON p.signal_id = d.signal_id
WHERE d.user_id = 'S4450'
  AND d.signal_type = 'ENTRY_BUY';
```

Each service owns its columns. Zero write-side coupling.

---

## 5. Extraction plan — 5 steps

Each step is:
- **Independently shippable** — commit + build clean + tests pass
- **Independently rollback-able** — revert the commit, nothing else breaks
- **Pure refactor** — no behavior change, no API change, no DB schema change

### Step 1 — Extract shared `types/` package (~1-2 hours)

**Why first:** unblocks everything else. Without a shared types package, splitting `publisher.go` and moving files into `signals/`/`positions/` sub-packages creates circular imports.

**Files created:**
- `internal/manthan/types/signal.go` — `ManthanSignal`, `UserStrategy`
- `internal/manthan/types/position.go` — `Position`, `Portfolio`, `PositionState`, `CooldownEntry`
- `internal/manthan/types/allocation.go` — `AllocationResult`, `CapCheck`

**Files modified:**
- `internal/manthan/models.go` → emptied or deleted (types moved out)
- `internal/manthan/consumer.go` — imports types
- `internal/manthan/allocator.go` — imports types
- `internal/manthan/order.go` — imports types
- `internal/manthan/portfolio.go` — imports types
- `internal/manthan/fill_consumer.go` — imports types
- `internal/manthan/tick_handler.go` — imports types
- `internal/manthan/trailing_sl.go` — imports types
- `internal/manthan/rehydrate.go` — imports types
- `internal/manthan/publisher.go` — imports types
- `internal/manthan/wire.go` — imports types
- `internal/manthan/notification_publisher.go` — imports types (if needed)
- `internal/manthan/projector/` — projector already sub-package; imports types
- Any file OUTSIDE `internal/manthan/` referencing these types

**Estimated blast radius:** ~15 files.

**Verification:**
```bash
cd services/rules-engine
go build ./...
go test ./...
```

**Rollback:** `git revert HEAD`. Types come back to `models.go`, imports revert.

**Concern:** Some type constructors may live in `models.go` (e.g. `NewPortfolio()`). Constructors move with types.

### Step 2 — Split `publisher.go` (~2-3 hours)

**Why second:** everything else needs a clean split before it can move into sub-packages.

**Current single struct:**
```go
type ManthanPublisher struct {
    // Kafka producers, DB pool, Redis client
}
// Signal methods:
func (p *ManthanPublisher) PublishEntryOrder(...) error
func (p *ManthanPublisher) PublishSLModify(...) error
func (p *ManthanPublisher) PublishSLExit(...) error
// Position methods:
func (p *ManthanPublisher) UpdatePositionFill(...)
func (p *ManthanPublisher) UpdatePositionSL(...)
func (p *ManthanPublisher) UpdatePortfolioState(...)
func (p *ManthanPublisher) dbInsertPosition(...) error
func (p *ManthanPublisher) dbInsertDecision(...) error
```

**Split into two structs, still in same file for now (or two files inside `internal/manthan/`):**
```go
// publisher_signals.go
type SignalPublisher struct { kafka, db }
func (p *SignalPublisher) PublishEntryOrder(...)
func (p *SignalPublisher) PublishSLModify(...)
func (p *SignalPublisher) PublishSLExit(...)

// publisher_positions.go
type PositionPublisher struct { db, redis }
func (p *PositionPublisher) UpdatePositionFill(...)
func (p *PositionPublisher) UpdatePositionSL(...)
func (p *PositionPublisher) UpdatePortfolioState(...)
```

**Files created:**
- `internal/manthan/publisher_signals.go`
- `internal/manthan/publisher_positions.go`

**Files deleted:**
- `internal/manthan/publisher.go` (contents split into the two above)

**Files modified:**
- `wire.go` — constructs both publishers instead of one
- `consumer.go`, `allocator.go`, `order.go` — use `SignalPublisher`
- `fill_consumer.go`, `tick_handler.go`, `portfolio.go`, `rehydrate.go` — use `PositionPublisher`

**Verification:** same as Step 1 — build + tests.

**Rollback:** revert commit. Two publishers merge back into one.

**Concern:** helper methods like `dbInsertDecision` may be called by BOTH sides. Trace call sites; put helper in whichever side is the primary caller and expose via method.

### Step 3 — Move position files into `positions/` sub-package (~2-3 hours)

Now that types + publishers are split, position files can move without circular imports.

**Files moved (with `git mv`, preserving history):**
- `internal/manthan/projector/*` → `internal/manthan/positions/projector/*`
- `internal/manthan/tick_handler.go` → `internal/manthan/positions/tick_handler.go`
- `internal/manthan/trailing_sl.go` → `internal/manthan/positions/trailing_sl.go`
- `internal/manthan/ltp_feed.go` → `internal/manthan/positions/ltp_feed.go`
- `internal/manthan/fill_consumer.go` → `internal/manthan/positions/fill_consumer.go`
- `internal/manthan/portfolio.go` → `internal/manthan/positions/portfolio.go`
- `internal/manthan/rehydrate.go` → `internal/manthan/positions/rehydrate.go`
- `internal/manthan/publisher_positions.go` → `internal/manthan/positions/publisher.go`

**Files created:**
- `internal/manthan/positions/doc.go` — package docstring

**Files modified:**
- Each moved file: `package manthan` → `package positions`
- Every file that references these types: update imports to `.../manthan/positions`
- `wire.go` — constructs positions package types with `positions.NewTickHandler(...)` etc.

**Verification:** same as before.

**Rollback:** `git mv` back to `internal/manthan/`, revert package names.

**Concern:** `portfolio.go` has `PortfolioManager` that both signal-side (allocator) and position-side (tick_handler) call. In the future positions svc split, allocator will call it via gRPC. For now, `signals/allocator.go` imports `positions.PortfolioManager` directly — a mild coupling but allowed.

### Step 4 — Move signal files into `signals/` sub-package (~1-2 hours)

Symmetric to Step 3.

**Files moved:**
- `internal/manthan/consumer.go` → `internal/manthan/signals/consumer.go`
- `internal/manthan/allocator.go` → `internal/manthan/signals/allocator.go`
- `internal/manthan/order.go` → `internal/manthan/signals/order.go`
- `internal/manthan/publisher_signals.go` → `internal/manthan/signals/publisher.go`

**Files created:**
- `internal/manthan/signals/doc.go`

**Files modified:**
- Each moved file: `package manthan` → `package signals`
- `wire.go` — constructs signals package types with `signals.NewConsumer(...)` etc.

**Verification:** same.

**Rollback:** same.

**Concern:** `allocator.go` imports `positions.PortfolioManager`. Explicitly document this dependency as "to be replaced by positions service client" in a comment. This is where the future positions-svc extraction unlocks.

### Step 5 — Move notification_publisher + finalize (~1 hour)

**Files moved:**
- `internal/manthan/notification_publisher.go` → `internal/manthan/notifications/publisher.go`

**Files modified:**
- Rename type: `NotificationPublisher` (unchanged) — just its package changes
- Callers in `positions/` package update import

**Files remaining at `internal/manthan/` top level:**
- `wire.go` only — orchestrates signals + positions + notifications
- `doc.go` — top-level Manthan package docstring

**Verification:** same.

**Rollback:** revert commit.

## 6. Order + dependencies

```
Step 1 (types)
  ↓  unblocks
Step 2 (split publisher)
  ↓  unblocks
Step 3 (move position files) ────┐
                                  ├─→ Step 5 (move notifications, finalize)
Step 4 (move signal files) ──────┘
```

**Steps 3 and 4 can happen in either order** or in parallel (different commits, different files touched).

**Step 5 depends on Steps 3 and 4 being done** (needs the sub-packages to exist so notifications sits alongside them).

**Cannot skip steps.** Step 3 without Step 2 = circular imports. Step 2 without Step 1 = repeats type-splitting work.

## 7. Risks + mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Circular imports after Step 1 | Build breaks | Extract types FIRST (that's Step 1's whole job) |
| Missed reference in another package | Build breaks in unrelated service | `grep -r "manthan.Portfolio\|manthan.ManthanSignal" services/` before + after each step |
| Tests referencing internal types | Tests fail | Update test imports in the same commit |
| Someone else pushes to `design/orderstatus-cqrs` mid-refactor | Merge conflicts | Branch off + rebase before final push |
| Runtime behavior change from a "pure refactor" | Undetected regression in prod | Deploy to local Docker first; run integration test suite; canary on staging (1 day) before prod |
| `git mv` history lost | Blame trail confusing | Use `git mv` explicitly; verify with `git log --follow <new-path>` |

## 8. Timeline estimate

| Step | Work time | Cumulative |
|---|---|---|
| Step 1 — types | 1-2 hours | 2h |
| Step 2 — split publisher | 2-3 hours | 5h |
| Step 3 — move position files | 2-3 hours | 8h |
| Step 4 — move signal files | 1-2 hours | 10h |
| Step 5 — notifications + finalize | 1 hour | 11h |
| Buffer for build fixes, test updates, review | 3-4 hours | 15h |

**Total: ~2 focused work days, spread over 3-5 calendar days for review + staging soak.**

## 9. Open questions

1. **Notification publisher home.** Does user-facing notification logic belong with positions (because it fires on position events) or as its own package? Design says `notifications/` sub-package for now, revisit if needed.

2. **`PortfolioManager` dependency from allocator.** After Step 4, `signals/allocator.go` still imports `positions.PortfolioManager` in-process. This is the last cross-cutting dependency. Acceptable? Or do we need to invert this before Step 4 ships?

3. **Test coverage during refactor.** Do we have integration tests that catch runtime regressions? If not, do we add smoke tests BEFORE Step 1 as a safety net?

4. **Deploy cadence.** Ship all 5 steps as one PR, or 5 separate PRs? Recommendation: 5 separate PRs merged one-per-day, so any regression is easily bisected.

5. **Position service extraction timing.** This refactor sets up the boundary. When do we actually lift `positions/` out into `services/positions/`? Not in this refactor, but should be the NEXT thing after Step 5.

## 10. What we ship at the end of each step

| After step | rules-engine behavior | Code organization |
|---|---|---|
| Step 1 | Unchanged | Shared types in `types/`, everything else in `manthan/` |
| Step 2 | Unchanged | Two publisher structs at `manthan/` level |
| Step 3 | Unchanged | Position code in `manthan/positions/` sub-package |
| Step 4 | Unchanged | Signal code in `manthan/signals/` sub-package |
| Step 5 | Unchanged | Notifications in `manthan/notifications/`. rules-engine is now ONLY wire.go + doc.go at top level. |

**After Step 5, extraction to positions service is a `git mv` operation — no logic changes.**

## 11. What this refactor UNBLOCKS

- **realized_pnl=0 bug fix in the cleanest way.** Once positions/ is boxed, all fill events flow through one place. Adding the WSS-path publisher wires cleanly.
- **CQRS Phase 3** (extract orderstatus binary) from [orderstatus_service_design.md](./orderstatus_service_design.md). rules-engine's clean boundary is a prereq for other services being extracted around it.
- **Multi-strategy support.** New strategies (not Manthan) can implement the same signal-engine boundary and slot in as siblings to `signals/`.
- **Testability.** Signal generation can be unit-tested against a fake `PortfolioSnapshot` instead of the real DB-backed `PortfolioManager`.

## 12. Sign-off checklist before we start Step 1

- [ ] Reviewed §7 risks — comfortable with mitigation approach.
- [ ] Answered §9 Q1: notifications sub-package location.
- [ ] Answered §9 Q2: PortfolioManager cross-cutting dependency acceptable during Step 4.
- [ ] Answered §9 Q3: existing test coverage adequate, or add smoke tests first.
- [ ] Answered §9 Q4: single PR or 5 PRs.
- [ ] Confirmed branch strategy: `refactor/rules-engine-signals-only` off `design/orderstatus-cqrs`.
