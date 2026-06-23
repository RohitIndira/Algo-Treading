# rules-engine: split + bounded-context design

> Status: **Phase 0 in progress** — see "Part 4: Migration path" for current state.
> Author: dev team
> Last updated: 2026-06-23
> Related: [communication-patterns.md](communication-patterns.md), [data-ownership.md](data-ownership.md)

## Why this document exists

`services/rules-engine` is a **10,700-LOC service that owns 5 tables, reads
from 3 separate DBs, consumes 4 Kafka topics, produces 2 topics, and runs
~7 concurrent subsystems in one process.** A junior dev opening main.go
(621 lines of wiring) cannot map "what does this service actually do" in
one sitting. That's the signal the service has outgrown its boundary.

This doc:
1. Names every responsibility rules-engine carries today
2. Identifies which ones genuinely belong together vs. drifted in
3. Proposes a 3-service split with clear inputs / outputs / state
4. Lays out a safe, reversible migration path
5. Is honest about the smells, the risks, and the trade-offs

**Goal**: by the end of this migration, a new joiner can sketch any of
the 3 services on a whiteboard from memory.

---

## Part 1 — Current state (what rules-engine is today)

### The seven responsibilities living in one process

```
┌──────────────────────────────────────────────────────────────────┐
│  rules-engine (today)                                            │
│  ──────────────────────────────────────────────────────────────  │
│                                                                  │
│  R1. Signal matching                                             │
│      Consume manthan.signals + news-events → match against       │
│      active strategies → publish trade-signals.                  │
│      Stateless. Hot path during market hours.                    │
│                                                                  │
│  R2. Capital allocation (Manthan-specific)                       │
│      For each matched signal: compute per-stock qty using sector │
│      / market-cap rules + portfolio caps. Stateful (reads        │
│      portfolio_state).                                           │
│                                                                  │
│  R3. Portfolio tracking                                          │
│      Owns manthan_positions, manthan_position_events,            │
│      manthan_portfolio_state, manthan_signal_decisions.          │
│      Consumes manthan.execution.events → updates state.          │
│      Stateful, durable.                                          │
│                                                                  │
│  R4. Orphan cleanup                                              │
│      Background scanner every 5 min: walks ACTIVE positions,     │
│      verifies owning strategy still exists, EXITs orphans.       │
│      Reads strategies table directly (anti-pattern).             │
│                                                                  │
│  R5. Trailing stop-loss                                          │
│      Consumes WS tick feed → maintains per-position trail state  │
│      → publishes SL_MODIFY when trail advances.                  │
│      Hot real-time path, microsecond-sensitive.                  │
│                                                                  │
│  R6. Risk pre-check client                                       │
│      gRPC client to risk-management service. Wraps                │
│      CheckPreTradeRisk for use inside R1's pipeline.             │
│                                                                  │
│  R7. ConfigStore sync                                            │
│      In-memory cache of active strategies. Populated at boot     │
│      via user-config gRPC, updated via user-config-events Kafka. │
│      Read by R1, R2, R4.                                         │
└──────────────────────────────────────────────────────────────────┘
```

### Data inputs (Kafka consumers)

| Topic | Producer | What rules-engine does with it |
|-------|----------|--------------------------------|
| `manthan.signals` | data-ingestion | R1: match against strategies |
| `news-events` | data-ingestion / news | R1: match for NEWS strategies |
| `user-config-events` | user-config | R7: update ConfigStore |
| `manthan.execution.events` | trade-execution | R3: update positions |

### Data outputs (Kafka producers)

| Topic | Consumers | Purpose |
|-------|-----------|---------|
| `trade-signals` | trade-execution | R1/R2: order intents |
| `manthan.notifications` | api-gateway (WSS push) | R3: portfolio change events |

### Tables (5 owned, 2 cross-service reads)

| Table | DB | Role |
|-------|-----|------|
| `trade_signals` | trading_db | OWN (write only) |
| `manthan_positions` | trading_db | OWN (write only) |
| `manthan_position_events` | trading_db | OWN (write only) |
| `manthan_portfolio_state` | trading_db | OWN (write only) |
| `manthan_signal_decisions` | trading_db | OWN (write only) |
| `strategies` | trading_db | **cross-service READ** (user-config owns) |
| `manthan_signals` | market_data | **cross-service READ** (data-ingestion owns) |

### Other services that REACH IN

This is the smell — when foreign services need data from rules-engine,
they can't ask (no gRPC server), so they query the DB:

| Foreign service | What it reaches into rules-engine for |
|-----------------|---------------------------------------|
| rebalancer | reads `manthan_positions`, `manthan_signal_decisions` |
| api-gateway | reads `manthan_positions` for `/holdings` endpoint |
| trade-execution (proposed) | needs portfolio state for risk decisions |

---

## Part 2 — Why this needs to split

### Smell A: The hot path and the stateful aggregate share a heartbeat

R1 (signal matching) is **stateless and fast**. R3 (portfolio tracking) is
**stateful and slow**. When R3's DB query lags (e.g., during EOD position
event replay), R1 takes the same Go runtime hit because they share the
event loop. In production trading, a 50ms stall on signal matching during
9:15 IST is real money lost.

### Smell B: R5 (trailing SL) has totally different latency profile

Trailing SL is **microsecond-sensitive** (ticks arrive 100s/sec per stock).
Everything else in rules-engine runs at 100ms+ scale. Mixing them means:
- A GC pause from R3's portfolio update blocks R5's tick processing
- Backpressure on R3 starves R5
- You can't independently scale R5 (it's tied to R1, R2, R3, R4, R7)

### Smell C: Foreign-DB reads ARE because there's no gRPC server

R3 owns valuable state (positions, portfolio). Other services want it.
With no gRPC server, the cheapest way to get it is `SELECT FROM
manthan_positions` directly. So the violation isn't laziness — it's a
**missing API surface**. Adding the surface dissolves the violation.

### Smell D: One service-restart = all hot paths down

Today, deploying any change to R1 means restarting the whole 10k-LOC
process. R5's tick state, R3's in-memory portfolio cache, R7's ConfigStore
all rebuild. During market hours, this is 10-30 seconds of fully blind
trading. With a split, R1 deploys hit only R1.

### Smell E: Tests are stuck at 14%

Splitting will FORCE clearer interfaces. You can't write a unit test for
R2 today without standing up Postgres + Kafka + user-config gRPC.
After the split, R2 takes an interface and you mock it.

---

## Part 3 — Target architecture (the 3-service split)

```
┌─────────────────────────────────────────────────────────────────┐
│  rules-engine (slim) — stateless matching                       │
│  ─────────────────────────────────────────────────────────────  │
│  Owns:        nothing (pure consumer + transformer)             │
│  Consumes:    manthan.signals, news-events                      │
│  Produces:    trade-signals  (order intents to trade-execution) │
│  Calls (gRPC): user-config.GetAllActiveStrategies (bootstrap)   │
│               manthan-portfolio.GetCapitalAllocation (per sig)  │
│               risk-management.CheckPreTradeRisk                 │
│  gRPC server: HealthCheck, GetMatchingStats (read-only)         │
│  Size target: ~2,500 LOC                                        │
│  Tests target: > 70% coverage                                   │
│  Restart cost: zero state to rebuild                            │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│  manthan-portfolio (NEW) — stateful aggregate                   │
│  ─────────────────────────────────────────────────────────────  │
│  Owns:        manthan_positions                                 │
│               manthan_position_events                           │
│               manthan_portfolio_state                           │
│               manthan_signal_decisions                          │
│               manthan_cooldown        ← currently in trading_db  │
│  Consumes:    manthan.execution.events (order.placed,           │
│                                          order.filled)          │
│  Produces:    portfolio.updated  (consumed by api-gateway WSS,  │
│                                    rebalancer)                  │
│  Background:  daily orphan reconciler (NOT every 5 min)         │
│               (drops to daily because event-driven primary now  │
│                exists — see "STRATEGY_DEACTIVATED event")       │
│  gRPC server:                                                   │
│    GetCapitalAllocation(user, strategy, symbol) → qty           │
│      ↑ called by rules-engine on every matched signal           │
│    GetPortfolioSnapshot(user) → all positions + P&L             │
│      ↑ called by api-gateway for mobile /holdings               │
│    GetPositionsByStrategy(strategy_id) → positions[]            │
│      ↑ called by manthan-tick-engine for SL state               │
│  Size target: ~3,500 LOC                                        │
│  Tests target: > 70%                                            │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│  manthan-tick-engine (NEW) — real-time hot path                 │
│  ─────────────────────────────────────────────────────────────  │
│  Owns:        in-memory trail state (durable snapshot           │
│               periodically to manthan_trail_state OR Redis)     │
│  Consumes:    WS tick feed (from data-ingestion or directly     │
│               from Codifi WS)                                   │
│               manthan-portfolio.PortfolioUpdated events         │
│                 (so it knows which positions to track)          │
│  Produces:    trade-signals (SL_MODIFY events)                  │
│  Calls (gRPC):manthan-portfolio.GetPositionsByStrategy at boot  │
│  gRPC server: GetTrailState(symbol, strategy) — for debug       │
│  Size target: ~2,000 LOC                                        │
│  Tests target: > 80% (most sensitive code)                      │
│  Restart cost: rebuild trail state from snapshot (~1 sec)       │
└─────────────────────────────────────────────────────────────────┘
```

### What each service definitively does NOT do

| Service | Doesn't do |
|---------|------------|
| rules-engine | Stores nothing. Touches no DB directly. Pure transform. |
| manthan-portfolio | Doesn't match signals. Doesn't process ticks. |
| manthan-tick-engine | Doesn't track P&L. Doesn't allocate capital. |

### gRPC RPC contracts to define (new proto)

```protobuf
// services/manthan-portfolio/proto/manthan_portfolio.proto (new file)
service ManthanPortfolioService {
  rpc GetCapitalAllocation(GetCapitalAllocationRequest)
      returns (GetCapitalAllocationResponse);
  rpc GetPortfolioSnapshot(GetPortfolioSnapshotRequest)
      returns (GetPortfolioSnapshotResponse);
  rpc GetPositionsByStrategy(GetPositionsByStrategyRequest)
      returns (GetPositionsByStrategyResponse);
  rpc HealthCheck(common.HealthCheckRequest)
      returns (common.HealthCheckResponse);
}

// services/manthan-tick-engine/proto/manthan_tick.proto (new file)
service ManthanTickEngineService {
  rpc GetTrailState(GetTrailStateRequest)
      returns (GetTrailStateResponse);
  rpc HealthCheck(common.HealthCheckRequest)
      returns (common.HealthCheckResponse);
}
```

### New Kafka topics to introduce

```
portfolio.updated       producer: manthan-portfolio
                        consumers: api-gateway (WSS), rebalancer,
                                   manthan-tick-engine

STRATEGY_DEACTIVATED    producer: user-config
                        consumers: manthan-portfolio (immediate exit
                                   of orphaned positions; daily
                                   reconciler becomes safety net only)
```

---

## Part 4 — Migration path (4 phases, fully reversible)

### Phase 0 — Pre-flight (this week)

Goal: no behaviour change. Just clean up the cross-service direct reads
in the existing rules-engine. This is the work we were starting today.

- [x] **Phase 0.1** — `strategies` orphan check → gRPC
      (commit `4cdcc75`, +123 / -54). Added `IsStrategyAlive` RPC wrapper on
      rules-engine's existing UserConfigClient; deleted legacy
      `isStrategySoftDeleted` SQL. Defensive posture unchanged.
- [x] **Phase 0.2** — `strategies` snapshot read in rebalancer → gRPC
      (commit `fc88bee`, +205 / -35). NEW gRPC client in rebalancer
      (`internal/userconfig_client.go`), `LoadActiveStrategies` now calls
      `GetAllActiveStrategies` via paginated bulk fetch. NOTE: this
      commit ALSO uncovered a second anti-pattern in the same function
      (`ResolveBrokerAuth` reads `user_credentials` directly) — deferred
      to Phase 0.6 to keep this commit focused.
- [x] **Phase 0.6a** — rebalancer's broker auth → user-config gRPC
      (commit `7d33090`, +94 / -61). The Phase 0.2 deferral closed out:
      `ResolveBrokerAuth` rewired to call `user-config.GetUserCredentials`
      (the RPC built in commit `a64265e`). Side-effect cleanup: rebalancer
      no longer opens the `trading_execution` database connection at all
      AND no longer holds the AES-GCM encryption key. Verified end-to-end
      against the real Indira broker — single audit log line per strategy,
      same broker numbers as the Phase 0.2 run.
- [ ] Phase 0.3 — `strategies` LoadConfig in hft-engine → gRPC (~120 LOC).
       hft-engine needs a NEW gRPC client. Uses single-strategy `GetStrategy`
       RPC instead of bulk.
- [ ] Phase 0.4 — Add gRPC server scaffold to data-ingestion + RPC for
                   `GetManthanSignals(date)`
- [ ] Phase 0.5 — `manthan_signals` read in rules-engine → gRPC
- [ ] Phase 0.6b — Finish `user_credentials` migration for the remaining
       two services: hft-engine + api-gateway switch from direct DB read
       to user-config gRPC. Same template as 0.6a. Closes out the last
       cross-service `user_credentials` reads in the codebase.

**Outcome**: rules-engine no longer reads from any foreign DB. We're
still a 10k-LOC monolith, but the boundaries are clean.

### Scorecard — what Phase 0 has unlocked so far

| Service           | Cross-service direct DB reads remaining       |
|-------------------|-----------------------------------------------|
| **rebalancer**    | **0** ✅ (was 2: strategies + user_credentials) |
| **rules-engine**  | 1 (manthan_signals — Phase 0.5)              |
| hft-engine        | 2 (strategies + user_credentials — 0.3 / 0.6b) |
| api-gateway       | 1 (user_credentials — 0.6b)                  |
| trade-execution   | 0 ✅ (Phase -1, user_credentials via gRPC + DB fallback) |

The rebalancer column already says ✅. That's the proof the pattern works.
Three more services to finish off.

### Phase 1 — Add the gRPC server to rules-engine itself

Goal: stop other services from reaching INTO rules-engine via DB.

- [ ] Phase 1.1 — Add gRPC server to rules-engine (port + reflection)
- [ ] Phase 1.2 — Define proto for `GetActiveStrategies`,
                   `GetMatchingStats`, `GetSignalDecisions`
- [ ] Phase 1.3 — Migrate api-gateway calls (mobile `/holdings`,
                   `/strategies`) to use new RPCs
- [ ] Phase 1.4 — Migrate rebalancer reads to use new RPCs

**Outcome**: rules-engine is now ASKABLE. No foreign DB reads in any
direction. Sets up Phase 2.

### Phase 2 — Extract manthan-portfolio (the big move)

Goal: split out the stateful aggregate into its own service. Highest
risk phase — needs careful dual-running.

- [ ] Phase 2.1 — Create `services/manthan-portfolio/` skeleton (cmd,
                  proto, internal/, go.mod). Empty service that compiles.
- [ ] Phase 2.2 — Move repository code (manthan_positions etc.) into
                  manthan-portfolio. Add gRPC server with
                  GetCapitalAllocation as the FIRST RPC (smallest).
- [ ] Phase 2.3 — Dual-run: manthan-portfolio reads + writes everything,
                  rules-engine ALSO reads + writes (idempotent). Sanity
                  check row-by-row consistency for 1 week.
- [ ] Phase 2.4 — Switch rules-engine to call manthan-portfolio gRPC
                  instead of its own repo. rules-engine stops writing.
- [ ] Phase 2.5 — Remove the duplicated repo code from rules-engine.
- [ ] Phase 2.6 — Add daily orphan reconciler in manthan-portfolio.
- [ ] Phase 2.7 — Add STRATEGY_DEACTIVATED Kafka flow (user-config emits,
                  manthan-portfolio consumes for immediate cleanup).
                  Reduce orphan reconciler to daily.

**Outcome**: rules-engine is now ~6k LOC (just R1 + R2 + R5 + R6 + R7).
manthan-portfolio is a clean ~3.5k LOC service.

### Phase 3 — Extract manthan-tick-engine

Goal: separate the real-time hot path from the rest. Lower risk than
Phase 2 (less state to move).

- [ ] Phase 3.1 — Create `services/manthan-tick-engine/` skeleton.
- [ ] Phase 3.2 — Move tick_handler.go + trailing_sl.go.
- [ ] Phase 3.3 — Wire up manthan-portfolio gRPC client for snapshot.
- [ ] Phase 3.4 — Switch rules-engine off the tick subscription.
- [ ] Phase 3.5 — Performance baseline: ensure tick latency unchanged.

**Outcome**: rules-engine is now ~2.5k LOC. Just signal matching + risk
client + ConfigStore sync. The split is done.

### Phase 4 — Cleanup + production hardening

- [ ] Add concurrency primitives properly across all 3 services
- [ ] Lift test coverage > 70% on each service
- [ ] Refactor each cmd/main.go using a builder pattern
- [ ] Per-service Postgres roles with minimal GRANTs
- [ ] mTLS between the 3 services + user-config + risk-management

---

## Part 5 — Risk register

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Dual-run drift in Phase 2.3 | High | High | Row-level consistency probe runs hourly; alert if mismatch > 0 |
| rules-engine gRPC client call to manthan-portfolio adds latency to hot path R1 | Medium | Medium | Cache GetCapitalAllocation result for ~30 sec at rules-engine side |
| Phase 2.4 cut-over leaves orphan writes from rules-engine | Medium | High | Cut over to read-only first for 1 week, then writes |
| WS tick feed reroute in Phase 3 misses ticks during switch | High | Critical | Double-subscribe for 1 hour; assert tick count match |
| Schema migrations span services during transition | High | Medium | Migration toolkit (each service runs only its own migrations) |
| New STRATEGY_DEACTIVATED event flow gets lost (Phase 2.7) | Medium | Medium | Keep daily reconciler — never remove it. Just reduces frequency. |
| Increased latency from synchronous gRPC calls during signal flood | Medium | High | Stress-test with 100x signal volume in staging before Phase 2.4 cut |

---

## Part 6 — Decisions deferred (for product / business)

These need someone above the architect:

1. **Should "Stop strategy" auto-liquidate?**
   Today: yes (orphan scanner EXITs all positions). My recommendation:
   no — split into "Stop strategy" (stop new signals only) and
   "Liquidate" (explicit). But this is a product decision.

2. **Multi-tenant scale target.**
   Are we sizing for 10k users? 100k? Affects whether we need read
   replicas + sharding.

3. **Real-time SLA on tick processing.**
   What's "acceptable" tick-to-SL-modify latency? 100ms? 50ms?
   Different answers drive R5 design choices.

---

## Part 7 — What changes for the user

External observable behaviour after the split:

| Behaviour | Today | After split |
|-----------|-------|-------------|
| Signal matching latency | ~50-200ms | ~30-100ms (no R3 contention) |
| Mobile `/holdings` response | ~80ms | ~30ms (gRPC instead of DB) |
| Stop-strategy → position exit | up to 5 min (scanner) | < 1 sec (event-driven) |
| Deploy rules-engine fix | full restart, all paths blind ~20 sec | only R1 restart, other paths uninterrupted |

No user-visible bug fixes from the split itself. The wins are:
operability, latency tail, scale ceiling, dev velocity.

---

## See also

- [communication-patterns.md](communication-patterns.md) — when to use gRPC vs Kafka vs DB
- [data-ownership.md](data-ownership.md) — ownership matrix (this doc UPDATES it for Phase 2+)
- [database-redesign-plan.md](database-redesign-plan.md) — 3-DB target (orthogonal to this split; can land before, during, or after)
