# orderstatus Service — CQRS Migration Design

**Status:** design ratified 2026-07-09. Ready to execute Phase 0 → 5.
**Author of decision:** rohitt (2026-07-09), full CQRS opt-in after weighing alternatives.
**Related:** [codify_wss_state_machine.md](./codify_wss_state_machine.md) — the verified WSS event surface this design consumes.
**Sign-off:** all 7 architectural calls in §5-8 approved (one topic, partition key = broker_order_id, feature-flagged dual-write, callbacks stay through Phase 3, rules-engine fixes realized_pnl, api-gateway consumes Kafka directly, OCO stays in trade-execution).

## 1. Problem statement

`services/trade-execution/` today owns three responsibilities that don't belong
together:

1. **Command** — placing / modifying / cancelling orders via broker REST.
2. **Query** — watching broker WSS + REST orderbook for state changes.
3. **Business logic** — Manthan entry / SL / trailing / safety.

When the process restarts, WSS subscriptions drop. When Manthan logic has a
bug, WSS event processing pauses. When the DB write path is slow, order
placement blocks. Three unrelated failure modes share one process.

## 2. Non-goals

- Not rewriting broker adapter code (`pkg/indira/`).
- Not changing DB schema for `orders` / `manthan_orders` / `manthan_positions`.
- Not changing the frontend `/ws/live-orders` WebSocket contract.
- Not adding a second broker in this migration (but architecture must not block it).

## 3. Target architecture

```
                    ┌──────────────────────────────────────┐
                    │  BROKER (Codify)                     │
                    └──────┬──────────────────────────────┘
                           │
              ┌────────────┼─────────────────────────┐
              │            │                         │
              │ REST       │ WSS: order-notify       │
              │ POST orders│                         │
              ▼            ▼                         │
   ┌───────────────────┐  ┌──────────────────────────┴────┐
   │ trade-execution   │  │ orderstatus service (NEW)     │
   │                   │  │                               │
   │ • place order     │  │ • single shared WSS per user  │
   │ • DB INSERT orders│  │ • REST orderbook reconciler   │
   │   row (INITIATED) │  │ • DB UPDATE orders row on     │
   │ • await Kafka     │  │   every state change          │
   │   fill event      │  │ • DB INSERT execution_events  │
   │ • business logic  │  │   audit row per WSS event     │
   │   (Manthan, OCO)  │  │ • PUBLISH order.events topic  │
   └─────────┬─────────┘  └──────────────┬────────────────┘
             │                           │
             │ subscribes to             │ produces
             │ its own orders            │ order.events
             │                           │
             │     ┌─────────────────────┴──────────────────┐
             │     │        Kafka topic: order.events       │
             │     │  partition key = broker_order_id       │
             │     └─────┬───────────────┬──────────────────┘
             │           │               │
             └───────────┤               ├─────────────┐
                         │               │             │
                         ▼               ▼             ▼
              ┌──────────────┐  ┌────────────────┐  ┌────────────────┐
              │ rules-engine │  │ api-gateway    │  │ trade-execution│
              │ (projector)  │  │ (frontend push)│  │ handlers       │
              │              │  │                │  │                │
              │ • updates    │  │ • pushes to    │  │ • entry_handler│
              │   manthan_   │  │   /ws/live-    │  │   waits for    │
              │   positions  │  │   orders       │  │   FILLED event │
              │ • computes   │  │                │  │ • sl_handler   │
              │   realized_  │  │                │  │   places new SL│
              │   pnl        │  │                │  │   on trigger   │
              └──────────────┘  └────────────────┘  └────────────────┘
```

## 4. Service responsibility contract

### 4.1 trade-execution (post-migration)

**Owns:**
- REST calls: `PlaceLimitBuy`, `PlaceLimitSell`, `PlaceMarketBuy`, `PlaceMarketSell`, `PlaceSLSell`, `PlaceAMOSell`, `ModifyOrder`, `CancelOrder`, `PlaceBracketOrder`.
- Row lifecycle **CREATE** for `orders` and `manthan_orders`.
- Business logic: entry handler, SL handler, safety monitor, OCO manager.
- Kafka subscriber to `order.events` — receives status updates for orders it placed.

**Does NOT own:**
- WSS connection to broker (deleted).
- Row lifecycle **UPDATE** for status columns (`broker_status`, `filled_price`, `filled_quantity`, `broker_ws_data`, etc). Only orderstatus writes these.
- REST orderbook polling for reconciliation (moved).
- `manthanBridge`, `manthanManualExitPub` in-process callbacks (deleted — become Kafka consumers).

**INSERT contract for `orders` table (on placement):**
- `order_id` (uuid)
- `user_id`, `strategy_id`, `symbol`, `exchange`, `side`, `order_type`, `quantity`, `price`, `trigger_price`
- `status` = `INITIATED`
- `broker_order_id` = whatever the REST place-order response returned
- `created_at` = now
- All status/fill fields NULL — orderstatus fills them in.

### 4.2 orderstatus (new service)

**Owns:**
- Single shared WSS connection per user (currently in `statusservice/service.go`).
- REST orderbook reconciler (currently in `manthan/reconciler.go`).
- Row lifecycle **UPDATE** for status/fill columns on `orders`.
- INSERT into `execution_events` (audit trail — every WSS payload verbatim).
- Kafka producer for `order.events`.
- WSS reconnect + backfill (fetch orderbook after reconnect, publish any events we missed).

**Does NOT own:**
- Row **CREATE** for `orders` — trade-execution does that.
- Any Manthan business logic (moved to rules-engine consumer).
- OCO group state (stays in trade-execution as in-memory state).
- Frontend push (api-gateway subscribes to Kafka).

**UPDATE contract for `orders` table:**
- Only touches columns: `broker_status`, `status`, `filled_quantity`, `filled_price`, `broker_ws_data`, `exchange_order_number`, `executed_at`, `stop_loss`, `rejection_reason`, `updated_at`.
- Never touches: `user_id`, `symbol`, `quantity`, `price` (immutable after create), `strategy_id`.
- Idempotent: uses `(broker_order_id, wsStatus.MessageSequenceNumber)` as dedup key.

**READ contract:**
- Reads `orders.broker_order_id` to correlate WSS events to rows.
- Reads `user_credentials.encrypted_token` to build WSS auth (unchanged).

### 4.3 rules-engine (post-migration)

**New responsibility:**
- Kafka subscriber to `order.events`.
- On `event_type=FILLED` and `buy_sell="2"` (SELL):
  - Look up `manthan_orders` by `broker_order_id`.
  - If `order_type IN (SL_SELL, SL_SELL_AMO)`: publish `SL_FILLED` internally, update `manthan_positions` to EXITED with `realized_pnl = (traded_price - entry_price) * qty`.
  - If not manthan: ignore.
- On `event_type=MANUAL_EXIT_DETECTED`: existing MANUAL_EXIT_DETECTED handler (unchanged, just fed from Kafka now).

**This is where `realized_pnl=0` gets fixed** — as a natural consequence of the split, not a bolt-on hook.

### 4.4 api-gateway (post-migration)

**New responsibility:**
- Kafka subscriber to `order.events`.
- Fan events for a given `user_id` to their `/ws/live-orders` connections.
- Replaces the in-process `wsBroadcaster` callback that trade-execution currently wires.

## 5. Kafka event contract

### 5.1 One topic: `order.events`

**Rationale:** at our volume (~1000 events/day), a single topic is simpler than 4. Consumers filter by `event_type`. If we ever hit >100k events/day we split.

**Partition key:** `broker_order_id`. Guarantees per-order event ordering. All events for order NYMZX000A697 land on the same partition = strict order. Cross-order ordering is not needed by any consumer.

**Retention:** 30 days (for audit + late replay).
**Replication:** 1 (matches existing local dev config in `deployments/docker/setup_kafka.sh`).
**Partitions:** 3 (matches existing convention).

**Broker location:** `localhost:9092` via `deployments/docker/docker-compose-kafka.yml`. Container name `trading-kafka`. Kafka UI at `localhost:8082` for inspection.

**Add to `setup_kafka.sh`** during Phase 0:
```bash
"order.events"    # orderstatus service - real-time order state stream (CQRS 2026-07-09)
```

**Existing topic `order-updates`** — currently publish-only from trade-execution, no Go consumers (per audit note in `setup_kafka.sh` line 100). Once `order.events` proves out through Phase 3, we retire `order-updates`.

### 5.2 Envelope schema (JSON)

Every message has this envelope regardless of `event_type`:

```json
{
  "event_id":              "01H8W3ABCDXYZ...",       // ULID or UUID — for dedup
  "event_type":            "STATUS_CHANGED",         // enum below
  "event_seq":             1783576973805,            // WSS MessageSequenceNumber or unix_micro
  "produced_at_ms":        1783576974001,            // orderstatus wall clock
  "broker_ts_ms":          1783576973805,            // WSS timestamp if present, else 0

  "user_id":               "S4450",
  "broker_order_id":       "NYMZX000A697",           // = WSS UniqueCode = our DB broker_order_id
  "exchange_order_number": "1100000045182580",       // NSE order number, may be "0" if pre-exchange
  "symbol":                "IDEA",
  "exchange":              "NSE",
  "buy_sell":              "1",                      // "1"=BUY "2"=SELL — strings for backward compat
  "order_type":            "REGULAR LIMIT",          // WSS vocabulary (not REST)
  "product":               "DELIVERY / CARRYFORWARD",
  "message_type":          "ORD_NRML",               // ORD_NRML | TRD_MSG

  "status":                "PENDING",                // one of the 6 verified WSS statuses
  "oms_status_code":       0,                        // OMSOrderStatus if populated
  "prev_status":           "",                       // orderstatus fills this from DB previous value

  "order_price":           13.45,
  "trigger_price":         0.0,
  "quantity":              1,
  "filled_qty":            0,
  "traded_price":          0.0,
  "pending_qty":           1,

  "reason":                ""                        // rejection or cancel reason text
}
```

### 5.3 `event_type` enum

| Value | When emitted | Which consumer cares |
|---|---|---|
| `STATUS_CHANGED` | Any WSS event that changed `status` for a known order | trade-execution (its own orders), api-gateway |
| `FILLED` | WSS EXECUTED event (TRD_MSG or ORD_NRML with TradedPrice) | rules-engine (SL fill → position EXITED), trade-execution (entry fill), api-gateway |
| `REJECTED` | `A.REJECTED` or `ORDER ERROR` | trade-execution (retry logic), rules-engine (ENTRY_REJECTED), api-gateway |
| `CANCELLED` | WSS `CANCELLED` | trade-execution, api-gateway |
| `MODIFIED` | WSS PENDING event where `OrderPrice` changed vs DB value | trade-execution (SL trail confirm), api-gateway |
| `MANUAL_EXIT_DETECTED` | WSS EXECUTED SELL for a `broker_order_id` NOT in our `orders` table | rules-engine (MANUAL_EXIT_DETECTED handler) |
| `PRICE_FREEZE_REJECT` | `ORDER ERROR` with reason containing "price freeze" | rules-engine (log-spam suppression + potentially retry with clamped price) |

Note `PRICE_FREEZE_REJECT` is a subtype of `REJECTED`. We emit BOTH — a `REJECTED` and a `PRICE_FREEZE_REJECT` — so consumers can subscribe at whichever granularity they need. Cost: 1 extra Kafka message per price-freeze; benefit: cleaner consumer filtering.

## 6. DB write ownership matrix

| Column | Before | After migration |
|---|---|---|
| `orders.user_id`, `symbol`, `quantity`, `price`, `order_type`, `strategy_id` | trade-execution INSERT | trade-execution INSERT (unchanged) |
| `orders.broker_order_id` | trade-execution INSERT | trade-execution INSERT (from REST response) |
| `orders.status` | statusservice + trade-execution | **orderstatus only** |
| `orders.broker_status` | statusservice | **orderstatus only** |
| `orders.filled_quantity`, `filled_price` | statusservice | **orderstatus only** |
| `orders.exchange_order_number` | statusservice | **orderstatus only** |
| `orders.broker_ws_data` | statusservice | **orderstatus only** |
| `orders.rejection_reason` | statusservice | **orderstatus only** |
| `orders.executed_at` | statusservice | **orderstatus only** |
| `orders.stop_loss` (trigger price) | statusservice | **orderstatus only** |
| `orders.updated_at` | both | **orderstatus for status changes**, trade-execution on manual updates |
| `execution_events` (audit) | statusservice INSERT | **orderstatus INSERT only** |
| `manthan_orders.*` | trade-execution (manthan handlers) | trade-execution (unchanged) |
| `manthan_positions.*` | rules-engine projector | rules-engine projector (unchanged, but now fed by Kafka) |

## 7. Migration phases

Each phase is independently shippable and independently rollback-able.

### Phase 0 — pre-flight

- Verify Kafka broker present (check `deployments/docker/docker-compose-kafka.yml`).
- Verify existing `manthan.execution.events` topic works (rules-engine consumer).
- Create new topic `order.events` with the settings above.
- Baseline metric: count of events/day currently processed by statusservice (for later comparison).

**Rollback:** nothing to roll back — this phase makes no code changes.

### Phase 1 — dual-write publisher

Inside existing `services/trade-execution/internal/statusservice/service.go`:
add a Kafka publisher that emits `order.events` for every WSS event it already
processes. No consumers yet.

- New file: `services/trade-execution/internal/statusservice/kafka_publisher.go`
- Add call in `processWSStatus` (near line 490) after DB update succeeds.
- Feature flag: `ORDER_EVENTS_PUBLISHER_ENABLED=1` env var. Off → no publish (safe rollback).

**Test:** deploy to staging, flag ON, verify Kafka topic receives events matching WSS input rate.
**Rollback:** flip flag OFF. Zero behavior change.
**Effort:** ~1 day.

### Phase 2 — new Kafka consumers, keep callbacks alive

Add Kafka consumers in rules-engine, api-gateway, trade-execution's own handlers.
Existing in-process callbacks (`manthanBridge`, `wsBroadcaster`, `manthanManualExitPub`) STAY WIRED for safety.

- rules-engine: new consumer for `order.events`, filters on `event_type=FILLED` and manthan `broker_order_id`. On match, publishes `SL_FILLED` (existing internal event).
- api-gateway: new consumer, fans events to `/ws/live-orders` clients by user_id.
- trade-execution: new consumer, feeds events into the existing wssBridge channels so entry_handler / sl_handler receive them without change.

**Test:** deploy to staging. Compare rules-engine's SL_FILLED events processed via new path vs old path — should be identical (both fire).
**Rollback:** disable consumers, callbacks alone still cover everything.
**Effort:** ~2 days.

**MILESTONE: `realized_pnl=0` bug is fixed at end of Phase 2.**

### Phase 3 — extract orderstatus binary

Move code:
```
services/trade-execution/internal/statusservice/          →  services/orderstatus/internal/wss/
services/trade-execution/internal/manthan/reconciler.go   →  services/orderstatus/internal/reconciler/
services/trade-execution/internal/manthan/external_activity_detector.go
                                                          →  services/orderstatus/internal/manual_exit_detector/
services/trade-execution/internal/manthan/wss_bridge.go   →  services/trade-execution/internal/wss_consumer/  (renamed: it's now a Kafka consumer)
```

Create `services/orderstatus/cmd/main.go`. Add PM2 config entry.

Both services deploy: `orderstatus` and `trade-execution`. Both write to the DB during this phase — with idempotency via `event_seq`, double-writes are harmless (last-writer-wins on the row, but both write the same values).

**Test:** run both in staging for 1 trading day. Diff `orders` table row-by-row vs pre-migration baseline. Expect zero divergence.
**Rollback:** shut down `orderstatus` PM2 process. Old statusservice-inside-trade-execution keeps working.
**Effort:** ~3 days.

### Phase 4 — flip authority to orderstatus, statusservice becomes no-op

Add env var `STATUSSERVICE_DB_WRITE_ENABLED=0` in trade-execution. When off,
the in-process statusservice (still there for now) skips DB writes and Kafka publish — orderstatus is now the sole authority.

Monitor for 1 day. If nothing broke, proceed.

**Rollback:** flip env var back ON. statusservice resumes writing.
**Effort:** ~1 day incl. validation.

### Phase 5 — delete legacy code

Remove statusservice code from `trade-execution`. Remove `manthanBridge`,
`manthanManualExitPub`, `wsBroadcaster` fields and callback wiring. Remove
`SetManthanBridge` and related setters. Trade-execution service becomes lean.

**Rollback:** revert commit.
**Effort:** ~1 day.

## 8. Deployment topology

### Before

| Process | Binary | Owns |
|---|---|---|
| `trade-execution` | trade-execution | REST + WSS + business |
| `rules-engine` | rules-engine | projector |
| `api-gateway` | api-gateway | HTTP + WS push |

### After

| Process | Binary | Owns |
|---|---|---|
| `trade-execution` | trade-execution | REST placement + Manthan business |
| `orderstatus` | orderstatus | WSS + reconciler + DB status writes |
| `rules-engine` | rules-engine | projector (now Kafka-fed) |
| `api-gateway` | api-gateway | HTTP + WS push (now Kafka-fed) |

Resource footprint of orderstatus:
- ~50MB RAM (per WSS user connection + Kafka producer)
- Negligible CPU (~1% steady state)
- Network: WSS keep-alive + Kafka produce
- PM2 restart policy: `restart_delay=5s`, `max_restarts=10/min`

## 9. Failure modes & mitigation

| Failure | Mitigation |
|---|---|
| orderstatus dies mid-day | Reconciler picks up all state on restart via REST orderbook. Kafka consumers may see gap but reconciler pushes reconciliation events on startup. |
| Kafka broker down | trade-execution keeps placing orders (no dependency on Kafka to place). orderstatus queues events in memory (bounded buffer, up to 1000). If buffer overflows, log and drop — reconciler will fix later. |
| DB down | orderstatus retries with exponential backoff. Events buffered in Kafka (already published). trade-execution's placement fails cleanly (already the case). |
| WSS drops permanently | orderstatus reconnects every 5s. On reconnect, calls REST orderbook to fetch state since last known event_seq. Publishes catch-up events with `event_seq` derived from broker order timestamp. |
| Event out of order (Kafka rebalance) | Consumers use `event_seq` for ordering. If they see a lower seq than already processed, skip (idempotent update). Actual out-of-order across restart is rare because partition key = broker_order_id. |
| trade-execution places order, then dies before receiving fill event | orderstatus still writes DB. On trade-execution restart, `recovery.go` fetches `orders` where `status IN (INITIATED, PLACED)` and rehydrates state from DB. |

## 10. Staging validation checklist per phase

Same checklist for each phase, applied to staging:

- [ ] Deploy to staging server `manthan` via PM2.
- [ ] Verify service starts, connects to WSS + Kafka.
- [ ] Fire test order (IDEA 1 share LIMIT) via Postman.
- [ ] Verify WSS event captured, DB row updated.
- [ ] Verify Kafka event produced (`kcat -C -t order.events -o -1 -c 1`).
- [ ] Verify rules-engine consumed it (log line).
- [ ] Verify api-gateway pushed it to `/ws/live-orders` client.
- [ ] Fire real MARKET BUY + MARKET SELL round-trip. Verify `manthan_positions.realized_pnl` updates correctly.
- [ ] Fire real SL fill test (buy 1 IDEA, place SL near LTP, wait for market move). Verify EXITED position with correct realized_pnl.
- [ ] Let run for 1 trading day. Diff `orders` table before/after — expect zero row-level divergence between statusservice (old path) and orderstatus (new path).
- [ ] Check log volume — Kafka producer errors should be zero.

Only advance to next phase if all checks pass.

## 11. Resolved decisions (was: open questions)

1. **Kafka broker location.** **DECIDED (2026-07-09):** local Docker at `localhost:9092`, container `trading-kafka`, per `deployments/docker/setup_kafka.sh`. No staging/prod Kafka yet — same local instance handles all dev. Kafka UI on `localhost:8082`.

2. **Existing consumer groups.** No collision expected — pick names like `orderstatus-writer`, `rules-engine-order-fills-consumer`, `api-gateway-order-events-consumer`, `trade-execution-self-orders-consumer`. Verify with `docker exec trading-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --list` before deploying each phase.

3. **Event dedup horizon.** **DECIDED:** 24h TTL in an in-memory LRU per consumer. Set map size cap at 100k entries; evict oldest on overflow. Small enough to not leak, long enough for any realistic retry window.

4. **Config store.** **DECIDED:** PM2 ecosystem env vars (`ORDER_EVENTS_PUBLISHER_ENABLED=1`). No config service. Matches existing pattern.

5. **Metrics.** **DECIDED:** none for now. No Prometheus stack running. Rely on structured logs (existing zap logger) + Kafka UI visual inspection. Add metrics later if we onboard Prometheus.

6. **Alerting.** **DECIDED:** none for now. Errors log at Warn/Error level via zap; user checks logs manually. Add proper alerting alongside metrics if we onboard Prometheus.

## 12. What we're NOT deciding here

- Whether to use gRPC for internal calls between services (Kafka is enough).
- Whether to migrate to Aeron/Solace for lower latency (Kafka's 5-20ms is fine).
- Whether to add schema registry (protobuf) for events (JSON is fine at our scale).
- Whether to use Postgres LISTEN/NOTIFY instead of Kafka (Kafka wins on replay + persistence).
- Second broker onboarding (deferred — architecture allows it, don't design for it now).

## 13. What we ship this session vs later

- **This session (already done):** WSS state-machine spec + verified enum. That's Phase 0's baseline.
- **This session (this doc):** design ratified.
- **Next session:** Phase 1 code — Kafka publisher inside statusservice.
- **Session after:** Phase 2 — new consumers.
- **Milestone:** end of Phase 2 = `realized_pnl=0` bug fixed as side effect.
- **Full CQRS complete:** end of Phase 5, ~10 working days from start.
