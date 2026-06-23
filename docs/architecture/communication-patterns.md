# Communication Patterns

> The single most important architecture document in this repo.
> Read this BEFORE adding any inter-service communication.

## TL;DR — the cheat sheet

```
              SYNC (need answer NOW)        ASYNC (eventually)
              ─────────────────────         ──────────────────

INTERNAL      gRPC                          Kafka
              ↑                             ↑
              user-config.GetCreds          manthan.signals
              risk.CheckPreTrade            order.placed
              trade.ModifyOrder             market.ticks

EXTERNAL      HTTP+JSON                     WebSocket
              ↑                             ↑
              mobile.GET /orders            mobile receives
              partner.POST /sub             order updates

DATA          Direct DB read                Direct DB write
              ↑                             ↑
              Own data + api-gateway lists  Owner service only
```

If you remember nothing else from this doc, remember the matrix above.

---

## The four layers of our system

```
╔══════════════════════════════════════════════════════════════════╗
║  Layer 1: EXTERNAL CLIENTS                                       ║
║    Mobile App, Web App, 3rd-party (via API key)                  ║
║    Protocols: HTTPS/JSON, WSS for real-time push                 ║
║    Cannot speak gRPC (browsers don't support it natively)        ║
╚══════════════════════════════════════════════════════════════════╝
                            │
                            ▼
╔══════════════════════════════════════════════════════════════════╗
║  Layer 2: TRANSLATION                                            ║
║    api-gateway is the ONLY service that speaks BOTH protocols    ║
║    - Accepts HTTPS/JSON from clients                             ║
║    - Calls backend services via gRPC                             ║
║    - Subscribes to Kafka, pushes events to clients via WSS       ║
║    - Reads stockk_trading directly for list views (CQRS)         ║
╚══════════════════════════════════════════════════════════════════╝
                            │
                            ▼
╔══════════════════════════════════════════════════════════════════╗
║  Layer 3: BUSINESS LOGIC (backend services)                      ║
║    user-config, trade-execution, rules-engine, risk-management,  ║
║    hft-engine, rebalancer, data-ingestion                        ║
║    Use gRPC for synchronous calls; Kafka for events              ║
╚══════════════════════════════════════════════════════════════════╝
                  │                              │
                  ▼                              ▼
╔══════════════════════════╗   ╔══════════════════════════════════╗
║  Layer 4a: EVENTS (Kafka)║   ║  Layer 4b: STATE (Postgres)      ║
║    Durable, replay-able  ║   ║    stockk_auth, stockk_trading,  ║
║    Decouples timing      ║   ║    stockk_market                 ║
╚══════════════════════════╝   ╚══════════════════════════════════╝
```

---

## The four communication patterns

### Pattern A: Direct DB read — "I own this, give it to me"

**When to use:**
- Reading data your own service writes (you own it)
- api-gateway reading public list views (CQRS read path)
- Read replicas for analytics / heavy reporting

**Latency:** ~5-30ms
**Coupling:** High (reader knows schema)
**Safety:** Use a Postgres role with `SELECT only` grants for non-owner readers

**Example (api-gateway → manthan_orders):**
```go
// Inside api-gateway handler — reading order list for mobile
rows, _ := db.Query(`
    SELECT id, symbol, qty, status, created_at
    FROM manthan_orders
    WHERE user_id = $1
    ORDER BY created_at DESC
    LIMIT 50`, userID)
```

**NEVER use direct read when:**
- Data needs decryption (use gRPC so owner decrypts)
- Data needs computed/derived values (use gRPC so owner computes)
- You need audit logging of who-read-what (use gRPC so owner logs)

---

### Pattern B: gRPC — "I need your answer NOW"

**When to use:**
- Caller cannot continue without the answer
- Answer involves business logic, decryption, or audit
- Strict typed contract is required
- Streaming (server push or client upload)

**Latency:** ~5-50ms
**Coupling:** Medium (caller knows the .proto contract, not the schema)
**Safety:** Use mTLS in production; authenticate the calling service

**Example (trade-execution → user-config):**
```go
// Inside trade-execution before placing order at broker
resp, err := userConfigClient.GetUserCredentials(ctx, &pb.GetUserCredentialsRequest{
    UserId: pos.UserID,
})
if err != nil {
    return fmt.Errorf("get creds: %w", err)
}
brokerJWT := resp.IndiraBearerToken  // already decrypted by user-config
```

**Real RPCs in our system:**

| Service           | RPC                            | Caller(s)                     |
|-------------------|--------------------------------|-------------------------------|
| user-config       | `GetUserCredentials`           | trade-execution, hft-engine, rebalancer, api-gateway |
| user-config       | `GetAllActiveStrategies`       | rules-engine, rebalancer      |
| user-config       | `CreateStrategy / UpdateStrategy` | api-gateway (from web/mobile) |
| risk-management   | `CheckPreTradeRisk`            | rules-engine                  |
| trade-execution   | `CancelOrder / ModifyOrder`    | api-gateway                   |
| hft-engine        | `Entry / Exit`                 | rules-engine                  |
| hft-engine        | `StreamState` (server stream)  | api-gateway → mobile via WSS  |
| hft-engine        | `SubmitFills` (client stream)  | trade-execution (bulk upload) |

---

### Pattern C: Kafka — "Take this, process whenever"

**When to use:**
- Publishing an EVENT others should know about
- Multiple consumers may want the same event
- Caller doesn't need the result NOW
- Decoupling timing (sender and receiver can deploy independently)

**Latency:** ~5-200ms (consumer lag dependent)
**Coupling:** Loose (only the topic schema is shared)
**Safety:** Idempotent consumers must handle duplicates

**Example (data-ingestion → rules-engine):**
```go
// data-ingestion publishes signals at 03:00 IST
producer.Produce(ctx, &kafka.Message{
    Topic: "manthan.signals",
    Key:   []byte(signal.Symbol),
    Value: signalJSON,
})

// rules-engine consumes whenever it's ready (decoupled timing)
for msg := range consumer.Messages() {
    var sig ManthanSignal
    json.Unmarshal(msg.Value, &sig)
    rules.Evaluate(sig)
    consumer.Commit(msg)
}
```

**Topics in our system (canonical owners):**

| Topic                        | Producer        | Consumers                |
|------------------------------|-----------------|--------------------------|
| `manthan.signals`            | data-ingestion  | rules-engine             |
| `trade-signals`              | rules-engine    | trade-execution          |
| `manthan.execution.events`   | trade-execution | rules-engine, rebalancer |
| `user-config-events`         | user-config     | rules-engine, trade-execution |
| `order.placed`               | trade-execution | rebalancer, api-gateway  |
| `order.filled`               | trade-execution | rebalancer, api-gateway  |
| `market.ticks` *(future)*    | data-ingestion  | hft-engine               |
| `portfolio.updated`          | rebalancer      | api-gateway              |

---

### Pattern D: HTTP/JSON & WSS — for EXTERNAL clients only

**When to use:**
- Mobile app, web app, partner integrations
- Anything that can't speak gRPC (browsers, most SDKs)
- Real-time push to clients via WebSocket

**Latency:** ~50-200ms (network + TLS handshake)
**Coupling:** Loose (REST contract, OpenAPI spec)
**Safety:** Auth via session cookie or API key; rate limited

**Example (mobile → api-gateway):**
```
GET /api/v1/subscriptions/sub_xyz/holdings
Authorization: Bearer <session>
       │
       ▼
api-gateway translates this to:
   - direct DB read on stockk_trading.manthan_positions (CQRS path)
   - OR gRPC call to trade-execution.GetUserPositions for complex aggregation
```

WSS subscription model:
```
Mobile opens WSS connection to wss://api.stockk.trade/v1/ws?token=<bearer>
       │
       ▼
api-gateway maintains the connection, listens to relevant Kafka topics
       │
       ▼
When an event arrives on a subscribed topic → push JSON frame to mobile
       │
       ▼
Mobile UI updates in real time
```

---

## Real-world flow examples (all 4 patterns together)

### Flow 1: Manthan BUY order, end-to-end

```
03:00 IST  data-ingestion runs Manthan algo (batch job)
         │
         ▼
Pattern C: Kafka publish to manthan.signals    [13 messages]
         │
         ▼
rules-engine consumes manthan.signals
         │
         ▼
Pattern B: gRPC → risk-management.CheckPreTradeRisk
         │   (rules-engine WAITS for answer, can't continue without it)
         │
         ▼
Pattern C: Kafka publish to trade-signals      [13 allowed orders]
         │
         ▼
trade-execution consumes trade-signals
         │
         ▼
Pattern B: gRPC → user-config.GetUserCredentials
         │   (need decrypted broker JWT to call Codifi)
         │
         ▼
HTTP POST → Codifi broker API (external HTTP, not in this matrix)
         │
         ▼
Pattern A: trade-execution INSERTs into manthan_orders (its own table)
         │
         ▼
Pattern C: Kafka publish to order.placed       [13 events]
         │
         ├──► rebalancer updates manthan_portfolio_state
         │
         └──► api-gateway pushes order update to mobile (WSS, Pattern D)
         │
         ▼
Pattern D: Mobile receives WSS message → updates UI
```

**Each transition uses the RIGHT pattern for that step.** That's the whole game.

---

### Flow 2: User opens "My Orders" screen

```
Mobile App                                api-gateway              stockk_trading
   │                                          │                         │
   ├─ GET /api/v1/orders ────────────────────►│                         │
   │   Authorization: Bearer <session>        │                         │
   │                                          │                         │
   │                            (Pattern A — direct read,               │
   │                             api-gateway has SELECT-only role       │
   │                             on stockk_trading)                     │
   │                                          │                         │
   │                                          ├─ SELECT FROM ──────────►│
   │                                          │   manthan_orders        │
   │                                          │   WHERE user_id=S4450   │
   │                                          │◄────────── 25 rows ─────┤
   │                                          │                         │
   │◄── JSON list of 25 orders ──────────────┤                         │
   │                                          │                         │
```

**Why direct DB instead of gRPC?**
- Reading public list, no decryption needed
- No business logic to compute
- Direct read = 30ms; gRPC would add 30-50ms with no benefit
- api-gateway has `SELECT only` role → can't accidentally write

---

### Flow 3: User logs in via SSO

```
Mobile         api-gateway     Codifi SSO     user-config     stockk_auth
  │                │                │              │              │
  ├─tap login────►│                │              │              │
  │                ├─302 redirect──►│              │              │
  │                │                │              │              │
  │◄── opens Codifi SSO URL ────────│              │              │
  │     (user signs in)             │              │              │
  │                │                │              │              │
  │                │◄──code+JWT─────┤              │              │
  │                │                                              │
  │       Pattern B: gRPC →┤              │              │
  │                ├──user-config.UpdateUserCredentials──►│       │
  │                │   (encrypted broker JWT)             │       │
  │                │                              ├─INSERT──────►│
  │                │                              │ user_creds   │
  │                │                              │ (AES-GCM)    │
  │                │◄──ok──────────────────────────┤             │
  │                │                                              │
  │       Pattern C: Kafka publish to user-config-events          │
  │                │                                              │
  │                │   (notifies rules-engine, trade-execution    │
  │                │    that this user is now logged in)          │
  │                │                                              │
  │◄──session cookie set──────────┤                              │
  │                │                                              │
```

**Patterns used:**
- B (gRPC) — sync write to user-config, needs success ACK
- C (Kafka) — async notify other services they should refresh user state

---

## The five veteran rules

### Rule 1: External clients → HTTPS/JSON only
Never expose gRPC ports to the internet. Mobile and web cannot speak gRPC.
The api-gateway is the SOLE public surface.

### Rule 2: gRPC when you need an answer NOW
If your code looks like `result := someService.X(); processNext(result)`,
that's gRPC territory. Need an answer = synchronous call.

### Rule 3: Kafka when you're publishing an EVENT others react to
If your code looks like "I did X, others might care", that's Kafka.
The producer doesn't know who consumes; consumers don't block the producer.

### Rule 4: Direct DB only for owned data + api-gateway list views

The matrix below is canonical. Memorise it before adding any read path.

```
                       OWNED DATA              CROSS-SERVICE DATA
                       ──────────              ──────────────────
api-gateway            (owns nothing)          Direct DB ✅
(translation layer)                            (with SELECT-only role +
                                                read replica when scaled)

Backend services       Direct DB ✅            Default: gRPC ✅
(rebalancer,           (your own tables)       Exceptions: see below
 trade-execution,
 rules-engine, …)

Owner service          Direct DB ✅            Don't — you're crossing
                       (its own tables)        a bounded context wrongly
```

**Why api-gateway gets the exception** (it's not laziness):

- api-gateway has NO business logic of its own
- Its job is to translate HTTPS/JSON ↔ gRPC ↔ DB rows
- Forcing api-gateway → gRPC → owner-service → DB adds 30-50 ms per request for ZERO business value (no decryption, no derived values, no audit gain beyond what nginx already logs)
- api-gateway is constrained by a SELECT-only Postgres role per Rule 5 — Postgres enforces the boundary even though the read is direct

**Backend services use gRPC by default** when reading data they don't own. Direct DB is allowed only when **ALL FOUR** of these hold:

```
✅ Read is purely raw (no decryption, no derived computation)
✅ Owner agrees in writing (data-ownership.md grants the read access)
✅ Reader has SELECT-only Postgres role (enforced, not aspirational)
✅ Latency truly matters (millisecond hot path) AND a read replica exists
```

In practice, almost no backend-to-backend read meets all four. **So gRPC wins by default.**

**When gRPC is MANDATORY for a backend service read** (no direct DB allowed even with the four conditions above):

```
❌ Decryption required             → gRPC (only owner has key)
❌ Computed/derived value          → gRPC (owner computes consistently)
❌ Audit log required              → gRPC (owner logs every access)
❌ Server-side filter/transform     → gRPC (owner enforces business rules)
```

**Concrete examples from this codebase:**

| Reader → Owner table                  | Pattern   | Reason                            |
|---------------------------------------|-----------|-----------------------------------|
| api-gateway → manthan_orders          | Direct DB | api-gateway exception, raw list   |
| api-gateway → user_credentials        | gRPC      | Decryption required (overrides)   |
| rebalancer → strategies               | gRPC      | Business logic (filter MANTHAN)   |
| rebalancer → user_credentials         | gRPC      | Decryption + audit                |
| trade-execution → user_credentials    | gRPC      | Decryption + audit + dual-write   |
| rules-engine → strategies (orphan ck) | gRPC      | NOT_FOUND semantic from owner     |
| rules-engine → its own manthan_*      | Direct DB | Owns the tables                   |

**Common mistake to avoid** (your author has fallen into this):
Motivating a new gRPC RPC by saying *"api-gateway currently reads X directly from the DB."* That's NOT a motivation — Rule 4 says api-gateway can read direct. A new gRPC RPC is motivated by ONE of:
1. A backend service (not api-gateway) needs the data
2. Decryption / derived value / audit / filter required
3. Future bounded-context extraction needs the surface to exist

If none of those three apply, you're building infra without a customer. Don't.

### Rule 5: Never share DB credentials between services
Each service gets its OWN Postgres role with minimal grants. Postgres enforces
the boundary. Code mistakes can't write to tables they shouldn't touch.

---

## Anti-patterns to reject in code review

| Smell                                | What to do instead                    |
|--------------------------------------|---------------------------------------|
| Service A reading Service B's table directly | gRPC RPC on Service B's API |
| Two services writing the same table  | One owner; others publish events / call RPCs |
| Synchronous HTTP call between Go services | Use gRPC (typed + faster) |
| gRPC call without timeout            | Always pass ctx with timeout |
| Kafka consumer that doesn't commit offsets | Commit after successful processing |
| Reading credentials/PII via direct DB | MUST go via owner's gRPC (audit + decrypt) |
| Direct DB write from api-gateway     | api-gateway is READ-only; writes go via gRPC |

---

## When in doubt, ask these questions

1. **"Does the caller need an answer to continue?"** → gRPC
2. **"Could the caller die after sending and the work still finish?"** → Kafka
3. **"Am I reading data I own, with no business logic?"** → Direct DB
4. **"Is this user code on a phone/browser?"** → HTTP/JSON via api-gateway

If none of these is a clear yes, **discuss with the team before writing code.**

---

## See also

- [data-ownership.md](data-ownership.md) — which service owns which table
- [database-redesign-plan.md](database-redesign-plan.md) — the DB migration plan
