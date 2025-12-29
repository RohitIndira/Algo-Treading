# Auto Square-Off System Architecture & Flow Diagrams

## System Architecture Diagram

```
┌────────────────────────────────────────────────────────────────────────┐
│                     Trade Execution Service                            │
│                                                                        │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │ SERVICE INITIALIZATION (main.go)                                 │  │
│  ├──────────────────────────────────────────────────────────────────┤  │
│  │ 1. Load Config (AUTO_SQUARE_OFF_TIME = 15:05)                    │  │
│  │ 2. Connect PostgreSQL                                            │  │
│  │ 3. Initialize Repositories (OrderRepo, CredsRepo)                │  │
│  │ 4. Create OrderExecutor                                          │  │
│  │ 5. Create AutoSquareOffScheduler ← **NEW**                       │  │
│  │ 6. Create Consumers (RabbitMQ, Kafka)                            │  │
│  │ 7. Create gRPC Server                                            │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                      ↓                                 │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │ CONCURRENT SERVICES (Running as Goroutines)                      │  │
│  ├──────────────────────────────────────────────────────────────────┤  │
│  │ ├─ Kafka Consumer     (consumes trade signals)                   │  │
│  │ ├─ RabbitMQ Consumer  (consumes order executions)                │  │
│  │ ├─ gRPC Server        (serves API requests)                      │  │
│  │ └─ AutoSquareOffScheduler ← **NOW RUNNING**                      │  │
│  │    ├─ Ticks every 1 minute                                       │  │
│  │    ├─ Check: Current time = Square-off time?                     │  │
│  │    └─ If YES → Execute square-off for all clients                │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
                               ↓
          ┌─────────────────────┴──────────────────────┐
          ↓                                            ↓
    ┌───────────────┐                        ┌──────────────────┐
    │  PostgreSQL   │                        │  Broker API      │
    │               │                        │  (Indira, ODIN)  │
    │ ┌─────────────┴──┐                    │                  │
    │ │ - orders      │                      │ - Place Orders   │
    │ │ - risk_limits │ ← Config here        │ - Check Status   │
    │ │ - positions   │                      │ - Get Fills      │
    │ └───────────────┘                      │                  │
    └───────────────┘                        └──────────────────┘
```

---

## Auto Square-Off Scheduler Timeline

```
TIME     EVENT
────     ──────────────────────────────────────────────────────────

09:15    Market Opens
         ├─ Kafka/RabbitMQ consumers receive orders
         └─ gRPC server handles API calls

14:00    Mid-session
         ├─ Orders being placed by strategies
         ├─ Positions accumulating
         └─ AutoSquareOffScheduler checking every minute

15:04    1 minute before square-off
         ├─ Last minute for new orders
         └─ Scheduler: "Next check will trigger square-off"

15:04:55 55 seconds before
         └─ Scheduler: Waiting...

15:05:00 ★ SQUARE-OFF TIME REACHED ★
         ├─ Scheduler detects: now == 15:05
         ├─ Is it weekday? YES
         └─ INITIATE SQUARE-OFF PROCESS

         PROCESS:
         ├─ Query: Get all INTRADAY positions
         │  └─ Filter: Status = FILLED or PARTIALLY_FILLED
         │
         ├─ For each position:
         │  ├─ User Alice: BUY 100 INFY → SELL 100 INFY
         │  ├─ User Bob:   SELL 50 TCS  → BUY 50 TCS
         │  └─ User Charlie: BUY 200 REL → SELL 150 REL
         │
         ├─ Create all reverse orders
         ├─ Execute all orders simultaneously
         └─ Log results

15:05:30 Square-off complete
         └─ All client positions closed

15:05:31 Resume normal operation
         ├─ Scheduler: "Next check in 29 minutes"
         └─ Consumers: Process new orders normally

15:30    Market Close
         ├─ No more orders accepted
         └─ End of trading day
```

---

## Per-Client Square-Off Flow

```
CLIENT STRATEGIES IN DATABASE:
┌─────────────────────────────────────────────────────────┐
│ Strategy 1: Alice (INFY trading)                       │
│ ├─ enable_auto_square_off: true                        │
│ └─ auto_square_off_time: "15:05" (uses global)         │
│                                                        │
│ Strategy 2: Bob (TCS trading)                          │
│ ├─ enable_auto_square_off: true                        │
│ └─ auto_square_off_time: "15:05" (uses global)         │
│                                                        │
│ Strategy 3: Charlie (Multi-stock)                      │
│ ├─ enable_auto_square_off: true                        │
│ └─ auto_square_off_time: "14:30" (OVERRIDE!)           │
└─────────────────────────────────────────────────────────┘

AT 14:30:
┌─────────────────────────────────────────────────────────┐
│ Scheduler checks time: 14:30 matches? YES               │
│ └─ Square off Charlie only (auto_square_off_time=14:30) │
└─────────────────────────────────────────────────────────┘

AT 15:05:
┌─────────────────────────────────────────────────────────┐
│ Scheduler checks time: 15:05 matches? YES               │
│ └─ Square off Alice & Bob (auto_square_off_time=15:05)  │
└─────────────────────────────────────────────────────────┘
```

---

## Order Processing Flow During Square-Off

```
┌──────────────────────────────────────────────────────────────┐
│ SQUARE-OFF INITIATED AT 15:05                               │
└────────────────┬─────────────────────────────────────────────┘
                 │
                 ▼
         ┌──────────────────┐
         │ Query Database:  │
         │ SELECT *         │
         │ FROM orders      │
         │ WHERE            │
         │  - product_type  │
         │    = 'INTRADAY'  │
         │  - status IN     │
         │    ('FILLED',    │
         │     'PARTIALLY') │
         └────────┬─────────┘
                  │
                  ▼
    ┌─────────────────────────────┐
    │ Found Orders:               │
    │ - ORDER_1 (Alice, BUY, 100) │
    │ - ORDER_2 (Bob, SELL, 50)   │
    │ - ORDER_3 (Charlie, BUY,150)│
    └──────────┬──────────────────┘
               │
        ┌──────┴──────────────────────────────┐
        │                                     │
        ▼                                     ▼
    ┌─────────────┐                  ┌─────────────┐
    │ ORDER_1     │                  │ ORDER_2     │
    │ ─────────── │                  │ ─────────── │
    │ Alice, BUY  │                  │ Bob, SELL   │
    │ INFY x 100  │                  │ TCS x 50    │
    │             │                  │             │
    │ Create      │                  │ Create      │
    │ Reverse:    │                  │ Reverse:    │
    │ ─────────── │                  │ ─────────── │
    │ SELL x 100  │                  │ BUY x 50    │
    │ MARKET, IOC │                  │ MARKET, IOC │
    └──────┬──────┘                  └──────┬──────┘
           │                                │
           └────────────┬────────────────────┘
                        │
                        ▼
            ┌─────────────────────┐
            │ Execute ALL Orders  │
            │ Simultaneously      │
            ├─────────────────────┤
            │ ORDER_1: SELL 100   │
            │ ORDER_2: BUY 50     │
            │ ORDER_3: SELL 150   │
            └──────────┬──────────┘
                       │
                       ▼
         ┌─────────────────────────┐
         │ Log Results             │
         ├─────────────────────────┤
         │ Success: 3              │
         │ Failed: 0               │
         │ Total: 3 positions      │
         │ Closed: 3               │
         └─────────────────────────┘
```

---

## Database Query Flow

```
┌────────────────────────────────────────────────────┐
│ PostgreSQL Query at Square-Off Time                │
├────────────────────────────────────────────────────┤
│                                                    │
│ Step 1: Find all INTRADAY open positions          │
│ ├─ Table: orders                                   │
│ ├─ Condition: product_type = 'INTRADAY'           │
│ └─ Filter: status IN ('FILLED', 'PARTIALLY_*')    │
│                                                    │
│ Step 2: For each matched order                    │
│ ├─ Get: UserID, StrategyID, Symbol                │
│ ├─ Get: OrderSide, FilledQuantity                 │
│ └─ Get: BearerToken, AppID (for broker)           │
│                                                    │
│ Step 3: Create reverse orders                     │
│ ├─ Same UserID                                    │
│ ├─ Same StrategyID                                │
│ ├─ Same Symbol                                    │
│ ├─ Opposite Side (BUY ↔ SELL)                     │
│ └─ Same FilledQuantity                            │
│                                                    │
│ Step 4: Save new orders                           │
│ ├─ Insert into orders table                       │
│ ├─ Status: RECEIVED                               │
│ └─ Source: SCHEDULER (auto square-off)            │
│                                                    │
│ Step 5: Execute via broker                        │
│ ├─ Send to Indira/ODIN API                        │
│ ├─ Wait for execution                             │
│ └─ Update order status                            │
│                                                    │
└────────────────────────────────────────────────────┘
```

---

## State Transitions During Square-Off

```
BEFORE SQUARE-OFF (14:50):
┌─────────────────────────────────────┐
│ Alice's Position                    │
├─────────────────────────────────────┤
│ Symbol: INFY                        │
│ Side: BUY                           │
│ Quantity: 100                       │
│ Status: FILLED                      │
│ FilledQuantity: 100                 │
│ UpdatedAt: 14:45                    │
└─────────────────────────────────────┘
           │
           │ (Scheduler runs at 15:05)
           ▼
DURING SQUARE-OFF (15:05):
┌─────────────────────────────────────┐
│ New Order Created                   │
├─────────────────────────────────────┤
│ Symbol: INFY                        │
│ Side: SELL ← REVERSED               │
│ Quantity: 100                       │
│ Status: RECEIVED                    │
│ Type: MARKET                        │
│ Validity: IOC                       │
│ RiskApproved: true                  │
│ CreatedAt: 15:05:00                 │
└─────────────────────────────────────┘
           │
           │ (Broker executes)
           ▼
AFTER SQUARE-OFF (15:05:30):
┌─────────────────────────────────────┐
│ Orders Summary                      │
├─────────────────────────────────────┤
│ Original Order                      │
│ ├─ BUY 100 INFY: FILLED            │
│ └─ Realized P&L: +5000 (example)    │
│                                    │
│ Reverse Order                       │
│ ├─ SELL 100 INFY: FILLED           │
│ └─ Price: Market price at 15:05    │
│                                    │
│ Net Position: 0                     │
│ Status: SQUARED OFF ✓               │
└─────────────────────────────────────┘
```

---

## Scheduler State Machine

```
┌──────────────────────────────────────────────────────────┐
│                 SCHEDULER STATE MACHINE                  │
└──────────────────────────────────────────────────────────┘

                    ┌─────────────┐
                    │   WAITING   │
                    └──────┬──────┘
                           │
                ┌──────────┴──────────┐
                │                    │
           Every 1 min          Service Stop Signal
                │                    │
                ▼                    ▼
           ┌──────────┐      ┌─────────────┐
           │CHECKING  │      │  STOPPING   │
           └────┬─────┘      └──────┬──────┘
                │                   │
    ┌───────────┴───────────┐       │
    │                       │       │
Is time = Is weekday?      │       │
    │        &             │       │
    │    enabled?           │       │
    │                       │       │
   YES                     NO      │
    │                       │       │
    ▼                       ▼       │
┌─────────────┐      ┌─────────┐   │
│ EXECUTING   │      │ SLEEPING │   │
│ SQUARE-OFF  │      │ 1 min    │   │
└──────┬──────┘      └────┬────┘   │
       │                  │        │
       │            (loop back)    │
       │                           │
       └───────────────────┬───────┤
                           │       │
                           ▼       ▼
                        ┌──────────────┐
                        │    STOPPED   │
                        │ (gracefully) │
                        └──────────────┘
```

---

## Logging Timeline

```
TIME       LOG MESSAGE
────       ─────────────────────────────────────────────────────

09:15:00   ✓ Connected to PostgreSQL
09:15:01   ✓ Repository layer initialized
09:15:02   ✓ Indira API client initialized
09:15:03   ✓ Order executor initialized
09:15:04   ✓ Auto Square-Off scheduler initialized    ← NEW
09:15:05   Connecting to RabbitMQ...
09:15:06   ✓ RabbitMQ consumer initialized
09:15:07   Initializing Kafka consumer...
09:15:08   ✓ Kafka consumer initialized
09:15:09   ✓ gRPC server initialized
09:15:10   Starting Kafka consumer...
09:15:11   Starting RabbitMQ consumer...
09:15:12   Starting Auto Square-Off Scheduler...    ← NEW
09:15:13   Auto Square-Off Scheduler (Time: 15:05)  ← NEW
09:15:13   Starting gRPC server...
09:15:14   ========================================
09:15:14   ✓ Trade Execution Service Started
09:15:14     - gRPC Server: localhost:9004
09:15:14     - RabbitMQ Queue: trade.executions
09:15:14     - Kafka Topic: trade-signals
09:15:14     - Workers: 10
09:15:14     - Auto Square-Off Time: 15:05        ← NEW
09:15:14   ========================================

14:59:00   (Normal operations, orders being placed)

15:05:00   Auto Square-Off Time Reached - Initiating...  ← KEY
15:05:01   Found 3 open orders to square off            ← KEY
15:05:02   Squaring off ORDER_1 for USER_A (INFY, 100)
15:05:03   Successfully created and executed square...
15:05:04   Squaring off ORDER_2 for USER_B (TCS, 50)
15:05:05   Successfully created and executed square...
15:05:06   Squaring off ORDER_3 for USER_C (REL, 150)
15:05:07   Successfully created and executed square...
15:05:08   === Auto Square-Off: Complete ===          ← KEY
15:05:08   (Success: 3, Failed: 0)                    ← KEY

15:30:00   (Market close, no more orders)
```

---

## Risk Management During Square-Off

```
┌──────────────────────────────────────────────────────────┐
│ RISK CHECKS - AUTO SQUARE-OFF BYPASS                     │
├──────────────────────────────────────────────────────────┤
│                                                          │
│ Normal Order:                                            │
│ ├─ Check: Max daily loss                                 │
│ ├─ Check: Portfolio exposure                             │
│ ├─ Check: Per-trade risk limit                           │
│ ├─ Check: Daily trade count                              │
│ └─ Result: May be REJECTED if limits exceeded            │
│                                                          │
│ Auto Square-Off Order:                                   │
│ ├─ Risk Check: BYPASSED ✓                                │
│ ├─ Reason: Closing position, not opening                 │
│ ├─ Flag: RiskApproved = true                             │
│ └─ Result: ALWAYS executed                               │
│                                                          │
│ Benefits:                                                │
│ ├─ Ensures positions are closed                          │
│ ├─ No risk check delays                                  │
│ ├─ Market impact minimized                               │
│ └─ Client protection maintained                          │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

---

## Environment & Configuration Diagram

```
┌─────────────────────────────────────────────────────────┐
│                    CONFIGURATION SOURCES                │
├─────────────────────────────────────────────────────────┤
│                                                         │
│ Priority 1 (Highest): Database (Per-Strategy)           │
│ ├─ Table: risk_limits                                   │
│ ├─ Column: auto_square_off_time                         │
│ └─ Example: "14:30" (override global)                   │
│           ↑                                             │
│           │ (queried per order during square-off)       │
│           │                                             │
│ Priority 2 (Middle): Environment Variable               │
│ ├─ Key: AUTO_SQUARE_OFF_TIME                            │
│ ├─ Source: .env file                                    │
│ └─ Example: "15:05"                                     │
│           ↑                                             │
│           │ (loaded on service startup)                 │
│           │                                             │
│ Priority 3 (Default): Hard-coded Default                │
│ ├─ Value: "15:05"                                       │
│ ├─ Source: auto_square_off.go line 31                   │
│ └─ Used: If env var not set                             │
│                                                         │
│ Final Config Used:                                      │
│ ├─ Database value IF exists                             │
│ ├─ ELSE Environment value IF set                        │
│ └─ ELSE Default "15:05"                                 │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

---

## Summary

This system provides:

✅ **Automatic Position Closure** - All clients' positions close at configured time
✅ **Flexible Configuration** - Global default + per-strategy override
✅ **Concurrent Execution** - All client positions squared off simultaneously
✅ **Risk Bypass** - Ensures execution regardless of risk limits
✅ **Comprehensive Logging** - Full audit trail of square-offs
✅ **Production Ready** - Minimal resource usage, graceful shutdown

