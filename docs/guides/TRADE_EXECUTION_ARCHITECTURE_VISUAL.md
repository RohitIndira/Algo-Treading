# Trade Execution Service - Visual Architecture Guide

## 📐 System Architecture Diagrams

### 1. High-Level System Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         ALGORITHMIC TRADING SYSTEM                       │
└─────────────────────────────────────────────────────────────────────────┘

┌──────────────┐         ┌──────────────┐         ┌──────────────┐
│   MongoDB    │────────▶│Data Ingestion│────────▶│    Kafka     │
│ (News Data)  │  Watch  │   Service    │  Publish│  (Events)    │
└──────────────┘         └──────────────┘         └──────────────┘
                                                           │
                                                           │ Consume
                                                           ▼
                                                   ┌──────────────┐
                                                   │Rules Engine  │
                                                   │(Match Logic) │
                                                   └──────────────┘
                                                           │
                                                           │ Order Signal
                                                           ▼
┌──────────────┐         ┌──────────────┐         ┌──────────────┐
│ Risk Mgmt    │◀───────│  RabbitMQ    │◀────────│Rules Engine  │
│  Service     │  Check  │ (Order Queue)│  Publish│              │
└──────────────┘         └──────────────┘         └──────────────┘
       │                         │
       │ Approved                │ Consume
       │                         ▼
       │                 ┌──────────────────┐
       │                 │ TRADE EXECUTION  │
       │                 │    SERVICE       │
       │                 └──────────────────┘
       │                         │
       │                         │ Place Order
       │                         ▼
       │                 ┌──────────────┐
       │                 │   Odin API   │
       │                 │   (Broker)   │
       │                 └──────────────┘
       │                         │
       │ Update Metrics          │ Order Status
       └─────────────────────────┘
```

---

### 2. Trade Execution Service - Internal Architecture

```
┌────────────────────────────────────────────────────────────────────┐
│                     TRADE EXECUTION SERVICE                         │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │                    CONSUMER LAYER                           │  │
│  │                                                             │  │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  │  │
│  │  │ Worker 1 │  │ Worker 2 │  │ Worker 3 │  │ Worker N │  │  │
│  │  └─────┬────┘  └─────┬────┘  └─────┬────┘  └─────┬────┘  │  │
│  │        │             │             │             │        │  │
│  │        └─────────────┴─────────────┴─────────────┘        │  │
│  │                          │                                 │  │
│  └──────────────────────────┼─────────────────────────────────┘  │
│                             ▼                                     │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │                   BUSINESS LOGIC LAYER                      │  │
│  │                                                             │  │
│  │  ┌───────────────┐     ┌───────────────┐                  │  │
│  │  │   Validator   │     │Order Executor │                  │  │
│  │  │               │────▶│               │                  │  │
│  │  │ • Validate    │     │ • Execute     │                  │  │
│  │  │ • Transform   │     │ • Retry Logic │                  │  │
│  │  │ • Duplicate   │     │ • Status Poll │                  │  │
│  │  │   Detection   │     │               │                  │  │
│  │  └───────────────┘     └───────┬───────┘                  │  │
│  │                                │                           │  │
│  └────────────────────────────────┼───────────────────────────┘  │
│                                   ▼                               │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │                   INTEGRATION LAYER                         │  │
│  │                                                             │  │
│  │  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐   │  │
│  │  │Odin Client  │    │  Repository │    │Redis Cache  │   │  │
│  │  │             │    │             │    │             │   │  │
│  │  │• PlaceOrder │    │• Create     │    │• Dedup      │   │  │
│  │  │• GetStatus  │    │• Update     │    │• RateLimit  │   │  │
│  │  │• Cancel     │    │• Query      │    │• Status     │   │  │
│  │  └──────┬──────┘    └──────┬──────┘    └──────┬──────┘   │  │
│  │         │                  │                   │          │  │
│  └─────────┼──────────────────┼───────────────────┼──────────┘  │
│            │                  │                   │              │
└────────────┼──────────────────┼───────────────────┼──────────────┘
             │                  │                   │
             ▼                  ▼                   ▼
      ┌──────────┐      ┌──────────────┐    ┌──────────┐
      │Odin API  │      │ PostgreSQL   │    │  Redis   │
      └──────────┘      └──────────────┘    └──────────┘

      ┌─────────────────────────────────────────────────────────┐
      │              gRPC SERVER (Port 9004)                    │
      │                                                         │
      │  • GetOrderStatus    • GetUserOrders                   │
      │  • CancelOrder       • GetOrderHistory                 │
      │  • ModifyOrder       • GetOrderStatistics              │
      └─────────────────────────────────────────────────────────┘
```

---

### 3. Order Lifecycle State Machine

```
                    ┌─────────────┐
                    │  RECEIVED   │  ◀── Order arrives from RabbitMQ
                    └──────┬──────┘
                           │
                           │ Validation
                           ▼
                    ┌─────────────┐
                    │  VALIDATED  │  ◀── Request validated
                    └──────┬──────┘
                           │
                           │ Risk Check
                           ▼
                    ┌─────────────┐
                    │   PENDING   │  ◀── Awaiting execution
                    └──────┬──────┘
                           │
                           │ Submit to Odin
                           ▼
                    ┌─────────────┐
                    │  SUBMITTED  │  ◀── Sent to broker
                    └──────┬──────┘
                           │
                           │ Order fills
                           │
              ┌────────────┼────────────┐
              │            │            │
              ▼            ▼            ▼
       ┌────────────┐ ┌────────┐ ┌──────────┐
       │  PARTIALLY │ │ FILLED │ │ REJECTED │
       │   FILLED   │ │        │ │          │
       └────────────┘ └────────┘ └──────────┘
              │                         ▲
              │ Continue filling        │
              └─────────────────────────┘
                                       
       User Cancel ──────┐
                         ▼
                  ┌─────────────┐
                  │  CANCELLED  │
                  └─────────────┘
                  
       System Error ──────┐
                          ▼
                   ┌─────────────┐
                   │   FAILED    │
                   └─────────────┘
```

---

### 4. Order Processing Flow (Detailed)

```
┌─────────────────────────────────────────────────────────────────────┐
│  Step 1: Order Intake                                               │
└─────────────────────────────────────────────────────────────────────┘

Rules Engine ───▶ Publish to RabbitMQ
                        │
                        │ {
                        │   "user_id": "user123",
                        │   "stock_code": 517170,
                        │   "order_type": "MARKET",
                        │   "quantity": 100,
                        │   "risk_approved": true
                        │ }
                        │
                        ▼
                 ┌─────────────┐
                 │ RabbitMQ    │
                 │ Queue       │
                 └──────┬──────┘
                        │
                        │ Consumer pulls (Prefetch: 10)
                        ▼
                 ┌─────────────┐
                 │ Worker Pool │
                 │ (10 workers)│
                 └──────┬──────┘
                        │
                        ▼

┌─────────────────────────────────────────────────────────────────────┐
│  Step 2: Validation & Persistence                                   │
└─────────────────────────────────────────────────────────────────────┘

                 ┌─────────────┐
                 │  Validator  │
                 └──────┬──────┘
                        │
        ┌───────────────┼───────────────┐
        │               │               │
        ▼               ▼               ▼
  Check Fields    Risk Approved?   Duplicate Check
  Valid?          Yes/No           (Redis)
        │               │               │
        └───────────────┴───────────────┘
                        │
                    Valid ✓
                        │
                        ▼
                 ┌─────────────┐
                 │  PostgreSQL │
                 │  INSERT     │
                 │  Status:    │
                 │  RECEIVED   │
                 └──────┬──────┘
                        │
                        ▼

┌─────────────────────────────────────────────────────────────────────┐
│  Step 3: Order Execution                                            │
└─────────────────────────────────────────────────────────────────────┘

                 ┌─────────────┐
                 │  Executor   │
                 └──────┬──────┘
                        │
                        ├──▶ Update Status: PENDING
                        │
                        ├──▶ Build Odin Request
                        │    {
                        │      "scrip_info": {...},
                        │      "transaction_type": "BUY",
                        │      "order_type": "MKT",
                        │      "quantity": 100
                        │    }
                        │
                        ▼
                 ┌─────────────┐
                 │  Odin API   │  POST /api/v1/orders
                 │  Client     │
                 └──────┬──────┘
                        │
                ┌───────┴───────┐
                │               │
            Success         Failure
                │               │
                ▼               ▼
         ┌──────────┐    ┌──────────┐
         │ Response │    │  Retry   │
         │ OrderID: │    │ (3 times)│
         │ ODN12345 │    └────┬─────┘
         └────┬─────┘         │
              │               │
              │               └──▶ Update Status: FAILED
              │
              ▼
       Update PostgreSQL
       Status: SUBMITTED
       odin_order_id: ODN12345
              │
              ▼

┌─────────────────────────────────────────────────────────────────────┐
│  Step 4: Status Polling                                             │
└─────────────────────────────────────────────────────────────────────┘

         ┌──────────────┐
         │Status Poller │  (Background worker)
         │ (Every 5s)   │
         └──────┬───────┘
                │
                ├──▶ Query orders with Status: SUBMITTED
                │
                ├──▶ For each order:
                │     GET /api/v1/orders/{odin_order_id}/status
                │
                ▼
         ┌──────────────┐
         │  Odin API    │
         │  Response    │
         └──────┬───────┘
                │
                │ {
                │   "status": "FILLED",
                │   "filled_quantity": 100,
                │   "filled_price": 2505.75,
                │   "commission": 25.50
                │ }
                │
                ▼
         ┌──────────────┐
         │   Update DB  │
         │   Status:    │
         │   FILLED     │
         └──────┬───────┘
                │
                ▼
         ┌──────────────┐
         │Notify Risk   │
         │Management    │
         │(gRPC call)   │
         └──────────────┘
```

---

### 5. Database Schema Relationships

```
┌───────────────────────────────────────────────────┐
│                  orders                           │
├───────────────────────────────────────────────────┤
│ PK  order_id          UUID                        │
│     user_id           VARCHAR(50)                 │
│     strategy_id       VARCHAR(50)                 │
│     event_id          UUID                        │
│     stock_code        BIGINT                      │
│     exchange          VARCHAR(10)                 │
│     symbol            VARCHAR(50)                 │
│     order_type        VARCHAR(10)                 │
│     order_side        VARCHAR(10)                 │
│     quantity          INT                         │
│     price             DECIMAL(15,2)               │
│     stop_loss         DECIMAL(15,2)               │
│     take_profit       DECIMAL(15,2)               │
│     validity          VARCHAR(10)                 │
│     status            VARCHAR(20)                 │
│     odin_order_id     VARCHAR(50)                 │
│     odin_response     TEXT                        │
│     filled_quantity   INT                         │
│     filled_price      DECIMAL(15,2)               │
│     commission        DECIMAL(10,2)               │
│     total_cost        DECIMAL(15,2)               │
│     risk_approved     BOOLEAN                     │
│     risk_score        DECIMAL(5,2)                │
│     created_at        TIMESTAMP                   │
│     updated_at        TIMESTAMP                   │
│     submitted_at      TIMESTAMP                   │
│     executed_at       TIMESTAMP                   │
│     error_message     TEXT                        │
│     rejection_reason  TEXT                        │
│     retry_count       INT                         │
│     metadata          JSONB                       │
└───────────────┬───────────────────────────────────┘
                │
                │ 1:N relationship
                │
                ▼
┌───────────────────────────────────────────────────┐
│            execution_events                       │
├───────────────────────────────────────────────────┤
│ PK  id                SERIAL                      │
│ FK  order_id          UUID  ───────────────┐      │
│     event_type        VARCHAR(20)          │      │
│     event_data        JSONB                │      │
│     created_at        TIMESTAMP            │      │
└─────────────────────────────────────────────┬─────┘
                                              │
                                              │
                        References orders.order_id

Indexes:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
• idx_orders_user_id (user_id, created_at DESC)
• idx_orders_status (status, created_at DESC)
• idx_orders_event_id (event_id)
• idx_orders_odin_id (odin_order_id)
• idx_orders_strategy_id (strategy_id)
• idx_orders_stock_code (stock_code)
• idx_execution_events_order_id (order_id)
• idx_execution_events_created_at (created_at DESC)
```

---

### 6. Integration Points

```
┌────────────────────────────────────────────────────────────────┐
│              TRADE EXECUTION SERVICE INTEGRATIONS               │
└────────────────────────────────────────────────────────────────┘

┌──────────────┐
│ Rules Engine │
│              │
│ Publishes:   │
│ - OrderReq   │
└──────┬───────┘
       │ RabbitMQ
       │ Queue: order.execution.queue
       │ Exchange: order.execution.exchange
       │ Routing Key: order.execution
       │
       ▼
┌────────────────┐
│Trade Execution │ ◀───────┐
│   Service      │         │
└────────┬───────┘         │
         │                 │ gRPC Call
         │                 │ CheckPreTradeRisk
         │                 │
         ├─────────────────┤
         │                 │
         ▼                 │
┌────────────────┐         │
│ Risk Mgmt      │─────────┘
│ Service        │
│                │
│ Updates:       │
│ - Trade counts │
│ - P&L metrics  │
│ - Positions    │
└────────────────┘

┌────────────────┐
│Trade Execution │
│   Service      │
└────────┬───────┘
         │
         │ HTTP REST API
         │ POST /api/v1/orders
         │ GET  /api/v1/orders/{id}/status
         │
         ▼
┌────────────────┐
│   Odin API     │
│   (Broker)     │
│                │
│ Returns:       │
│ - Order ID     │
│ - Status       │
│ - Fills        │
└────────────────┘

┌────────────────┐
│API Gateway     │
│                │
│ Calls:         │
│ - GetOrder     │
│ - ListOrders   │
│ - CancelOrder  │
└────────┬───────┘
         │
         │ gRPC (Port 9004)
         │
         ▼
┌────────────────┐
│Trade Execution │
│   Service      │
│   gRPC Server  │
└────────────────┘
```

---

### 7. Error Handling & Retry Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    ERROR HANDLING FLOW                           │
└─────────────────────────────────────────────────────────────────┘

Order Processing
      │
      ├──▶ Validation Error ──▶ NACK (Don't Requeue) ──▶ DLQ
      │                          Log Error
      │
      ├──▶ Database Error ───▶ NACK (Requeue) ──▶ Retry
      │                         (Transient)
      │
      ├──▶ Odin API Error
      │         │
      │         ├──▶ 4xx Client Error ──▶ REJECT ──▶ Update Status
      │         │                                    Don't Retry
      │         │
      │         └──▶ 5xx Server Error
      │                   │
      │                   ├──▶ Retry 1 (1s delay)
      │                   ├──▶ Retry 2 (2s delay)
      │                   ├──▶ Retry 3 (4s delay)
      │                   │
      │                   └──▶ All Failed ──▶ Update Status: FAILED
      │                                       Send to DLQ
      │
      └──▶ Success ──▶ ACK ──▶ Remove from Queue


┌─────────────────────────────────────────────────────────────────┐
│                    DEAD LETTER QUEUE (DLQ)                       │
└─────────────────────────────────────────────────────────────────┘

      Failed Messages
            │
            ▼
      ┌──────────┐
      │   DLQ    │  Manual Review Required
      └────┬─────┘
           │
           ├──▶ Alert Ops Team
           ├──▶ Log to Database
           └──▶ Manual Intervention
```

---

### 8. Performance & Scalability

```
┌─────────────────────────────────────────────────────────────────┐
│                    SCALABILITY STRATEGY                          │
└─────────────────────────────────────────────────────────────────┘

                    Load Balancer
                         │
         ┌───────────────┼───────────────┐
         │               │               │
         ▼               ▼               ▼
    ┌────────┐      ┌────────┐      ┌────────┐
    │Instance│      │Instance│      │Instance│
    │   1    │      │   2    │      │   3    │
    │10 Workers│    │10 Workers│    │10 Workers│
    └────┬───┘      └────┬───┘      └────┬───┘
         │               │               │
         └───────────────┴───────────────┘
                         │
                         │ Shared Infrastructure
                         │
         ┌───────────────┼───────────────┐
         │               │               │
         ▼               ▼               ▼
    ┌────────┐      ┌────────┐      ┌────────┐
    │RabbitMQ│      │  PG    │      │ Redis  │
    │        │      │(Primary│      │Cluster │
    │ Queue  │      │+Replica│      │        │
    └────────┘      └────────┘      └────────┘


Performance Targets:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
• Order Intake:     1000 orders/sec
• Processing Time:  < 500ms (p95)
• DB Query Time:    < 10ms (p95)
• Odin API Call:    < 300ms (p95)
• Queue Depth:      < 1000 messages
• Worker Util:      60-80%
```

---

### 9. Monitoring Dashboard Layout

```
┌─────────────────────────────────────────────────────────────────┐
│              TRADE EXECUTION SERVICE DASHBOARD                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐   │
│  │ Orders/Min     │  │ Success Rate   │  │ Avg Latency    │   │
│  │    1,245       │  │    98.5%       │  │    325ms       │   │
│  └────────────────┘  └────────────────┘  └────────────────┘   │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │            Order Status Distribution                      │  │
│  │  ██████████ FILLED      (65%)                            │  │
│  │  ████ SUBMITTED         (20%)                            │  │
│  │  ███ PENDING            (10%)                            │  │
│  │  █ REJECTED             (4%)                             │  │
│  │  █ FAILED               (1%)                             │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │         Processing Time (p50, p95, p99)                  │  │
│  │  [Line graph showing latency over time]                  │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                  │
│  ┌─────────────────────┐  ┌─────────────────────────────────┐ │
│  │  RabbitMQ Queue     │  │  Odin API Errors                │ │
│  │  Depth: 245         │  │  Rate: 0.5%                     │ │
│  │  [Graph]            │  │  [Error types]                  │ │
│  └─────────────────────┘  └─────────────────────────────────┘ │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │         Recent Failed Orders (DLQ)                        │  │
│  │  Order ID      | User    | Error         | Timestamp     │  │
│  │  uuid-1234...  | user123 | Validation    | 10:23:45     │  │
│  │  uuid-5678...  | user456 | Odin Timeout  | 10:22:10     │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

---

## 📊 Quick Reference Tables

### Message Queue Configuration

| Parameter | Value | Purpose |
|-----------|-------|---------|
| Queue Name | order.execution.queue | Main order queue |
| Prefetch Count | 10 | Messages per worker |
| Worker Count | 10 | Concurrent processors |
| DLQ | order.execution.dlq | Failed messages |
| TTL | 24 hours | Message expiry |
| Max Priority | 10 | Priority ordering |

### Database Connection Pool

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| Max Open Conns | 25 | Optimal for 10 workers |
| Max Idle Conns | 5 | Balance resources |
| Conn Lifetime | 5 min | Prevent stale conns |
| Timeout | 30s | Query timeout |

### Odin API Settings

| Parameter | Value | Purpose |
|-----------|-------|---------|
| Base URL | Configured | Broker endpoint |
| Timeout | 30s | HTTP timeout |
| Max Retries | 3 | Failure handling |
| Retry Delay | 1s, 2s, 4s | Exponential backoff |
| Circuit Breaker | 60s | Fail fast |

---

This visual guide provides a comprehensive view of the Trade Execution Service architecture, workflows, and integrations!
