# Kafka Topics Guide - Trading System

## Overview
This document explains the purpose and data format for each Kafka topic in the trading system.

---

## 📊 **Topic: market.data.news**

**Producer:** Data Ingestion Service  
**Consumer:** Rules Engine Service  
**Purpose:** Market news events with analysis and stock data

### Message Format
```json
{
  "event_id": "691ae1a12e50b0afe260781e",
  "timestamp": "2024-11-17T14:30:00Z",
  "news_data": {
    "title": "Company XYZ Reports Record Profits",
    "description": "Quarterly earnings exceed analyst expectations",
    "source": "Financial Times",
    "category": "earnings",
    "url": "https://example.com/news/xyz-earnings",
    "published_at": "2024-11-17T14:29:50Z"
  },
  "stock_data": {
    "stock_code": 500325,
    "symbol": "RELIANCE",
    "company_name": "Reliance Industries Ltd",
    "exchange": "NSE"
  },
  "analysis": {
    "impact_score": 8,
    "sentiment": "positive",
    "confidence": 0.92,
    "summary": "Strong earnings beat with positive outlook"
  },
  "market_data": {
    "last_traded_price": 2450.75,
    "price_change": 35.25,
    "price_change_percent": 1.46,
    "volume": 1250000,
    "day_high": 2455.00,
    "day_low": 2420.50
  }
}
```

### Key Fields
- `event_id`: Unique identifier for the event
- `impact_score`: 1-10, how significant the news is
- `sentiment`: positive, neutral, negative
- `stock_code`: BSE/NSE stock identifier
- `last_traded_price`: Current stock price

---

## 🎯 **Topic: trade-signals**

**Producer:** Rules Engine Service  
**Consumer:** Trade Execution Service, Risk Management Service  
**Purpose:** Generated trade orders when strategies match market events

### Message Format
```json
{
  "order_id": "550e8400-e29b-41d4-a716-446655440000",
  "user_id": "user-123",
  "strategy_id": "a403304f-f6b2-43e4-b5e7-699f8b5d2018",
  "strategy_name": "MATCH ALL NEWS",
  "event_id": "691ae1a12e50b0afe260781e",
  "stock_code": 500325,
  "symbol": "RELIANCE",
  "exchange": "NSE",
  "order_type": "MARKET",
  "quantity": 1,
  "price": 2450.75,
  "stop_loss": 2401.74,
  "take_profit": 2573.29,
  "match_score": 100.0,
  "impact_score": 8,
  "sentiment": "positive",
  "news_category": "earnings",
  "timestamp": "2024-11-17T14:30:01Z"
}
```

### Kafka Headers
- `order_id`: For partitioning and tracking
- `user_id`: For user-specific routing
- `strategy_id`: Which strategy generated this
- `event_id`: Source market event
- `order_type`: MARKET or LIMIT

### Usage
- **Trade Execution** reads this to place orders with broker
- **Risk Management** validates before execution
- **Analytics** tracks order generation patterns

---

## 👤 **Topic: user-config-events**

**Producer:** User Config Service  
**Consumer:** Rules Engine Service (Strategy Sync)  
**Purpose:** Strategy create/update/delete events for real-time sync

### Message Format
```json
{
  "event_type": "CREATE",
  "timestamp": "2024-11-17T14:25:00Z",
  "strategy": {
    "strategy_id": "a403304f-f6b2-43e4-b5e7-699f8b5d2018",
    "user_id": "user-123",
    "strategy_name": "MATCH ALL NEWS",
    "description": "Trade everything - no restrictions",
    "active": true,
    "conditions": {
      "match_all_news": false,
      "impact_score_threshold": 1,
      "sentiments": [],
      "categories": [],
      "stock_codes": [],
      "exchanges": ["NSE", "BSE"],
      "price_range": {
        "min_price": 0,
        "max_price": 999999999
      },
      "volume_threshold": 0,
      "pct_change_threshold": 0
    },
    "trade_config": {
      "order_type": "MARKET",
      "order_side": "BUY",
      "quantity": 1,
      "exchange": "NSE",
      "validity": "DAY"
    },
    "risk_limits": {
      "max_daily_trades": 100,
      "max_loss_per_day": 50000.00,
      "enable_risk_checks": true,
      "position_sizing": "FIXED"
    },
    "created_at": "2024-11-17T14:25:00Z",
    "updated_at": "2024-11-17T14:25:00Z"
  }
}
```

### Event Types
- `CREATE`: New strategy created
- `UPDATE`: Strategy modified
- `DELETE`: Strategy removed
- `ACTIVATE`: Strategy activated
- `DEACTIVATE`: Strategy deactivated

### Usage
Rules Engine subscribes to this topic and:
1. Updates strategy cache in Redis
2. Enables/disables matching for the strategy

---

## ✅ **Topic: risk-approvals** *(Future Use)*

**Producer:** Risk Management Service  
**Consumer:** Trade Execution Service  
**Purpose:** Risk-approved orders ready for execution

### Expected Message Format
```json
{
  "order_id": "550e8400-e29b-41d4-a716-446655440000",
  "approval_id": "risk-approval-12345",
  "user_id": "user-123",
  "approved": true,
  "risk_checks": {
    "daily_trade_limit": "PASSED",
    "daily_loss_limit": "PASSED",
    "position_size": "PASSED",
    "portfolio_exposure": "PASSED"
  },
  "approved_quantity": 1,
  "approved_price": 2450.75,
  "risk_score": 45,
  "timestamp": "2024-11-17T14:30:02Z"
}
```

### Purpose
- Risk Management validates orders from `trade-signals`
- If approved, publishes to `risk-approvals`
- Trade Execution only executes approved orders

---

## 🔄 **Topic: trade-executions** *(Future Use)*

**Producer:** Trade Execution Service  
**Consumer:** User Config Service, Analytics, Monitoring  
**Purpose:** Execution results and status updates

### Expected Message Format
```json
{
  "order_id": "550e8400-e29b-41d4-a716-446655440000",
  "execution_id": "exec-98765",
  "user_id": "user-123",
  "strategy_id": "a403304f-f6b2-43e4-b5e7-699f8b5d2018",
  "broker_order_id": "BROKER123456",
  "status": "EXECUTED",
  "execution_price": 2451.00,
  "execution_quantity": 1,
  "execution_time": "2024-11-17T14:30:05Z",
  "exchange": "NSE",
  "stock_code": 500325,
  "symbol": "RELIANCE",
  "fees": 12.50,
  "error_message": null
}
```

### Status Values
- `PENDING`: Order submitted to broker
- `SENT`: Confirmed received by broker
- `EXECUTED`: Order filled
- `PARTIALLY_FILLED`: Partial execution
- `FAILED`: Order rejected
- `CANCELLED`: Order cancelled

### Purpose
- Updates PostgreSQL `trade_signals` table status
- Notifies users of execution
- Analytics and reporting
- Performance tracking

---

## 📧 **Topic: order-updates** *(Future Use)*

**Producer:** Trade Execution Service  
**Consumer:** Notification Service, Frontend API  
**Purpose:** Real-time order status updates for users

### Expected Message Format
```json
{
  "order_id": "550e8400-e29b-41d4-a716-446655440000",
  "user_id": "user-123",
  "status": "EXECUTED",
  "message": "Order executed successfully at ₹2451.00",
  "execution_details": {
    "price": 2451.00,
    "quantity": 1,
    "time": "2024-11-17T14:30:05Z",
    "broker_ref": "BROKER123456"
  },
  "timestamp": "2024-11-17T14:30:05Z"
}
```

---

## 📈 **Topic: news-events** *(Currently Empty)*

**Note:** This topic exists but is currently unused. May be deprecated or repurposed.

**Potential Future Use:** Aggregated news events for analytics dashboard

---

## 🔧 **Current Data Flow**

```
┌─────────────────────┐
│ Data Ingestion      │
│ Service             │
└──────┬──────────────┘
       │
       │ publishes
       ▼
┌─────────────────────┐
│ market.data.news    │ ◄─── Market events with stock data
└──────┬──────────────┘
       │
       │ consumed by
       ▼
┌─────────────────────┐
│ Rules Engine        │
│ Service             │──────┐
└──────┬──────────────┘      │
       │                     │
       │ publishes           │ syncs from
       ▼                     ▼
┌─────────────────────┐ ┌──────────────────┐
│ trade-signals       │ │ user-config-events     │
└──────┬──────────────┘ └──────────────────┘
       │                     ▲
       │                     │ publishes
       │                     │
       ▼                ┌────┴────────────┐
┌─────────────────────┐│ User Config     │
│ RabbitMQ            ││ Service         │
│ (trade_orders)      │└─────────────────┘
└──────┬──────────────┘
       │
       │ consumed by
       ▼
┌─────────────────────┐
│ Trade Execution     │
│ Service             │
└─────────────────────┘
```

---

## 📝 **PostgreSQL Integration**

The `trade_signals` table in PostgreSQL tracks all orders:

```sql
SELECT order_id, user_id, strategy_name, symbol, 
       price, status, created_at
FROM trade_signals
WHERE user_id = 'user-123'
ORDER BY created_at DESC
LIMIT 10;
```

**Status Tracking:**
- Initial: `PENDING` (saved when order generated)
- After Kafka publish: remains `PENDING`
- After execution: updated to `EXECUTED` / `FAILED`

---

## 🚀 **Testing Topics**

### Check if topics have messages:
```bash
# market.data.news
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic market.data.news --from-beginning --max-messages 1

# trade-signals
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic trade-signals --from-beginning --max-messages 1

# user-config-events
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic user-config-events --from-beginning --max-messages 1
```

### Monitor real-time:
```bash
# Watch trade signals being generated
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic trade-signals --property print.headers=true
```

---

## 📊 **Topic Configuration**

All topics are configured with:
- **Partitions**: 3 (for parallel processing)
- **Replication Factor**: 1 (single broker setup)
- **Retention**: 7 days (168 hours)

To view topic details:
```bash
kafka-topics --bootstrap-server localhost:9092 --describe --topic trade-signals
```

---

## 🎯 **Summary**

| Topic | Producer | Consumer | Current Status |
|-------|----------|----------|----------------|
| **market.data.news** | Data Ingestion | Rules Engine | ✅ **ACTIVE** (63 messages) |
| **user-config-events** | User Config | Rules Engine | ✅ **ACTIVE** (5 messages) |
| **trade-signals** | Rules Engine | Trade Execution | ✅ **ACTIVE** (NEW!) |
| risk-approvals | Risk Management | Trade Execution | 🔜 Future |
| trade-executions | Trade Execution | Multiple | 🔜 Future |
| order-updates | Trade Execution | Notifications | 🔜 Future |
| news-events | - | - | ❌ Unused |

**Orders now flow through:**
1. ✅ PostgreSQL (trade_signals table)
2. ✅ Kafka (trade-signals topic)  
3. ✅ RabbitMQ (trade_orders queue)

Triple redundancy for reliability! 🎉
