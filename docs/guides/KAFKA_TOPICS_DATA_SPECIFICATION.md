# Kafka Topics - Detailed Data Specifications

## 🔄 **Topic: trade-executions**

### Purpose
Published by **Trade Execution Service** after executing orders with the broker. This is the authoritative source of execution results.

### **Full Message Specification**

```json
{
  // Order Identification
  "execution_id": "exec-550e8400-e29b-41d4-a716-446655440000",
  "order_id": "550e8400-e29b-41d4-a716-446655440000",
  "signal_id": "signal-uuid-from-postgres",
  
  // User & Strategy Info
  "user_id": "user-123",
  "strategy_id": "a403304f-f6b2-43e4-b5e7-699f8b5d2018",
  "strategy_name": "MATCH ALL NEWS",
  
  // Broker Information
  "broker_order_id": "ODIN_ORD_20241117_123456",
  "broker_name": "ODIN_TRADING",
  "exchange_order_id": "NSE_ORDER_789012",
  
  // Execution Status
  "status": "EXECUTED",  // EXECUTED, PARTIALLY_FILLED, FAILED, REJECTED, CANCELLED
  "status_message": "Order executed successfully",
  
  // Stock Details
  "stock_code": 500325,
  "symbol": "RELIANCE",
  "company_name": "Reliance Industries Ltd",
  "exchange": "NSE",
  
  // Order Details (Original)
  "order_type": "MARKET",
  "order_side": "BUY",
  "requested_quantity": 1,
  "requested_price": 2450.75,
  
  // Execution Details (Actual)
  "executed_quantity": 1,
  "executed_price": 2451.00,
  "average_price": 2451.00,
  "total_value": 2451.00,
  
  // Partial Fill Details (if applicable)
  "fills": [
    {
      "fill_id": "fill-001",
      "quantity": 1,
      "price": 2451.00,
      "timestamp": "2024-11-17T14:30:05.123Z"
    }
  ],
  
  // Financial Details
  "brokerage": 12.25,
  "exchange_charges": 2.45,
  "gst": 2.61,
  "stt": 2.45,
  "stamp_duty": 0.25,
  "total_charges": 19.76,
  "net_amount": 2470.76,
  
  // Timestamps
  "order_placed_at": "2024-11-17T14:30:01.500Z",
  "execution_time": "2024-11-17T14:30:05.123Z",
  "broker_timestamp": "2024-11-17T14:30:05.100Z",
  
  // Related Information
  "event_id": "691ae1a12e50b0afe260781e",
  "news_category": "earnings",
  "impact_score": 8,
  "match_score": 100.0,
  
  // Error Information (if failed)
  "error_code": null,
  "error_message": null,
  "rejection_reason": null,
  
  // Metadata
  "metadata": {
    "latency_ms": 3623,
    "retry_count": 0,
    "api_version": "v1.0",
    "execution_venue": "NSE_MAIN"
  }
}
```

### **Kafka Headers**
```
order_id: 550e8400-e29b-41d4-a716-446655440000
user_id: user-123
status: EXECUTED
exchange: NSE
timestamp: 2024-11-17T14:30:05.123Z
```

### **Status Values**
- `SENT_TO_BROKER`: Order submitted to broker
- `BROKER_ACCEPTED`: Broker confirmed receipt
- `PENDING_EXCHANGE`: Waiting for exchange confirmation
- `PARTIALLY_FILLED`: Part of order executed
- `EXECUTED`: Fully executed
- `FAILED`: Technical failure
- `REJECTED`: Broker/exchange rejected
- `CANCELLED`: User/system cancelled
- `EXPIRED`: Order expired (DAY validity)

### **Who Consumes This?**
1. **Rules Engine** - Updates PostgreSQL `trade_signals` table status
2. **User Config Service** - Updates user order history
3. **Analytics Service** - Performance tracking
4. **Notification Service** - Triggers alerts
5. **Risk Management** - Updates position tracking

---

## 📧 **Topic: order-updates**

### Purpose
Published by **Trade Execution Service** for real-time user notifications. Simpler, user-friendly format.

### **Full Message Specification**

```json
{
  // Order Identification
  "update_id": "upd-550e8400-e29b-41d4-a716-446655440001",
  "order_id": "550e8400-e29b-41d4-a716-446655440000",
  "user_id": "user-123",
  
  // Update Type
  "update_type": "EXECUTION_SUCCESS",  // See types below
  "priority": "HIGH",  // HIGH, MEDIUM, LOW
  
  // User-Friendly Message
  "title": "Order Executed ✓",
  "message": "Your RELIANCE buy order executed at ₹2,451.00",
  "detailed_message": "Successfully bought 1 share of Reliance Industries at ₹2,451.00 on NSE. Total amount: ₹2,470.76 (including charges)",
  
  // Status Display
  "status": "EXECUTED",
  "status_emoji": "✅",
  "status_color": "#00C851",  // Green for success
  
  // Order Summary (User-Friendly)
  "order_summary": {
    "stock": "RELIANCE (Reliance Industries Ltd)",
    "action": "BUY",
    "quantity": 1,
    "price": "₹2,451.00",
    "exchange": "NSE",
    "order_type": "MARKET"
  },
  
  // Execution Details (if applicable)
  "execution_details": {
    "executed_at": "2024-11-17T14:30:05.123Z",
    "executed_price": "₹2,451.00",
    "total_amount": "₹2,470.76",
    "charges": "₹19.76",
    "broker_ref": "ODIN_ORD_20241117_123456"
  },
  
  // Strategy Context
  "strategy": {
    "id": "a403304f-f6b2-43e4-b5e7-699f8b5d2018",
    "name": "MATCH ALL NEWS",
    "trigger": "Earnings announcement detected"
  },
  
  // Action Items for User
  "actions": [
    {
      "action_type": "VIEW_POSITION",
      "label": "View Position",
      "url": "/portfolio/positions/RELIANCE"
    },
    {
      "action_type": "VIEW_ORDER_HISTORY",
      "label": "Order Details",
      "url": "/orders/550e8400-e29b-41d4-a716-446655440000"
    }
  ],
  
  // Notification Preferences
  "notification_channels": {
    "push": true,
    "email": true,
    "sms": false,
    "in_app": true
  },
  
  // Timestamps
  "created_at": "2024-11-17T14:30:05.200Z",
  "expires_at": "2024-11-17T15:30:05.200Z"  // For in-app notifications
}
```

### **Update Types**
```javascript
// Success States
"ORDER_PLACED"          // Order sent to broker
"ORDER_CONFIRMED"       // Broker confirmed
"PARTIALLY_FILLED"      // Partial execution
"EXECUTION_SUCCESS"     // Fully executed
"ORDER_CANCELLED"       // Successfully cancelled

// Failure States  
"ORDER_REJECTED"        // Broker rejected
"EXECUTION_FAILED"      // Technical failure
"INSUFFICIENT_FUNDS"    // Margin issues
"POSITION_LIMIT_EXCEEDED"
"CIRCUIT_LIMIT_HIT"     // Stock circuit breaker

// Warning States
"SLOW_EXECUTION"        // Taking longer than expected
"PRICE_DEVIATION"       // Executed at significantly different price
"HIGH_VOLATILITY"       // Market conditions warning
```

### **Kafka Headers**
```
update_type: EXECUTION_SUCCESS
user_id: user-123
priority: HIGH
notification_channel: push,email,in_app
timestamp: 2024-11-17T14:30:05.200Z
```

### **Who Consumes This?**
1. **Notification Service** - Sends push notifications, emails, SMS
2. **Frontend API / WebSocket** - Real-time updates to user dashboard
3. **Mobile App** - Push notifications
4. **Analytics** - User engagement tracking

---

## 🔄 **Complete Data Flow Diagram**

```
┌──────────────────┐
│ Rules Engine     │
│                  │
└────────┬─────────┘
         │
         │ publishes
         ▼
┌──────────────────────┐
│ trade-signals        │ ◄─── New order generated
└────────┬─────────────┘
         │
         │ consumed by
         ▼
┌──────────────────────┐
│ Trade Execution      │
│ Service              │
└────────┬────────┬────┘
         │        │
         │        │ publishes
         ▼        ▼
┌────────────┐  ┌─────────────────┐
│trade-      │  │order-updates    │
│executions  │  │                 │
└──────┬─────┘  └────────┬────────┘
       │                 │
       │                 │
       ▼                 ▼
┌─────────────┐   ┌──────────────────┐
│Rules Engine │   │Notification      │
│(Update DB)  │   │Service           │
│             │   │(Send to users)   │
└─────────────┘   └──────────────────┘
```

---

## 📝 **Example: Complete Order Lifecycle**

### **Step 1: Order Generated (trade-signals)**
```json
{
  "order_id": "ord-001",
  "user_id": "user-123",
  "symbol": "RELIANCE",
  "order_type": "MARKET",
  "quantity": 1,
  "status": "PENDING"
}
```

### **Step 2: Execution Result (trade-executions)**
```json
{
  "execution_id": "exec-001",
  "order_id": "ord-001",
  "user_id": "user-123",
  "status": "EXECUTED",
  "executed_price": 2451.00,
  "executed_quantity": 1,
  "broker_order_id": "BROKER_123"
}
```

### **Step 3: User Notification (order-updates)**
```json
{
  "update_id": "upd-001",
  "order_id": "ord-001",
  "user_id": "user-123",
  "update_type": "EXECUTION_SUCCESS",
  "message": "Your RELIANCE buy order executed at ₹2,451.00",
  "notification_channels": {
    "push": true,
    "email": true
  }
}
```

---

## 🎯 **Implementation Priority**

### **Phase 1 (Now):**
- ✅ trade-signals (Implemented)
- ✅ user-configs (Implemented)
- ✅ market.data.news (Implemented)

### **Phase 2 (Next):**
- 🔜 trade-executions (When Trade Execution service is ready)
- 🔜 order-updates (When Trade Execution service is ready)

### **Phase 3 (Future):**
- 🔜 risk-approvals
- 🔜 position-updates
- 🔜 portfolio-snapshots

---

## 🔧 **Testing Commands**

### **Publish Test Message to trade-executions:**
```bash
echo '{
  "execution_id": "test-exec-001",
  "order_id": "test-ord-001",
  "user_id": "user-123",
  "status": "EXECUTED",
  "symbol": "RELIANCE",
  "executed_price": 2450.00,
  "executed_quantity": 1,
  "execution_time": "'$(date -u +%Y-%m-%dT%H:%M:%S.000Z)'"
}' | kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic trade-executions
```

### **Consume from order-updates:**
```bash
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic order-updates \
  --from-beginning \
  --property print.key=true \
  --property print.headers=true
```

---

## 📊 **Database Integration**

When **trade-executions** message is received, update PostgreSQL:

```sql
UPDATE trade_signals
SET 
    status = 'EXECUTED',
    execution_price = 2451.00,
    execution_time = '2024-11-17T14:30:05.123Z',
    broker_order_id = 'ODIN_ORD_20241117_123456',
    updated_at = NOW()
WHERE order_id = '550e8400-e29b-41d4-a716-446655440000';
```

This keeps PostgreSQL as the source of truth while Kafka handles real-time messaging!

---

## ✅ **Summary**

| Topic | Purpose | Producer | Key Data |
|-------|---------|----------|----------|
| **trade-executions** | Technical execution results | Trade Execution | Broker IDs, prices, fees, timestamps |
| **order-updates** | User-friendly notifications | Trade Execution | Messages, status, action buttons |

Both work together to provide complete order lifecycle tracking! 🎉
