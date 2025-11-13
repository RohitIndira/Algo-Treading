# User Config Service - Data Flow Explained

## ✅ Current Integration Status

Your User Config Service is **already fully integrated** with both PostgreSQL and Kafka!

## 🔄 What Happens When You Add a Strategy

### Complete Flow Diagram:

```
User Creates Strategy
        ↓
[gRPC Request] → User Config Service
                        ↓
                   ┌────┴────┐
                   │         │
            [PostgreSQL]  [Kafka]
                   │         │
            Saves Strategy  Publishes Event
            Permanently     to "user-configs" topic
                   │         │
                   ↓         ↓
            ✅ Stored    ✅ Published
```

## 📊 Detailed Step-by-Step

### When You Call: `CreateStrategy()`

**Step 1: Save to PostgreSQL** ✅
```go
strategy, err := s.repo.Create(ctx, req)
```
- Strategy is saved to `strategies` table
- Gets a unique `strategy_id`
- All data persisted permanently

**Step 2: Publish to Kafka** ✅
```go
if err := s.publishToKafka(ctx, "CREATE", strategy); err != nil {
    fmt.Printf("Warning: failed to publish to kafka: %v\n", err)
}
```
- Event published to Kafka topic: `user-configs`
- Event type: `CREATE`
- Contains full strategy details

### Kafka Message Structure

```json
{
  "event_type": "CREATE",
  "strategy": {
    "strategy_id": "550e8400-e29b-41d4-a716-446655440000",
    "user_id": "IS14415",
    "strategy_name": "High Impact News Trading",
    "conditions": {
      "sentiment": ["positive"],
      "categories": ["earnings"],
      "impact_score_threshold": 8,
      "stock_codes": [12345]
    },
    "trade_config": {
      "action": "BUY",
      "quantity": 100,
      "order_type": "MARKET",
      "exchange": "NSE"
    },
    "risk_limits": {
      "max_position_size": 10000.00,
      "daily_loss_limit": 5000.00,
      "max_trades_per_day": 10
    },
    "is_active": true
  },
  "timestamp": 1699876543
}
```

## 🎯 All Supported Operations

Your service automatically handles ALL these operations with dual persistence:

| Operation | PostgreSQL | Kafka Topic | Event Type |
|-----------|-----------|-------------|------------|
| **Create Strategy** | ✅ Saves | ✅ Publishes | `CREATE` |
| **Update Strategy** | ✅ Updates | ✅ Publishes | `UPDATE` |
| **Delete Strategy** | ✅ Deletes | ✅ Publishes | `DELETE` |
| **Activate Strategy** | ✅ Updates | ✅ Publishes | `ACTIVATE` |
| **Deactivate Strategy** | ✅ Updates | ✅ Publishes | `DEACTIVATE` |
| **Get Strategy** | ✅ Reads | ❌ No event | - |
| **List Strategies** | ✅ Reads | ❌ No event | - |

**Note**: Read operations (Get, List) only query PostgreSQL - no Kafka events.

## 🔍 How to Verify It's Working

### 1. Check PostgreSQL (Persistent Storage)

```bash
# Connect to PostgreSQL
psql -h localhost -U postgres -d trading_db

# View your strategies
SELECT strategy_id, user_id, strategy_name, is_active 
FROM strategies 
ORDER BY created_at DESC;
```

### 2. Check Kafka (Event Stream)

```bash
# List topics (should include user-configs)
docker exec trading-kafka kafka-topics --list --bootstrap-server localhost:9092

# View messages in user-configs topic
docker exec trading-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic user-configs \
  --from-beginning
```

### 3. Check Kafka UI (Visual Interface)

Open http://localhost:8080
- Navigate to **Topics** → **user-configs**
- See all strategy events in real-time
- View message details

## 📝 Current Configuration

Your `.env` file shows:
```env
# PostgreSQL - Primary storage
DB_HOST=localhost
DB_PORT=5432
DB_NAME=trading_db

# Kafka - Event streaming
KAFKA_ENABLED=true
KAFKA_BROKERS=localhost:9092
KAFKA_TOPIC=user-configs
```

## 🎭 Failure Handling

**Important**: If Kafka fails, your strategy still gets saved!

```go
// Publish to Kafka
if err := s.publishToKafka(ctx, "CREATE", strategy); err != nil {
    // Log error but DON'T fail the operation
    fmt.Printf("Warning: failed to publish to kafka: %v\n", err)
}
```

**Why?**
- PostgreSQL is the source of truth (permanent storage)
- Kafka is for real-time events (notifications to other services)
- If Kafka is down, users can still create strategies

## 🔄 Who Consumes Kafka Events?

These services will listen to the `user-configs` topic:

1. **Rules Engine** 
   - Receives strategy updates
   - Matches incoming news against active strategies

2. **Risk Management**
   - Receives strategy updates
   - Validates against risk limits

3. **Monitoring/Analytics**
   - Tracks strategy changes
   - Generates reports

## 🧪 Test It Yourself

### 1. Start Your Service
```bash
cd services/user-config
go run cmd/main.go
```

### 2. Create a Strategy (using test client)
```bash
# In another terminal
cd services/user-config
# Use your gRPC client or test script
```

### 3. Watch Kafka in Real-Time
```bash
# Open a terminal to watch Kafka messages
docker exec -it trading-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic user-configs \
  --from-beginning
```

### 4. Check PostgreSQL
```bash
# Connect and query
psql -h localhost -U postgres -d trading_db -c "SELECT * FROM strategies ORDER BY created_at DESC LIMIT 5;"
```

## 🎉 Summary

**Yes! When you create/update/delete a strategy:**

✅ **PostgreSQL** - Immediately saves/updates/deletes the data
✅ **Kafka** - Immediately publishes event to `user-configs` topic
✅ **Automatic** - No extra code needed, it's already built-in!
✅ **Resilient** - If Kafka fails, PostgreSQL still works

## 🚀 Next Steps

Your User Config Service is ready! The next service to build is:

**Rules Engine** - Will consume these Kafka events and match strategies against incoming news!

## 📖 Related Documentation

- PostgreSQL Setup: `docs/guides/postgresql-setup.md`
- Kafka Setup: `docs/guides/kafka-setup.md`
- System Architecture: `docs/guides/trading-system-architecture.md`
- Viewing Data: `services/user-config/VIEWING_DATA.md`
