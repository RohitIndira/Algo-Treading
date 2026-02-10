# Phase 1: Cash52W Enhanced Configuration - API Testing Guide

## 🎯 Overview
Complete Phase 1 implementation with multi-level profit/SL, portfolio config, and manual controls.

---

## 📋 Prerequisites

### 1. Apply Database Migration
```bash
cd /home/ubuntu/Algo-Treading
psql -U postgres -d user_config_db -f services/user-config/migrations/004_enhance_cash52w_config.sql
```

### 2. Build Services
```bash
# Build user-config service
cd services/user-config
go build -o bin/user-config ./cmd/main.go

# Build API Gateway
cd ../../api/gateway
go build -o bin/gateway ./cmd/main.go
```

### 3. Start Services
```bash
# Terminal 1: User-Config Service
cd services/user-config
./bin/user-config

# Terminal 2: API Gateway
cd api/gateway
./bin/gateway
```

---

## 🔗 API Endpoints (Phase 1)

### Base URL
```
http://localhost:8080/api/v1
```

---

## 📡 1. Configure Enhanced Strategy (Full Phase 1)

**POST** `/strategies/cash52w/configure-enhanced`

### Request Body (Full Configuration):
```json
{
  "user_id": "user123",
  "enabled": true,
  "total_capital": 500000,
  "capital_per_stock": 20000,
  "max_stocks": 25,
  "auto_rebalance": true,
  "stop_loss_levels": {
    "level_1": {
      "trigger_percent": -10,
      "exit_quantity_percent": 50,
      "type": "fixed",
      "enabled": true
    },
    "level_2": {
      "trigger_percent": -20,
      "exit_quantity_percent": 100,
      "type": "trailing",
      "enabled": true
    }
  },
  "profit_levels": {
    "level_1": {
      "trigger_percent": 15,
      "exit_quantity_percent": 33,
      "type": "fixed",
      "enabled": true
    },
    "level_2": {
      "trigger_percent": 30,
      "exit_quantity_percent": 50,
      "type": "fixed",
      "enabled": true
    },
    "level_3": {
      "trigger_percent": 50,
      "exit_quantity_percent": 100,
      "type": "trailing",
      "trail_percent": 10,
      "enabled": true
    }
  },
  "trading_mode": "PAPER",
  "force_exit_all": false,
  "force_exit_stocks": [],
  "pause_new_entries": false
}
```

### cURL Command:
```bash
curl -X POST http://localhost:8080/api/v1/strategies/cash52w/configure-enhanced \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "user123",
    "enabled": true,
    "total_capital": 500000,
    "capital_per_stock": 20000,
    "max_stocks": 25,
    "auto_rebalance": true,
    "stop_loss_levels": {
      "level_1": {
        "trigger_percent": -10,
        "exit_quantity_percent": 50,
        "type": "fixed",
        "enabled": true
      },
      "level_2": {
        "trigger_percent": -20,
        "exit_quantity_percent": 100,
        "type": "trailing",
        "enabled": true
      }
    },
    "profit_levels": {
      "level_1": {
        "trigger_percent": 15,
        "exit_quantity_percent": 33,
        "type": "fixed",
        "enabled": true
      },
      "level_2": {
        "trigger_percent": 30,
        "exit_quantity_percent": 50,
        "type": "fixed",
        "enabled": true
      },
      "level_3": {
        "trigger_percent": 50,
        "exit_quantity_percent": 100,
        "type": "trailing",
        "trail_percent": 10,
        "enabled": true
      }
    },
    "trading_mode": "PAPER"
  }' | jq
```

### Expected Response:
```json
{
  "success": true,
  "config": {
    "user_id": "user123",
    "enabled": true,
    "total_capital": 500000,
    "capital_per_stock": 20000,
    "max_stocks": 25,
    "auto_rebalance": true,
    "stop_loss_levels": { ... },
    "profit_levels": { ... },
    "trading_mode": "PAPER",
    "version": 1
  }
}
```

---

## 📡 2. Get Configuration

**GET** `/strategies/cash52w/config/{user_id}`

### cURL Command:
```bash
curl -X GET http://localhost:8080/api/v1/strategies/cash52w/config/user123 | jq
```

### Expected Response:
```json
{
  "success": true,
  "config": {
    "user_id": "user123",
    "enabled": true,
    "total_capital": 500000,
    "capital_per_stock": 20000,
    "max_stocks": 25,
    "auto_rebalance": true,
    "stop_loss_levels": {
      "level_1": {
        "trigger_percent": -10,
        "exit_quantity_percent": 50,
        "type": "fixed",
        "enabled": true
      },
      "level_2": {
        "trigger_percent": -20,
        "exit_quantity_percent": 100,
        "type": "trailing",
        "enabled": true
      }
    },
    "profit_levels": {
      "level_1": {
        "trigger_percent": 15,
        "exit_quantity_percent": 33,
        "type": "fixed",
        "enabled": true
      },
      "level_2": {
        "trigger_percent": 30,
        "exit_quantity_percent": 50,
        "type": "fixed",
        "enabled": true
      },
      "level_3": {
        "trigger_percent": 50,
        "exit_quantity_percent": 100,
        "type": "trailing",
        "trail_percent": 10,
        "enabled": true
      }
    },
    "trading_mode": "PAPER",
    "force_exit_all": false,
    "force_exit_stocks": [],
    "pause_new_entries": false,
    "version": 1,
    "updated_at": { "seconds": 1707553200 }
  }
}
```

---

## 📡 3. Force Exit All Positions (Emergency)

**PUT** `/strategies/cash52w/force-exit-all`

### Request Body:
```json
{
  "user_id": "user123"
}
```

### cURL Command:
```bash
curl -X PUT http://localhost:8080/api/v1/strategies/cash52w/force-exit-all \
  -H "Content-Type: application/json" \
  -d '{"user_id": "user123"}' | jq
```

### Expected Response:
```json
{
  "success": true,
  "message": "Force exit all triggered successfully"
}
```

---

## 📡 4. Force Exit Specific Stocks

**PUT** `/strategies/cash52w/force-exit-stocks`

### Request Body:
```json
{
  "user_id": "user123",
  "stocks": ["NSE:RELIANCE", "NSE:TCS", "NSE:INFY"]
}
```

### cURL Command:
```bash
curl -X PUT http://localhost:8080/api/v1/strategies/cash52w/force-exit-stocks \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "user123",
    "stocks": ["NSE:RELIANCE", "NSE:TCS", "NSE:INFY"]
  }' | jq
```

### Expected Response:
```json
{
  "success": true,
  "message": "Force exit triggered for 3 stocks"
}
```

---

## 📡 5. Update Manual Controls

**PUT** `/strategies/cash52w/manual-controls`

### Request Body:
```json
{
  "user_id": "user123",
  "pause_new_entries": true,
  "reset_force_exit": false
}
```

### cURL Command:
```bash
curl -X PUT http://localhost:8080/api/v1/strategies/cash52w/manual-controls \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "user123",
    "pause_new_entries": true,
    "reset_force_exit": false
  }' | jq
```

### Expected Response:
```json
{
  "success": true,
  "message": "Manual controls updated successfully"
}
```

---

## 📡 6. Disable Strategy

**PUT** `/strategies/cash52w/disable`

### Request Body:
```json
{
  "user_id": "user123"
}
```

### cURL Command:
```bash
curl -X PUT http://localhost:8080/api/v1/strategies/cash52w/disable \
  -H "Content-Type: application/json" \
  -d '{"user_id": "user123"}' | jq
```

### Expected Response:
```json
{
  "success": true,
  "message": "Cash 52W strategy disabled successfully"
}
```

---

## 📡 7. Get All Enabled Configs (Admin)

**GET** `/strategies/cash52w/enabled-configs`

### cURL Command:
```bash
curl -X GET http://localhost:8080/api/v1/strategies/cash52w/enabled-configs | jq
```

### Expected Response:
```json
{
  "success": true,
  "configs": [
    {
      "user_id": "user123",
      "enabled": true,
      "total_capital": 500000,
      "capital_per_stock": 20000,
      ...
    },
    {
      "user_id": "user456",
      "enabled": true,
      ...
    }
  ]
}
```

---

## 🧪 Complete Testing Flow

### Test Scenario 1: Create & Configure
```bash
# 1. Create enhanced configuration
curl -X POST http://localhost:8080/api/v1/strategies/cash52w/configure-enhanced \
  -H "Content-Type: application/json" \
  -d '{"user_id": "test_user", "enabled": true, "total_capital": 500000, ...}' | jq

# 2. Verify configuration
curl -X GET http://localhost:8080/api/v1/strategies/cash52w/config/test_user | jq

# 3. Check it appears in enabled configs
curl -X GET http://localhost:8080/api/v1/strategies/cash52w/enabled-configs | jq
```

### Test Scenario 2: Emergency Controls
```bash
# 1. Trigger force exit for specific stocks
curl -X PUT http://localhost:8080/api/v1/strategies/cash52w/force-exit-stocks \
  -H "Content-Type: application/json" \
  -d '{"user_id": "test_user", "stocks": ["NSE:RELIANCE"]}' | jq

# 2. Verify force_exit_stocks updated
curl -X GET http://localhost:8080/api/v1/strategies/cash52w/config/test_user | jq

# 3. Reset and pause new entries
curl -X PUT http://localhost:8080/api/v1/strategies/cash52w/manual-controls \
  -H "Content-Type: application/json" \
  -d '{"user_id": "test_user", "pause_new_entries": true, "reset_force_exit": true}' | jq
```

### Test Scenario 3: Disable
```bash
# 1. Disable strategy
curl -X PUT http://localhost:8080/api/v1/strategies/cash52w/disable \
  -H "Content-Type: application/json" \
  -d '{"user_id": "test_user"}' | jq

# 2. Verify it's disabled
curl -X GET http://localhost:8080/api/v1/strategies/cash52w/config/test_user | jq

# 3. Check it's not in enabled configs
curl -X GET http://localhost:8080/api/v1/strategies/cash52w/enabled-configs | jq
```

---

## 🔍 Verification Checklist

- [ ] Migration applied successfully
- [ ] Services build without errors
- [ ] Services start successfully
- [ ] Can create enhanced configuration
- [ ] Can retrieve configuration
- [ ] Can trigger force exit all
- [ ] Can trigger force exit for specific stocks
- [ ] Can update manual controls
- [ ] Can disable strategy
- [ ] Can retrieve all enabled configs
- [ ] Kafka messages published (check logs)
- [ ] PostgreSQL data persisted correctly

---

## 📊 Database Verification

### Check PostgreSQL:
```sql
-- Check configuration
SELECT * FROM cash52w_configs WHERE user_id = 'test_user';

-- Check JSONB fields
SELECT 
    user_id,
    enabled,
    total_capital,
    capital_per_stock,
    stop_loss_levels::text,
    profit_levels::text,
    trading_mode,
    version
FROM cash52w_configs 
WHERE user_id = 'test_user';

-- Check all enabled
SELECT user_id, enabled, total_capital, max_stocks 
FROM cash52w_configs 
WHERE enabled = TRUE;
```

---

## 🎛️ Kafka Verification

### Check Kafka Topic:
```bash
# Check messages published
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic user-config-updates \
  --from-beginning \
  --max-messages 10
```

---

## 🚀 Production Deployment Steps

1. **Apply Migration** (in maintenance window)
   ```bash
   psql -U postgres -d user_config_db -f migrations/004_enhance_cash52w_config.sql
   ```

2. **Build Services**
   ```bash
   cd services/user-config && ./build.sh
   cd ../../api/gateway && ./build.sh
   ```

3. **Deploy** (zero-downtime)
   - Deploy user-config service first
   - Deploy API gateway second
   - Verify health checks

4. **Smoke Test**
   - Test one enhanced configuration
   - Verify Kafka publishing
   - Verify DB persistence

---

## 📞 Support

- **Logs**: Check service logs for errors
- **Health**: `GET /api/v1/health`
- **Kafka**: Monitor `user-config-updates` topic
- **DB**: Query `cash52w_configs` table

---

**✅ Phase 1 Implementation Complete!**
All endpoints ready for end-to-end testing.
