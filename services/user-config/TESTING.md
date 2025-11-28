# User Config Service - API Testing Guide

This guide shows how to test all User Config Service endpoints using Postman (with gRPC support) or grpcurl.

## Prerequisites

### Option 1: Postman (Recommended)
- Download Postman (version 9.7.1 or later for gRPC support)
- Enable gRPC in Postman settings

### Option 2: grpcurl
```bash
# Install grpcurl
go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest

# Or on Ubuntu/Debian
sudo apt-get install grpcurl
```

## Service Information

- **Server Address**: `localhost:50051`
- **Service Name**: `user_config.UserConfigService`
- **Protocol**: gRPC (not REST)

## 📡 All Available Endpoints

### 1. Health Check
**Method**: `HealthCheck`

```bash
# Using grpcurl
grpcurl -plaintext localhost:50051 user_config.UserConfigService/HealthCheck
```

**Request** (JSON):
```json
{
  "service": "user-config-service"
}
```

**Response**:
```json
{
  "healthy": true,
  "service": "user-config-service",
  "version": "1.0.0",
  "timestamp": {
    "seconds": 1699564800
  }
}
```

---

### 2. Create Strategy
**Method**: `CreateStrategy`

```bash
# Using grpcurl
grpcurl -plaintext -d @ localhost:50051 user_config.UserConfigService/CreateStrategy <<EOF
{
  "user_id": "IS14415",
  "strategy_name": "High Impact News Trader",
  "description": "Trades on high-impact news with positive sentiment",
  "activate_immediately": true,
  "conditions": {
    "impact_score_threshold": 7,
    "sentiments": ["SENTIMENT_POSITIVE", "SENTIMENT_NEUTRAL"],
    "categories": ["Results", "Board Meeting", "Announcements"],
    "stock_codes": [500325, 532174],
    "price_range": {
      "min_price": 10.0,
      "max_price": 1000.0
    },
    "volume_threshold": 100000,
    "pct_change_threshold": 2.0,
    "exchanges": ["EXCHANGE_NSE", "EXCHANGE_BSE"]
  },
  "trade_config": {
    "order_type": "ORDER_TYPE_MARKET",
    "quantity": 100,
    "exchange": "EXCHANGE_NSE",
    "order_side": "ORDER_SIDE_BUY",
    "validity": "DAY",
    "max_position_size": 50000.0,
    "stop_loss_pct": 2.0,
    "take_profit_pct": 5.0
  },
  "risk_limits": {
    "max_daily_trades": 10,
    "max_loss_per_day": 10000.0,
    "position_sizing": "POSITION_SIZING_FIXED",
    "max_portfolio_exposure_pct": 25.0,
    "max_per_trade_risk": 1000.0,
    "enable_risk_checks": true
  }
}
EOF
```

**Postman Request** (JSON):
```json
{
  "user_id": "IS14415",
  "strategy_name": "High Impact News Trader",
  "description": "Trades on high-impact news with positive sentiment",
  "activate_immediately": true,
  "conditions": {
    "impact_score_threshold": 7,
    "sentiments": ["SENTIMENT_POSITIVE", "SENTIMENT_NEUTRAL"],
    "categories": ["Results", "Board Meeting"],
    "exchanges": ["EXCHANGE_NSE", "EXCHANGE_BSE"],
    "price_range": {
      "min_price": 10.0,
      "max_price": 1000.0
    },
    "volume_threshold": 100000,
    "pct_change_threshold": 2.0
  },
  "trade_config": {
    "order_type": "ORDER_TYPE_MARKET",
    "quantity": 100,
    "exchange": "EXCHANGE_NSE",
    "order_side": "ORDER_SIDE_BUY",
    "validity": "DAY",
    "max_position_size": 50000.0,
    "stop_loss_pct": 2.0,
    "take_profit_pct": 5.0
  },
  "risk_limits": {
    "max_daily_trades": 10,
    "max_loss_per_day": 10000.0,
    "position_sizing": "POSITION_SIZING_FIXED",
    "max_portfolio_exposure_pct": 25.0,
    "max_per_trade_risk": 1000.0,
    "enable_risk_checks": true
  }
}
```

**Response**:
```json
{
  "success": true,
  "strategy": {
    "strategy_id": "550e8400-e29b-41d4-a716-446655440000",
    "user_id": "IS14415",
    "strategy_name": "High Impact News Trader",
    "description": "Trades on high-impact news with positive sentiment",
    "active": true,
    "version": 1,
    "created_at": {...},
    "updated_at": {...},
    "conditions": {...},
    "trade_config": {...},
    "risk_limits": {...}
  }
}
```

---

### 3. Get Strategy
**Method**: `GetStrategy`

```bash
# Using grpcurl
grpcurl -plaintext -d '{
  "strategy_id": "550e8400-e29b-41d4-a716-446655440000",
  "user_id": "IS14415"
}' localhost:50051 user_config.UserConfigService/GetStrategy
```

**Postman Request**:
```json
{
  "strategy_id": "550e8400-e29b-41d4-a716-446655440000",
  "user_id": "IS14415"
}
```

**Response**:
```json
{
  "success": true,
  "strategy": {
    "strategy_id": "550e8400-e29b-41d4-a716-446655440000",
    "user_id": "IS14415",
    "strategy_name": "High Impact News Trader",
    "active": true,
    ...
  }
}
```

---

### 4. List User Strategies
**Method**: `ListUserStrategies`

```bash
# Using grpcurl
grpcurl -plaintext -d '{
  "user_id": "IS14415",
  "active_only": true,
  "pagination": {
    "page": 1,
    "page_size": 20
  }
}' localhost:50051 user_config.UserConfigService/ListUserStrategies
```

**Postman Request**:
```json
{
  "user_id": "IS14415",
  "active_only": true,
  "pagination": {
    "page": 1,
    "page_size": 20
  }
}
```

**Response**:
```json
{
  "success": true,
  "strategies": [
    {
      "strategy_id": "550e8400-e29b-41d4-a716-446655440000",
      "user_id": "IS14415",
      "strategy_name": "High Impact News Trader",
      "active": true,
      ...
    }
  ],
  "pagination": {
    "page": 1,
    "page_size": 20,
    "total_items": 1,
    "total_pages": 1,
    "has_next": false,
    "has_previous": false
  }
}
```

---

### 5. Update Strategy
**Method**: `UpdateStrategy`

```bash
# Using grpcurl
grpcurl -plaintext -d '{
  "strategy_id": "550e8400-e29b-41d4-a716-446655440000",
  "user_id": "IS14415",
  "strategy_name": "Updated Strategy Name",
  "description": "Updated description",
  "version": 1
}' localhost:50051 user_config.UserConfigService/UpdateStrategy
```

**Postman Request**:
```json
{
  "strategy_id": "550e8400-e29b-41d4-a716-446655440000",
  "user_id": "IS14415",
  "strategy_name": "Updated Strategy Name",
  "description": "Updated description",
  "version": 1
}
```

**Note**: Only provide fields you want to update. `version` is required for optimistic locking.

---

### 6. Activate Strategy
**Method**: `ActivateStrategy`

```bash
# Using grpcurl
grpcurl -plaintext -d '{
  "strategy_id": "550e8400-e29b-41d4-a716-446655440000",
  "user_id": "IS14415"
}' localhost:50051 user_config.UserConfigService/ActivateStrategy
```

**Postman Request**:
```json
{
  "strategy_id": "550e8400-e29b-41d4-a716-446655440000",
  "user_id": "IS14415"
}
```

**Response**:
```json
{
  "success": true,
  "strategy": {
    "strategy_id": "550e8400-e29b-41d4-a716-446655440000",
    "active": true,
    ...
  }
}
```

---

### 7. Deactivate Strategy
**Method**: `DeactivateStrategy`

```bash
# Using grpcurl
grpcurl -plaintext -d '{
  "strategy_id": "550e8400-e29b-41d4-a716-446655440000",
  "user_id": "IS14415"
}' localhost:50051 user_config.UserConfigService/DeactivateStrategy
```

**Postman Request**:
```json
{
  "strategy_id": "550e8400-e29b-41d4-a716-446655440000",
  "user_id": "IS14415"
}
```

---

### 8. Delete Strategy
**Method**: `DeleteStrategy`

```bash
# Using grpcurl
grpcurl -plaintext -d '{
  "strategy_id": "550e8400-e29b-41d4-a716-446655440000",
  "user_id": "IS14415"
}' localhost:50051 user_config.UserConfigService/DeleteStrategy
```

**Postman Request**:
```json
{
  "strategy_id": "550e8400-e29b-41d4-a716-446655440000",
  "user_id": "IS14415"
}
```

**Response**:
```json
{
  "success": true,
  "message": "Strategy deleted successfully"
}
```

---

### 9. Get Strategies By IDs (Internal Use)
**Method**: `GetStrategiesByIDs`

```bash
# Using grpcurl
grpcurl -plaintext -d '{
  "strategy_ids": [
    "550e8400-e29b-41d4-a716-446655440000",
    "660e8400-e29b-41d4-a716-446655440001"
  ]
}' localhost:50051 user_config.UserConfigService/GetStrategiesByIDs
```

**Postman Request**:
```json
{
  "strategy_ids": [
    "550e8400-e29b-41d4-a716-446655440000",
    "660e8400-e29b-41d4-a716-446655440001"
  ]
}
```

---

## 🎯 Testing in Postman

### Setup gRPC in Postman:

1. **Create New Request**:
   - Click "New" → "gRPC Request"

2. **Configure Server**:
   - Enter server URL: `localhost:50051`
   - Select "Use server reflection"

3. **Select Method**:
   - Service: `user_config.UserConfigService`
   - Method: Choose from list (e.g., `CreateStrategy`)

4. **Set Message**:
   - Paste JSON request body
   - Click "Invoke"

### Example Postman Collection Structure:
```
User Config Service
├── Health Check
├── Create Strategy
├── Get Strategy
├── List User Strategies
├── Update Strategy
├── Activate Strategy
├── Deactivate Strategy
├── Delete Strategy
└── Get Strategies By IDs
```

---

## 📝 Important Notes

### Enum Values
When sending requests, use the full enum name:

**Sentiments**:
- `SENTIMENT_POSITIVE`
- `SENTIMENT_NEUTRAL`
- `SENTIMENT_NEGATIVE`

**Exchanges**:
- `EXCHANGE_NSE`
- `EXCHANGE_BSE`

**Order Types**:
- `ORDER_TYPE_MARKET`
- `ORDER_TYPE_LIMIT`
- `ORDER_TYPE_STOP_LOSS`

**Order Sides**:
- `ORDER_SIDE_BUY`
- `ORDER_SIDE_SELL`

**Position Sizing**:
- `POSITION_SIZING_FIXED`
- `POSITION_SIZING_PERCENTAGE`
- `POSITION_SIZING_RISK_BASED`

### Error Responses
All endpoints return errors in this format:
```json
{
  "success": false,
  "error": {
    "code": "CREATION_FAILED",
    "message": "strategy_name is required"
  }
}
```

### Version Control
- The `version` field is used for optimistic locking
- When updating, provide the current version number
- If version mismatch occurs, re-fetch and retry

---

## 🔍 Testing Workflow

### 1. Start the Service
```bash
cd /home/rohitt/Desktop/trading-system/services/user-config
go run cmd/main.go
```

### 2. Test Health Check
```bash
grpcurl -plaintext localhost:50051 user_config.UserConfigService/HealthCheck
```

### 3. Create a Strategy
Use the CreateStrategy example above

### 4. List Strategies
Verify your strategy was created

### 5. Update & Manage
Test activate, deactivate, update, and delete operations

---

## 🛠️ Troubleshooting

### Connection Refused
- Ensure service is running: `go run cmd/main.go`
- Check port 50051 is not in use: `lsof -i :50051`

### Invalid Enum Value
- Use full enum names (e.g., `SENTIMENT_POSITIVE`, not `POSITIVE`)

### Strategy Not Found
- Verify strategy_id is correct UUID
- Ensure user_id matches the strategy owner

### Database Errors
- Check PostgreSQL is running: `pg_isready`
- Verify database connection in `.env`

---

## 📚 Additional Resources

- [gRPC Documentation](https://grpc.io/docs/)
- [Postman gRPC Support](https://learning.postman.com/docs/sending-requests/grpc/grpc-request-interface/)
- [grpcurl GitHub](https://github.com/fullstorydev/grpcurl)

---

## 💡 Quick Test Script

Save this as `test_all.sh`:

```bash
#!/bin/bash

echo "1. Health Check"
grpcurl -plaintext localhost:50051 user_config.UserConfigService/HealthCheck

echo -e "\n2. Create Strategy"
grpcurl -plaintext -d '{
  "user_id": "IS14415",
  "strategy_name": "Test Strategy",
  "description": "Test description",
  "activate_immediately": true,
  "conditions": {
    "impact_score_threshold": 7,
    "sentiments": ["SENTIMENT_POSITIVE"],
    "categories": ["Results"],
    "exchanges": ["EXCHANGE_NSE"]
  },
  "trade_config": {
    "order_type": "ORDER_TYPE_MARKET",
    "quantity": 100,
    "exchange": "EXCHANGE_NSE",
    "order_side": "ORDER_SIDE_BUY",
    "validity": "DAY"
  },
  "risk_limits": {
    "max_daily_trades": 10,
    "max_loss_per_day": 10000.0,
    "position_sizing": "POSITION_SIZING_FIXED",
    "enable_risk_checks": true
  }
}' localhost:50051 user_config.UserConfigService/CreateStrategy

echo -e "\n3. List Strategies"
grpcurl -plaintext -d '{
  "user_id": "IS14415",
  "pagination": {"page": 1, "page_size": 10}
}' localhost:50051 user_config.UserConfigService/ListUserStrategies
```

Run with: `chmod +x test_all.sh && ./test_all.sh`
