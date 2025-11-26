# REST API Test Commands

All REST API endpoints for testing the API Gateway (running on port 8081).

## Base URL
```
http://localhost:8081/api/v1
```

---

## 1. Health Check

```bash
curl http://localhost:8081/api/v1/health
```

---

## 2. Create Strategy

```bash
curl -X POST http://localhost:8081/api/v1/strategies \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "IS14415",
    "strategy_name": "High Impact News Trader",
    "description": "Trades on high-impact news with positive sentiment",
    "activate_immediately": true,
    "conditions": {
      "match_all_news": false,
      "impact_score_threshold": 7,
      "sentiments": [1, 2],
      "categories": ["Results", "Board Meeting"],
      "exchanges": [1, 2],
      "price_range": {
        "min": 10.0,
        "max": 1000.0
      },
      "volume_threshold": 100000,
      "pct_change_threshold": 2.0
    },
    "trade_config": {
      "order_type": 1,
      "quantity": 100,
      "exchange": 1,
      "order_side": 1,
      "validity": "DAY",
      "max_position_size": 50000.0,
      "stop_loss_pct": 2.0,
      "take_profit_pct": 5.0
    },
    "risk_limits": {
      "max_daily_trades": 10,
      "max_loss_per_day": 10000.0,
      "position_sizing": 1,
      "max_portfolio_exposure_pct": 25.0,
      "enable_risk_checks": true
    }
  }'
```

### Create Simple Strategy (Minimal)

```bash
curl -X POST http://localhost:8081/api/v1/strategies \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "IS14415",
    "strategy_name": "Simple Test Strategy",
    "description": "A minimal strategy for testing",
    "activate_immediately": true,
    "conditions": {
      "impact_score_threshold": 5,
      "sentiments": [1]
    },
    "trade_config": {
      "order_type": 1,
      "quantity": 10,
      "exchange": 1,
      "order_side": 1,
      "validity": "DAY"
    },
    "risk_limits": {
      "max_daily_trades": 5,
      "max_loss_per_day": 5000.0,
      "enable_risk_checks": true
    }
  }'
```

### Create "Match All News" Strategy

```bash
curl -X POST http://localhost:8081/api/v1/strategies \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "IS14415",
    "strategy_name": "Match All News Strategy",
    "description": "Matches every news event",
    "activate_immediately": true,
    "conditions": {
      "match_all_news": true,
      "impact_score_threshold": 5
    },
    "trade_config": {
      "order_type": 1,
      "quantity": 50,
      "exchange": 1,
      "order_side": 1,
      "validity": "DAY",
      "max_position_size": 25000.0
    },
    "risk_limits": {
      "max_daily_trades": 20,
      "max_loss_per_day": 15000.0,
      "enable_risk_checks": true
    }
  }'
```

---

## 3. List All User Strategies

```bash
curl http://localhost:8081/api/v1/users/IS14415/strategies
```

### List Active Strategies Only

```bash
curl "http://localhost:8081/api/v1/users/IS14415/strategies?active_only=true"
```

### List with Pagination

```bash
curl "http://localhost:8081/api/v1/users/IS14415/strategies?page=1&page_size=5"
```

### List Inactive Strategies

```bash
curl "http://localhost:8081/api/v1/users/IS14415/strategies?active_only=false"
```

---

## 4. Get Specific Strategy

**Replace `{strategy_id}` with actual strategy ID from create response**

```bash
curl "http://localhost:8081/api/v1/strategies/{strategy_id}?user_id=IS14415"
```

Example:
```bash
curl "http://localhost:8081/api/v1/strategies/550e8400-e29b-41d4-a716-446655440000?user_id=IS14415"
```

---

## 5. Update Strategy

**Replace `{strategy_id}` with actual strategy ID**

### Update Strategy Name

```bash
curl -X PUT http://localhost:8081/api/v1/strategies/{strategy_id} \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "IS14415",
    "strategy_name": "Updated Strategy Name",
    "version": 1
  }'
```

### Update Strategy Conditions

```bash
curl -X PUT http://localhost:8081/api/v1/strategies/{strategy_id} \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "IS14415",
    "conditions": {
      "impact_score_threshold": 8,
      "sentiments": [1],
      "categories": ["Results"]
    },
    "version": 1
  }'
```

### Full Strategy Update

```bash
curl -X PUT http://localhost:8081/api/v1/strategies/{strategy_id} \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "IS14415",
    "strategy_name": "Fully Updated Strategy",
    "description": "This is a completely updated strategy",
    "conditions": {
      "impact_score_threshold": 6,
      "sentiments": [1, 2],
      "categories": ["Results", "Dividends"]
    },
    "trade_config": {
      "order_type": 1,
      "quantity": 200,
      "exchange": 1,
      "order_side": 1,
      "validity": "DAY",
      "max_position_size": 75000.0
    },
    "risk_limits": {
      "max_daily_trades": 15,
      "max_loss_per_day": 12000.0,
      "enable_risk_checks": true
    },
    "version": 1
  }'
```

---

## 6. Activate Strategy

**Replace `{strategy_id}` with actual strategy ID**

```bash
curl -X POST http://localhost:8081/api/v1/strategies/{strategy_id}/activate \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "IS14415"
  }'
```

Example:
```bash
curl -X POST http://localhost:8081/api/v1/strategies/550e8400-e29b-41d4-a716-446655440000/activate \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "IS14415"
  }'
```

---

## 7. Deactivate Strategy

**Replace `{strategy_id}` with actual strategy ID**

```bash
curl -X POST http://localhost:8081/api/v1/strategies/{strategy_id}/deactivate \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "IS14415"
  }'
```

Example:
```bash
curl -X POST http://localhost:8081/api/v1/strategies/550e8400-e29b-41d4-a716-446655440000/deactivate \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "IS14415"
  }'
```

---

## 8. Delete Strategy

**Replace `{strategy_id}` with actual strategy ID**

```bash
curl -X DELETE "http://localhost:8081/api/v1/strategies/{strategy_id}?user_id=IS14415"
```

Example:
```bash
curl -X DELETE "http://localhost:8081/api/v1/strategies/550e8400-e29b-41d4-a716-446655440000?user_id=IS14415"
```

---

## Using jq for Pretty Output

If you have `jq` installed, pipe the output for better formatting:

```bash
curl http://localhost:8081/api/v1/health | jq .
```

```bash
curl http://localhost:8081/api/v1/users/IS14415/strategies | jq .
```

---

## Quick Test Sequence

### 1. Test Health
```bash
curl http://localhost:8081/api/v1/health
```

### 2. Create a Strategy
```bash
curl -X POST http://localhost:8081/api/v1/strategies \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "IS14415",
    "strategy_name": "Quick Test",
    "description": "Quick test strategy",
    "activate_immediately": true,
    "conditions": {
      "impact_score_threshold": 5,
      "sentiments": [1]
    },
    "trade_config": {
      "order_type": 1,
      "quantity": 10,
      "exchange": 1,
      "order_side": 1,
      "validity": "DAY"
    },
    "risk_limits": {
      "max_daily_trades": 5,
      "max_loss_per_day": 5000.0,
      "enable_risk_checks": true
    }
  }' | jq .
```

### 3. List All Strategies
```bash
curl http://localhost:8081/api/v1/users/IS14415/strategies | jq .
```

---

## Enum Values Reference

When creating/updating strategies, use these numeric values:

### Sentiment
- `1` = POSITIVE
- `2` = NEUTRAL
- `3` = NEGATIVE

### Exchange
- `1` = NSE
- `2` = BSE

### OrderType
- `1` = MARKET
- `2` = LIMIT
- `3` = STOP_LOSS
- `4` = STOP_LOSS_MARKET

### OrderSide
- `1` = BUY
- `2` = SELL

### PositionSizing
- `1` = FIXED
- `2` = PERCENTAGE

---

## Testing Tips

1. **Save Strategy ID**: After creating a strategy, save the `strategy_id` from the response
2. **Check Version**: When updating, use the correct `version` number to prevent conflicts
3. **Test Pagination**: Try different page numbers and page sizes
4. **Test Filters**: Try `active_only=true` and `active_only=false`
5. **Monitor Logs**: Check both gateway and user-config service logs for any errors

---

## Running the Test Script

Make the test script executable and run it:

```bash
cd /home/rohitt/Desktop/trading-system/api/gateway
chmod +x test_api.sh
./test_api.sh
```

This will run all tests in sequence and prompt you for the strategy_id after creation.
