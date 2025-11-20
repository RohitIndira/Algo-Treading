# Postman/cURL Commands for Test User Configs

## Create Test Strategy 1 - Match Most News

```bash
curl --location 'http://localhost:50051/user_config.UserConfigService/CreateStrategy' \
--header 'Content-Type: application/json' \
--data '{
  "user_id": "TEST_USER_001",
  "strategy_name": "Match All News - Test Strategy",
  "description": "Simple test strategy that matches most news events with minimal filters",
  "activate_immediately": true,
  "conditions": {
    "impact_score_threshold": 1,
    "sentiments": [
      "SENTIMENT_POSITIVE",
      "SENTIMENT_NEUTRAL",
      "SENTIMENT_NEGATIVE"
    ],
    "categories": [
      "Results",
      "Board Meeting",
      "Announcements",
      "Corporate Actions",
      "Dividends",
      "Mergers & Acquisitions",
      "Regulatory",
      "Financial Results",
      "Management Changes",
      "Market News"
    ],
    "stock_codes": [],
    "exchanges": [
      "EXCHANGE_NSE",
      "EXCHANGE_BSE"
    ]
  },
  "trade_config": {
    "order_type": "ORDER_TYPE_MARKET",
    "quantity": 1,
    "exchange": "EXCHANGE_NSE",
    "order_side": "ORDER_SIDE_BUY",
    "validity": "DAY",
    "max_position_size": 10000.0,
    "stop_loss_pct": 1.0,
    "take_profit_pct": 2.0
  },
  "risk_limits": {
    "max_daily_trades": 100,
    "max_loss_per_day": 50000.0,
    "position_sizing": "POSITION_SIZING_FIXED",
    "max_portfolio_exposure_pct": 50.0,
    "max_per_trade_risk": 5000.0,
    "enable_risk_checks": true
  }
}'
```

## Create Test Strategy 2 - Match ALL News

```bash
curl --location 'http://localhost:50051/user_config.UserConfigService/CreateStrategy' \
--header 'Content-Type: application/json' \
--data '{
  "user_id": "TEST_USER_002",
  "strategy_name": "Match ALL News Events",
  "description": "Matches every single news event regardless of sentiment, category, or stock",
  "activate_immediately": true,
  "conditions": {
    "match_all_news": true,
    "impact_score_threshold": 1
  },
  "trade_config": {
    "order_type": "ORDER_TYPE_MARKET",
    "quantity": 1,
    "exchange": "EXCHANGE_NSE",
    "order_side": "ORDER_SIDE_BUY",
    "validity": "DAY",
    "max_position_size": 5000.0,
    "stop_loss_pct": 0.5,
    "take_profit_pct": 1.0
  },
  "risk_limits": {
    "max_daily_trades": 200,
    "max_loss_per_day": 100000.0,
    "position_sizing": "POSITION_SIZING_FIXED",
    "max_portfolio_exposure_pct": 75.0,
    "max_per_trade_risk": 2000.0,
    "enable_risk_checks": true
  }
}'
```

## Create Test Strategy 3 - GOLDEN STRATEGY

```bash
curl --location 'http://localhost:50051/user_config.UserConfigService/CreateStrategy' \
--header 'Content-Type: application/json' \
--data '{
  "user_id": "user-123",
  "strategy_name": "GOLDEN-STRATEGY",
  "description": "Trade everything - no restrictions",
  "activate_immediately": true,
  "conditions": {
    "match_all_news": false,
    "impact_score_threshold": 1,
    "sentiments": [],
    "categories": [],
    "stock_codes": [],
    "exchanges": [],
    "price_range": {
      "min_price": 0,
      "max_price": 999999999
    },
    "volume_threshold": 0,
    "pct_change_threshold": 0
  },
  "trade_config": {
    "order_type": "ORDER_TYPE_MARKET",
    "order_side": "ORDER_SIDE_BUY",
    "quantity": 1,
    "exchange": "EXCHANGE_NSE",
    "validity": "DAY"
  },
  "risk_limits": {
    "max_daily_trades": 100,
    "max_loss_per_day": 50000.0,
    "enable_risk_checks": true,
    "position_sizing": "POSITION_SIZING_FIXED"
  }
}'
```

## List Strategies

### List TEST_USER_001 Strategies
```bash
curl --location 'http://localhost:50051/user_config.UserConfigService/ListUserStrategies' \
--header 'Content-Type: application/json' \
--data '{
  "user_id": "TEST_USER_001"
}'
```

### List TEST_USER_002 Strategies
```bash
curl --location 'http://localhost:50051/user_config.UserConfigService/ListUserStrategies' \
--header 'Content-Type: application/json' \
--data '{
  "user_id": "TEST_USER_002"
}'
```

### List user-123 Strategies
```bash
curl --location 'http://localhost:50051/user_config.UserConfigService/ListUserStrategies' \
--header 'Content-Type: application/json' \
--data '{
  "user_id": "user-123"
}'
```

## Health Check

```bash
curl --location 'http://localhost:50051/user_config.UserConfigService/HealthCheck' \
--header 'Content-Type: application/json' \
--data '{
  "service": "user-config-service"
}'
```

---

## Note for Postman

**Important**: gRPC services typically don't work with regular HTTP cURL commands. You need to:

1. **Use Postman with gRPC support** (v9.7.1+):
   - New → gRPC Request
   - Server URL: `localhost:50051`
   - Select method: `user_config.UserConfigService/CreateStrategy`
   - Paste JSON body from above

2. **Or use grpcurl** (command-line tool for gRPC):
   ```bash
   # Windows PowerShell - use JSON files
   Get-Content test_news_config.json | grpcurl -plaintext -d '@' localhost:50051 user_config.UserConfigService/CreateStrategy
   ```

3. **Or install gRPC extension** in Postman and import the proto file:
   - File: `d:\Algo_Trade\Algo-Treading\api\proto\user_config\user_config.proto`
