# Create Strategy API Documentation

## Overview
This API allows users to create automated trading strategies that execute trades when specific news events and market conditions are met.

## Endpoint

```
POST /api/v1/strategies
```

**Base URL**: `http://localhost:8080` (API Gateway)

## Authentication
Include authentication headers as required by your API Gateway configuration.

---

## Request Body

### Content-Type
```
application/json
```

### Schema

```json
{
  "user_id": "string (required)",
  "strategy_name": "string (required)",
  "description": "string (optional)",
  "activate_immediately": "boolean (optional)",
  "conditions": {
    "match_all_news": "boolean (optional)",
    "impact_score_threshold": "integer (required)",
    "sentiments": "array of integers (optional)",
    "categories": "array of strings (optional)",
    "stock_codes": "array of int64 (optional)",
    "price_range": {
      "min_price": "float (optional)",
      "max_price": "float (optional)"
    },
    "volume_threshold": "int64 (optional)",
    "pct_change_threshold": "float (optional)",
    "exchanges": "array of integers (optional)"
  },
  "trade_config": {
    "order_type": "integer (required)",
    "quantity": "integer (required)",
    "max_position_size": "float (optional)",
    "stop_loss_pct": "float (optional)",
    "take_profit_pct": "float (optional)",
    "exchange": "integer (required)",
    "order_side": "integer (required)",
    "limit_price": "float (optional)",
    "validity": "string (required)"
  },
  "risk_limits": {
    "max_daily_trades": "integer (optional)",
    "max_loss_per_day": "float (optional)",
    "position_sizing": "integer (required)",
    "max_portfolio_exposure_pct": "float (optional)",
    "max_per_trade_risk": "float (optional)",
    "enable_risk_checks": "boolean (required)"
  }
}
```

---

## Field Descriptions

### Top-Level Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `user_id` | string | **Yes** | Unique identifier for the user creating the strategy |
| `strategy_name` | string | **Yes** | Name of the strategy (e.g., "Tech Stock Buy on Positive News") |
| `description` | string | No | Detailed description of what this strategy does |
| `activate_immediately` | boolean | No | If `true`, strategy becomes active immediately after creation. If `false` or omitted, strategy is created in inactive state |

---

### conditions Object

Defines what news events and market conditions will trigger this strategy.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `match_all_news` | boolean | No | `false` | **Special flag**: When set to `true`, this strategy will match EVERY news event, ignoring all other condition filters except `impact_score_threshold`. Use this for catch-all strategies |
| `impact_score_threshold` | int32 | **Yes** | - | Minimum news impact score (1-10). Only news with impact score ≥ this value will trigger the strategy. This filter applies even when `match_all_news=true` |
| `sentiments` | array of int32 | No | - | Filter by news sentiment. **Values**: `1` = POSITIVE, `2` = NEUTRAL, `3` = NEGATIVE. Example: `[1, 2]` matches positive or neutral news. **Ignored if** `match_all_news=true` |
| `categories` | array of string | No | - | News categories to monitor (e.g., `["earnings", "merger", "regulatory"]`). Only news in these categories will trigger. **Ignored if** `match_all_news=true` |
| `stock_codes` | array of int64 | No | - | Specific stock codes to monitor (e.g., `[517170, 500325]`). Only news about these stocks will trigger. **Ignored if** `match_all_news=true` |
| `price_range` | object | No | - | Filter stocks by current price range. Contains `min_price` (float) and `max_price` (float). **Ignored if** `match_all_news=true` |
| `volume_threshold` | int64 | No | - | Minimum trading volume required for stock. **Ignored if** `match_all_news=true` |
| `pct_change_threshold` | float | No | - | Trigger only if stock price has changed by this percentage (e.g., `2.5` for 2.5% change). **Ignored if** `match_all_news=true` |
| `exchanges` | array of int32 | No | - | Exchanges to monitor. **Values**: `1` = NSE, `2` = BSE. Example: `[1]` for NSE only. **Ignored if** `match_all_news=true` |

#### Important Notes on Conditions:
- When `match_all_news=true`, **only** `impact_score_threshold` is evaluated
- All other condition filters are **ignored** when `match_all_news=true`
- This allows creating broad strategies like "trade on all high-impact news"

---

### trade_config Object

Defines how trades will be executed when the strategy triggers.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `order_type` | int32 | **Yes** | Order type. **Values**: `1` = MARKET, `2` = LIMIT, `3` = STOP_LOSS, `4` = STOP_LOSS_MARKET |
| `quantity` | int32 | **Yes** | Fixed number of shares to trade (e.g., `10` shares) |
| `max_position_size` | float | No | Maximum position value in rupees (e.g., `50000.00` for ₹50,000). System will not create position larger than this value |
| `stop_loss_pct` | float | No | Stop loss percentage (e.g., `2.5` means exit if price drops 2.5% from entry) |
| `take_profit_pct` | float | No | Take profit percentage (e.g., `5.0` means exit if price gains 5% from entry) |
| `exchange` | int32 | **Yes** | Primary exchange for trade execution. **Values**: `1` = NSE, `2` = BSE |
| `order_side` | int32 | **Yes** | Trade direction. **Values**: `1` = BUY, `2` = SELL |
| `limit_price` | float | No | Price limit for LIMIT orders (e.g., `125.50`). **Required** when `order_type=2`, otherwise optional |
| `validity` | string | **Yes** | Order validity. **Values**: `"DAY"` (valid until market close), `"IOC"` (Immediate or Cancel) |

---

### risk_limits Object

Risk management controls to prevent excessive losses and over-trading.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `max_daily_trades` | int32 | No | Maximum number of trades allowed per day for this strategy (e.g., `5`). Prevents over-trading |
| `max_loss_per_day` | float | No | Maximum loss in rupees allowed per day (e.g., `10000.00` for ₹10,000). Strategy will be paused if reached |
| `position_sizing` | int32 | **Yes** | Position sizing method. **Values**: `1` = FIXED (use fixed quantity from trade_config), `2` = PERCENTAGE (% of capital), `3` = RISK_BASED (based on risk amount) |
| `max_portfolio_exposure_pct` | float | No | Maximum portfolio exposure as percentage (e.g., `20.0` for 20% of total portfolio). Limits concentration risk |
| `max_per_trade_risk` | float | No | Maximum risk per individual trade in rupees (e.g., `5000.00` for ₹5,000) |
| `enable_risk_checks` | boolean | **Yes** | Enable or disable risk validation before trade execution. **Recommendation**: Always set to `true` for safety |

---

## Response

### Success Response (201 Created)

```json
{
  "success": true,
  "strategy": {
    "strategy_id": "550e8400-e29b-41d4-a716-446655440000",
    "user_id": "USER123",
    "strategy_name": "Buy Tech Stocks on Positive News",
    "description": "Automatically buy tech stocks when positive earnings news is released",
    "active": true,
    "conditions": {
      "match_all_news": false,
      "impact_score_threshold": 7,
      "sentiments": [1],
      "categories": ["earnings", "financial_results"],
      "stock_codes": [517170, 500325],
      "exchanges": [1],
      "price_range": null,
      "volume_threshold": 0,
      "pct_change_threshold": 0
    },
    "trade_config": {
      "order_type": 1,
      "quantity": 10,
      "max_position_size": 50000.00,
      "stop_loss_pct": 2.0,
      "take_profit_pct": 5.0,
      "exchange": 1,
      "order_side": 1,
      "limit_price": 0,
      "validity": "DAY"
    },
    "risk_limits": {
      "max_daily_trades": 5,
      "max_loss_per_day": 10000.00,
      "position_sizing": 1,
      "max_portfolio_exposure_pct": 20.0,
      "max_per_trade_risk": 5000.00,
      "enable_risk_checks": true
    },
    "created_at": {
      "seconds": 1702224000,
      "nanos": 0
    },
    "updated_at": {
      "seconds": 1702224000,
      "nanos": 0
    },
    "version": 1
  }
}
```

### Error Response (400 Bad Request)

```json
{
  "success": false,
  "error": {
    "code": "CREATION_FAILED",
    "message": "Invalid request: user_id is required"
  }
}
```

### Error Response (500 Internal Server Error)

```json
{
  "success": false,
  "error": {
    "code": "CREATION_FAILED",
    "message": "Failed to create strategy: database connection error"
  }
}
```

---

## Example Requests

### Example 1: Basic Buy Strategy on Positive Earnings News

```json
{
  "user_id": "USER123",
  "strategy_name": "Buy Tech Stocks on Positive Earnings",
  "description": "Automatically buy tech stocks when positive earnings news is released",
  "activate_immediately": true,
  "conditions": {
    "match_all_news": false,
    "impact_score_threshold": 7,
    "sentiments": [1],
    "categories": ["earnings", "financial_results"],
    "stock_codes": [517170, 500325],
    "exchanges": [1]
  },
  "trade_config": {
    "order_type": 1,
    "quantity": 10,
    "max_position_size": 50000.00,
    "stop_loss_pct": 2.0,
    "take_profit_pct": 5.0,
    "exchange": 1,
    "order_side": 1,
    "validity": "DAY"
  },
  "risk_limits": {
    "max_daily_trades": 5,
    "max_loss_per_day": 10000.00,
    "position_sizing": 1,
    "max_portfolio_exposure_pct": 20.0,
    "max_per_trade_risk": 5000.00,
    "enable_risk_checks": true
  }
}
```

### Example 2: Catch-All Strategy (Match All News)

```json
{
  "user_id": "USER456",
  "strategy_name": "Trade All High-Impact News",
  "description": "Execute trades on any news with impact score >= 8",
  "activate_immediately": false,
  "conditions": {
    "match_all_news": true,
    "impact_score_threshold": 8
  },
  "trade_config": {
    "order_type": 1,
    "quantity": 5,
    "exchange": 1,
    "order_side": 1,
    "validity": "DAY"
  },
  "risk_limits": {
    "max_daily_trades": 10,
    "max_loss_per_day": 15000.00,
    "position_sizing": 1,
    "enable_risk_checks": true
  }
}
```

### Example 3: Limit Order Strategy with Price Range Filter

```json
{
  "user_id": "USER789",
  "strategy_name": "Buy Undervalued Stocks",
  "description": "Buy stocks in price range 100-500 on negative news (contrarian strategy)",
  "activate_immediately": true,
  "conditions": {
    "match_all_news": false,
    "impact_score_threshold": 6,
    "sentiments": [3],
    "price_range": {
      "min_price": 100.0,
      "max_price": 500.0
    },
    "exchanges": [1, 2]
  },
  "trade_config": {
    "order_type": 2,
    "quantity": 20,
    "limit_price": 250.00,
    "max_position_size": 75000.00,
    "stop_loss_pct": 3.0,
    "take_profit_pct": 10.0,
    "exchange": 1,
    "order_side": 1,
    "validity": "DAY"
  },
  "risk_limits": {
    "max_daily_trades": 3,
    "max_loss_per_day": 20000.00,
    "position_sizing": 1,
    "max_portfolio_exposure_pct": 30.0,
    "max_per_trade_risk": 7500.00,
    "enable_risk_checks": true
  }
}
```

---

## Enum Reference

### Sentiment Values
| Value | Description |
|-------|-------------|
| `1` | POSITIVE |
| `2` | NEUTRAL |
| `3` | NEGATIVE |

### Order Type Values
| Value | Description |
|-------|-------------|
| `1` | MARKET - Execute at current market price |
| `2` | LIMIT - Execute at specified price or better |
| `3` | STOP_LOSS - Trigger when price reaches stop level |
| `4` | STOP_LOSS_MARKET - Stop loss as market order |

### Exchange Values
| Value | Description |
|-------|-------------|
| `1` | NSE (National Stock Exchange) |
| `2` | BSE (Bombay Stock Exchange) |

### Order Side Values
| Value | Description |
|-------|-------------|
| `1` | BUY |
| `2` | SELL |

### Position Sizing Values
| Value | Description |
|-------|-------------|
| `1` | FIXED - Use fixed quantity from trade_config |
| `2` | PERCENTAGE - Calculate based on % of capital |
| `3` | RISK_BASED - Calculate based on risk amount |

---

## Important Implementation Notes

### For Frontend Developers:

1. **Data Types**
   - All enum fields use **integers**, not strings
   - Use **floats** for monetary values and percentages (e.g., `2.5` not `"2.5"`)
   - Use **int64** for stock codes (may be large numbers)

2. **Timestamp Format**
   - Timestamps are returned as objects with `seconds` (Unix timestamp) and `nanos` (nanoseconds)
   - Convert to JavaScript Date: `new Date(timestamp.seconds * 1000)`

3. **Version Control**
   - The `version` field returned is used for optimistic locking
   - Store this value and send it back when updating the strategy
   - Prevents concurrent modification conflicts

4. **Match All News Feature**
   - When `match_all_news=true`, most condition filters are ignored
   - Only `impact_score_threshold` still applies
   - Useful for broad strategies that react to any significant news

5. **Inactive Strategies**
   - If `activate_immediately=false` or omitted, strategy is created but inactive
   - User must explicitly activate it using the Activate Strategy API
   - Allows users to test/review strategy before going live

6. **Required vs Optional Fields**
   - **Always required**: `user_id`, `strategy_name`, `conditions`, `trade_config`, `risk_limits`
   - Within nested objects, some fields are required (see tables above)
   - Optional fields can be omitted entirely or set to `null`

7. **Validation**
   - Impact score must be 1-10
   - Percentages should be positive numbers
   - Quantity must be positive integer
   - Exchange and order_type must use valid enum values

---

## Error Codes

| Code | HTTP Status | Description |
|------|-------------|-------------|
| `CREATION_FAILED` | 400/500 | Strategy creation failed due to validation or system error |
| `INVALID_INPUT` | 400 | Request body validation failed |
| `INVALID_STRATEGY_ID` | 400 | Strategy ID format is invalid (for other endpoints) |

---

## Related Endpoints

- **GET** `/api/v1/strategies` - List all user strategies
- **GET** `/api/v1/strategies/{strategy_id}` - Get specific strategy
- **PUT** `/api/v1/strategies/{strategy_id}` - Update strategy
- **DELETE** `/api/v1/strategies/{strategy_id}` - Delete strategy
- **POST** `/api/v1/strategies/{strategy_id}/activate` - Activate strategy
- **POST** `/api/v1/strategies/{strategy_id}/deactivate` - Deactivate strategy

---

## Testing with cURL

```bash
curl -X POST http://localhost:8080/api/v1/strategies \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "USER123",
    "strategy_name": "Test Strategy",
    "description": "Testing strategy creation",
    "activate_immediately": true,
    "conditions": {
      "impact_score_threshold": 7,
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
      "position_sizing": 1,
      "enable_risk_checks": true
    }
  }'
```

---

## Support

For questions or issues, contact the backend team or refer to:
- API Gateway Documentation: `/docs/api/`
- System Architecture: `/docs/architecture/`
- Kafka Topics Guide: `/docs/guides/KAFKA_TOPICS_GUIDE.md`
