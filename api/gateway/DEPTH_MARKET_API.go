package main

/*
DEPTH MARKET STRATEGY API DOCUMENTATION

This file documents the REST API for creating trading strategies optimized for market depth analysis.

================================================================================
ENDPOINT: POST /api/v1/strategies/depth-market/create
================================================================================

DESCRIPTION:
Creates a new trading strategy specialized for market depth-based trading.
This endpoint simplifies the creation process by automatically setting depth-specific
parameters and validating depth-market requirements.

AUTHENTICATION HEADERS:
- Authorization: Bearer <JWT_TOKEN>  (Required)
- appId: <APP_ID>                     (Required)
- source: <SOURCE_PLATFORM>           (Required)

REQUEST BODY SCHEMA:
{
  "user_id": "string",                              // Required: Unique user identifier
  "strategy_name": "string",                        // Required: Name of the strategy
  "description": "string",                          // Optional: Strategy description

  // DEPTH MARKET SPECIFIC CONDITIONS
  "stock_codes": [1, 2, 3],                        // Required: Array of stock codes
  "exchanges": ["NSE", "BSE"],                     // Required: Trading exchanges
  "min_bid_quantity": 100,                         // Optional: Minimum bid quantity for depth
  "min_ask_quantity": 100,                         // Optional: Minimum ask quantity for depth
  "max_spread_pct": 0.5,                           // Optional: Maximum bid-ask spread (%)
  "require_ltp_between_spread": true,              // Optional: LTP must be between bid/ask
  "price_range_min": 100.0,                        // Optional: Minimum stock price
  "price_range_max": 5000.0,                       // Optional: Maximum stock price
  "volume_threshold": 1000000,                     // Optional: Minimum daily volume
  "min_market_cap": 100.0,                         // Optional: Minimum market cap (crores)
  "max_market_cap": 100000.0,                      // Optional: Maximum market cap (crores)

  // TRADE EXECUTION CONFIGURATION
  "order_type": "LIMIT",                           // Required: LIMIT or MARKET
  "order_side": "BUY",                             // Required: BUY or SELL
  "quantity": 100,                                 // Required: Order quantity
  "exchange": "NSE",                               // Required: Execution exchange
  "limit_price": 245.50,                           // Optional: For LIMIT orders
  "max_position_size": 100000.0,                   // Optional: Maximum position value
  "validity": "DAY",                               // Optional: Order validity (DAY, GTC, IOC)
  "product_type": "INTRADAY",                      // Optional: INTRADAY, DELIVERY, CASH

  // STOP LOSS & TAKE PROFIT
  "stop_loss_pct": 2.0,                            // Optional: Stop loss percentage
  "take_profit_pct": 3.0,                          // Optional: Take profit percentage
  "stop_loss_type": "FIXED",                       // Optional: FIXED or TRAILING
  "trailing_sl_pct": 1.5,                          // Optional: Trailing SL percentage

  // RISK MANAGEMENT
  "max_daily_trades": 20,                          // Optional: Maximum trades per day
  "max_loss_per_day": 10000.0,                     // Optional: Maximum daily loss (Rs)
  "position_sizing": "FIXED",                      // Optional: FIXED or PERCENTAGE
  "max_portfolio_exposure_pct": 10.0,              // Optional: Max portfolio exposure (%)
  "max_per_trade_risk": 1000.0,                    // Optional: Max risk per trade (Rs)
  "enable_risk_checks": true,                      // Optional: Enable risk validations
  "enable_auto_square_off": true,                  // Optional: Auto-close at market end
  "auto_square_off_time": "15:05",                 // Optional: Close time (HH:MM format)

  // AUTHENTICATION & ACTIVATION
  "activate_immediately": true,                    // Optional: Activate after creation
  "bearer_token": "jwt_token_here",                // Optional: Override header token
  "app_id": "app_id_here",                         // Optional: Override header app_id
  "source": "WEB"                                  // Optional: Override header source
}

RESPONSE (Success - 201 Created):
{
  "success": true,
  "strategy": {
    "strategy_id": "550e8400-e29b-41d4-a716-446655440000",
    "user_id": "user_123",
    "strategy_name": "Balanced Market Depth Strategy",
    "description": "Trade stocks with good liquidity and tight spreads",
    "active": true,
    "version": 1,
    "created_at": "2025-01-02T10:30:00Z",
    "updated_at": "2025-01-02T10:30:00Z",
    "conditions": {
      "condition_id": "550e8400-e29b-41d4-a716-446655440001",
      "stock_codes": [1, 2, 3],
      "exchanges": ["NSE"],
      "min_bid_quantity": 100,
      "min_ask_quantity": 100,
      "max_spread_pct": 0.5,
      "depth_only": true
    },
    "trade_config": {
      "trade_config_id": "550e8400-e29b-41d4-a716-446655440002",
      "order_type": "LIMIT",
      "order_side": "BUY",
      "quantity": 100,
      "exchange": "NSE",
      "stop_loss_pct": 2.0,
      "take_profit_pct": 3.0
    },
    "risk_limits": {
      "risk_limit_id": "550e8400-e29b-41d4-a716-446655440003",
      "max_daily_trades": 20,
      "max_loss_per_day": 10000.0,
      "enable_auto_square_off": true,
      "auto_square_off_time": "15:05"
    }
  }
}

RESPONSE (Error - 400 Bad Request):
{
  "error": "stock_codes and exchanges are required"
}

RESPONSE (Error - 500 Internal Server Error):
{
  "error": "Failed to create depth market strategy: [error details]"
}

================================================================================
CURL EXAMPLES
================================================================================

1. BASIC DEPTH MARKET STRATEGY:
curl -X POST http://localhost:8080/api/v1/strategies/depth-market/create \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer your_jwt_token" \
  -H "appId: your_app_id" \
  -H "source: WEB" \
  -d '{
    "user_id": "user_123",
    "strategy_name": "Depth Market Strategy",
    "description": "Trade high-liquidity stocks based on market depth",
    "stock_codes": [1, 2, 3, 4, 5],
    "exchanges": ["NSE"],
    "min_bid_quantity": 200,
    "min_ask_quantity": 200,
    "max_spread_pct": 0.3,
    "order_type": "LIMIT",
    "order_side": "BUY",
    "quantity": 100,
    "exchange": "NSE",
    "stop_loss_pct": 2.0,
    "take_profit_pct": 3.0,
    "validity": "DAY",
    "product_type": "INTRADAY",
    "position_sizing": "FIXED",
    "enable_risk_checks": true,
    "activate_immediately": true
  }'

2. ADVANCED DEPTH MARKET STRATEGY WITH RISK LIMITS:
curl -X POST http://localhost:8080/api/v1/strategies/depth-market/create \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer your_jwt_token" \
  -H "appId: your_app_id" \
  -H "source: WEB" \
  -d '{
    "user_id": "user_456",
    "strategy_name": "Conservative Depth Trading",
    "description": "Conservative depth-based strategy with strict risk controls",
    "stock_codes": [10, 11, 12],
    "exchanges": ["NSE", "BSE"],
    "min_bid_quantity": 500,
    "min_ask_quantity": 500,
    "max_spread_pct": 0.25,
    "require_ltp_between_spread": true,
    "price_range_min": 500.0,
    "price_range_max": 2000.0,
    "volume_threshold": 5000000,
    "min_market_cap": 1000.0,
    "order_type": "LIMIT",
    "order_side": "BUY",
    "quantity": 50,
    "exchange": "NSE",
    "limit_price": 1250.0,
    "max_position_size": 50000.0,
    "stop_loss_pct": 1.5,
    "take_profit_pct": 2.5,
    "stop_loss_type": "FIXED",
    "validity": "DAY",
    "product_type": "INTRADAY",
    "max_daily_trades": 10,
    "max_loss_per_day": 5000.0,
    "position_sizing": "FIXED",
    "max_portfolio_exposure_pct": 5.0,
    "max_per_trade_risk": 500.0,
    "enable_risk_checks": true,
    "enable_auto_square_off": true,
    "auto_square_off_time": "15:00",
    "activate_immediately": true
  }'

3. DEPTH MARKET STRATEGY WITH TRAILING STOP LOSS:
curl -X POST http://localhost:8080/api/v1/strategies/depth-market/create \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer your_jwt_token" \
  -H "appId: your_app_id" \
  -H "source: WEB" \
  -d '{
    "user_id": "user_789",
    "strategy_name": "Trailing Stop Depth Strategy",
    "description": "Depth strategy with trailing stop loss for trend following",
    "stock_codes": [20, 21, 22, 23],
    "exchanges": ["NSE"],
    "min_bid_quantity": 300,
    "min_ask_quantity": 300,
    "max_spread_pct": 0.4,
    "order_type": "LIMIT",
    "order_side": "BUY",
    "quantity": 200,
    "exchange": "NSE",
    "stop_loss_type": "TRAILING",
    "trailing_sl_pct": 2.0,
    "take_profit_pct": 5.0,
    "validity": "DAY",
    "product_type": "INTRADAY",
    "position_sizing": "PERCENTAGE",
    "enable_risk_checks": true,
    "enable_auto_square_off": true,
    "auto_square_off_time": "15:30",
    "activate_immediately": true
  }'

================================================================================
KEY FEATURES OF DEPTH MARKET STRATEGY API
================================================================================

1. SIMPLIFIED INTERFACE:
   - Dedicated endpoint for depth market strategies
   - Automatic setting of depth_only flag
   - Reduced configuration complexity

2. DEPTH-SPECIFIC PARAMETERS:
   - min_bid_quantity: Filters stocks with sufficient bid volume
   - min_ask_quantity: Filters stocks with sufficient ask volume
   - max_spread_pct: Ensures tight bid-ask spreads for better execution
   - require_ltp_between_spread: Validates LTP within bid-ask range

3. MARKET QUALITY FILTERS:
   - volume_threshold: Ensures adequate trading volume
   - price_range: Controls stock price range
   - market_cap_range: Filters by company market capitalization

4. COMPREHENSIVE RISK MANAGEMENT:
   - Daily trade limits
   - Daily loss limits
   - Per-trade risk management
   - Auto square-off at market close
   - Position sizing strategies (FIXED or PERCENTAGE)

5. FLEXIBLE STOP LOSS OPTIONS:
   - FIXED: Static stop loss percentage
   - TRAILING: Dynamic stop loss that follows price

6. AUTHENTICATION:
   - Bearer token for JWT validation
   - App ID for client identification
   - Source field for platform tracking

================================================================================
VALIDATION RULES
================================================================================

Required Fields:
- user_id: Must not be empty
- strategy_name: Must not be empty
- stock_codes: Must have at least 1 stock code
- exchanges: Must have at least 1 exchange
- order_type: Must be one of [LIMIT, MARKET]
- order_side: Must be one of [BUY, SELL]
- quantity: Must be greater than 0
- exchange: Must not be empty
- bearer_token/Authorization: Required for execution
- app_id: Required for execution
- source: Required for execution

Conditional Fields:
- limit_price: Required if order_type is LIMIT
- stop_loss_pct: Typically 0.5-5.0%
- take_profit_pct: Typically 1.0-10.0%
- trailing_sl_pct: Required if stop_loss_type is TRAILING

Range Validations:
- max_spread_pct: 0.1-2.0 (percentage)
- price_range_min < price_range_max (if both specified)
- min_market_cap < max_market_cap (if both specified)
- volume_threshold: > 0 (minimum shares)

================================================================================
USAGE PATTERNS
================================================================================

Pattern 1: Quick Create with Defaults
Only specify essential parameters; system uses sensible defaults for others.

Pattern 2: Conservative Trading
Enable all risk checks, set low max_daily_trades, use FIXED stop loss.

Pattern 3: Aggressive Trading
Higher max_daily_trades, TRAILING stop loss, larger quantities.

Pattern 4: Market-Cap Filtered
Use min_market_cap and max_market_cap to focus on specific company sizes.

Pattern 5: Multi-Exchange Strategy
Specify multiple exchanges in the exchanges array.

================================================================================
HTTP STATUS CODES
================================================================================

201 Created - Strategy successfully created
400 Bad Request - Invalid parameters or missing required fields
401 Unauthorized - Invalid authentication credentials
500 Internal Server Error - Server-side error during creation
503 Service Unavailable - Backend service unreachable

================================================================================
RELATED ENDPOINTS
================================================================================

GET /api/v1/strategies/{strategy_id}
  - Retrieve a specific strategy

PUT /api/v1/strategies/{strategy_id}
  - Update an existing strategy

DELETE /api/v1/strategies/{strategy_id}
  - Delete a strategy

POST /api/v1/strategies/{strategy_id}/activate
  - Activate a strategy

POST /api/v1/strategies/{strategy_id}/deactivate
  - Deactivate a strategy

GET /api/v1/users/{user_id}/strategies
  - List all strategies for a user

*/
