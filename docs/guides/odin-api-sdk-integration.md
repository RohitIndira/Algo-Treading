# Odin Trading API SDK Integration Guide

## Overview

The `b2c-api-python` SDK (IBT/Odin Trading API) provides comprehensive trading capabilities for Indian stock markets. This document details all available methods and their integration into our Go-based trading system.

---

## SDK Architecture

### Core Components

1. **IBTConnect Class** - Main client for API interactions
2. **Authentication** - Session management with TOTP
3. **REST API** - Order management and portfolio operations
4. **WebSocket** - Real-time market data and order updates

### Base Configuration

```python
ibt_connect = IBTConnect(params={
    "baseurl": "API_ENDPOINT",
    "api_key": "API_KEY",
    "x-api-key": "X_API_KEY",
    "debug": True
})
```

---

## Authentication & Session Management

### 1. Login
**Method**: `login(params)`

**Purpose**: Authenticate user and obtain access token

**Parameters**:
```python
{
    "userId": "USER123",
    "password": "PASSWORD",
    "totp": "123456"  # Time-based OTP
}
```

**Response**:
```python
{
    "data": {
        "user_id": "USER123",
        "access_token": "Bearer_token...",
        "others": {
            "broadCastSocket": "wss://...",
            "messageSocket": "wss://..."
        }
    }
}
```

**Usage Notes**:
- Required before any other API calls
- Access token expires after session timeout
- Store tokens securely for subsequent requests

### 2. Balance
**Method**: `balance()`

**Purpose**: Get user account balance

**Response**:
```python
{
    "data": {
        "available_balance": 100000.00,
        "used_margin": 25000.00,
        "collateral": 50000.00
    }
}
```

### 3. Validate Session
**Method**: `validateSession()`

**Purpose**: Check if current session is valid

**Usage**: Call periodically to ensure session is active

### 4. Logout
**Method**: `logout()`

**Purpose**: Invalidate current session

---

## Order Management

### Regular Orders

#### 1. Place Order
**Method**: `place_order(params)`

**Purpose**: Place a regular market/limit order

**Parameters**:
```python
{
    "scrip_info": {
        "exchange": "NSE_EQ",        # NSE_EQ, BSE_EQ, NSE_FO, etc.
        "scrip_token": 15124,        # Security token
        "symbol": "RELIANCE",        # Trading symbol
        "series": "EQ",              # EQ, BE, etc.
        "expiry_date": "",           # For F&O only
        "strike_price": "",          # For options only
        "option_type": ""            # CE/PE for options
    },
    "transaction_type": "BUY",       # BUY/SELL
    "product_type": "INTRADAY",      # INTRADAY/DELIVERY/MTF
    "order_type": "RL",              # RL (Limit), MKT (Market), SL (Stop Loss)
    "quantity": 10,
    "price": 2500.50,                # For limit orders
    "trigger_price": 0,              # For SL orders
    "disclosed_quantity": 0,         # Iceberg order quantity
    "validity": "DAY",               # DAY/IOC/GTD
    "validity_days": 0,              # For GTD orders
    "is_amo": False,                 # After Market Order
    "order_identifier": "",          # Custom order reference
    "part_code": "",
    "algo_id": "",
    "strategy_id": "",
    "vender_code": ""
}
```

**Response**:
```python
{
    "data": "ORDER12345",  # Order ID
    "message": "Order placed successfully"
}
```

**Order Types**:
- **RL** (Regular Limit) - Limit order with specified price
- **MKT** (Market) - Execute at best available price
- **SL** (Stop Loss) - Trigger order when price reaches trigger price
- **SL-M** (Stop Loss Market) - Market order after trigger

**Product Types**:
- **INTRADAY** - Square off by end of day
- **DELIVERY** - Hold for delivery
- **MTF** - Margin Trading Facility

#### 2. Modify Order
**Method**: `modify_order(params)`

**Purpose**: Modify pending order

**Parameters**:
```python
{
    "exchange": "NSE_EQ",
    "order_id": "ORDER12345",
    "quantity": 20,              # New quantity
    "price": 2505.00,           # New price
    "trigger_price": 2490.00,   # New trigger
    "order_type": "RL",         # Can change order type
    "disclosed_quantity": 0
}
```

**Modifiable Fields**:
- Quantity (can increase/decrease)
- Price (for limit orders)
- Trigger price (for SL orders)
- Order type (within same product type)
- Disclosed quantity

**Non-Modifiable**:
- Exchange
- Symbol
- Transaction type (BUY/SELL)
- Product type

#### 3. Cancel Order
**Method**: `cancel_order(params)`

**Purpose**: Cancel a pending order

**Parameters**:
```python
{
    "exchange": "NSE_EQ",
    "order_id": "ORDER12345"
}
```

**Response**:
```python
{
    "data": "Order cancelled successfully"
}
```

---

### Advanced Order Types

#### 1. Cover Orders
**Purpose**: Orders with automatic stop loss

**Place Cover Order**:
```python
place_cover_order({
    "scrip_info": {...},
    "transaction_type": "BUY",
    "quantity": 100,
    "price": 2500,
    "trigger_price": 2450,  # Mandatory for cover orders
    "validity": "DAY"
})
```

**Modify Cover Order**:
```python
modify_cover_order({
    "exchange": "NSE_EQ",
    "order_id": "COVER123",
    "trigger_price": 2460  # Can modify SL
})
```

**Cancel Cover Order**:
```python
cancel_cover_order({
    "exchange": "NSE_EQ",
    "order_id": "COVER123"
})
```

#### 2. Bracket Orders
**Purpose**: Orders with both stop loss and target

**Place Bracket Order**:
```python
place_bracket_order({
    "scrip_info": {...},
    "transaction_type": "BUY",
    "quantity": 50,
    "price": 2500,
    "stop_loss": 2450,      # SL price
    "target": 2550,         # Target price
    "trailing_stop_loss": 10  # Trailing SL in points
})
```

**Modify Bracket Order**:
```python
modify_bracket_order({
    "exchange": "NSE_EQ",
    "order_id": "BRACKET123",
    "stop_loss": 2455,
    "target": 2560
})
```

#### 3. Multileg Orders
**Purpose**: Complex option strategies (spreads, straddles, etc.)

**Place Multileg Order**:
```python
place_multileg_order({
    "order_legs": [
        {
            "scrip_info": {...},  # Call option
            "transaction_type": "BUY",
            "quantity": 50
        },
        {
            "scrip_info": {...},  # Put option
            "transaction_type": "SELL",
            "quantity": 50
        }
    ],
    "order_type": "RL",
    "price": 150.00  # Net premium
})
```

---

## Order & Trade Information

### 1. Order Book
**Method**: `get_order_book(params)`

**Purpose**: Get all orders with filtering

**Parameters**:
```python
{
    "offset": 1,            # Pagination offset
    "limit": 20,            # Results per page
    "orderStatus": "OPEN",  # Filter by status (optional)
    "order_id": "ORDER123"  # Filter by specific order (optional)
}
```

**Response**:
```python
{
    "data": [
        {
            "order_id": "ORDER123",
            "exchange": "NSE_EQ",
            "symbol": "RELIANCE",
            "transaction_type": "BUY",
            "quantity": 10,
            "price": 2500,
            "filled_quantity": 5,
            "status": "PARTIALLY_FILLED",
            "order_time": "2025-01-10T10:30:00",
            "product_type": "INTRADAY"
        }
    ],
    "pagination": {
        "total": 100,
        "offset": 1,
        "limit": 20
    }
}
```

**Order Statuses**:
- **OPEN** - Order placed, waiting execution
- **PARTIALLY_FILLED** - Partially executed
- **FILLED** - Completely executed
- **CANCELLED** - Cancelled by user/system
- **REJECTED** - Rejected by exchange
- **PENDING** - Awaiting confirmation

### 2. Trade Book
**Method**: `get_trade_book(params)`

**Purpose**: Get executed trades

**Parameters**:
```python
{
    "offset": 1,
    "limit": 20
}
```

**Response**:
```python
{
    "data": [
        {
            "trade_id": "TRADE123",
            "order_id": "ORDER123",
            "exchange": "NSE_EQ",
            "symbol": "RELIANCE",
            "transaction_type": "BUY",
            "quantity": 5,
            "price": 2500.50,
            "trade_time": "2025-01-10T10:35:00",
            "product_type": "INTRADAY"
        }
    ]
}
```

### 3. Order History
**Method**: `get_order_history(params)`

**Purpose**: Get complete history of specific order

**Parameters**:
```python
{
    "orderId": "ORDER123"
}
```

**Response**:
```python
{
    "data": [
        {
            "timestamp": "2025-01-10T10:30:00",
            "status": "OPEN",
            "message": "Order placed"
        },
        {
            "timestamp": "2025-01-10T10:35:00",
            "status": "PARTIALLY_FILLED",
            "filled_quantity": 5,
            "price": 2500.50
        }
    ]
}
```

---

## Portfolio Management

### 1. Get Positions
**Method**: `get_positions(params)`

**Purpose**: Get current trading positions

**Parameters**:
```python
{
    "type": "DAY"  # DAY or NET
}
```

**Position Types**:
- **DAY** - Today's positions only
- **NET** - Carry forward + today's positions

**Response**:
```python
{
    "data": [
        {
            "exchange": "NSE_EQ",
            "symbol": "RELIANCE",
            "token": 2885,
            "product_type": "INTRADAY",
            "quantity": 10,
            "average_price": 2500.00,
            "current_price": 2515.00,
            "pnl": 150.00,
            "pnl_percentage": 0.60,
            "day_buy_quantity": 10,
            "day_sell_quantity": 0,
            "cf_buy_quantity": 0,
            "cf_sell_quantity": 0,
            "net_quantity": 10
        }
    ]
}
```

**Key Fields**:
- **quantity**: Net position quantity
- **average_price**: Average buy/sell price
- **pnl**: Profit/Loss in rupees
- **day_buy/sell_quantity**: Today's trades
- **cf_buy/sell_quantity**: Carry forward quantities

### 2. Position Conversion
**Method**: `position_conversion(params)`

**Purpose**: Convert position type (INTRADAY ↔ DELIVERY)

**Parameters**:
```python
{
    "exchange": "NSE_EQ",
    "token": 2885,
    "from_product": "INTRADAY",
    "to_product": "DELIVERY",
    "quantity": 10,
    "transaction_type": "BUY"
}
```

**Use Cases**:
- Convert intraday position to delivery before market close
- Convert delivery to intraday (requires margin)

### 3. Get Holdings
**Method**: `get_holdings()`

**Purpose**: Get delivery holdings

**Response**:
```python
{
    "data": [
        {
            "isin": "INE002A01018",
            "symbol": "RELIANCE",
            "exchange": "NSE_EQ",
            "quantity": 100,
            "average_price": 2400.00,
            "current_price": 2515.00,
            "pnl": 11500.00,
            "pnl_percentage": 4.79,
            "collateral_quantity": 50,  # Used as collateral
            "collateral_type": "CATEGORY_1",
            "t1_quantity": 0,  # T+1 pending
            "haircut": 10.00
        }
    ],
    "total_investment": 240000.00,
    "current_value": 251500.00,
    "total_pnl": 11500.00
}
```

**Important Fields**:
- **collateral_quantity**: Holdings pledged as margin
- **t1_quantity**: Shares in T+1 settlement
- **haircut**: Margin percentage applied

---

## Real-Time Market Data (WebSocket)

### Broadcast Socket
**Purpose**: Real-time price updates

**Connect**:
```python
await ibt_connect.connect_broadcast_socket()
```

**Callbacks**:
```python
async def on_open_broadcast_socket(message):
    print("Connected to broadcast socket")
    
async def on_close_broadcast_socket(close_msg):
    print("Disconnected:", close_msg)
    
async def on_error_broadcast_socket(error):
    print("Error:", error)
```

### Touchline Subscription
**Purpose**: Subscribe to last traded price updates

**Subscribe**:
```python
await touchline_subscription([
    {"MktSegId": "1", "token": "2885"},  # NSE, RELIANCE
    {"MktSegId": "1", "token": "22"}     # NSE, ACC
])
```

**Callback**:
```python
async def on_touchline(message):
    # message["data"] contains price updates
    print("Price update:", message)
```

**Touchline Data**:
```python
{
    "data": [
        {
            "token": "2885",
            "ltp": 2515.50,
            "change": 15.50,
            "change_percentage": 0.62,
            "volume": 1234567,
            "open": 2500.00,
            "high": 2520.00,
            "low": 2495.00,
            "close": 2500.00
        }
    ]
}
```

**Unsubscribe**:
```python
await touchline_unsubscription([
    {"MktSegId": "1", "token": "2885"}
])
```

### Market Depth (Best 5)
**Purpose**: Get order book depth (5 best bids/asks)

**Subscribe**:
```python
await bestfive_subscription({
    "MktSegId": "1",
    "token": "2885"
})
```

**Callback**:
```python
async def on_bestfive(message):
    print("Market depth:", message)
```

**Market Depth Data**:
```python
{
    "data": {
        "token": "2885",
        "bids": [
            {"price": 2515.00, "quantity": 100, "orders": 5},
            {"price": 2514.50, "quantity": 200, "orders": 8},
            ...
        ],
        "asks": [
            {"price": 2515.50, "quantity": 150, "orders": 6},
            {"price": 2516.00, "quantity": 250, "orders": 10},
            ...
        ]
    }
}
```

### Message Socket
**Purpose**: Order status updates in real-time

**Connect**:
```python
await ibt_connect.connect_message_socket()
```

**Callback**:
```python
async def on_msg_message_socket(response):
    # Real-time order updates
    print("Order update:", response)
```

**Order Update Types**:
- Order placement confirmation
- Order execution (full/partial)
- Order rejection
- Order cancellation
- Order modification

---

## Error Handling

### Common Error Codes

| Code | Description | Resolution |
|------|-------------|------------|
| 401 | Unauthorized | Re-login required |
| 403 | Insufficient margin | Add funds or reduce quantity |
| 404 | Order not found | Verify order ID |
| 422 | Invalid parameters | Check order parameters |
| 429 | Rate limit exceeded | Reduce API call frequency |
| 500 | Server error | Retry after delay |

### Error Response Format

```python
{
    "error": {
        "code": "INSUFFICIENT_MARGIN",
        "message": "Insufficient funds to place order",
        "details": {
            "required_margin": 25000.00,
            "available_margin": 20000.00
        }
    }
}
```

---

## Integration Strategy for Go Services

### 1. Create Odin Client Package

**File**: `pkg/odin/client.go`

**Approach**:
- Use Python subprocess to call SDK methods
- OR Create HTTP wrapper around Python SDK
- OR Port critical functions to Go

### 2. Pre-Trade Validations

Before placing order:
1. ✅ Check user balance (`balance()`)
2. ✅ Verify holdings if selling (`get_holdings()`)
3. ✅ Check existing positions (`get_positions()`)
4. ✅ Validate margins (internal calculation)
5. ✅ Risk checks (Risk Management Service)

### 3. Order Execution Flow

```
1. Rules Engine matches event
2. Risk Management approves
3. Trade Execution Service:
   a. Fetch user balance
   b. Check holdings (for SELL orders)
   c. Calculate required margin
   d. Place order via Odin API
   e. Store order in PostgreSQL
   f. Subscribe to order updates
```

### 4. Position Monitoring

- Subscribe to position updates via WebSocket
- Periodically sync positions (`get_positions()`)
- Track P&L in real-time
- Update Risk Management Service

### 5. End-of-Day Process

1. Get final positions (`get_positions("NET")`)
2. Get all holdings (`get_holdings()`)
3. Calculate realized/unrealized P&L
4. Update daily risk counters
5. Generate daily reports

---

## Critical Considerations

### 1. Order Validation Checklist

✅ **Before SELL Orders**:
- Check holdings quantity >= order quantity
- Verify T+1 quantity (can't sell)
- Check collateral quantity (can't sell pledged)

✅ **Before BUY Orders**:
- Check available margin
- Verify circuit limits
- Check position limits

### 2. Margin Requirements

| Product Type | Margin Requirement |
|--------------|-------------------|
| INTRADAY | ~20-40% of value |
| DELIVERY | 100% + transaction charges |
| F&O | SPAN + Exposure margin |
| MTF | ~30-50% of value |

### 3. Rate Limits

- Order placement: ~10 orders/second
- Order modification: ~5 mods/second
- Market data: Unlimited (WebSocket)
- Order book queries: ~1/second

### 4. Session Management

- Session timeout: 24 hours
- Auto-logout: Market close + 30 mins
- Concurrent sessions: Not allowed
- Re-login: Requires new TOTP

---

## Testing Strategy

### 1. Paper Trading Mode

Use demo environment:
- Test all order types
- Verify WebSocket connectivity
- Validate error handling

### 2. Integration Tests

- Mock Odin API responses
- Test order flow end-to-end
- Verify database persistence
- Check WebSocket reconnection

### 3. Load Testing

- Concurrent order placement
- WebSocket subscription limits
- API rate limit handling

---

## Next Steps

1. ✅ **Create Go wrapper for Odin SDK** (`pkg/odin/`)
2. ✅ **Implement order execution in Trade Service**
3. ✅ **Add WebSocket connection management**
4. ✅ **Create holdings/position cache in Redis**
5. ✅ **Implement pre-trade validation logic**
6. ✅ **Add comprehensive error handling**
7. ✅ **Set up monitoring and alerts**

---

## API Endpoints Summary

| Category | Method | Endpoint | Purpose |
|----------|--------|----------|---------|
| **Auth** | login | POST /authentication/v1/user/session | Login |
| | balance | GET /authentication/v1/user/balance | Get balance |
| | logout | DELETE /authentication/v1/user/session | Logout |
| **Orders** | place_order | POST /transactional/v1/orders/regular | Place order |
| | modify_order | PUT /transactional/v1/orders/regular/{exchange}/{id} | Modify |
| | cancel_order | DELETE /transactional/v1/orders/regular/{exchange}/{id} | Cancel |
| | get_order_book | GET /transactional/v1/orders | Order book |
| | get_trade_book | GET /transactional/v1/trades | Trade book |
| **Portfolio** | get_positions | GET /transactional/v1/portfolio/positions/{type} | Positions |
| | get_holdings | GET /transactional/v1/portfolio/holdings | Holdings |
| | position_conversion | PUT /transactional/v1/portfolio/positions | Convert |
