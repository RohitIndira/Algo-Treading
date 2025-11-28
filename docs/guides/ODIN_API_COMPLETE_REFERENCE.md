# Odin API Complete Reference

## Overview
Complete reference for all Odin Trading APIs available through the wrapper service.

---

## Authentication APIs

### 1. Login
**Endpoint:** `POST /auth/login`

**Request Body:**
```json
{
  "user_id": "string (optional, uses env var)",
  "password": "string (optional, uses env var)",
  "client_id": "string (optional)",
  "api_key": "string (optional)",
  "source": "string (optional, default: MOBILEAPI)",
  "login_type": "string (optional, default: PASSWORD)",
  "second_auth_type": "string (optional, default: TOTP)",
  "totp_secret": "string (optional, base32 encoded)",
  "totp_code": "string (optional, 6-digit code)"
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "access_token": "string",
    "user_name": "string",
    "login_time": "string",
    "exchanges": ["NSE_EQ", "NSE_FO", "BSE_EQ", ...],
    "product_types": ["INTRADAY", "DELIVERY", ...],
    "others": {
      "broadCastSocket": "wss://...",
      "messageSocket": "wss://..."
    }
  },
  "message": "Login successful"
}
```

### 2. Logout
**Endpoint:** `DELETE /auth/logout`

**Headers:**
- `X-User-ID`: User identifier

**Response:**
```json
{
  "success": true,
  "message": "User logged out"
}
```

### 3. Get Balance
**Endpoint:** `GET /auth/balance`

**Headers:**
- `X-User-ID`: User identifier

**Response:**
```json
{
  "success": true,
  "data": {
    "available_balance": "number",
    "used_balance": "number",
    "total_balance": "number"
  }
}
```

### 4. Validate Session
**Endpoint:** `PUT /auth/session/validate`

**Headers:**
- `X-User-ID`: User identifier

**Response:**
```json
{
  "success": true,
  "data": {
    "status": "valid",
    "message": "Session is active"
  }
}
```

---

## Order Management APIs

### 5. Place Regular Order
**Endpoint:** `POST /orders/place`

**Headers:**
- `X-User-ID`: User identifier

**Request Body:**
```json
{
  "scrip_info": {
    "exchange": "NSE_EQ",
    "token": "3045",
    "symbol": "SBIN"
  },
  "transaction_type": "BUY",
  "product_type": "INTRADAY",
  "order_type": "LIMIT",
  "quantity": 10,
  "price": 500.50,
  "trigger_price": 0,
  "disclosed_quantity": 0,
  "validity": "DAY",
  "validity_days": 0,
  "is_amo": false
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "order_id": "string",
    "orderId": "string"
  },
  "message": "Order placed successfully"
}
```

### 6. Modify Order
**Endpoint:** `PUT /orders/modify`

**Headers:**
- `X-User-ID`: User identifier

**Request Body:**
```json
{
  "exchange": "NSE_EQ",
  "order_id": "string",
  "quantity": 20,
  "price": 505.00,
  "trigger_price": 0,
  "order_type": "LIMIT",
  "disclosed_quantity": 0
}
```

### 7. Cancel Order
**Endpoint:** `DELETE /orders/cancel`

**Headers:**
- `X-User-ID`: User identifier

**Request Body:**
```json
{
  "exchange": "NSE_EQ",
  "order_id": "string"
}
```

### 8. Place Cover Order
**Endpoint:** `POST /orders/cover`

**Headers:**
- `X-User-ID`: User identifier

### 9. Modify Cover Order
**Endpoint:** `PUT /orders/cover`

**Headers:**
- `X-User-ID`: User identifier

### 10. Cancel Cover Order
**Endpoint:** `DELETE /orders/cover`

**Headers:**
- `X-User-ID`: User identifier

### 11. Place Bracket Order
**Endpoint:** `POST /orders/bracket`

**Headers:**
- `X-User-ID`: User identifier

### 12. Modify Bracket Order
**Endpoint:** `PUT /orders/bracket`

**Headers:**
- `X-User-ID`: User identifier

### 13. Delete Bracket Order
**Endpoint:** `DELETE /orders/bracket`

**Headers:**
- `X-User-ID`: User identifier

### 14. Place Multileg Order
**Endpoint:** `POST /orders/multileg`

**Headers:**
- `X-User-ID`: User identifier

### 15. Cancel Multileg Order
**Endpoint:** `PUT /orders/multileg/{order_flag}/{gateway_order_no}`

**Headers:**
- `X-User-ID`: User identifier

---

## Order Information APIs

### 16. Get Order Book
**Endpoint:** `GET /orders/book`

**Headers:**
- `X-User-ID`: User identifier

**Query Parameters:**
- `offset`: Page offset (default: 1)
- `limit`: Records per page (default: 20)
- `order_status`: Filter by status (optional)
- `order_id`: Specific order ID (optional)

**Response:**
```json
{
  "success": true,
  "data": {
    "orders": [
      {
        "order_id": "string",
        "exchange": "NSE_EQ",
        "symbol": "SBIN",
        "quantity": 10,
        "price": 500.50,
        "status": "COMPLETE",
        "transaction_type": "BUY",
        "order_time": "2024-01-01T10:00:00"
      }
    ]
  }
}
```

### 17. Get Trade Book
**Endpoint:** `GET /orders/trades`

**Headers:**
- `X-User-ID`: User identifier

**Query Parameters:**
- `offset`: Page offset (default: 1)
- `limit`: Records per page (default: 20)

### 18. Get Order History
**Endpoint:** `GET /orders/{order_id}/history`

**Headers:**
- `X-User-ID`: User identifier

---

## Portfolio Management APIs

### 19. Get Positions
**Endpoint:** `GET /portfolio/positions`

**Headers:**
- `X-User-ID`: User identifier

**Query Parameters:**
- `position_type`: "DAY" or "NET" (default: "DAY")

**Response:**
```json
{
  "success": true,
  "data": {
    "positions": [
      {
        "exchange": "NSE_EQ",
        "symbol": "SBIN",
        "quantity": 10,
        "average_price": 500.50,
        "current_price": 505.00,
        "pnl": 45.00,
        "pnl_percentage": 0.90
      }
    ]
  }
}
```

### 20. Get Holdings
**Endpoint:** `GET /portfolio/holdings`

**Headers:**
- `X-User-ID`: User identifier

**Response:**
```json
{
  "success": true,
  "data": {
    "holdings": [
      {
        "exchange": "NSE_EQ",
        "symbol": "SBIN",
        "quantity": 100,
        "average_price": 450.00,
        "current_price": 505.00,
        "pnl": 5500.00
      }
    ]
  }
}
```

### 21. Convert Position
**Endpoint:** `PUT /portfolio/positions/convert`

**Headers:**
- `X-User-ID`: User identifier

**Query Parameters:**
- `exchange`: Exchange name
- `token`: Security token
- `from_product`: Source product type
- `to_product`: Target product type
- `quantity`: Quantity to convert
- `transaction_type`: "BUY" or "SELL"

---

## Health Check

### 22. Health Check
**Endpoint:** `GET /health`

**Response:**
```json
{
  "status": "healthy",
  "service": "odin-api-wrapper",
  "active_sessions": 5
}
```

---

## SDK Native Routes (from connect.py)

The following routes are available in the SDK:

```python
routes = {
    "session": "/authentication/v1/user/session",
    "balance": "/authentication/v1/user/balance",
    
    "placeOrder": "/transactional/v1/orders/regular",
    "modifyOrders": "/transactional/v1/orders/regular/{exchange}/{order_id}",
    
    "coverOrder": "/transactional/v1/orders/cover",
    "modifyCoverOrder": "/transactional/v1/orders/cover/{exchange}/{order_id}",
    
    "bracketOrder": "/transactional/v1/orders/bracket",
    "modifyBracketOrder": "/transactional/v1/orders/bracket/{exchange}/{order_id}",
    
    "placeMultilegOrder": "/transactional/v1/orders/multileg",
    "cancelMultilegOrder": "/transactional/v1/orders/multileg/{order_flag}/{gateway_order_no}",
    
    "orders": "/transactional/v1/orders",
    "trades": "/transactional/v1/trades",
    "orderHistory": "/transactional/v1/orders/{order_id}",
    
    "positions": "/transactional/v1/portfolio/positions/{type}",
    "positionConvertion": "/transactional/v1/portfolio/positions",
    "holdings": "/transactional/v1/portfolio/holdings"
}
```

---

## Additional Postman APIs (Not Yet in Wrapper)

### User Management
- **Send OTP**: `POST /authentication/v1/user/password/reset/send-otp`
- **Forgot User ID**: `GET /authentication/v1/user/details?pan={PAN}`
- **Register MPIN**: `POST /authentication/v1/user/mpin`
- **Change Password**: `PUT /authentication/v1/user/password`
- **Change MPIN**: `PUT /authentication/v1/user/mpin`
- **Get User Details**: `GET /authentication/v1/getUserDetails/{user_id}`
- **Verify OTP**: `POST /authentication/v1/user/otp/verify`
- **Set Password**: `PUT /authentication/v1/user/password/reset`
- **Forgot MPIN Send OTP**: `POST /authentication/v1/user/mpin/reset/send-otp`
- **Set MPIN**: `POST /authentication/v1/user/mpin/reset/verify-otp`
- **Set Fingerprint**: `POST /authentication/{tenantid}/v1/setMPIN`
- **User Profile**: `GET /authentication/v1/user/profile`
- **User Profile v2**: `GET /authentication/v2/user/profile`
- **Register TOTP**: `POST /authentication/v1/user/registertotp`
- **Delete TOTP**: `DELETE /authentication/v1/user/registertotp`
- **Verify TOTP**: `POST /authentication/v1/user/verifytotp`

---

## WebSocket APIs

### Broadcast Socket (Market Data)
**Connection:** Via `broadCastSocket` URL from login response

**Features:**
- Real-time price updates (Touchline)
- Market depth (Best 5 bids/asks)
- Trade execution updates

**Methods:**
```python
# Subscribe to touchline
await client.touchline_subscription(["NSE_EQ|3045", "NSE_EQ|2885"])

# Unsubscribe from touchline
await client.touchline_unsubscription(["NSE_EQ|3045"])

# Subscribe to best five
await client.bestfive_subscription({
    "exchange": "NSE_EQ",
    "token": "3045"
})

# Unsubscribe from best five
await client.bestfive_unsubscription({
    "exchange": "NSE_EQ",
    "token": "3045"
})
```

### Message Socket (Order Updates)
**Connection:** Via `messageSocket` URL from login response

**Features:**
- Order execution updates
- Order rejection notifications
- Order modification confirmations

---

## Authentication Flow

### Login Types
1. **PASSWORD + TOTP**: Most secure, recommended
2. **PASSWORD + OTP**: SMS-based OTP
3. **TOKEN + TOTP**: Using register_token
4. **MPIN + FINGERPRINT**: Mobile device authentication

### Session Management
- Sessions use JWT access tokens
- Include `Authorization: Bearer {access_token}` in API calls
- Session timeout: Configurable (typically 24 hours)
- Validate session periodically with `PUT /auth/session/validate`

---

## Error Codes

### Success Codes
- `s-101`: Success

### Error Codes
- `e-401`: Unauthorized
- `e-400`: Bad Request
- `e-500`: Internal Server Error
- `e-403`: Forbidden

---

## Rate Limits
- Order placement: 10 requests/second
- Market data: No limit on subscriptions
- Portfolio queries: 5 requests/second

---

## Best Practices

1. **Session Management**
   - Store access_token securely
   - Refresh tokens before expiry
   - Handle session expiry gracefully

2. **Order Placement**
   - Validate scrip info before placing orders
   - Check available balance before orders
   - Handle order rejection gracefully

3. **WebSocket Connections**
   - Implement reconnection logic
   - Subscribe only to required symbols
   - Handle connection drops

4. **Error Handling**
   - Log all API errors
   - Implement retry logic for network errors
   - Show user-friendly error messages

---

## Complete API Summary

**Total APIs Available: 22+ endpoints**

### Authentication (4)
- Login, Logout, Balance, Session Validation

### Order Management (11)
- Regular, Cover, Bracket, Multileg Orders
- Place, Modify, Cancel operations

### Order Information (3)
- Order Book, Trade Book, Order History

### Portfolio (3)
- Positions (Day/Net), Holdings, Position Conversion

### Utility (1)
- Health Check

### Additional User Management (16)
- Available in Postman, can be added to wrapper

---

## SDK Client Methods

```python
# Authentication
client.login(params)
client.logout()
client.balance()
client.validateSession()

# Orders
client.place_order(params)
client.modify_order(params)
client.cancel_order(params)
client.place_cover_order(params)
client.modify_cover_order(params)
client.cancel_cover_order(params)
client.place_bracket_order(params)
client.modify_bracket_order(params)
client.delete_bracket_order(params)
client.place_multileg_order(params)
client.cancel_multileg_order(params)

# Order Information
client.get_order_book(params)
client.get_trade_book(params)
client.get_order_history(params)

# Portfolio
client.get_positions(params)
client.get_holdings()
client.position_conversion(params)

# WebSocket
await client.connect_broadcast_socket()
await client.connect_message_socket()
await client.touchline_subscription(scrips)
await client.touchline_unsubscription(scrips)
await client.bestfive_subscription(params)
await client.bestfive_unsubscription(params)
```

---

## Next Steps

