# 🚀 Complete API Gateway Documentation

Unified REST API Gateway for Trading System - Access both User Config and Authentication services through a single endpoint.

## 📋 Table of Contents

- [Overview](#overview)
- [Quick Start](#quick-start)
- [Authentication Service](#authentication-service)
- [User Config Service](#user-config-service)
- [Error Handling](#error-handling)
- [Frontend Integration](#frontend-integration)
- [Testing](#testing)

---

## Overview

### Base URL
```
http://localhost:8081/api/v1
```

### Services Integrated
1. **User Config Service** - Trading strategy management (gRPC → REST)
2. **User Login Service** - Authentication & session management (REST → REST proxy)

### Architecture
```
┌─────────────────────────────────────────────────────────┐
│                    Frontend App                          │
└────────────────────┬────────────────────────────────────┘
                     │ HTTP/JSON
                     ▼
┌─────────────────────────────────────────────────────────┐
│              API Gateway (Port 8081)                     │
│  ┌──────────────────────────────────────────────────┐  │
│  │             REST API Endpoints                    │  │
│  └──────┬────────────────────────────────┬──────────┘  │
│         │                                 │              │
│         ▼                                 ▼              │
│  ┌─────────────┐                  ┌──────────────┐     │
│  │  gRPC       │                  │  HTTP        │     │
│  │  Client     │                  │  Proxy       │     │
│  └─────────────┘                  └──────────────┘     │
└────────┬────────────────────────────────┬───────────────┘
         │                                 │
         ▼                                 ▼
┌──────────────────┐            ┌──────────────────┐
│  User Config     │            │  User Login      │
│  Service         │            │  Service         │
│  (gRPC:50051)    │            │  (REST:8002)     │
└──────────────────┘            └──────────────────┘
```

---

## Quick Start

### Prerequisites
```bash
# 1. Start User Config Service
cd /home/rohitt/Desktop/trading-system/services/user-config
go run cmd/main.go

# 2. Start User Login Service
cd /home/rohitt/Desktop/trading-system/services/user-login-service
python -m uvicorn src.main:app --host 0.0.0.0 --port 8002

# 3. Start API Gateway
cd /home/rohitt/Desktop/trading-system/api/gateway
go run cmd/main.go
```

### Health Check
```bash
curl http://localhost:8081/api/v1/health
```

---

## Authentication Service

Access the User Login Service through `/api/v1/auth`, `/api/v1/credentials`, `/api/v1/session` endpoints.

### 1. Register User Credentials

Store user credentials for automatic login.

**Endpoint:** `POST /api/v1/credentials/register`

**Request:**
```bash
curl -X POST http://localhost:8081/api/v1/credentials/register \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "IS14415",
    "api_key": "your_jwt_token_here",
    "x_api_key": "your_x_api_key",
    "api_url": "https://jri4df7kaa.execute-api.ap-south-1.amazonaws.com/prod/interactive",
    "totp_secret": "DBUESNYUFRNQMD3Q",
    "source": "MOBILEAPI",
    "preferred_login_type": "PASSWORD",
    "preferred_second_auth": "TOTP",
    "client_id": "IS14415"
  }'
```

**Response:**
```json
{
  "success": true,
  "data": {
    "user_id": "IS14415",
    "jwt_user_id": "IS14415",
    "created_at": "2024-01-01T10:00:00Z",
    "updated_at": "2024-01-01T10:00:00Z"
  },
  "message": "Credentials registered successfully. Backend will auto-generate TOTP codes from the secret during login."
}
```

### 2. Login

Create a user session with automatic TOTP generation.

**Endpoint:** `POST /api/v1/auth/login`

**Request:**
```bash
curl -X POST http://localhost:8081/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "IS14415",
    "login_type": "PASSWORD",
    "password": "Poly@123#",
    "second_auth_type": "TOTP",
    "second_auth": "",
    "source": "MOBILEAPI",
    "udid": "device-uuid-123",
    "version": "2.0.0",
    "device_info": {
      "platform": "web",
      "os": "Windows 10"
    }
  }'
```

**Response:**
```json
{
  "success": true,
  "data": {
    "session": {
      "session_id": "uuid-here",
      "user_id": "IS14415",
      "user_name": "John Doe",
      "access_token": "access_token_here",
      "broadcast_token": "broadcast_token_here",
      "login_time": "2024-01-01T10:00:00Z",
      "expires_at": "2024-01-02T10:00:00Z",
      "is_active": true,
      "exchanges": ["NSE", "BSE"],
      "product_types": ["CNC", "MIS"]
    },
    "odin_response": {
      "status": "success",
      "data": {...}
    }
  },
  "message": "Login successful"
}
```

### 3. Validate Session

Check if session is still valid and update activity.

**Endpoint:** `PUT /api/v1/session/validate`

**Request:**
```bash
curl -X PUT http://localhost:8081/api/v1/session/validate \
  -H "X-Session-ID: your-session-id-here"
```

**Response:**
```json
{
  "success": true,
  "data": {
    "session_id": "uuid",
    "user_id": "IS14415",
    "is_active": true,
    "expires_at": "2024-01-02T10:00:00Z",
    "last_activity": "2024-01-01T11:30:00Z"
  },
  "message": "Session is valid"
}
```

### 4. Get Active Session

Retrieve current active session for a user.

**Endpoint:** `GET /api/v1/session/user/{user_id}/active`

**Request:**
```bash
curl http://localhost:8081/api/v1/session/user/IS14415/active
```

### 5. Get All User Sessions

**Endpoint:** `GET /api/v1/session/user/{user_id}/all?include_inactive=false`

**Request:**
```bash
curl "http://localhost:8081/api/v1/session/user/IS14415/all?include_inactive=false"
```

### 6. Logout

Invalidate a specific session.

**Endpoint:** `POST /api/v1/auth/logout`

**Request:**
```bash
curl -X POST http://localhost:8081/api/v1/auth/logout \
  -H "X-Session-ID: your-session-id-here"
```

### 7. Logout All Sessions

Invalidate all sessions for a user.

**Endpoint:** `POST /api/v1/auth/logout-all/{user_id}`

**Request:**
```bash
curl -X POST http://localhost:8081/api/v1/auth/logout-all/IS14415
```

### 8. Get Login History

**Endpoint:** `GET /api/v1/history/{user_id}?limit=50`

**Request:**
```bash
curl "http://localhost:8081/api/v1/history/IS14415?limit=50"
```

**Response:**
```json
{
  "success": true,
  "data": [
    {
      "user_id": "IS14415",
      "session_id": "uuid",
      "login_type": "PASSWORD",
      "second_auth_type": "TOTP",
      "status": "SUCCESS",
      "error_message": null,
      "device_platform": "web",
      "ip_address": "192.168.1.100",
      "attempt_time": "2024-01-01T10:00:00Z"
    }
  ],
  "total": 1,
  "message": "Retrieved 1 login history records"
}
```

### 9. Generate TOTP

**Endpoint:** `POST /api/v1/totp/generate`

**Request:**
```bash
curl -X POST http://localhost:8081/api/v1/totp/generate \
  -H "Content-Type: application/json" \
  -d '{
    "secret": "DBUESNYUFRNQMD3Q"
  }'
```

### 10. Get Service Statistics

**Endpoint:** `GET /api/v1/admin/stats`

**Request:**
```bash
curl http://localhost:8081/api/v1/admin/stats
```

---

## User Config Service

Manage trading strategies through `/api/v1/strategies` and `/api/v1/users` endpoints.

### 1. Create Strategy

**Endpoint:** `POST /api/v1/strategies`

**Request:**
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

**Response:**
```json
{
  "success": true,
  "strategy": {
    "strategy_id": "uuid-here",
    "user_id": "IS14415",
    "strategy_name": "High Impact News Trader",
    "active": true,
    "match_all_news": false,
    "version": 1,
    "created_at": "2024-01-01T10:00:00Z",
    "conditions": {...},
    "trade_config": {...},
    "risk_limits": {...}
  }
}
```

### 2. List User Strategies

**Endpoint:** `GET /api/v1/users/{user_id}/strategies`

**Query Parameters:**
- `active_only` (boolean): Filter active strategies only
- `page` (int): Page number (default: 1)
- `page_size` (int): Items per page (default: 10)

**Request:**
```bash
# All strategies
curl http://localhost:8081/api/v1/users/IS14415/strategies

# Active only
curl "http://localhost:8081/api/v1/users/IS14415/strategies?active_only=true"

# With pagination
curl "http://localhost:8081/api/v1/users/IS14415/strategies?page=1&page_size=5"
```

**Response:**
```json
{
  "success": true,
  "strategies": [
    {
      "strategy_id": "uuid-1",
      "strategy_name": "Strategy 1",
      "active": true,
      ...
    }
  ],
  "pagination": {
    "page": 1,
    "page_size": 10,
    "total_items": 5,
    "total_pages": 1
  }
}
```

### 3. Get Strategy

**Endpoint:** `GET /api/v1/strategies/{strategy_id}?user_id={user_id}`

**Request:**
```bash
curl "http://localhost:8081/api/v1/strategies/uuid-here?user_id=IS14415"
```

### 4. Update Strategy

**Endpoint:** `PUT /api/v1/strategies/{strategy_id}`

**Request:**
```bash
curl -X PUT http://localhost:8081/api/v1/strategies/uuid-here \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "IS14415",
    "strategy_name": "Updated Strategy Name",
    "description": "Updated description",
    "version": 1
  }'
```

### 5. Activate Strategy

**Endpoint:** `POST /api/v1/strategies/{strategy_id}/activate`

**Request:**
```bash
curl -X POST http://localhost:8081/api/v1/strategies/uuid-here/activate \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "IS14415"
  }'
```

### 6. Deactivate Strategy

**Endpoint:** `POST /api/v1/strategies/{strategy_id}/deactivate`

**Request:**
```bash
curl -X POST http://localhost:8081/api/v1/strategies/uuid-here/deactivate \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "IS14415"
  }'
```

### 7. Delete Strategy

**Endpoint:** `DELETE /api/v1/strategies/{strategy_id}?user_id={user_id}`

**Request:**
```bash
curl -X DELETE "http://localhost:8081/api/v1/strategies/uuid-here?user_id=IS14415"
```

---

## Error Handling

### Common Error Codes

| Code | Description |
|------|-------------|
| 200 | Success |
| 201 | Created |
| 400 | Bad Request - Invalid input |
| 401 | Unauthorized - Invalid session |
| 404 | Not Found - Resource doesn't exist |
| 500 | Internal Server Error |
| 502 | Bad Gateway - Backend service unavailable |

### Error Response Format

```json
{
  "error": "Error message here",
  "success": false
}
```

---

## Frontend Integration

### React/TypeScript Example

```typescript
// api/config.ts
const API_BASE_URL = 'http://localhost:8081/api/v1';

// api/auth.ts
export const authAPI = {
  // Register credentials
  registerCredentials: async (data: RegisterCredentialsRequest) => {
    const response = await fetch(`${API_BASE_URL}/credentials/register`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(data),
    });
    return response.json();
  },

  // Login
  login: async (data: LoginRequest) => {
    const response = await fetch(`${API_BASE_URL}/auth/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(data),
    });
    return response.json();
  },

  // Validate session
  validateSession: async (sessionId: string) => {
    const response = await fetch(`${API_BASE_URL}/session/validate`, {
      method: 'PUT',
      headers: { 'X-Session-ID': sessionId },
    });
    return response.json();
  },

  // Logout
  logout: async (sessionId: string) => {
    const response = await fetch(`${API_BASE_URL}/auth/logout`, {
      method: 'POST',
      headers: { 'X-Session-ID': sessionId },
    });
    return response.json();
  },
};

// api/strategies.ts
export const strategyAPI = {
  // Create strategy
  createStrategy: async (data: CreateStrategyRequest) => {
    const response = await fetch(`${API_BASE_URL}/strategies`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(data),
    });
    return response.json();
  },

  // List strategies
  listStrategies: async (userId: string, activeOnly = true, page = 1, pageSize = 10) => {
    const params = new URLSearchParams({
      active_only: activeOnly.toString(),
      page: page.toString(),
      page_size: pageSize.toString(),
    });
    const response = await fetch(
      `${API_BASE_URL}/users/${userId}/strategies?${params}`
    );
    return response.json();
  },

  // Get strategy
  getStrategy: async (strategyId: string, userId: string) => {
    const response = await fetch(
      `${API_BASE_URL}/strategies/${strategyId}?user_id=${userId}`
    );
    return response.json();
  },

  // Update strategy
  updateStrategy: async (strategyId: string, data: UpdateStrategyRequest) => {
    const response = await fetch(`${API_BASE_URL}/strategies/${strategyId}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(data),
    });
    return response.json();
  },

  // Activate strategy
  activateStrategy: async (strategyId: string, userId: string) => {
    const response = await fetch(
      `${API_BASE_URL}/strategies/${strategyId}/activate`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ user_id: userId }),
      }
    );
    return response.json();
  },

  // Deactivate strategy
  deactivateStrategy: async (strategyId: string, userId: string) => {
    const response = await fetch(
      `${API_BASE_URL}/strategies/${strategyId}/deactivate`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ user_id: userId }),
      }
    );
    return response.json();
  },

  // Delete strategy
  deleteStrategy: async (strategyId: string, userId: string) => {
    const response = await fetch(
      `${API_BASE_URL}/strategies/${strategyId}?user_id=${userId}`,
      {
        method: 'DELETE',
      }
    );
    return response.json();
  },
};
```

### Usage Example

```typescript
// Login flow
async function handleLogin() {
  try {
    const loginResponse = await authAPI.login({
      user_id: 'IS14415',
      login_type: 'PASSWORD',
      password: 'your_password',
      second_auth_type: 'TOTP',
      second_auth: '', // Auto-generated
      source: 'MOBILEAPI',
      udid: 'device-uuid',
      version: '2.0.0',
      device_info: {
        platform: 'web',
        os: navigator.platform,
      },
    });

    if (loginResponse.success) {
      const sessionId = loginResponse.data.session.session_id;
      // Store session ID
      localStorage.setItem('sessionId', sessionId);
      
      // Load strategies
      const strategies = await strategyAPI.listStrategies('IS14415', true);
      console.log('Active strategies:', strategies);
    }
  } catch (error) {
    console.error('Login failed:', error);
  }
}

// Create strategy flow
async function handleCreateStrategy() {
  try {
    const newStrategy = await strategyAPI.createStrategy({
      user_id: 'IS14415',
      strategy_name: 'My New Strategy',
      description: 'Trading strategy description',
      activate_immediately: true,
      conditions: {
        impact_score_threshold: 7,
        sentiments: [1], // POSITIVE
        categories: ['Results'],
      },
      trade_config: {
        order_type: 1, // MARKET
        quantity: 10,
        exchange: 1, // NSE
        order_side: 1, // BUY
        validity: 'DAY',
      },
      risk_limits: {
        max_daily_trades: 5,
        max_loss_per_day: 5000.0,
        enable_risk_checks: true,
      },
    });

    if (newStrategy.success) {
      console.log('Strategy created:', newStrategy.strategy);
    }
  } catch (error) {
    console.error('Failed to create strategy:', error);
  }
}
```

---

## Testing

### Complete Test Flow

```bash
# 1. Health Check
curl http://localhost:8081/api/v1/health

# 2. Register Credentials
curl -X POST http://localhost:8081/api/v1/credentials/register \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "IS14415",
    "api_key": "your_jwt_token",
    "x_api_key": "your_x_api_key",
    "api_url": "https://api.example.com",
    "totp_secret": "DBUESNYUFRNQMD3Q",
    "source": "MOBILEAPI",
    "preferred_login_type": "PASSWORD",
    "preferred_second_auth": "TOTP"
  }'

# 3. Login
curl -X POST http://localhost:8081/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "IS14415",
    "login_type": "PASSWORD",
    "password": "your_password",
    "second_auth_type": "TOTP",
    "source": "MOBILEAPI",
    "udid": "device-uuid"
  }'

# Save the session_id from response

# 4. Create Strategy
curl -X POST http://localhost:8081/api/v1/strategies \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "IS14415",
    "strategy_name": "Test Strategy",
    "description": "Testing",
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

# Save the strategy_id from response

# 5. List Strategies
curl "http://localhost:8081/api/v1/users/IS14415/strategies?active_only=true"

# 6. Validate Session
curl -X PUT http://localhost:8081/api/v1/session/validate \
  -H "X-Session-ID: your-session-id"

# 7. Get Login History
curl "http://localhost:8081/api/v1/history/IS14415?limit=10"

# 8. Logout
curl -X POST http://localhost:8081/api/v1/auth/logout \
  -H "X-Session-ID: your-session-id"
```

### Enum Values Reference

#### Sentiment
- `1` = POSITIVE
- `2` = NEUTRAL
- `3` = NEGATIVE

#### Exchange
- `1` = NSE
- `2` = BSE

#### OrderType
- `1` = MARKET
- `2` = LIMIT
- `3` = STOP_LOSS
- `4` = STOP_LOSS_MARKET

#### OrderSide
- `1` = BUY
- `2` = SELL

#### PositionSizing
- `1` = FIXED
- `2` = PERCENTAGE

---

## Troubleshooting

### Gateway Won't Start

**Issue:** Port already in use
```
Error: Failed to start server: listen tcp :8081: bind: address already in use
```

**Solution:**
```bash
# Check what's using the port
lsof -i :8081

# Change port in .env
HTTP_PORT=8082
```

### Backend Service Not Responding

**Issue:** `Failed to proxy request: connection refused`

**Solution:**
```bash
# Check user-config service
curl http://localhost:50051

# Check user-login service  
curl http://localhost:8002/health

# Restart services if needed
```

### CORS Errors

**Issue:** `Access-Control-Allow-Origin header is missing`

**Solution:** Add your frontend origin to `.env`:
```bash
CORS_ALLOWED_ORIGINS=http://localhost:3000,http://localhost:5173,http://your-frontend-url
```

---

## Production Deployment

### Environment Variables

```bash
# Production .env
HTTP_PORT=80
GRPC_TIMEOUT=30s
USER_CONFIG_GRPC_ADDR=user-config-service:50051
USER_LOGIN_SERVICE_URL=http://user-login-service:8002
CORS_ALLOWED_ORIGINS=https://yourdomain.com
LOG_LEVEL=INFO
```

### Docker Compose Example

```yaml
version: '3.8'
services:
  api-gateway:
    build: ./api/gateway
    ports:
      - "8081:8081"
    environment:
      - USER_CONFIG_GRPC_ADDR=user-config:50051
      - USER_LOGIN_SERVICE_URL=http://user-login:8002
    depends_on:
      - user-config
      - user-login

  user-config:
    build: ./services/user-config
    ports:
      - "50051:50051"
    
  user-login:
    build: ./services/user-login-service
    ports:
      - "8002:8002"
```

---

## Support

For issues and questions:
- Check the troubleshooting section above
- Review service-specific README files
- Check service logs for detailed error messages

---

**Last Updated:** November 2024  
**Version:** 1.0.0
