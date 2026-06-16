# API Gateway

REST API Gateway for the Trading System. Provides HTTP/REST endpoints for frontend applications to interact with backend gRPC microservices.

## 📋 Overview

The API Gateway serves as the entry point for frontend applications, translating REST API calls to gRPC service calls. It provides:

- **REST API Endpoints**: Easy-to-use HTTP endpoints for frontend integration
- **CORS Support**: Configured for frontend development servers
- **Request/Response Translation**: Converts between JSON (REST) and Protocol Buffers (gRPC)
- **Service Routing**: Routes requests to appropriate backend microservices

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     Frontend App                         │
│            (React/Vue/Angular - Port 3000/5173)         │
└────────────────────┬────────────────────────────────────┘
                     │ HTTP/JSON
                     ▼
┌─────────────────────────────────────────────────────────┐
│                   API Gateway (Port 8081)                │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────┐  │
│  │   Router     │───▶│   Handlers   │───▶│  gRPC    │  │
│  │   + CORS     │    │              │    │ Clients  │  │
│  └──────────────┘    └──────────────┘    └──────────┘  │
└────────────────────┬────────────────────────────────────┘
                     │ gRPC
                     ▼
┌─────────────────────────────────────────────────────────┐
│            Backend Microservices (gRPC)                  │
│  - User Config Service (Port 50051)                      │
│  - Rules Engine                                          │
│  - Trade Execution                                       │
│  - Risk Management                                       │
└─────────────────────────────────────────────────────────┘
```

## ✨ Features

- ✅ RESTful API design
- ✅ CORS enabled for frontend development
- ✅ JSON request/response format
- ✅ Error handling and validation
- ✅ Health check endpoint
- ✅ Pagination support
- ✅ Query parameter support

## 📦 Prerequisites

- **Go**: 1.23 or later
- **User Config Service**: Running on port 50051
- **PostgreSQL**: For user config service

## 🚀 Installation & Setup

### 1. Navigate to Gateway Directory

```bash
cd /home/rohitt/Desktop/trading-system/api/gateway
```

### 2. Configure Environment

The `.env` file is already configured. Review and modify if needed:

```bash
# Server Configuration
HTTP_PORT=8081
GRPC_TIMEOUT=30s

# Service Endpoints
USER_CONFIG_GRPC_ADDR=localhost:50051

# CORS Configuration
CORS_ALLOWED_ORIGINS=http://localhost:3000,http://localhost:5173
CORS_ALLOWED_METHODS=GET,POST,PUT,DELETE,OPTIONS
CORS_ALLOWED_HEADERS=Content-Type,Authorization

# Logging
LOG_LEVEL=INFO
```

### 3. Install Dependencies

```bash
go mod tidy
```

### 4. Start Backend Services

**First, start the User Config Service:**

```bash
# In a new terminal
cd /home/rohitt/Desktop/trading-system/services/user-config
go run cmd/main.go
```

### 5. Start API Gateway

```bash
# In another terminal
cd /home/rohitt/Desktop/trading-system/api/gateway
go run cmd/main.go
```

The gateway will start on port 8081:
```
2025/11/25 16:43:14 Starting API Gateway...
2025/11/25 16:43:14 Connected to User Config Service at localhost:50051
2025/11/25 16:43:14 API Gateway listening on port 8081
```

## 📡 API Endpoints

Base URL: `http://localhost:8081/api/v1`

### Health Check

Check if the gateway and backend services are healthy.

**Endpoint:** `GET /api/v1/health`

**Response:**
```json
{
  "status": "SERVING"
}
```

### Strategy Management

#### 1. Create Strategy

Create a new trading strategy for a user.

**Endpoint:** `POST /api/v1/strategies`

**Request Body:**
```json
{
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
}
```

**Response:** `201 Created`
```json
{
  "success": true,
  "strategy": {
    "strategy_id": "uuid-here",
    "user_id": "IS14415",
    "strategy_name": "High Impact News Trader",
    "active": true,
    ...
  }
}
```

#### 2. Get Strategy

Retrieve a specific strategy by ID.

**Endpoint:** `GET /api/v1/strategies/{strategy_id}?user_id=IS14415`

**Response:** `200 OK`
```json
{
  "success": true,
  "strategy": {
    "strategy_id": "uuid-here",
    "user_id": "IS14415",
    "strategy_name": "High Impact News Trader",
    ...
  }
}
```

#### 3. Update Strategy

Update an existing strategy.

**Endpoint:** `PUT /api/v1/strategies/{strategy_id}`

**Request Body:**
```json
{
  "user_id": "IS14415",
  "strategy_name": "Updated Strategy Name",
  "version": 1
}
```

**Response:** `200 OK`
```json
{
  "success": true,
  "strategy": {
    ...
  }
}
```

#### 4. Delete Strategy

Delete a strategy.

**Endpoint:** `DELETE /api/v1/strategies/{strategy_id}?user_id=IS14415`

**Response:** `200 OK`
```json
{
  "success": true,
  "message": "Strategy deleted successfully"
}
```

#### 5. List User Strategies

List all strategies for a user with pagination.

**Endpoint:** `GET /api/v1/users/{user_id}/strategies?active_only=true&page=1&page_size=10`

**Query Parameters:**
- `active_only` (optional): Filter to only active strategies (true/false)
- `page` (optional): Page number (default: 1)
- `page_size` (optional): Items per page (default: 10)

**Response:** `200 OK`
```json
{
  "success": true,
  "strategies": [
    {
      "strategy_id": "uuid-1",
      "strategy_name": "Strategy 1",
      "active": true,
      ...
    },
    {
      "strategy_id": "uuid-2",
      "strategy_name": "Strategy 2",
      "active": true,
      ...
    }
  ],
  "pagination": {
    "page": 1,
    "page_size": 10,
    "total_items": 2,
    "total_pages": 1
  }
}
```

#### 6. Activate Strategy

Activate a deactivated strategy.

**Endpoint:** `POST /api/v1/strategies/{strategy_id}/activate`

**Request Body:**
```json
{
  "user_id": "IS14415"
}
```

**Response:** `200 OK`
```json
{
  "success": true,
  "strategy": {
    "strategy_id": "uuid-here",
    "active": true,
    ...
  }
}
```

#### 7. Deactivate Strategy

Deactivate an active strategy.

**Endpoint:** `POST /api/v1/strategies/{strategy_id}/deactivate`

**Request Body:**
```json
{
  "user_id": "IS14415"
}
```

**Response:** `200 OK`
```json
{
  "success": true,
  "strategy": {
    "strategy_id": "uuid-here",
    "active": false,
    ...
  }
}
```

## 🔧 Testing with cURL

### Health Check
```bash
curl http://localhost:8081/api/v1/health
```

### Create Strategy
```bash
curl -X POST http://localhost:8081/api/v1/strategies \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "IS14415",
    "strategy_name": "Test Strategy",
    "description": "Test strategy description",
    "activate_immediately": true,
    "conditions": {
      "impact_score_threshold": 5,
      "sentiments": [1],
      "categories": ["Results"]
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

### List User Strategies
```bash
curl "http://localhost:8081/api/v1/users/IS14415/strategies?active_only=true&page=1&page_size=10"
```

### Get Strategy
```bash
curl "http://localhost:8081/api/v1/strategies/{strategy_id}?user_id=IS14415"
```

## 🎨 Frontend Integration

### React Example

```javascript
// Create a strategy
const createStrategy = async (strategyData) => {
  try {
    const response = await fetch('http://localhost:8081/api/v1/strategies', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(strategyData),
    });
    
    const data = await response.json();
    
    if (data.success) {
      console.log('Strategy created:', data.strategy);
      return data.strategy;
    } else {
      console.error('Error:', data.error);
    }
  } catch (error) {
    console.error('Failed to create strategy:', error);
  }
};

// List user strategies
const listStrategies = async (userId, activeOnly = true) => {
  try {
    const response = await fetch(
      `http://localhost:8081/api/v1/users/${userId}/strategies?active_only=${activeOnly}`,
      {
        method: 'GET',
        headers: {
          'Content-Type': 'application/json',
        },
      }
    );
    
    const data = await response.json();
    
    if (data.success) {
      console.log('Strategies:', data.strategies);
      return data.strategies;
    }
  } catch (error) {
    console.error('Failed to fetch strategies:', error);
  }
};

// Update strategy
const updateStrategy = async (strategyId, updates) => {
  try {
    const response = await fetch(
      `http://localhost:8081/api/v1/strategies/${strategyId}`,
      {
        method: 'PUT',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(updates),
      }
    );
    
    const data = await response.json();
    return data;
  } catch (error) {
    console.error('Failed to update strategy:', error);
  }
};

// Activate strategy
const activateStrategy = async (strategyId, userId) => {
  try {
    const response = await fetch(
      `http://localhost:8081/api/v1/strategies/${strategyId}/activate`,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ user_id: userId }),
      }
    );
    
    const data = await response.json();
    return data;
  } catch (error) {
    console.error('Failed to activate strategy:', error);
  }
};
```

### Axios Example

```javascript
import axios from 'axios';

const API_BASE_URL = 'http://localhost:8081/api/v1';

const api = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Create strategy
export const createStrategy = (strategyData) => 
  api.post('/strategies', strategyData);

// Get strategy
export const getStrategy = (strategyId, userId) => 
  api.get(`/strategies/${strategyId}`, { params: { user_id: userId } });

// Update strategy
export const updateStrategy = (strategyId, updates) => 
  api.put(`/strategies/${strategyId}`, updates);

// Delete strategy
export const deleteStrategy = (strategyId, userId) => 
  api.delete(`/strategies/${strategyId}`, { params: { user_id: userId } });

// List strategies
export const listStrategies = (userId, activeOnly = true, page = 1, pageSize = 10) => 
  api.get(`/users/${userId}/strategies`, {
    params: { active_only: activeOnly, page, page_size: pageSize }
  });

// Activate strategy
export const activateStrategy = (strategyId, userId) => 
  api.post(`/strategies/${strategyId}/activate`, { user_id: userId });

// Deactivate strategy
export const deactivateStrategy = (strategyId, userId) => 
  api.post(`/strategies/${strategyId}/deactivate`, { user_id: userId });
```

## 📊 Enum Values Reference

### Sentiment (sentiments field)
- `1`: POSITIVE
- `2`: NEUTRAL
- `3`: NEGATIVE

### Exchange (exchanges field)
- `1`: NSE
- `2`: BSE

### OrderType (order_type field)
- `1`: MARKET
- `2`: LIMIT
- `3`: STOP_LOSS
- `4`: STOP_LOSS_MARKET

### OrderSide (order_side field)
- `1`: BUY
- `2`: SELL

### PositionSizing (position_sizing field)
- `1`: FIXED
- `2`: PERCENTAGE

## 🐛 Troubleshooting

### Gateway Won't Start

**Error:** `Failed to initialize user config client`

**Solution:** Ensure the User Config Service is running on port 50051:
```bash
cd /home/rohitt/Desktop/trading-system/services/user-config
go run cmd/main.go
```

### CORS Errors

**Error:** `Access-Control-Allow-Origin header is missing`

**Solution:** Check that your frontend origin is listed in `.env`:
```bash
CORS_ALLOWED_ORIGINS=http://localhost:3000,http://localhost:5173
```

Add your frontend's origin if it's different.

### Port Already in Use

**Error:** `bind: address already in use`

**Solution:** Change the port in `.env`:
```bash
HTTP_PORT=8082  # or any available port
```

## 📝 Development

### Project Structure

```
api/gateway/
├── cmd/
│   └── main.go              # Application entry point
├── config/
│   └── config.go            # Configuration management
├── internal/
│   ├── grpc_clients/        # gRPC client wrappers
│   │   └── user_config_client.go
│   ├── handlers/            # HTTP request handlers
│   │   └── user_config_handler.go
│   ├── middleware/          # HTTP middleware
│   │   └── cors.go
│   └── router/              # Route definitions
│       └── router.go
├── .env                     # Environment configuration
├── .env.example             # Example configuration
├── go.mod                   # Go module definition
└── README.md               # This file
```

### Adding New Endpoints

1. **Add gRPC client method** in `internal/grpc_clients/`
2. **Create handler function** in `internal/handlers/`
3. **Register route** in `internal/router/router.go`

## 🔗 Related Services

- **User Config Service**: Backend gRPC service (port 50051)
- **Frontend Application**: React/Vue/Angular app (ports 3000/5173)

## 📚 Additional Resources

- [User Config Service Documentation](../../services/user-config/README.md)
- [Protocol Buffers Documentation](https://protobuf.dev/)
- [gRPC Documentation](https://grpc.io/docs/)
- [Gorilla Mux Documentation](https://github.com/gorilla/mux)

## 🎯 Next Steps

1. **Start the gateway** as shown above
2. **Test endpoints** using cURL or Postman
3. **Integrate with frontend** using the provided examples
4. **Monitor logs** for any issues

The API Gateway is now ready to serve REST API requests from your frontend application!
