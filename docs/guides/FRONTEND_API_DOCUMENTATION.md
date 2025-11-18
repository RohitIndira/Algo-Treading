# Frontend Developer API Documentation
## Algo Trading System Integration Guide

**Last Updated:** November 13, 2025  
**System Version:** 1.0.0  
**Environment:** Development

---

## Table of Contents
1. [System Overview](#system-overview)
2. [Running Services](#running-services)
3. [Architecture](#architecture)
4. [Authentication](#authentication)
5. [User Configuration Service API](#user-configuration-service-api)
6. [Trade Execution Service API](#trade-execution-service-api)
7. [WebSocket Connections](#websocket-connections)
8. [Error Handling](#error-handling)
9. [Data Models](#data-models)
10. [Code Examples](#code-examples)
11. [Testing & Debugging](#testing--debugging)

---

## System Overview

The Algo Trading System is a microservices-based platform that enables automated trading based on news events and user-defined strategies.

### Key Features
- **Strategy Management**: Create, update, and manage trading strategies
- **Automated Order Execution**: Execute trades based on news events and market conditions
- **Risk Management**: Built-in risk controls and position limits
- **Real-time Updates**: WebSocket connections for live order status
- **Integration with Odin API**: Direct broker integration for order execution

### Technology Stack
- **Backend**: Go (Golang) microservices with gRPC
- **Message Queue**: Kafka (news events), RabbitMQ (order execution)
- **Database**: PostgreSQL (orders, strategies), MongoDB (market data)
- **Cache**: Redis
- **API Gateway**: REST/HTTP to gRPC translation layer

---

## Running Services

### Infrastructure Services (Already Running)

All infrastructure services are running via Docker:

```bash
# Service Status
┌────────────────────┬────────────────────────┬──────────────────────────────────┐
│ Service            │ Status                 │ Access URL                       │
├────────────────────┼────────────────────────┼──────────────────────────────────┤
│ PostgreSQL         │ ✓ Running              │ localhost:5432                   │
│ RabbitMQ           │ ✓ Running              │ localhost:5672 (AMQP)            │
│ RabbitMQ UI        │ ✓ Running              │ http://localhost:15672           │
│ Kafka              │ ✓ Running              │ localhost:9092                   │
│ Kafka UI           │ ✓ Running              │ http://localhost:8080            │
│ Zookeeper          │ ✓ Running              │ localhost:2181                   │
│ Redis              │ ✓ Running              │ localhost:6379                   │
└────────────────────┴────────────────────────┴──────────────────────────────────┘

# Database Credentials
- PostgreSQL User: postgres
- PostgreSQL Password: password
- Database: orders
- RabbitMQ User: guest
- RabbitMQ Password: guest
```

### Microservices (gRPC)

#### Trade Execution Service
**Status:** ✓ Running  
**Port:** 9004  
**Endpoint:** `localhost:9004`

```bash
# Service Info
- gRPC Server: localhost:9004
- RabbitMQ Queue: order.execution.queue
- Worker Count: 10
- Status: Healthy
```

#### User Config Service
**Status:** ⚠ Requires Module Fix  
**Port:** 9001  
**Endpoint:** `localhost:9001`

**To Start:**
```bash
cd services/user-config
$env:DB_HOST="localhost"
$env:DB_PORT="5432"
$env:DB_USER="postgres"
$env:DB_PASSWORD="password"
$env:DB_NAME="orders"
$env:DB_SSLMODE="disable"
$env:GRPC_PORT="9001"
$env:KAFKA_ENABLED="true"
$env:KAFKA_BROKERS="localhost:9092"
$env:KAFKA_TOPIC="strategy.events"
go run cmd/main.go
```

#### Other Services (Risk Management, Rules Engine, Data Ingestion)
**Status:** Not started (optional for basic frontend integration)

---

## Architecture

### System Flow

```
┌─────────────┐
│   Frontend  │
│   (React)   │
└──────┬──────┘
       │
       ▼
┌─────────────────┐
│  API Gateway    │ (To be implemented or direct gRPC-Web)
│  REST/HTTP      │
└──────┬──────────┘
       │
       ├──────────────┬──────────────┬─────────────┐
       ▼              ▼              ▼             ▼
┌────────────┐ ┌────────────┐ ┌──────────┐ ┌──────────┐
│User Config │ │Trade Exec  │ │Rules     │ │Risk Mgmt │
│Service     │ │Service     │ │Engine    │ │Service   │
│:9001       │ │:9004       │ │:9003     │ │:9005     │
└─────┬──────┘ └─────┬──────┘ └────┬─────┘ └────┬─────┘
      │              │              │            │
      └──────────────┴──────────────┴────────────┘
                     │
              ┌──────┴──────┐
              ▼             ▼
      ┌──────────┐   ┌──────────┐
      │PostgreSQL│   │  Kafka   │
      │          │   │RabbitMQ  │
      └──────────┘   └──────────┘
```

### Communication Protocols

1. **gRPC**: Inter-service communication (microservices to microservices)
2. **REST/HTTP**: Frontend to backend (via API Gateway or gRPC-Web)
3. **WebSocket**: Real-time updates (order status, market data)
4. **Kafka**: Event streaming (news events, market data)
5. **RabbitMQ**: Message queue (order execution)

---

## Authentication

### JWT Token-Based Authentication

**Note:** Authentication service is not yet implemented. For development, use placeholder user IDs.

### Headers Required

```http
Authorization: Bearer <jwt_token>
Content-Type: application/json
X-User-ID: <user_id>
```

### Development User IDs
For testing purposes, you can use:
- `user_123`
- `test_user_001`
- Any string identifier

---

## User Configuration Service API

**Base URL:** `localhost:9001` (gRPC)  
**Protocol:** gRPC (requires gRPC-Web adapter or REST gateway)

### Endpoints

#### 1. Create Strategy

**Purpose:** Create a new trading strategy

**Method:** `CreateStrategy`

**Request:**
```json
{
  "user_id": "user_123",
  "strategy_name": "News-Based RELIANCE Trading",
  "description": "Buy RELIANCE on positive news with high impact",
  "conditions": {
    "impact_score_threshold": 7,
    "sentiments": ["POSITIVE"],
    "categories": ["CORPORATE", "MARKET"],
    "stock_codes": [15124],
    "price_range": {
      "min": 2000.0,
      "max": 3000.0
    },
    "volume_threshold": 100000,
    "pct_change_threshold": 2.0,
    "exchanges": ["NSE_EQ"]
  },
  "trade_config": {
    "order_type": "LIMIT",
    "quantity": 10,
    "max_position_size": 50000.0,
    "stop_loss_pct": 2.0,
    "take_profit_pct": 5.0,
    "exchange": "NSE_EQ",
    "order_side": "BUY",
    "limit_price": 2500.0,
    "validity": "DAY"
  },
  "risk_limits": {
    "max_daily_trades": 5,
    "max_loss_per_day": 10000.0,
    "position_sizing": "FIXED",
    "max_portfolio_exposure_pct": 30.0,
    "max_per_trade_risk": 5000.0,
    "enable_risk_checks": true
  },
  "activate_immediately": true
}
```

**Response:**
```json
{
  "success": true,
  "strategy": {
    "strategy_id": "stg_abc123def456",
    "user_id": "user_123",
    "strategy_name": "News-Based RELIANCE Trading",
    "description": "Buy RELIANCE on positive news with high impact",
    "active": true,
    "conditions": { /* ... */ },
    "trade_config": { /* ... */ },
    "risk_limits": { /* ... */ },
    "created_at": "2025-11-13T12:00:00Z",
    "updated_at": "2025-11-13T12:00:00Z",
    "version": 1
  },
  "error": null
}
```

**Enums:**
- **Sentiment**: `POSITIVE`, `NEUTRAL`, `NEGATIVE`
- **Exchange**: `NSE_EQ`, `BSE_EQ`, `NSE_FO`, `BSE_FO`, `MCX`
- **OrderType**: `MARKET`, `LIMIT`, `STOP_LOSS`, `STOP_LOSS_MARKET`
- **OrderSide**: `BUY`, `SELL`
- **PositionSizing**: `FIXED`, `PERCENTAGE`, `RISK_BASED`

---

#### 2. Update Strategy

**Purpose:** Update an existing strategy

**Method:** `UpdateStrategy`

**Request:**
```json
{
  "strategy_id": "stg_abc123def456",
  "user_id": "user_123",
  "strategy_name": "Updated Strategy Name",
  "conditions": {
    "impact_score_threshold": 8
  },
  "version": 1
}
```

**Response:**
```json
{
  "success": true,
  "strategy": { /* Updated strategy object */ },
  "error": null
}
```

---

#### 3. Get Strategy

**Purpose:** Retrieve a specific strategy

**Method:** `GetStrategy`

**Request:**
```json
{
  "strategy_id": "stg_abc123def456",
  "user_id": "user_123"
}
```

**Response:**
```json
{
  "success": true,
  "strategy": { /* Strategy object */ },
  "error": null
}
```

---

#### 4. List User Strategies

**Purpose:** Get all strategies for a user

**Method:** `ListUserStrategies`

**Request:**
```json
{
  "user_id": "user_123",
  "active_only": false,
  "pagination": {
    "page": 1,
    "page_size": 20,
    "sort_by": "created_at",
    "sort_order": "DESC"
  }
}
```

**Response:**
```json
{
  "success": true,
  "strategies": [
    { /* Strategy 1 */ },
    { /* Strategy 2 */ }
  ],
  "pagination": {
    "page": 1,
    "page_size": 20,
    "total_count": 5,
    "total_pages": 1
  },
  "error": null
}
```

---

#### 5. Activate Strategy

**Purpose:** Activate a strategy to start receiving signals

**Method:** `ActivateStrategy`

**Request:**
```json
{
  "strategy_id": "stg_abc123def456",
  "user_id": "user_123"
}
```

**Response:**
```json
{
  "success": true,
  "strategy": { /* Strategy with active=true */ },
  "error": null
}
```

---

#### 6. Deactivate Strategy

**Purpose:** Deactivate a strategy to stop receiving signals

**Method:** `DeactivateStrategy`

**Request:**
```json
{
  "strategy_id": "stg_abc123def456",
  "user_id": "user_123"
}
```

**Response:**
```json
{
  "success": true,
  "strategy": { /* Strategy with active=false */ },
  "error": null
}
```

---

#### 7. Delete Strategy

**Purpose:** Permanently delete a strategy

**Method:** `DeleteStrategy`

**Request:**
```json
{
  "strategy_id": "stg_abc123def456",
  "user_id": "user_123"
}
```

**Response:**
```json
{
  "success": true,
  "message": "Strategy deleted successfully",
  "error": null
}
```

---

## Trade Execution Service API

**Base URL:** `localhost:9004` (gRPC)  
**Protocol:** gRPC

### Endpoints

#### 1. Get Order Status

**Purpose:** Retrieve the current status of an order

**Method:** `GetOrderStatus`

**Request:**
```json
{
  "order_id": "ord_xyz789abc123",
  "user_id": "user_123"
}
```

**Response:**
```json
{
  "success": true,
  "order": {
    "order_id": "ord_xyz789abc123",
    "user_id": "user_123",
    "strategy_id": "stg_abc123def456",
    "event_id": "evt_news_001",
    "stock_code": 15124,
    "exchange": "NSE_EQ",
    "symbol": "RELIANCE",
    "order_type": "LIMIT",
    "order_side": "BUY",
    "quantity": 10,
    "price": 2500.0,
    "stop_loss": 2450.0,
    "take_profit": 2625.0,
    "validity": "DAY",
    "status": "FILLED",
    "odin_order_id": "OD001234567890",
    "filled_quantity": 10,
    "filled_price": 2498.5,
    "commission": 25.50,
    "total_cost": 24985.0,
    "created_at": "2025-11-13T10:00:00Z",
    "updated_at": "2025-11-13T10:00:15Z",
    "submitted_at": "2025-11-13T10:00:05Z",
    "executed_at": "2025-11-13T10:00:15Z",
    "error_message": null,
    "rejection_reason": null,
    "metadata": {
      "news_headline": "Reliance announces major expansion",
      "impact_score": "8"
    }
  },
  "error": null
}
```

**Order Status Enums:**
- `PENDING`: Order created but not yet submitted
- `SUBMITTED`: Submitted to broker
- `ACKNOWLEDGED`: Broker acknowledged the order
- `PARTIALLY_FILLED`: Partially executed
- `FILLED`: Fully executed
- `REJECTED`: Rejected by broker
- `CANCELLED`: Cancelled by user or system
- `EXPIRED`: Order expired

---

#### 2. Get User Orders

**Purpose:** Get all orders for a user with filters

**Method:** `GetUserOrders`

**Request:**
```json
{
  "user_id": "user_123",
  "filter": {
    "statuses": ["FILLED", "PARTIALLY_FILLED"],
    "exchanges": ["NSE_EQ"],
    "start_date": "2025-11-01T00:00:00Z",
    "end_date": "2025-11-13T23:59:59Z",
    "stock_codes": [15124],
    "strategy_ids": ["stg_abc123def456"],
    "order_sides": ["BUY"]
  },
  "pagination": {
    "page": 1,
    "page_size": 50,
    "sort_by": "created_at",
    "sort_order": "DESC"
  }
}
```

**Response:**
```json
{
  "success": true,
  "orders": [
    { /* Order 1 */ },
    { /* Order 2 */ },
    { /* Order 3 */ }
  ],
  "pagination": {
    "page": 1,
    "page_size": 50,
    "total_count": 125,
    "total_pages": 3
  },
  "error": null
}
```

---

#### 3. Cancel Order

**Purpose:** Cancel a pending order

**Method:** `CancelOrder`

**Request:**
```json
{
  "order_id": "ord_xyz789abc123",
  "user_id": "user_123",
  "reason": "User requested cancellation"
}
```

**Response:**
```json
{
  "success": true,
  "order": { /* Updated order with status=CANCELLED */ },
  "message": "Order cancelled successfully",
  "error": null
}
```

---

#### 4. Modify Order

**Purpose:** Modify a pending order (quantity, price, etc.)

**Method:** `ModifyOrder`

**Request:**
```json
{
  "order_id": "ord_xyz789abc123",
  "user_id": "user_123",
  "new_quantity": 15,
  "new_price": 2505.0,
  "new_order_type": "LIMIT",
  "new_validity": "DAY"
}
```

**Response:**
```json
{
  "success": true,
  "order": { /* Updated order */ },
  "message": "Order modified successfully",
  "error": null
}
```

---

#### 5. Get Order History

**Purpose:** Get historical orders with full details

**Method:** `GetOrderHistory`

**Request:**
```json
{
  "user_id": "user_123",
  "filter": {
    "start_date": "2025-11-01T00:00:00Z",
    "end_date": "2025-11-13T23:59:59Z"
  },
  "pagination": {
    "page": 1,
    "page_size": 100
  },
  "include_cancelled": true
}
```

**Response:**
```json
{
  "success": true,
  "orders": [ /* Array of orders */ ],
  "pagination": { /* Pagination info */ },
  "error": null
}
```

---

#### 6. Get Order Statistics

**Purpose:** Get aggregated statistics for user's trading activity

**Method:** `GetOrderStatistics`

**Request:**
```json
{
  "user_id": "user_123",
  "start_date": "2025-11-01T00:00:00Z",
  "end_date": "2025-11-13T23:59:59Z",
  "strategy_ids": ["stg_abc123def456"]
}
```

**Response:**
```json
{
  "success": true,
  "statistics": {
    "user_id": "user_123",
    "total_orders": 50,
    "filled_orders": 45,
    "pending_orders": 2,
    "rejected_orders": 2,
    "cancelled_orders": 1,
    "fill_rate_pct": 90.0,
    "rejection_rate_pct": 4.0,
    "total_traded_value": 2500000.0,
    "avg_order_value": 55555.56,
    "avg_execution_time_ms": 1250.5,
    "p95_execution_time_ms": 2500.0,
    "orders_by_exchange": {
      "NSE_EQ": 40,
      "BSE_EQ": 10
    },
    "orders_by_type": {
      "LIMIT": 35,
      "MARKET": 15
    },
    "orders_by_strategy": {
      "stg_abc123def456": 30,
      "stg_def789ghi012": 20
    },
    "start_date": "2025-11-01T00:00:00Z",
    "end_date": "2025-11-13T23:59:59Z"
  },
  "error": null
}
```

---

## WebSocket Connections

### Real-Time Order Updates

**Endpoint:** `ws://localhost:9004/ws/orders` (To be implemented)

**Connection:**
```javascript
const ws = new WebSocket('ws://localhost:9004/ws/orders?user_id=user_123&token=<jwt>');

ws.onmessage = (event) => {
  const update = JSON.parse(event.data);
  console.log('Order Update:', update);
};
```

**Message Format:**
```json
{
  "type": "ORDER_UPDATE",
  "order_id": "ord_xyz789abc123",
  "status": "FILLED",
  "filled_quantity": 10,
  "filled_price": 2498.5,
  "timestamp": "2025-11-13T10:00:15Z"
}
```

---

## Error Handling

### Standard Error Response

```json
{
  "success": false,
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Invalid strategy configuration",
    "details": {
      "field": "trade_config.quantity",
      "reason": "Quantity must be greater than 0"
    },
    "timestamp": "2025-11-13T12:00:00Z",
    "request_id": "req_abc123"
  }
}
```

### Error Codes

| Code | HTTP Status | Description |
|------|-------------|-------------|
| `VALIDATION_ERROR` | 400 | Invalid request parameters |
| `UNAUTHORIZED` | 401 | Authentication failed |
| `FORBIDDEN` | 403 | User doesn't have permission |
| `NOT_FOUND` | 404 | Resource not found |
| `CONFLICT` | 409 | Resource conflict (e.g., duplicate) |
| `RATE_LIMIT_EXCEEDED` | 429 | Too many requests |
| `INTERNAL_ERROR` | 500 | Server error |
| `SERVICE_UNAVAILABLE` | 503 | Service temporarily unavailable |
| `ORDER_REJECTED` | 422 | Order rejected by broker |
| `INSUFFICIENT_FUNDS` | 422 | Insufficient balance |
| `RISK_CHECK_FAILED` | 422 | Risk limits exceeded |

---

## Data Models

### Common Types

#### Timestamp
```json
{
  "seconds": 1699875600,
  "nanos": 0
}
```

#### PriceRange
```json
{
  "min": 2000.0,
  "max": 3000.0
}
```

#### Pagination
```json
{
  "page": 1,
  "page_size": 20,
  "total_count": 100,
  "total_pages": 5
}
```

### Stock Codes Reference

Common stock codes for testing:
- **RELIANCE**: 15124
- **TCS**: 11536
- **INFY**: 10940
- **HDFC BANK**: 1333
- **ICICI BANK**: 4963

---

## Code Examples

### React/TypeScript Integration

#### 1. Create Strategy Component

```typescript
import { useState } from 'react';

interface CreateStrategyRequest {
  user_id: string;
  strategy_name: string;
  description: string;
  conditions: StrategyConditions;
  trade_config: TradeConfig;
  risk_limits: RiskLimits;
  activate_immediately: boolean;
}

const CreateStrategy: React.FC = () => {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const createStrategy = async (data: CreateStrategyRequest) => {
    setLoading(true);
    setError(null);

    try {
      const response = await fetch('http://localhost:9001/api/v1/strategies', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${localStorage.getItem('token')}`,
          'X-User-ID': data.user_id
        },
        body: JSON.stringify(data)
      });

      if (!response.ok) {
        throw new Error('Failed to create strategy');
      }

      const result = await response.json();
      console.log('Strategy created:', result.strategy);
      return result.strategy;
    } catch (err) {
      setError(err.message);
      console.error('Error creating strategy:', err);
    } finally {
      setLoading(false);
    }
  };

  return (
    // Your component JSX
  );
};
```

#### 2. Fetch User Orders

```typescript
import { useEffect, useState } from 'react';

interface Order {
  order_id: string;
  symbol: string;
  status: string;
  quantity: number;
  price: number;
  // ... other fields
}

const OrdersList: React.FC<{ userId: string }> = ({ userId }) => {
  const [orders, setOrders] = useState<Order[]>([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    fetchOrders();
  }, [userId]);

  const fetchOrders = async () => {
    setLoading(true);
    try {
      const response = await fetch('http://localhost:9004/api/v1/orders', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${localStorage.getItem('token')}`,
        },
        body: JSON.stringify({
          user_id: userId,
          filter: {
            statuses: ['FILLED', 'PARTIALLY_FILLED', 'PENDING']
          },
          pagination: {
            page: 1,
            page_size: 50
          }
        })
      });

      const result = await response.json();
      if (result.success) {
        setOrders(result.orders);
      }
    } catch (err) {
      console.error('Error fetching orders:', err);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div>
      {loading ? (
        <p>Loading orders...</p>
      ) : (
        <ul>
          {orders.map(order => (
            <li key={order.order_id}>
              {order.symbol} - {order.status} - Qty: {order.quantity}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
};
```

#### 3. WebSocket Order Updates

```typescript
import { useEffect, useState } from 'react';

const useOrderUpdates = (userId: string) => {
  const [orderUpdates, setOrderUpdates] = useState<any[]>([]);
  const [ws, setWs] = useState<WebSocket | null>(null);

  useEffect(() => {
    const token = localStorage.getItem('token');
    const websocket = new WebSocket(
      `ws://localhost:9004/ws/orders?user_id=${userId}&token=${token}`
    );

    websocket.onopen = () => {
      console.log('WebSocket connected');
    };

    websocket.onmessage = (event) => {
      const update = JSON.parse(event.data);
      setOrderUpdates(prev => [update, ...prev]);
    };

    websocket.onerror = (error) => {
      console.error('WebSocket error:', error);
    };

    websocket.onclose = () => {
      console.log('WebSocket disconnected');
    };

    setWs(websocket);

    return () => {
      websocket.close();
    };
  }, [userId]);

  return { orderUpdates, ws };
};
```

---

## Testing & Debugging

### Service Health Checks

```bash
# Check if Trade Execution Service is running
curl -X POST http://localhost:9004/health

# Check if User Config Service is running
curl -X POST http://localhost:9001/health
```

### Database Queries

```sql
-- Check strategies
SELECT * FROM strategies WHERE user_id = 'user_123';

-- Check orders
SELECT * FROM orders WHERE user_id = 'user_123' ORDER BY created_at DESC LIMIT 10;

-- Check order statistics
SELECT 
  status,
  COUNT(*) as count,
  SUM(total_cost) as total_value
FROM orders
WHERE user_id = 'user_123'
GROUP BY status;
```

### RabbitMQ Management UI

Access: http://localhost:15672
- Username: guest
- Password: guest

**Useful Pages:**
- **Queues**: View order.execution.queue
- **Exchanges**: View order.execution.exchange
- **Connections**: Monitor active connections

### Kafka UI

Access: http://localhost:8080

**Topics to Monitor:**
- `market.data.news`: News events
- `strategy.events`: Strategy changes
- `order.events`: Order lifecycle events

### gRPC Testing with grpcurl

```bash
# Install grpcurl
# Windows: scoop install grpcurl
# Mac: brew install grpcurl

# List services
grpcurl -plaintext localhost:9004 list

# Call GetOrderStatus
grpcurl -plaintext -d '{
  "order_id": "ord_xyz789abc123",
  "user_id": "user_123"
}' localhost:9004 trade_execution.TradeExecutionService/GetOrderStatus

# Call ListUserStrategies
grpcurl -plaintext -d '{
  "user_id": "user_123",
  "active_only": false
}' localhost:9001 user_config.UserConfigService/ListUserStrategies
```

---

## Additional Resources

### Documentation Files
- **Trade Execution Guide**: `docs/guides/TRADE_EXECUTION_COMPLETE_GUIDE.md`
- **Risk Management**: `docs/guides/RISK_MANAGEMENT_IMPLEMENTATION.md`
- **Odin API Integration**: `docs/guides/odin-api-sdk-integration.md`
- **Architecture**: `docs/guides/trading-system-architecture.md`

### Proto Definitions
- User Config: `api/proto/user_config/user_config.proto`
- Trade Execution: `api/proto/trade_execution/trade_execution.proto`
- Common Types: `api/proto/common/common.proto`

### Test Clients
- Trade Execution: `services/trade-execution/test_client.go`
- Risk Management: `services/risk-management/test_client.go`

---

## Contact & Support

For questions or issues:
1. Check the documentation in `docs/` directory
2. Review service README files in `services/*/README.md`
3. Check service logs for detailed error messages
4. Contact the backend team

---

**Document Version:** 1.0.0  
**Last Updated:** November 13, 2025  
**Maintained By:** Backend Team
