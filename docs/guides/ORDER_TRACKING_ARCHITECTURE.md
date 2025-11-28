# Order Tracking Service Architecture

## Overview
Rl-time order tracking sysysttmat monitors broker order status and puspuhh dates to fron via WebSocket.

## Architecture

```
         W app   → Kafka (order-placed) →          Odin API Stat
                                                  ↓↓
                            Redis Pub/Sub (upPostgreSQLd(ordersatable)
               ↓                    ↓
                    API Gateway WebSocket  in AP  StatusPolling
↓
Rdsub/Sub (updas)
 Coponents↓
APIGatewayWebSocket
### 1 Order Tracking Service (New Serv↓
icFrntn(Reac)
```

## Componen

###1.OrderTrackingService(NewService-ython/o)

**Responsibilities:**
- Consume order placement events from Kafka/RabbitMQ
- Store orders in PostgreSQL with tracking information
- Poll Odin API for order status updates (every 5-10 seconds)
- Publish status updates to Redis Pub/Sub
- Provide REST API for order history

**Database Schema:**
```sql
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(50) UNIQUE NOT NULL,
    broker_order_id VARCHAR(50),
    user_id VARCHAR(50) NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    exchange VARCHAR(20),
    token BIGINT,
    order_side VARCHAR(10), -- BUY/SELL
    quantity INTEGER,
    price DECIMAL(10, 2),
    order_type VARCHAR(20),
    product_type VARCHAR(20),
    
    -- Status tracking
    status VARCHAR(50) DEFAULT 'PENDING', -- PENDING, OPEN, PARTIAL, FILLED, REJECTED, CANCELLED
    filled_quantity INTEGER DEFAULT 0,
    average_price DECIMAL(10, 2),
    
    -- Timestamps
    placed_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    filled_at TIMESTAMP,
    
    -- Additional info
    strategy_id VARCHAR(50),
    error_message TEXT,
    
    INDEX idx_user_orders (user_id, placed_at DESC),
    INDEX idx_broker_order (broker_order_id),
    INDEX idx_status (status)
);
```

**Redis Pub/Sub Channels:**
- `orders:{user_id}` - User-specific order updates
- `orders:all` - All order updates (admin)

**Message Format:**
```json
{
    "order_id": "eecbd784-ac8d-4707-8702-0e9e1a7f6375",
    "broker_order_id": "NZVND00002K",
    "user_id": "ISPL19027",
    "symbol": "PETRONET",
    "status": "FILLED",
    "filled_quantity": 1,
    "average_price": 280.5,
    "timestamp": "2025-11-27T17:15:00Z"
}
```

