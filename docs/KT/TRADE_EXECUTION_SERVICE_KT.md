# Trade Execution Service - Knowledge Transfer Documentation

## Table of Contents
1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Core Components](#core-components)
4. [Order Execution Flow](#order-execution-flow)
5. [gRPC API Reference](#grpc-api-reference)
6. [Message Queue Integration](#message-queue-integration)
7. [Database Schema](#database-schema)
8. [Indira Securities API Integration](#indira-securities-api-integration)
9. [Configuration](#configuration)
10. [Setup & Deployment](#setup--deployment)
11. [Testing](#testing)
12. [Monitoring & Troubleshooting](#monitoring--troubleshooting)
13. [Best Practices](#best-practices)

---

## 1. Overview

### Purpose
The **Trade Execution Service** is the core order processing engine of the algorithmic trading system. It receives risk-approved trade signals, executes them through the Indira Securities broker API, and tracks the complete order lifecycle from creation to final settlement.

### Key Responsibilities
- **Order Lifecycle Management**: Tracks orders from RECEIVED → VALIDATED → SUBMITTED → FILLED
- **Risk-Approved Order Processing**: Only executes orders validated by Risk Management Service
- **Broker API Integration**: Executes trades through Indira Securities Trading API
- **Multi-Consumer Architecture**: Consumes from both Kafka (trade signals) and RabbitMQ (order executions)
- **Retry Mechanism**: Implements exponential backoff for failed order placements
- **Credential Management**: Securely fetches and uses user broker credentials
- **Execution Tracking**: Records all execution events and state transitions
- **Real-Time Updates**: Publishes order status updates via RabbitMQ

### Technology Stack
- **Language**: Go 1.23+
- **Protocol**: gRPC (Port 50054)
- **Database**: PostgreSQL 13+ (order storage, execution history)
- **Cache**: Redis (optional - credentials cache)
- **Message Queue (Input)**: Kafka (`trade-signals` topic), RabbitMQ (`trade.executions` queue)
- **Message Queue (Output)**: RabbitMQ (`order.executions` exchange)
- **External API**: Indira Securities Trading API (broker integration)
- **Migration Tool**: SQL migrations for schema versioning

### Integration Points
```
┌─────────────────────┐
│   Rules Engine      │ ──► Publishes trade-signals to Kafka
└─────────────────────┘
           │
           ▼
┌─────────────────────┐
│ Kafka Consumer      │ ──► Reads trade-signals topic
│ (Trade Signals)     │
└─────────────────────┘
           │
           ▼
┌─────────────────────┐
│ Signal Processor    │ ──► Calls Risk Management gRPC
└─────────────────────┘     (CheckPreTradeRisk)
           │
           ▼
┌─────────────────────┐
│ RabbitMQ Publisher  │ ──► Publishes to trade.executions
└─────────────────────┘
           │
           ▼
┌─────────────────────┐
│ RabbitMQ Consumer   │ ──► Reads trade.executions queue
│ (Order Execution)   │
└─────────────────────┘
           │
           ▼
┌─────────────────────┐
│ Order Executor      │ ──► Fetches credentials, places order via Indira Securities API
└─────────────────────┘
           │
           ▼
┌─────────────────────┐
│ PostgreSQL          │ ──► Stores orders, execution events
└─────────────────────┘
```

---

## 2. Architecture

### High-Level Architecture
```
┌───────────────────────────────────────────────────────────────────┐
│                    Trade Execution Service                        │
├───────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌──────────────┐        ┌──────────────┐      ┌──────────────┐ │
│  │   Kafka      │        │  RabbitMQ    │      │  gRPC Server │ │
│  │  Consumer    │────────│  Publisher   │      │  (Port 50054) │ │
│  │(trade-signals)        │(to indira-api) │      └──────────────┘ │
│  └──────────────┘        └──────────────┘              │         │
│         │                        │                      │         │
│         ▼                        ▼                      ▼         │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │                  Signal Processor                          │  │
│  │  - Validates trade signals                                 │  │
│  │  - Calls Risk Management Service                           │  │
│  │  - Creates order records                                   │  │
│  └────────────────────────────────────────────────────────────┘  │
│                                │                                  │
│                                ▼                                  │
│  ┌──────────────┐        ┌──────────────┐                        │
│  │  RabbitMQ    │        │ PostgreSQL   │                        │
│  │  Consumer    │────────│  Repository  │                        │
│  │(trade.exec)  │        │  (Orders)    │                        │
│  └──────────────┘        └──────────────┘                        │
│         │                        │                                │
│         ▼                        ▼                                │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │                  Order Executor                            │  │
│  │  - Fetches user credentials                                │  │
│  │  - Calls Indira Securities API with retry logic                         │  │
│  │  - Updates order status                                    │  │
│  │  - Records execution events                                │  │
│  └────────────────────────────────────────────────────────────┘  │
│                                │                                  │
│                                ▼                                  │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │                  Indira Client                               │  │
│  │  - Authentication (TOTP, API Key)                          │  │
│  │  - Order placement API calls                               │  │
│  │  - Response parsing                                        │  │
│  └────────────────────────────────────────────────────────────┘  │
│                                                                   │
└───────────────────────────────────────────────────────────────────┘
```

### Design Patterns
1. **Repository Pattern**: Database access abstracted through repository interfaces
2. **Worker Pool Pattern**: Multiple concurrent workers process RabbitMQ messages
3. **Retry with Exponential Backoff**: Failed order placements retried with increasing delays
4. **Command Pattern**: Order execution encapsulated as executable commands
5. **Event Sourcing**: All state changes recorded in execution_events table
6. **Dual Consumer Pattern**: Separate consumers for Kafka (signals) and RabbitMQ (executions)

---

## 3. Core Components

### 3.1 Kafka Consumer (`internal/consumer/kafka_consumer.go`)

**Purpose**: Consumes trade signals from Kafka `trade-signals` topic.

**Configuration**:
```go
type KafkaConsumer struct {
    reader    *kafka.Reader
    processor TradeSignalProcessor
    logger    *zap.Logger
}

kafka.ReaderConfig{
    Brokers:        []string{"localhost:9092"},
    Topic:          "trade-signals",
    GroupID:        "trade-execution-service",
    CommitInterval: time.Second,
    StartOffset:    kafka.LastOffset,
}
```

**Message Format** (Trade Signal):
```json
{
  "event_id": "123e4567-e89b-12d3-a456-426614174000",
  "user_id": "user123",
  "strategy_id": "strat456",
  "stock_code": 3456,
  "symbol": "RELIANCE",
  "exchange": "NSE",
  "action": "BUY",
  "quantity": 100,
  "order_type": "LIMIT",
  "price": 2500.50,
  "stop_loss": 2450.00,
  "take_profit": 2600.00,
  "timestamp": "2025-01-15T10:30:00Z"
}
```

**Processing Flow**:
```
1. Fetch message from Kafka
2. Unmarshal JSON → TradeSignal struct
3. Call processor.ProcessTradeSignal()
4. Commit message offset
```

### 3.2 Signal Processor (`internal/processor/signal_processor.go`)

**Purpose**: Validates trade signals and performs risk checks before creating orders.

**Key Functions**:
```go
// ProcessTradeSignal validates and creates order from signal
func (sp *SignalProcessor) ProcessTradeSignal(ctx context.Context, signal *TradeSignal) error {
    // 1. Validate signal data
    if err := sp.validateSignal(signal); err != nil {
        return fmt.Errorf("invalid signal: %w", err)
    }
    
    // 2. Call Risk Management Service
    riskResp, err := sp.riskClient.CheckPreTradeRisk(ctx, &riskReq)
    if err != nil || !riskResp.Approved {
        return fmt.Errorf("risk check failed: %v", err)
    }
    
    // 3. Create order record
    order := sp.createOrderFromSignal(signal, riskResp)
    
    // 4. Save to database
    if err := sp.orderRepo.Create(ctx, order); err != nil {
        return err
    }
    
    // 5. Publish to RabbitMQ for execution
    return sp.publisher.PublishOrder(ctx, order)
}
```

**Risk Management Integration**:
```protobuf
// gRPC call to Risk Management Service (port 9005)
riskReq := &pb.PreTradeRiskRequest{
    UserId:          signal.UserID,
    StrategyId:      signal.StrategyID,
    StockCode:       signal.StockCode,
    Exchange:        signal.Exchange,
    OrderType:       signal.OrderType,
    OrderSide:       signal.Action,
    Quantity:        signal.Quantity,
    Price:           signal.Price,
    // ... risk limits from strategy config
}
riskResp := riskClient.CheckPreTradeRisk(ctx, riskReq)
```

### 3.3 RabbitMQ Publisher (`internal/publisher/rabbitmq_publisher.go`)

**Purpose**: Publishes risk-approved orders to `trade.executions` queue for execution.

**Configuration**:
```go
type RabbitMQPublisher struct {
    conn        *amqp.Connection
    channel     *amqp.Channel
    exchange    string  // "order.executions"
    routingKey  string  // "trade.executions"
    logger      *zap.Logger
}
```

**Order Message Format**:
```json
{
  "order_id": "550e8400-e29b-41d4-a716-446655440000",
  "user_id": "user123",
  "strategy_id": "strat456",
  "event_id": "123e4567-e89b-12d3-a456-426614174000",
  "token": 3456,
  "symbol": "RELIANCE",
  "exchange": "NSE",
  "order_type": "LIMIT",
  "order_side": "BUY",
  "quantity": 100,
  "price": 2500.50,
  "stop_loss": 2450.00,
  "take_profit": 2600.00,
  "validity": "DAY",
  "timestamp": "2025-01-15T10:30:00Z"
}
```

**Publisher Confirms**:
- Enabled for guaranteed delivery
- Waits for acknowledgment before returning

### 3.4 RabbitMQ Consumer (`internal/consumer/rabbitmq_consumer.go`)

**Purpose**: Consumes orders from `trade.executions` queue and executes them.

**Worker Pool Configuration**:
```go
type RabbitMQConsumer struct {
    conn          *amqp.Connection
    channel       *amqp.Channel
    queueName     string           // "trade.executions"
    prefetchCount int              // 10 (concurrent messages per worker)
    executor      *OrderExecutor
    workerCount   int              // 5 (number of worker goroutines)
}
```

**Consumer Flow**:
```
1. Declare exchange "order.executions" (topic, durable)
2. Declare queue "trade.executions" (durable)
3. Bind queue to exchange with routing key "trade.executions"
4. Set QoS prefetch count (10 messages per worker)
5. Start worker pool (5 workers)
6. Each worker:
   - Consumes message
   - Unmarshals order
   - Calls executor.ExecuteOrder()
   - Acknowledges message (or rejects on failure)
```

**Dead Letter Exchange (DLQ)**:
```go
// Failed messages after max retries go to DLQ
args := amqp.Table{
    "x-dead-letter-exchange":    "order.executions.dlx",
    "x-dead-letter-routing-key": "dead.letter",
}
```

### 3.5 Order Executor (`internal/executor/executor.go`)

**Purpose**: Core execution logic - places orders via Indira Securities API with retry mechanism.

**Structure**:
```go
type OrderExecutor struct {
    repo       repository.OrderRepository
    credsRepo  repository.CredentialsRepository
    indiraClient *indira.ExecutionClient
    maxRetries int              // 3
    retryDelay time.Duration    // 2 seconds base delay
}
```

**Execution Flow**:
```
1. Verify order.RiskApproved == true
2. Update order status to PENDING
3. Fetch user credentials from database
4. Retry loop (max 3 attempts):
   a. Call indiraClient.PlaceOrderWithCredentials()
   b. If success:
      - Store Indira order ID
      - Update status to SUBMITTED
      - Record execution event
      - Return success
   c. If failure:
      - Increment retry count
      - Exponential backoff (2s, 4s, 8s)
      - Continue retry
5. If all retries exhausted:
   - Update status to FAILED
   - Record error message
   - Return failure
```

**Exponential Backoff**:
```go
for attempt := 0; attempt <= maxRetries; attempt++ {
    if attempt > 0 {
        delay := retryDelay * time.Duration(attempt)  // 2s, 4s, 8s
        time.Sleep(delay)
    }
    // ... place order attempt
}
```

### 3.6 Indira Client (`internal/indira/client.go`)

**Purpose**: Handles authentication and order placement with Indira Securities Trading API.

**Authentication Methods**:
1. **API Key + Password**: Basic authentication
2. **TOTP (Time-based OTP)**: For 2FA-enabled accounts
3. **Session Management**: Maintains active sessions with token refresh

**Key Functions**:
```go
// PlaceOrderWithCredentials authenticates and places order
func (c *ExecutionClient) PlaceOrderWithCredentials(
    ctx context.Context,
    order *models.Order,
    apiKey string,
    passwordEncrypted string,
    totpSecret string,
) (string, error) {
    // 1. Decrypt password
    password := decrypt(passwordEncrypted)
    
    // 2. Generate TOTP if needed
    totp := generateTOTP(totpSecret)
    
    // 3. Authenticate
    authToken, err := c.authenticate(apiKey, password, totp)
    
    // 4. Build order request
    indiraReq := c.buildIndiraOrderRequest(order)
    
    // 5. Place order
    indiraResp, err := c.httpClient.Post(
        "https://livemiddleware.indiratrade.com/v1/orders",
        indiraReq,
        authToken,
    )
    
    // 6. Return Indira order ID
    return indiraResp.OrderID, nil
}
```

**Order Translation** (Internal → Indira Securities Format):
```go
// Internal Order Model
Order{
    OrderType: "LIMIT",
    OrderSide: "BUY",
    Quantity:  100,
    Price:     2500.50,
}

// Indira Securities API Request
{
    "exchange": "NSE",
    "tradingsymbol": "RELIANCE",
    "transaction_type": "BUY",
    "order_type": "LIMIT",
    "quantity": 100,
    "price": 2500.50,
    "product": "MIS",
    "validity": "DAY"
}
```

### 3.7 Order Repository (`internal/repository/order_repository.go`)

**Purpose**: Database operations for order persistence.

**Interface**:
```go
type OrderRepository interface {
    Create(ctx context.Context, order *Order) error
    Update(ctx context.Context, order *Order) error
    GetByID(ctx context.Context, orderID uuid.UUID) (*Order, error)
    GetByIndiraOrderID(ctx context.Context, indiraOrderID string) (*Order, error)
    GetUserOrders(ctx context.Context, userID string, limit, offset int) ([]*Order, error)
    GetOrdersByStatus(ctx context.Context, status OrderStatus, limit int) ([]*Order, error)
    UpdateStatus(ctx context.Context, orderID uuid.UUID, status OrderStatus) error
    RecordExecutionEvent(ctx context.Context, orderID uuid.UUID, eventType string, eventData map[string]interface{}) error
}
```

**Key Operations**:
```go
// Create - Insert new order
INSERT INTO orders (
    order_id, user_id, strategy_id, event_id,
    stock_code, exchange, symbol,
    order_type, order_side, quantity, price,
    status, risk_approved, created_at
) VALUES (...)

// Update - Update order status and execution details
UPDATE orders SET
    status = $1, indira_order_id = $2,
    filled_quantity = $3, filled_price = $4,
    executed_at = $5, updated_at = NOW()
WHERE order_id = $6

// RecordExecutionEvent - Log state transitions
INSERT INTO execution_events (
    order_id, event_type, event_data, created_at
) VALUES ($1, $2, $3, NOW())
```

### 3.8 Credentials Repository (`internal/repository/credentials_repository.go`)

**Purpose**: Fetches encrypted user broker credentials.

**Interface**:
```go
type CredentialsRepository interface {
    GetUserCredentials(ctx context.Context, userID string) (*UserCredentials, error)
}

type UserCredentials struct {
    UserID            string
    APIKEY            string
    PasswordEncrypted string
    TOTPSecret        string
}
```

**Query**:
```sql
SELECT api_key, password_encrypted, totp_secret
FROM user_credentials
WHERE user_id = $1 AND is_active = true
```

### 3.9 gRPC Server (`internal/server/grpc_server.go`)

**Purpose**: Exposes gRPC endpoints for order management and status queries.

**Service Definition**:
```protobuf
service TradeExecutionService {
    rpc GetOrderStatus(GetOrderStatusRequest) returns (GetOrderStatusResponse);
    rpc GetUserOrders(GetUserOrdersRequest) returns (GetUserOrdersResponse);
    rpc CancelOrder(CancelOrderRequest) returns (CancelOrderResponse);
}
```

**Example gRPC Call**:
```bash
grpcurl -plaintext -d '{
  "order_id": "550e8400-e29b-41d4-a716-446655440000"
}' localhost:50054 trade_execution.TradeExecutionService/GetOrderStatus

# Response
{
  "order_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "SUBMITTED",
  "indira_order_id": "INDIRA123456",
  "created_at": "2025-01-15T10:30:00Z",
  "submitted_at": "2025-01-15T10:30:05Z"
}
```

---

## 4. Order Execution Flow

### Complete Lifecycle Flow
```
┌────────────────────────────────────────────────────────────────┐
│ STEP 1: Signal Generation (Rules Engine)                      │
├────────────────────────────────────────────────────────────────┤
│ Rules Engine publishes trade signal to Kafka topic            │
│ Topic: trade-signals                                           │
│ Message: {event_id, user_id, strategy_id, stock_code, ...}    │
└────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────────────┐
│ STEP 2: Signal Consumption (Kafka Consumer)                   │
├────────────────────────────────────────────────────────────────┤
│ Trade Execution Service consumes signal from Kafka            │
│ GroupID: trade-execution-service                               │
│ Status: RECEIVED                                               │
└────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────────────┐
│ STEP 3: Signal Processing (Signal Processor)                  │
├────────────────────────────────────────────────────────────────┤
│ 1. Validate signal data (non-null, positive quantity)         │
│ 2. Call Risk Management Service (gRPC port 9005)              │
│    - CheckPreTradeRisk(signal data + risk limits)             │
│    - Receive approval/rejection + risk score                  │
│ 3. If APPROVED:                                                │
│    - Create Order record in PostgreSQL                        │
│    - Status: VALIDATED, risk_approved: true                   │
│ 4. If REJECTED:                                                │
│    - Log rejection reason                                      │
│    - Terminate processing                                      │
└────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────────────┐
│ STEP 4: Order Publishing (RabbitMQ Publisher)                 │
├────────────────────────────────────────────────────────────────┤
│ Publish order to RabbitMQ                                      │
│ Exchange: order.executions (topic, durable)                    │
│ Routing Key: trade.executions                                  │
│ Queue: trade.executions (durable)                              │
│ Message: {order_id, user_id, token, price, quantity, ...}     │
└────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────────────┐
│ STEP 5: Order Consumption (RabbitMQ Consumer)                 │
├────────────────────────────────────────────────────────────────┤
│ Worker pool (5 workers) consumes from trade.executions        │
│ Prefetch count: 10 messages per worker                        │
│ Status: PENDING                                                │
└────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────────────┐
│ STEP 6: Order Execution (Order Executor)                      │
├────────────────────────────────────────────────────────────────┤
│ 1. Fetch user credentials from database                       │
│    SELECT api_key, password, totp_secret FROM credentials     │
│                                                                │
│ 2. Retry loop (max 3 attempts):                               │
│    Attempt 1: Call Indira Securities API                                   │
│    - If success → STEP 7                                      │
│    - If failure → Wait 2s, retry                              │
│    Attempt 2: Call Indira Securities API (after 2s delay)                  │
│    - If success → STEP 7                                      │
│    - If failure → Wait 4s, retry                              │
│    Attempt 3: Call Indira Securities API (after 4s delay)                  │
│    - If success → STEP 7                                      │
│    - If failure → STEP 8 (FAILED)                             │
└────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────────────┐
│ STEP 7: Indira Securities API Call (Indira Client)                           │
├────────────────────────────────────────────────────────────────┤
│ 1. Decrypt user password                                       │
│ 2. Generate TOTP (if 2FA enabled)                             │
│ 3. Authenticate with Indira Securities API                                  │
│    POST /api/v1/login                                          │
│    → Receive auth token                                        │
│                                                                │
│ 4. Place order via Indira Securities API                                   │
│    POST /api/v1/orders                                         │
│    Headers: Authorization: Bearer {token}                     │
│    Body: {exchange, symbol, quantity, price, ...}             │
│    → Receive Indira order ID                                    │
│                                                                │
│ 5. Update PostgreSQL:                                          │
│    - indira_order_id: "INDIRA123456"                              │
│    - status: SUBMITTED                                         │
│    - submitted_at: NOW()                                       │
│                                                                │
│ 6. Record execution event:                                     │
│    INSERT INTO execution_events                                │
│    (order_id, event_type: "SUBMITTED", event_data)            │
└────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────────────┐
│ STEP 8: Status Updates (External - Indira Securities WebSocket/Polling)      │
├────────────────────────────────────────────────────────────────┤
│ Indira Securities sends order updates via wss://livemiddleware.indiratrade.com:                                      │
│ - PARTIALLY_FILLED: filled_quantity < quantity                │
│ - FILLED: filled_quantity == quantity, executed_at = NOW()    │
│ - CANCELLED: User/system cancellation                         │
│ - REJECTED: Broker rejection                                   │
│                                                                │
│ Update PostgreSQL accordingly                                  │
└────────────────────────────────────────────────────────────────┘
```

### Order Status State Machine
```
RECEIVED (Signal consumed from Kafka)
    │
    ▼
VALIDATED (Risk check passed, order created in DB)
    │
    ▼
PENDING (Published to RabbitMQ, awaiting execution)
    │
    ▼
SUBMITTED (Sent to Indira Securities API successfully)
    │
    ├──► PARTIALLY_FILLED (Partial execution)
    │
    ├──► FILLED (Complete execution) [TERMINAL STATE]
    │
    ├──► REJECTED (Broker rejection) [TERMINAL STATE]
    │
    ├──► CANCELLED (User/system cancel) [TERMINAL STATE]
    │
    └──► FAILED (Execution error after retries) [TERMINAL STATE]
```

---

## 5. gRPC API Reference

### Service Definition
**File**: `api/proto/trade_execution/trade_execution.proto`

### Method 1: GetOrderStatus

**Purpose**: Retrieves current status of an order.

**Request**:
```protobuf
message GetOrderStatusRequest {
    string order_id = 1;  // UUID format
}
```

**Response**:
```protobuf
message GetOrderStatusResponse {
    string order_id = 1;
    string status = 2;  // RECEIVED, VALIDATED, PENDING, SUBMITTED, FILLED, etc.
    string indira_order_id = 3;
    int32 quantity = 4;
    int32 filled_quantity = 5;
    double filled_price = 6;
    string created_at = 7;
    string submitted_at = 8;
    string executed_at = 9;
    string error_message = 10;
}
```

**Example Call**:
```bash
grpcurl -plaintext -d '{
  "order_id": "550e8400-e29b-41d4-a716-446655440000"
}' localhost:50054 trade_execution.TradeExecutionService/GetOrderStatus
```

### Method 2: GetUserOrders

**Purpose**: Retrieves all orders for a specific user.

**Request**:
```protobuf
message GetUserOrdersRequest {
    string user_id = 1;
    int32 limit = 2;   // Default: 50
    int32 offset = 3;  // Default: 0
}
```

**Response**:
```protobuf
message GetUserOrdersResponse {
    repeated Order orders = 1;
    int32 total_count = 2;
}
```

### Method 3: CancelOrder

**Purpose**: Cancels a pending/submitted order.

**Request**:
```protobuf
message CancelOrderRequest {
    string order_id = 1;
}
```

**Response**:
```protobuf
message CancelOrderResponse {
    bool success = 1;
    string message = 2;
}
```

---

## 6. Message Queue Integration

### Kafka Integration

**Topic**: `trade-signals`  
**Producer**: Rules Engine Service  
**Consumer**: Trade Execution Service

**Consumer Group Configuration**:
```yaml
brokers:
  - localhost:9092
topic: trade-signals
group_id: trade-execution-service
commit_interval: 1s
start_offset: last  # Only consume new signals
max_bytes: 10MB
```

**Message Schema**:
```json
{
  "event_id": "uuid",
  "user_id": "string",
  "strategy_id": "string",
  "stock_code": "int64",
  "symbol": "string",
  "exchange": "NSE|BSE",
  "action": "BUY|SELL",
  "quantity": "int32",
  "order_type": "MARKET|LIMIT|STOP_LOSS",
  "price": "float64",
  "stop_loss": "float64",
  "take_profit": "float64",
  "timestamp": "ISO8601"
}
```

### RabbitMQ Integration

**Exchange**: `order.executions` (type: topic, durable: true)  
**Queue**: `trade.executions` (durable: true, prefetch: 10)  
**Routing Key**: `trade.executions`

**Publisher Configuration**:
```yaml
url: amqp://guest:guest@localhost:5672/
exchange: order.executions
routing_key: trade.executions
publisher_confirms: true  # Wait for acknowledgment
```

**Consumer Configuration**:
```yaml
url: amqp://guest:guest@localhost:5672/
queue_name: trade.executions
prefetch_count: 10  # Max concurrent messages per worker
worker_count: 5     # Number of worker goroutines
durable: true
auto_ack: false     # Manual acknowledgment
```

**Dead Letter Exchange (DLQ)**:
```yaml
exchange: order.executions.dlx
queue: trade.executions.dlq
routing_key: dead.letter
ttl: 86400000  # 24 hours
```

**Queue Binding**:
```bash
# Setup queue with DLQ
rabbitmqadmin declare queue name=trade.executions \
  durable=true \
  arguments='{"x-dead-letter-exchange":"order.executions.dlx","x-dead-letter-routing-key":"dead.letter"}'

# Bind queue to exchange
rabbitmqadmin declare binding source=order.executions \
  destination=trade.executions \
  routing_key=trade.executions
```

---

## 7. Database Schema

### PostgreSQL Schema
**File**: `migrations/001_create_orders_table.sql`

### Orders Table
```sql
CREATE TABLE orders (
    order_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id VARCHAR(50) NOT NULL,
    strategy_id VARCHAR(50) NOT NULL,
    event_id UUID NOT NULL,
    
    -- Stock information
    stock_code BIGINT NOT NULL,
    exchange VARCHAR(10) NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    
    -- Order details
    order_type VARCHAR(10) NOT NULL,  -- MARKET, LIMIT, STOP_LOSS
    order_side VARCHAR(10) NOT NULL,  -- BUY, SELL
    quantity INT NOT NULL,
    price DECIMAL(15,2),
    
    -- Stop loss and take profit
    stop_loss DECIMAL(15,2),
    take_profit DECIMAL(15,2),
    
    -- Order validity
    validity VARCHAR(10) DEFAULT 'DAY',
    
    -- Order status
    status VARCHAR(20) NOT NULL DEFAULT 'RECEIVED',
    
    -- Indira Securities API integration
    indira_order_id VARCHAR(50),
    indira_response TEXT,
    
    -- Execution details
    filled_quantity INT DEFAULT 0,
    filled_price DECIMAL(15,2),
    commission DECIMAL(10,2),
    total_cost DECIMAL(15,2),
    
    -- Risk approval
    risk_approved BOOLEAN DEFAULT false,
    risk_score DECIMAL(5,2),
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    submitted_at TIMESTAMP,
    executed_at TIMESTAMP,
    
    -- Error handling
    error_message TEXT,
    rejection_reason TEXT,
    retry_count INT DEFAULT 0,
    
    -- Constraints
    CONSTRAINT chk_quantity_positive CHECK (quantity > 0),
    CONSTRAINT chk_filled_quantity CHECK (filled_quantity >= 0 AND filled_quantity <= quantity)
);
```

### Execution Events Table
```sql
CREATE TABLE execution_events (
    id SERIAL PRIMARY KEY,
    order_id UUID NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    event_type VARCHAR(20) NOT NULL,  -- SUBMITTED, FILLED, FAILED, CANCELLED
    event_data JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);
```

### Indexes
```sql
CREATE INDEX idx_orders_user_id ON orders(user_id, created_at DESC);
CREATE INDEX idx_orders_status ON orders(status, created_at DESC);
CREATE INDEX idx_orders_event_id ON orders(event_id);
CREATE INDEX idx_orders_indira_id ON orders(indira_order_id) WHERE indira_order_id IS NOT NULL;
CREATE INDEX idx_orders_strategy_id ON orders(strategy_id);
CREATE INDEX idx_orders_stock_code ON orders(stock_code);
CREATE INDEX idx_orders_created_at ON orders(created_at DESC);
```

### Sample Queries

#### Get User's Recent Orders
```sql
SELECT order_id, symbol, order_side, quantity, price, status, created_at
FROM orders
WHERE user_id = 'user123'
ORDER BY created_at DESC
LIMIT 20;
```

#### Get Pending Orders
```sql
SELECT order_id, user_id, symbol, quantity, created_at
FROM orders
WHERE status IN ('RECEIVED', 'VALIDATED', 'PENDING')
ORDER BY created_at ASC;
```

#### Get Order Execution Timeline
```sql
SELECT o.order_id, o.symbol, o.status, 
       e.event_type, e.event_data, e.created_at
FROM orders o
LEFT JOIN execution_events e ON o.order_id = e.order_id
WHERE o.order_id = '550e8400-e29b-41d4-a716-446655440000'
ORDER BY e.created_at ASC;
```

---

## 8. Indira Securities API Integration

### Authentication Flow

**Step 1: Login**
```http
POST https://livemiddleware.indiratrade.com/v1/login
Content-Type: application/json

{
  "api_key": "user123_api_key",
  "password": "decrypted_password",
  "totp": "123456"
}

Response:
{
  "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "expires_in": 3600
}
```

**Step 2: Place Order**
```http
POST https://livemiddleware.indiratrade.com/v1/orders
Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
Content-Type: application/json

{
  "exchange": "NSE",
  "tradingsymbol": "RELIANCE",
  "transaction_type": "BUY",
  "order_type": "LIMIT",
  "quantity": 100,
  "price": 2500.50,
  "product": "MIS",
  "validity": "DAY"
}

Response:
{
  "order_id": "INDIRA123456",
  "status": "PENDING",
  "message": "Order placed successfully"
}
```

### Order Type Mapping
```
Internal → Indira Securities API
──────────────────────
MARKET → MARKET
LIMIT  → LIMIT
STOP_LOSS → SL
```

### Order Side Mapping
```
Internal → Indira Securities API
──────────────────────
BUY  → BUY
SELL → SELL
```

### Exchange Mapping
```
Internal → Indira Securities API
──────────────────────
NSE → NSE
BSE → BSE
```

---

## 9. Configuration

### Service Configuration
**File**: `config/config.yaml`

```yaml
server:
  grpc_port: 50054
  enable_reflection: true

database:
  host: localhost
  port: 5432
  user: postgres
  password: password
  dbname: trade_execution
  sslmode: disable
  max_open_conns: 25
  max_idle_conns: 5
  conn_max_lifetime: 5m

kafka:
  brokers:
    - localhost:9092
  topic: trade-signals
  group_id: trade-execution-service
  commit_interval: 1s

rabbitmq:
  url: amqp://guest:guest@localhost:5672/
  exchange: order.executions
  queue: trade.executions
  routing_key: trade.executions
  prefetch_count: 10
  worker_count: 5

executor:
  max_retries: 3
  retry_delay: 2s

indira:
  base_url: https://livemiddleware.indiratrade.com/v1
  websocket_url: wss://livemiddleware.indiratrade.com
  timeout: 30s

risk_management:
  grpc_address: localhost:9005

logging:
  level: info
  format: json
  output: trade_execution.log
```

### Environment Variables
```bash
# Service configuration
GRPC_PORT=50054

# Database
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=password
DB_NAME=trade_execution

# Kafka
KAFKA_BROKERS=localhost:9092
KAFKA_TOPIC=trade-signals
KAFKA_GROUP_ID=trade-execution-service

# RabbitMQ
RABBITMQ_URL=amqp://guest:guest@localhost:5672/
RABBITMQ_QUEUE=trade.executions

# Executor
MAX_RETRIES=3
RETRY_DELAY=2s

# Indira Securities API
INDIRA_BASE_URL=https://livemiddleware.indiratrade.com/v1
INDIRA_WEBSOCKET_URL=wss://livemiddleware.indiratrade.com
INDIRA_TIMEOUT=30s

# Risk Management
RISK_MGMT_ADDRESS=localhost:9005

# Logging
LOG_LEVEL=info
LOG_OUTPUT=trade_execution.log
```

---

## 10. Setup & Deployment

### Prerequisites
```bash
# Go 1.23+
go version

# PostgreSQL 13+
psql --version

# Kafka 3.0+
kafka-topics.sh --version

# RabbitMQ 3.9+
rabbitmqctl version

# Protocol Buffers
protoc --version
```

### Installation Steps

#### 1. Clone Repository
```bash
cd /home/stockkask/algo-trading/Algo-Treading/services/trade-execution
```

#### 2. Install Dependencies
```bash
go mod download
```

#### 3. Setup PostgreSQL
```bash
# Create database
createdb trade_execution

# Run migrations
psql -U postgres -d trade_execution -f migrations/001_create_orders_table.sql

# Verify tables
psql -U postgres -d trade_execution -c "\dt"
```

#### 4. Setup Kafka Topic
```bash
# Create trade-signals topic
kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic trade-signals \
  --partitions 3 \
  --replication-factor 1

# Verify topic
kafka-topics.sh --describe --bootstrap-server localhost:9092 --topic trade-signals
```

#### 5. Setup RabbitMQ
```bash
# Declare exchange
rabbitmqadmin declare exchange name=order.executions type=topic durable=true

# Declare queue
rabbitmqadmin declare queue name=trade.executions durable=true

# Bind queue
rabbitmqadmin declare binding \
  source=order.executions \
  destination=trade.executions \
  routing_key=trade.executions

# Verify
rabbitmqctl list_queues name messages
```

#### 6. Configure Service
```bash
cp config/config.example.yaml config/config.yaml
nano config/config.yaml
```

#### 7. Build Service
```bash
./build.sh

# Or manual build
go build -o bin/trade-execution cmd/main.go
```

#### 8. Run Service
```bash
./run.sh

# Expected output:
# 2025-01-15 10:00:00 INFO Starting Trade Execution Service
# 2025-01-15 10:00:00 INFO Connected to PostgreSQL
# 2025-01-15 10:00:00 INFO Kafka consumer started (trade-signals)
# 2025-01-15 10:00:00 INFO RabbitMQ consumer started (trade.executions)
# 2025-01-15 10:00:00 INFO gRPC server listening on port 50054
```

### Docker Deployment

**Dockerfile**:
```dockerfile
FROM golang:1.23-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o bin/trade-execution cmd/main.go

FROM alpine:latest
RUN apk --no-cache add ca-certificates postgresql-client

WORKDIR /root/
COPY --from=builder /app/bin/trade-execution .
COPY --from=builder /app/config ./config
COPY --from=builder /app/migrations ./migrations

EXPOSE 50054

CMD ["./trade-execution"]
```

**docker-compose.yml**:
```yaml
version: '3.8'

services:
  trade-execution:
    build: .
    ports:
      - "50054:50054"
    environment:
      - DB_HOST=postgres
      - KAFKA_BROKERS=kafka:9092
      - RABBITMQ_URL=amqp://guest:guest@rabbitmq:5672/
      - RISK_MGMT_ADDRESS=risk-management:9005
    depends_on:
      - postgres
      - kafka
      - rabbitmq
    networks:
      - algo-trading-network

networks:
  algo-trading-network:
    external: true
```

---

## 11. Testing

### Unit Tests
```bash
# Run all tests
go test ./... -v

# Test with coverage
go test ./... -cover -coverprofile=coverage.out
go tool cover -html=coverage.out
```

### Integration Tests

**Test Order Execution Flow**:
```bash
# 1. Publish test signal to Kafka
kafka-console-producer.sh --broker-list localhost:9092 --topic trade-signals << EOF
{"event_id":"123e4567-e89b-12d3-a456-426614174000","user_id":"testuser","strategy_id":"strat1","stock_code":3456,"symbol":"RELIANCE","exchange":"NSE","action":"BUY","quantity":100,"order_type":"LIMIT","price":2500.50,"timestamp":"2025-01-15T10:30:00Z"}
EOF

# 2. Check database for order creation
psql -U postgres -d trade_execution -c "SELECT order_id, symbol, status FROM orders WHERE user_id='testuser';"

# 3. Verify RabbitMQ message published
rabbitmqadmin get queue=trade.executions

# 4. Check order execution
psql -U postgres -d trade_execution -c "SELECT order_id, indira_order_id, status FROM orders WHERE user_id='testuser';"
```

### End-to-End Testing

**Scenario 1: Successful Order Placement**
```bash
# Expected flow:
# 1. Signal → Kafka
# 2. Risk check → APPROVED
# 3. Order created → DB (status: VALIDATED)
# 4. Published → RabbitMQ
# 5. Consumed → Executor
# 6. Indira Securities API → Success (status: SUBMITTED)
```

**Scenario 2: Risk Rejection**
```bash
# Expected flow:
# 1. Signal → Kafka
# 2. Risk check → REJECTED (daily limit exceeded)
# 3. Order NOT created
# 4. Log rejection reason
```

**Scenario 3: Retry Mechanism**
```bash
# Simulate Indira Securities API failure
# Expected flow:
# 1. First attempt → Fail (wait 2s)
# 2. Second attempt → Fail (wait 4s)
# 3. Third attempt → Success (status: SUBMITTED)
```

---

## 12. Monitoring & Troubleshooting

### Logging

**Log Levels**:
- **INFO**: Normal operations (signal consumed, order placed)
- **WARN**: Retries, transient failures
- **ERROR**: Critical failures (DB errors, Indira Securities API errors)

**Log Format**:
```json
{
  "timestamp": "2025-01-15T10:30:00Z",
  "level": "INFO",
  "service": "trade-execution",
  "message": "Order placed successfully",
  "order_id": "550e8400-e29b-41d4-a716-446655440000",
  "indira_order_id": "INDIRA123456",
  "user_id": "user123"
}
```

### Metrics to Monitor

#### Service Health
```bash
# gRPC server status
grpcurl -plaintext localhost:50054 grpc.health.v1.Health/Check

# Kafka consumer lag
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group trade-execution-service --describe

# RabbitMQ queue depth
rabbitmqctl list_queues name messages
```

#### Business Metrics
```sql
-- Total orders today
SELECT COUNT(*) FROM orders WHERE created_at >= CURRENT_DATE;

-- Orders by status
SELECT status, COUNT(*) FROM orders GROUP BY status;

-- Failed orders
SELECT COUNT(*) FROM orders WHERE status = 'FAILED' AND created_at >= CURRENT_DATE;

-- Average execution time
SELECT AVG(EXTRACT(EPOCH FROM (executed_at - created_at))) 
FROM orders 
WHERE executed_at IS NOT NULL;
```

### Common Issues

#### Issue 1: Kafka Consumer Lag
**Symptom**: Delays in signal processing

**Solution**:
```bash
# Check consumer lag
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group trade-execution-service --describe

# Increase consumer instances or partitions
```

#### Issue 2: RabbitMQ Queue Backlog
**Symptom**: Orders not being executed

**Solution**:
```bash
# Check queue depth
rabbitmqctl list_queues name messages

# Increase worker_count in config
# Increase prefetch_count for more concurrency
```

#### Issue 3: Indira Securities API Authentication Failure
**Symptom**: All orders failing with "Authentication failed"

**Solution**:
```bash
# Verify credentials in database
psql -U postgres -d trade_execution -c "SELECT user_id, api_key FROM user_credentials WHERE user_id='user123';"

# Check TOTP generation
# Ensure system time is synchronized (NTP)
```

#### Issue 4: Database Connection Pool Exhausted
**Symptom**: "too many connections" errors

**Solution**:
```yaml
# Increase connection pool in config.yaml
database:
  max_open_conns: 50
  max_idle_conns: 10
```

---

## 13. Best Practices

### Development Best Practices

1. **Idempotent Processing**
   - Use unique order_id (UUID) to prevent duplicates
   - Check existing orders before creating new ones

2. **Graceful Shutdown**
   - Wait for in-flight orders to complete
   - Close Kafka/RabbitMQ consumers gracefully

3. **Retry with Backoff**
   - Exponential backoff for transient failures
   - Max retries to prevent infinite loops

4. **Comprehensive Logging**
   - Log every state transition
   - Include context (order_id, user_id, stock_code)

5. **Error Handling**
   - Distinguish transient vs permanent errors
   - Transient → Retry
   - Permanent → Fail fast

### Operational Best Practices

1. **Monitor Consumer Lag**
   - Alert on lag > 1000 messages
   - Scale consumers horizontally

2. **Database Maintenance**
   - Vacuum/analyze tables regularly
   - Archive old orders (> 90 days)

3. **Credentials Security**
   - Encrypt passwords at rest
   - Rotate API keys regularly
   - Use secret management (Vault, AWS Secrets Manager)

4. **Dead Letter Queue Monitoring**
   - Investigate DLQ messages daily
   - Replay valid messages after fixing issues

5. **Rate Limiting**
   - Implement rate limits for Indira Securities API calls
   - Prevent account suspension

---

## Conclusion

The **Trade Execution Service** is the operational heart of the algorithmic trading system, responsible for translating validated trade signals into real broker orders. Through its dual-consumer architecture (Kafka + RabbitMQ), robust retry mechanism, and comprehensive execution tracking, it ensures reliable and transparent order processing.

### Quick Reference

**Service Details**:
- Protocol: gRPC + Kafka + RabbitMQ
- Port: 50054
- Database: PostgreSQL
- Language: Go 1.23+

**Core Functions**:
- Consume trade signals from Kafka
- Validate with Risk Management Service
- Execute orders via Indira Securities API
- Track order lifecycle in PostgreSQL

**Key Files**:
- `internal/executor/executor.go` - Order execution logic
- `internal/indira/client.go` - Indira Securities API client (ExecutionClient)
- `internal/consumer/kafka_consumer.go` - Kafka signal consumption
- `internal/consumer/rabbitmq_consumer.go` - RabbitMQ order consumption
- `migrations/001_create_orders_table.sql` - Database schema

**Critical Metrics**:
- Kafka consumer lag
- RabbitMQ queue depth
- Order execution success rate
- Average execution time

For detailed guides, refer to:
- [Risk Management KT](./RISK_MANAGEMENT_SERVICE_KT.md) - Risk validation integration
- [Indira Securities API Wrapper KT](./INDIRA_API_WRAPPER_SERVICE_KT.md) - Broker API details
- [Master KT Index](./README.md) - Complete documentation index

---

**Document Version**: 1.0  
**Last Updated**: January 2025  
**Maintainer**: Development Team
