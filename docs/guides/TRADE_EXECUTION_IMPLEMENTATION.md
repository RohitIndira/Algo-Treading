# Trade Execution Service - Implementation Guide

## 📋 Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Architecture & Design](#architecture--design)
4. [Implementation Steps](#implementation-steps)
5. [Database Setup](#database-setup)
6. [Integration Points](#integration-points)
7. [Testing Strategy](#testing-strategy)
8. [Deployment](#deployment)
9. [Monitoring & Observability](#monitoring--observability)

---

## 🎯 Overview

The **Trade Execution Service** is a critical microservice that executes trades via the Odin API broker. It consumes validated order requests from RabbitMQ, places orders through Odin API, tracks order status, and manages the complete order lifecycle.

### Key Responsibilities

- **Order Processing**: Consume order requests from RabbitMQ queue
- **Risk Validation**: Verify risk approval before execution
- **Odin Integration**: Place orders via Odin Trading API
- **Order Tracking**: Monitor and update order status in PostgreSQL
- **Retry Logic**: Handle failures with exponential backoff
- **Status Reporting**: Provide gRPC APIs for order queries

### Service Specifications

- **Port**: 9004 (gRPC)
- **Protocol**: gRPC for external queries, RabbitMQ consumer for order intake
- **Storage**: PostgreSQL for order persistence
- **Cache**: Redis for rate limiting and quick lookups
- **Message Queue**: RabbitMQ consumer

---

## 📦 Prerequisites

### Required Infrastructure

#### 1. PostgreSQL Database
```bash
# PostgreSQL 13+ required
# Create database: trading_execution
# Schema will be created via migrations
```

#### 2. RabbitMQ
```bash
# RabbitMQ 3.9+ required
# Queue: order.execution.queue
# Exchange: order.execution.exchange
# Dead Letter Queue: order.execution.dlq
```

#### 3. Redis
```bash
# Redis 6+ for caching and rate limiting
# Used for:
# - Order status cache
# - API rate limiting
# - Duplicate order detection
```

#### 4. Odin API Access
```bash
# Required credentials:
# - API Base URL
# - API Key
# - X-API-Key
# - User credentials for authentication
```

### Development Tools

```bash
# Go 1.21+
go version

# Protocol Buffer Compiler
protoc --version

# gRPC tools
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# Database migration tool (optional)
go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@latest
```

### Environment Variables

Create `.env` file or set environment variables:

```bash
# Service Configuration
SERVICE_NAME=trade-execution-service
SERVICE_PORT=9004
ENVIRONMENT=development

# PostgreSQL
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=trading_user
POSTGRES_PASSWORD=your_password
POSTGRES_DB=trading_execution
POSTGRES_SSL_MODE=disable
MAX_OPEN_CONNS=25
MAX_IDLE_CONNS=5

# RabbitMQ
RABBITMQ_URL=amqp://guest:guest@localhost:5672/
RABBITMQ_QUEUE=order.execution.queue
RABBITMQ_EXCHANGE=order.execution.exchange
RABBITMQ_PREFETCH=10
RABBITMQ_DLQ=order.execution.dlq

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=
REDIS_DB=0

# Odin API
ODIN_BASE_URL=https://api.odin.example.com
ODIN_API_KEY=your_api_key
ODIN_X_API_KEY=your_x_api_key
ODIN_TIMEOUT=30s
ODIN_MAX_RETRIES=3

# Logging
LOG_LEVEL=info
LOG_FORMAT=json

# Metrics
METRICS_PORT=9104
METRICS_PATH=/metrics
```

---

## 🏗️ Architecture & Design

### System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Trade Execution Service                     │
│                                                                 │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    │
│  │   RabbitMQ   │───▶│   Consumer   │───▶│   Executor   │    │
│  │   Consumer   │    │   Handler    │    │   Engine     │    │
│  └──────────────┘    └──────────────┘    └──────────────┘    │
│         │                    │                    │            │
│         │                    │                    ▼            │
│         │                    │            ┌──────────────┐    │
│         │                    │            │  Odin API    │    │
│         │                    │            │  Client      │    │
│         │                    │            └──────────────┘    │
│         │                    │                    │            │
│         │                    ▼                    │            │
│         │            ┌──────────────┐            │            │
│         │            │  Repository  │◀───────────┘            │
│         │            │    Layer     │                         │
│         │            └──────────────┘                         │
│         │                    │                                │
│         ▼                    ▼                                │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │    Redis     │    │  PostgreSQL  │    │ gRPC Server  │  │
│  │   (Cache)    │    │  (Orders)    │    │  (Queries)   │  │
│  └──────────────┘    └──────────────┘    └──────────────┘  │
│                                                   │          │
└───────────────────────────────────────────────────┼──────────┘
                                                    │
                                                    ▼
                                            ┌──────────────┐
                                            │ API Gateway  │
                                            │  & Clients   │
                                            └──────────────┘
```

### Component Flow

#### 1. Order Intake Flow
```
Rules Engine (generates order)
    → RabbitMQ (order.execution.queue)
    → Trade Execution Consumer
    → Validate request
    → Check duplicate order (Redis)
    → Save to PostgreSQL (RECEIVED status)
```

#### 2. Execution Flow
```
Order Executor picks order
    → Verify risk approval
    → Call Odin API (place_order)
    → Update status to SUBMITTED
    → Poll order status from Odin
    → Update filled_quantity, filled_price
    → Update status to FILLED/REJECTED
    → Update Redis cache
    → Notify Risk Management Service
```

#### 3. Query Flow
```
Client (API Gateway)
    → gRPC call to Trade Execution
    → Check Redis cache
    → Query PostgreSQL if not cached
    → Return order details
```

### Order Lifecycle States

```
RECEIVED     → Order received from RabbitMQ
    ↓
VALIDATED    → Request validation passed
    ↓
PENDING      → Awaiting execution
    ↓
SUBMITTED    → Sent to Odin API
    ↓
PARTIALLY_FILLED → Partial execution
    ↓
FILLED       → Fully executed
REJECTED     → Rejected by broker
CANCELLED    → Cancelled by user
FAILED       → System error
```

### Database Schema

```sql
-- PostgreSQL Schema
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
    order_type VARCHAR(10) NOT NULL, -- MARKET, LIMIT, STOP_LOSS
    order_side VARCHAR(10) NOT NULL, -- BUY, SELL
    quantity INT NOT NULL,
    price DECIMAL(15,2),
    
    -- Stop loss and take profit
    stop_loss DECIMAL(15,2),
    take_profit DECIMAL(15,2),
    
    -- Order validity
    validity VARCHAR(10) DEFAULT 'DAY', -- DAY, IOC, GTD
    
    -- Order status
    status VARCHAR(20) NOT NULL DEFAULT 'RECEIVED',
    
    -- Odin API integration
    odin_order_id VARCHAR(50),
    odin_response TEXT,
    
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
    
    -- Metadata
    metadata JSONB,
    
    -- Indexes
    CONSTRAINT chk_quantity_positive CHECK (quantity > 0),
    CONSTRAINT chk_price_positive CHECK (price IS NULL OR price > 0)
);

-- Indexes for performance
CREATE INDEX idx_orders_user_id ON orders(user_id, created_at DESC);
CREATE INDEX idx_orders_status ON orders(status, created_at DESC);
CREATE INDEX idx_orders_event_id ON orders(event_id);
CREATE INDEX idx_orders_odin_id ON orders(odin_order_id);
CREATE INDEX idx_orders_strategy_id ON orders(strategy_id);
CREATE INDEX idx_orders_stock_code ON orders(stock_code);

-- Execution history for analytics
CREATE TABLE execution_events (
    id SERIAL PRIMARY KEY,
    order_id UUID REFERENCES orders(order_id),
    event_type VARCHAR(20) NOT NULL, -- SUBMITTED, FILLED, REJECTED, etc.
    event_data JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_execution_events_order_id ON execution_events(order_id);
CREATE INDEX idx_execution_events_created_at ON execution_events(created_at DESC);
```

---

## 🔨 Implementation Steps

### Step 1: Project Structure Setup

```bash
# Navigate to service directory
cd services/trade-execution

# Create directory structure
mkdir -p cmd
mkdir -p config
mkdir -p internal/consumer
mkdir -p internal/executor
mkdir -p internal/models
mkdir -p internal/odin
mkdir -p internal/repository
mkdir -p internal/server
mkdir -p migrations
```

### Step 2: Configuration Management

Create `config/config.yaml`:

```yaml
service:
  name: trade-execution-service
  port: 9004
  environment: development

postgres:
  host: localhost
  port: 5432
  user: trading_user
  password: ${POSTGRES_PASSWORD}
  database: trading_execution
  ssl_mode: disable
  max_open_conns: 25
  max_idle_conns: 5
  conn_max_lifetime: 5m

rabbitmq:
  url: ${RABBITMQ_URL}
  queue: order.execution.queue
  exchange: order.execution.exchange
  routing_key: order.execution
  prefetch_count: 10
  consumer_tag: trade-executor-1
  auto_ack: false
  dlq: order.execution.dlq

redis:
  host: localhost
  port: 6379
  password: ${REDIS_PASSWORD}
  db: 0
  pool_size: 10
  min_idle_conns: 5

odin:
  base_url: ${ODIN_BASE_URL}
  api_key: ${ODIN_API_KEY}
  x_api_key: ${ODIN_X_API_KEY}
  timeout: 30s
  max_retries: 3
  retry_delay: 1s
  circuit_breaker:
    max_requests: 100
    interval: 10s
    timeout: 60s

logging:
  level: info
  format: json
  output: stdout

metrics:
  enabled: true
  port: 9104
  path: /metrics

execution:
  max_workers: 10
  batch_size: 100
  status_poll_interval: 5s
  max_retry_count: 3
```

### Step 3: Data Models

Create `internal/models/order.go`:

```go
package models

import (
    "time"
    "github.com/google/uuid"
)

// OrderStatus represents order lifecycle status
type OrderStatus string

const (
    StatusReceived        OrderStatus = "RECEIVED"
    StatusValidated       OrderStatus = "VALIDATED"
    StatusPending         OrderStatus = "PENDING"
    StatusSubmitted       OrderStatus = "SUBMITTED"
    StatusPartiallyFilled OrderStatus = "PARTIALLY_FILLED"
    StatusFilled          OrderStatus = "FILLED"
    StatusRejected        OrderStatus = "REJECTED"
    StatusCancelled       OrderStatus = "CANCELLED"
    StatusFailed          OrderStatus = "FAILED"
)

// OrderType represents order type
type OrderType string

const (
    OrderTypeMarket    OrderType = "MARKET"
    OrderTypeLimit     OrderType = "LIMIT"
    OrderTypeStopLoss  OrderType = "STOP_LOSS"
)

// OrderSide represents buy or sell
type OrderSide string

const (
    OrderSideBuy  OrderSide = "BUY"
    OrderSideSell OrderSide = "SELL"
)

// Exchange represents stock exchange
type Exchange string

const (
    ExchangeNSE Exchange = "NSE"
    ExchangeBSE Exchange = "BSE"
)

// Order represents a trading order
type Order struct {
    OrderID      uuid.UUID  `json:"order_id" db:"order_id"`
    UserID       string     `json:"user_id" db:"user_id"`
    StrategyID   string     `json:"strategy_id" db:"strategy_id"`
    EventID      uuid.UUID  `json:"event_id" db:"event_id"`
    
    // Stock information
    StockCode    int64      `json:"stock_code" db:"stock_code"`
    Exchange     Exchange   `json:"exchange" db:"exchange"`
    Symbol       string     `json:"symbol" db:"symbol"`
    
    // Order details
    OrderType    OrderType  `json:"order_type" db:"order_type"`
    OrderSide    OrderSide  `json:"order_side" db:"order_side"`
    Quantity     int32      `json:"quantity" db:"quantity"`
    Price        *float64   `json:"price,omitempty" db:"price"`
    
    // Stop loss and take profit
    StopLoss     *float64   `json:"stop_loss,omitempty" db:"stop_loss"`
    TakeProfit   *float64   `json:"take_profit,omitempty" db:"take_profit"`
    
    // Order validity
    Validity     string     `json:"validity" db:"validity"`
    
    // Order status
    Status       OrderStatus `json:"status" db:"status"`
    
    // Odin API integration
    OdinOrderID  *string    `json:"odin_order_id,omitempty" db:"odin_order_id"`
    OdinResponse *string    `json:"odin_response,omitempty" db:"odin_response"`
    
    // Execution details
    FilledQuantity int32    `json:"filled_quantity" db:"filled_quantity"`
    FilledPrice    *float64 `json:"filled_price,omitempty" db:"filled_price"`
    Commission     *float64 `json:"commission,omitempty" db:"commission"`
    TotalCost      *float64 `json:"total_cost,omitempty" db:"total_cost"`
    
    // Risk approval
    RiskApproved bool       `json:"risk_approved" db:"risk_approved"`
    RiskScore    *float64   `json:"risk_score,omitempty" db:"risk_score"`
    
    // Timestamps
    CreatedAt    time.Time  `json:"created_at" db:"created_at"`
    UpdatedAt    time.Time  `json:"updated_at" db:"updated_at"`
    SubmittedAt  *time.Time `json:"submitted_at,omitempty" db:"submitted_at"`
    ExecutedAt   *time.Time `json:"executed_at,omitempty" db:"executed_at"`
    
    // Error handling
    ErrorMessage    *string `json:"error_message,omitempty" db:"error_message"`
    RejectionReason *string `json:"rejection_reason,omitempty" db:"rejection_reason"`
    RetryCount      int32   `json:"retry_count" db:"retry_count"`
    
    // Metadata
    Metadata     map[string]interface{} `json:"metadata,omitempty" db:"metadata"`
}

// OrderRequest from RabbitMQ
type OrderRequest struct {
    RequestID   string     `json:"request_id"`
    UserID      string     `json:"user_id"`
    StrategyID  string     `json:"strategy_id"`
    EventID     string     `json:"event_id"`
    
    StockCode   int64      `json:"stock_code"`
    Exchange    string     `json:"exchange"`
    Symbol      string     `json:"symbol"`
    
    OrderType   string     `json:"order_type"`
    OrderSide   string     `json:"order_side"`
    Quantity    int32      `json:"quantity"`
    Price       *float64   `json:"price,omitempty"`
    
    StopLoss    *float64   `json:"stop_loss,omitempty"`
    TakeProfit  *float64   `json:"take_profit,omitempty"`
    
    Validity    string     `json:"validity"`
    
    RiskApproved bool      `json:"risk_approved"`
    RiskScore    *float64  `json:"risk_score,omitempty"`
    
    Timestamp   time.Time  `json:"timestamp"`
    RetryCount  int32      `json:"retry_count"`
}
```

### Step 4: Repository Layer

Create `internal/repository/order_repository.go`:

```go
package repository

import (
    "context"
    "database/sql"
    "fmt"
    "time"
    
    "github.com/google/uuid"
    "github.com/jmoiron/sqlx"
    "trade-execution/internal/models"
)

type OrderRepository interface {
    Create(ctx context.Context, order *models.Order) error
    Update(ctx context.Context, order *models.Order) error
    GetByID(ctx context.Context, orderID uuid.UUID) (*models.Order, error)
    GetByOdinOrderID(ctx context.Context, odinOrderID string) (*models.Order, error)
    GetUserOrders(ctx context.Context, userID string, limit, offset int) ([]*models.Order, error)
    GetOrdersByStatus(ctx context.Context, status models.OrderStatus, limit int) ([]*models.Order, error)
    UpdateStatus(ctx context.Context, orderID uuid.UUID, status models.OrderStatus) error
    RecordExecutionEvent(ctx context.Context, orderID uuid.UUID, eventType string, eventData map[string]interface{}) error
}

type orderRepository struct {
    db *sqlx.DB
}

func NewOrderRepository(db *sqlx.DB) OrderRepository {
    return &orderRepository{db: db}
}

func (r *orderRepository) Create(ctx context.Context, order *models.Order) error {
    query := `
        INSERT INTO orders (
            order_id, user_id, strategy_id, event_id,
            stock_code, exchange, symbol,
            order_type, order_side, quantity, price,
            stop_loss, take_profit, validity,
            status, risk_approved, risk_score,
            retry_count, created_at, updated_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
            $12, $13, $14, $15, $16, $17, $18, $19, $20
        )
    `
    
    _, err := r.db.ExecContext(ctx, query,
        order.OrderID, order.UserID, order.StrategyID, order.EventID,
        order.StockCode, order.Exchange, order.Symbol,
        order.OrderType, order.OrderSide, order.Quantity, order.Price,
        order.StopLoss, order.TakeProfit, order.Validity,
        order.Status, order.RiskApproved, order.RiskScore,
        order.RetryCount, order.CreatedAt, order.UpdatedAt,
    )
    
    return err
}

func (r *orderRepository) Update(ctx context.Context, order *models.Order) error {
    query := `
        UPDATE orders SET
            status = $1, odin_order_id = $2, odin_response = $3,
            filled_quantity = $4, filled_price = $5, commission = $6,
            total_cost = $7, submitted_at = $8, executed_at = $9,
            error_message = $10, rejection_reason = $11, retry_count = $12,
            updated_at = $13
        WHERE order_id = $14
    `
    
    _, err := r.db.ExecContext(ctx, query,
        order.Status, order.OdinOrderID, order.OdinResponse,
        order.FilledQuantity, order.FilledPrice, order.Commission,
        order.TotalCost, order.SubmittedAt, order.ExecutedAt,
        order.ErrorMessage, order.RejectionReason, order.RetryCount,
        time.Now(), order.OrderID,
    )
    
    return err
}

func (r *orderRepository) GetByID(ctx context.Context, orderID uuid.UUID) (*models.Order, error) {
    var order models.Order
    query := `SELECT * FROM orders WHERE order_id = $1`
    
    err := r.db.GetContext(ctx, &order, query, orderID)
    if err == sql.ErrNoRows {
        return nil, fmt.Errorf("order not found: %s", orderID)
    }
    
    return &order, err
}

func (r *orderRepository) GetUserOrders(ctx context.Context, userID string, limit, offset int) ([]*models.Order, error) {
    var orders []*models.Order
    query := `
        SELECT * FROM orders 
        WHERE user_id = $1 
        ORDER BY created_at DESC 
        LIMIT $2 OFFSET $3
    `
    
    err := r.db.SelectContext(ctx, &orders, query, userID, limit, offset)
    return orders, err
}

func (r *orderRepository) GetOrdersByStatus(ctx context.Context, status models.OrderStatus, limit int) ([]*models.Order, error) {
    var orders []*models.Order
    query := `
        SELECT * FROM orders 
        WHERE status = $1 
        ORDER BY created_at ASC 
        LIMIT $2
    `
    
    err := r.db.SelectContext(ctx, &orders, query, status, limit)
    return orders, err
}

func (r *orderRepository) UpdateStatus(ctx context.Context, orderID uuid.UUID, status models.OrderStatus) error {
    query := `UPDATE orders SET status = $1, updated_at = $2 WHERE order_id = $3`
    _, err := r.db.ExecContext(ctx, query, status, time.Now(), orderID)
    return err
}

func (r *orderRepository) RecordExecutionEvent(ctx context.Context, orderID uuid.UUID, eventType string, eventData map[string]interface{}) error {
    query := `INSERT INTO execution_events (order_id, event_type, event_data) VALUES ($1, $2, $3)`
    _, err := r.db.ExecContext(ctx, query, orderID, eventType, eventData)
    return err
}
```

### Step 5: Odin API Client Integration

Create `internal/odin/client.go` (extends existing pkg/odin/client.go):

```go
package odin

import (
    "context"
    "fmt"
    "trade-execution/internal/models"
    odinpkg "github.com/RohitIndira/Algo-Treading/pkg/odin"
)

// ExecutionClient wraps Odin API for trade execution
type ExecutionClient struct {
    client *odinpkg.Client
}

func NewExecutionClient(baseURL string) *ExecutionClient {
    return &ExecutionClient{
        client: odinpkg.NewClient(baseURL),
    }
}

// PlaceOrder places order via Odin API
func (c *ExecutionClient) PlaceOrder(ctx context.Context, order *models.Order) (*odinpkg.StandardResponse, error) {
    // Convert internal order model to Odin API request
    orderReq := c.convertToOdinRequest(order)
    
    // Call Odin API
    resp, err := c.client.PlaceOrder(ctx, "user_id", orderReq)
    if err != nil {
        return nil, fmt.Errorf("failed to place order: %w", err)
    }
    
    return resp, nil
}

// GetOrderStatus queries order status from Odin
func (c *ExecutionClient) GetOrderStatus(ctx context.Context, exchange, orderID string) (*odinpkg.StandardResponse, error) {
    return c.client.GetOrderStatus(ctx, "user_id", exchange, orderID)
}

// CancelOrder cancels an order
func (c *ExecutionClient) CancelOrder(ctx context.Context, exchange, orderID string) (*odinpkg.StandardResponse, error) {
    cancelReq := odinpkg.CancelOrderRequest{
        Exchange: exchange,
        OrderID:  orderID,
    }
    
    return c.client.CancelOrder(ctx, "user_id", cancelReq)
}

func (c *ExecutionClient) convertToOdinRequest(order *models.Order) odinpkg.OrderRequest {
    req := odinpkg.OrderRequest{
        ScripInfo: odinpkg.ScripInfo{
            Exchange:   string(order.Exchange),
            ScripToken: int(order.StockCode),
            Symbol:     order.Symbol,
        },
        TransactionType: string(order.OrderSide),
        OrderType:       c.mapOrderType(order.OrderType),
        Quantity:        int(order.Quantity),
        Validity:        order.Validity,
    }
    
    if order.Price != nil {
        req.Price = *order.Price
    }
    
    return req
}

func (c *ExecutionClient) mapOrderType(orderType models.OrderType) string {
    switch orderType {
    case models.OrderTypeMarket:
        return "MKT"
    case models.OrderTypeLimit:
        return "RL"
    case models.OrderTypeStopLoss:
        return "SL"
    default:
        return "MKT"
    }
}
```

### Step 6: Order Executor

Create `internal/executor/executor.go`:

```go
package executor

import (
    "context"
    "fmt"
    "time"
    
    "github.com/google/uuid"
    "trade-execution/internal/models"
    "trade-execution/internal/odin"
    "trade-execution/internal/repository"
)

type OrderExecutor struct {
    repo        repository.OrderRepository
    odinClient  *odin.ExecutionClient
    maxRetries  int
    retryDelay  time.Duration
}

func NewOrderExecutor(repo repository.OrderRepository, odinClient *odin.ExecutionClient, maxRetries int, retryDelay time.Duration) *OrderExecutor {
    return &OrderExecutor{
        repo:       repo,
        odinClient: odinClient,
        maxRetries: maxRetries,
        retryDelay: retryDelay,
    }
}

// ExecuteOrder processes and executes an order
func (e *OrderExecutor) ExecuteOrder(ctx context.Context, order *models.Order) error {
    // Verify risk approval
    if !order.RiskApproved {
        return e.rejectOrder(ctx, order, "Risk not approved")
    }
    
    // Update status to PENDING
    order.Status = models.StatusPending
    if err := e.repo.Update(ctx, order); err != nil {
        return fmt.Errorf("failed to update order status: %w", err)
    }
    
    // Execute order with retries
    var lastErr error
    for attempt := 0; attempt <= e.maxRetries; attempt++ {
        if attempt > 0 {
            time.Sleep(e.retryDelay * time.Duration(attempt))
        }
        
        // Place order via Odin
        resp, err := e.odinClient.PlaceOrder(ctx, order)
        if err != nil {
            lastErr = err
            order.RetryCount++
            continue
        }
        
        // Order placed successfully
        if resp.Success {
            return e.handleSuccessfulPlacement(ctx, order, resp)
        }
        
        // Order rejected
        return e.rejectOrder(ctx, order, resp.Message)
    }
    
    // All retries exhausted
    return e.failOrder(ctx, order, fmt.Sprintf("Max retries exceeded: %v", lastErr))
}

func (e *OrderExecutor) handleSuccessfulPlacement(ctx context.Context, order *models.Order, resp interface{}) error {
    now := time.Now()
    order.Status = models.StatusSubmitted
    order.SubmittedAt = &now
    
    // Extract Odin order ID from response
    // order.OdinOrderID = extractOrderID(resp)
    
    if err := e.repo.Update(ctx, order); err != nil {
        return err
    }
    
    // Record execution event
    e.repo.RecordExecutionEvent(ctx, order.OrderID, "SUBMITTED", map[string]interface{}{
        "odin_order_id": order.OdinOrderID,
        "timestamp":     now,
    })
    
    return nil
}

func (e *OrderExecutor) rejectOrder(ctx context.Context, order *models.Order, reason string) error {
    order.Status = models.StatusRejected
    order.RejectionReason = &reason
    
    return e.repo.Update(ctx, order)
}

func (e *OrderExecutor) failOrder(ctx context.Context, order *models.Order, errorMsg string) error {
    order.Status = models.StatusFailed
    order.ErrorMessage = &errorMsg
    
    return e.repo.Update(ctx, order)
}

// PollOrderStatus polls Odin for order status updates
func (e *OrderExecutor) PollOrderStatus(ctx context.Context, order *models.Order) error {
    if order.OdinOrderID == nil {
        return fmt.Errorf("no Odin order ID")
    }
    
    resp, err := e.odinClient.GetOrderStatus(ctx, string(order.Exchange), *order.OdinOrderID)
    if err != nil {
        return err
    }
    
    // Update order based on status
    // Parse response and update filled_quantity, filled_price, etc.
    
    return e.repo.Update(ctx, order)
}
```

---

## 📚 Continued in Part 2...

Due to length, the implementation guide continues with:
- RabbitMQ Consumer Implementation
- gRPC Server Implementation
- Main Service Entry Point
- Testing Strategy
- Deployment Steps
- Monitoring & Observability

Would you like me to continue with Part 2 of the implementation guide?
