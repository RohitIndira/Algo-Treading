# User Config Service - Knowledge Transfer Document

## 📋 Table of Contents

1. [Overview](#overview)
2. [Architecture & Design](#architecture--design)
3. [Project Structure](#project-structure)
4. [Core Components](#core-components)
5. [Database Schema](#database-schema)
6. [gRPC API Reference](#grpc-api-reference)
7. [Kafka Integration](#kafka-integration)
8. [Configuration](#configuration)
9. [Setup & Deployment](#setup--deployment)
10. [Testing](#testing)
11. [Troubleshooting](#troubleshooting)
12. [Best Practices](#best-practices)

---

## Overview

### Purpose
The User Config Service is a **gRPC-based microservice** responsible for managing user trading strategies and configurations in the algorithmic trading system. It acts as the central repository for all strategy definitions and publishes configuration changes to downstream services.

### Key Responsibilities
- **Strategy CRUD Operations**: Create, Read, Update, Delete trading strategies
- **Configuration Storage**: Persist strategy configurations in PostgreSQL with ACID guarantees
- **Event Publishing**: Publish strategy changes to Kafka for real-time synchronization
- **Data Validation**: Validate strategy configurations before persisting
- **Version Control**: Track strategy modifications with timestamps
- **Activation Management**: Enable/disable strategies dynamically

### Technology Stack
- **Language**: Go 1.23+
- **RPC Framework**: gRPC with Protocol Buffers
- **Database**: PostgreSQL 13+
- **Message Queue**: Apache Kafka
- **Configuration**: Environment variables

---

## Architecture & Design

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────┐
│                  User Config Service                     │
│                     (Port 50051)                         │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────┐ │
│  │   gRPC       │───▶│   Service    │───▶│  Kafka   │ │
│  │   Server     │    │   Layer      │    │ Producer │ │
│  └──────────────┘    └──────┬───────┘    └──────────┘ │
│         ▲                    │                   │      │
│         │                    ▼                   │      │
│    Client Requests   ┌──────────────┐           │      │
│                      │  Repository  │           │      │
│                      │    Layer     │           │      │
│                      └──────┬───────┘           │      │
│                             │                   │      │
│                             ▼                   │      │
│                      ┌──────────────┐           │      │
│                      │  PostgreSQL  │           │      │
│                      │   Database   │           │      │
│                      └──────────────┘           │      │
│                                                  │      │
└──────────────────────────────────────────────────┼──────┘
                                                   │
                                                   ▼
                                        ┌──────────────────┐
                                        │  Kafka Topic:    │
                                        │ strategy.updates │
                                        └──────────────────┘
                                                   │
                                                   ▼
                                        ┌──────────────────┐
                                        │  Rules Engine    │
                                        │  (Subscriber)    │
                                        └──────────────────┘
```

### Request Flow

#### Create Strategy Flow
```
1. gRPC Client → CreateStrategy RPC
2. Service Layer → Validate request
3. Service Layer → Create Strategy ID (UUID)
4. Repository Layer → Insert into PostgreSQL
5. Kafka Producer → Publish CREATE event
6. Response → Return strategy details
```

#### Update Strategy Flow
```
1. gRPC Client → UpdateStrategy RPC
2. Service Layer → Validate changes
3. Repository Layer → Begin transaction
4. Repository Layer → Check if exists
5. Repository Layer → Update strategy
6. Repository Layer → Commit transaction
7. Kafka Producer → Publish UPDATE event
8. Response → Return updated strategy
```

### Layered Architecture

```
┌─────────────────────────────────────┐
│        gRPC Transport Layer         │  (Protocol handling, serialization)
├─────────────────────────────────────┤
│          Service Layer              │  (Business logic, validation)
├─────────────────────────────────────┤
│        Repository Layer             │  (Data access abstraction)
├─────────────────────────────────────┤
│         Database Layer              │  (PostgreSQL persistence)
└─────────────────────────────────────┘
```

---

## Project Structure

```
services/user-config/
├── cmd/
│   └── main.go                      # Application entry point
├── config/
│   └── config.go                    # Configuration loader
├── internal/
│   ├── models/
│   │   ├── strategy.go              # Strategy domain model
│   │   └── news_config.go           # News configuration model
│   ├── repository/
│   │   ├── postgres_repository.go   # PostgreSQL implementation
│   │   └── repository.go            # Repository interface
│   ├── server/
│   │   └── grpc_server.go           # gRPC server implementation
│   └── service/
│       ├── strategy_service.go      # Business logic layer
│       └── kafka_publisher.go       # Kafka event publisher
├── migrations/
│   └── 001_create_strategies.sql    # Database schema
├── .env                             # Environment configuration
├── .env.example                     # Example configuration
├── go.mod                           # Go module dependencies
├── go.sum                           # Dependency checksums
├── build.sh                         # Build script
├── run.sh                           # Run script
└── README.md                        # Service documentation
```

---

## Core Components

### 1. Main Application (`cmd/main.go`)

**Purpose:** Application bootstrap and dependency injection.

**Key Responsibilities:**
- Load configuration from environment
- Initialize database connection
- Initialize Kafka producer
- Create repository instance
- Create service instance
- Start gRPC server
- Handle graceful shutdown

**Code Structure:**
```go
func main() {
    // 1. Load configuration
    cfg := config.LoadConfig()
    
    // 2. Initialize database
    db := initDatabase(cfg.DatabaseURL)
    defer db.Close()
    
    // 3. Initialize Kafka producer
    producer := initKafka(cfg.KafkaBootstrapServers)
    defer producer.Close()
    
    // 4. Create layers
    repo := repository.NewPostgresRepository(db)
    svc := service.NewStrategyService(repo, producer)
    
    // 5. Start gRPC server
    server := server.NewGRPCServer(svc)
    server.Start(cfg.GRPCPort)
    
    // 6. Wait for shutdown signal
    <-shutdown
    server.GracefulStop()
}
```

### 2. Configuration (`config/config.go`)

**Purpose:** Centralized configuration management.

**Configuration Structure:**
```go
type Config struct {
    // Service
    ServiceName    string
    GRPCPort       int
    
    // Database
    DatabaseURL    string
    MaxConnections int
    
    // Kafka
    KafkaBootstrapServers string
    KafkaTopicStrategy    string
    
    // Logging
    LogLevel       string
}
```

**Environment Variables:**
```bash
# Service Configuration
SERVICE_NAME=user-config-service
GRPC_PORT=50051

# Database
DATABASE_URL=postgresql://user:pass@localhost:5432/trading_system
DB_MAX_CONNECTIONS=25

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC_STRATEGY=strategy.updates

# Logging
LOG_LEVEL=INFO
```

### 3. Models (`internal/models/`)

#### Strategy Model (`strategy.go`)

**Purpose:** Domain model representing a trading strategy.

```go
type Strategy struct {
    StrategyID    string           `json:"strategy_id" db:"strategy_id"`
    UserID        string           `json:"user_id" db:"user_id"`
    StrategyName  string           `json:"strategy_name" db:"strategy_name"`
    Description   string           `json:"description" db:"description"`
    IsActive      bool             `json:"is_active" db:"is_active"`
    
    // News Filtering
    NewsConfig    NewsConfig       `json:"news_config" db:"news_config"`
    
    // Trade Configuration
    TradeConfig   TradeConfig      `json:"trade_config" db:"trade_config"`
    
    // Metadata
    CreatedAt     time.Time        `json:"created_at" db:"created_at"`
    UpdatedAt     time.Time        `json:"updated_at" db:"updated_at"`
    Version       int              `json:"version" db:"version"`
}

type NewsConfig struct {
    Keywords         []string  `json:"keywords"`
    Sentiment        string    `json:"sentiment"`         // POSITIVE, NEGATIVE, NEUTRAL
    MinImpactScore   float64   `json:"min_impact_score"`  // 1-10
    Categories       []string  `json:"categories"`        // earnings, merger, etc.
    StockCodes       []int     `json:"stock_codes"`       // Specific stocks to monitor
}

type TradeConfig struct {
    Exchange         string    `json:"exchange"`          // NSE, BSE
    Segment          string    `json:"segment"`           // EQ, FO
    ActionType       string    `json:"action_type"`       // BUY, SELL
    OrderType        string    `json:"order_type"`        // MARKET, LIMIT
    ProductType      string    `json:"product_type"`      // INTRADAY, DELIVERY
    Quantity         int       `json:"quantity"`
    PriceRange       PriceRange `json:"price_range"`
    StopLoss         float64   `json:"stop_loss"`
    TakeProfit       float64   `json:"take_profit"`
}

type PriceRange struct {
    Min float64 `json:"min"`
    Max float64 `json:"max"`
}
```

### 4. Repository Layer (`internal/repository/`)

**Purpose:** Abstract data access logic from business logic.

**Interface:**
```go
type StrategyRepository interface {
    // CRUD Operations
    Create(ctx context.Context, strategy *models.Strategy) error
    GetByID(ctx context.Context, strategyID, userID string) (*models.Strategy, error)
    Update(ctx context.Context, strategy *models.Strategy) error
    Delete(ctx context.Context, strategyID, userID string) error
    
    // Query Operations
    ListByUserID(ctx context.Context, userID string, page, pageSize int) ([]*models.Strategy, int, error)
    GetByIDs(ctx context.Context, strategyIDs []string) ([]*models.Strategy, error)
    ListActiveStrategies(ctx context.Context) ([]*models.Strategy, error)
    
    // State Management
    Activate(ctx context.Context, strategyID, userID string) error
    Deactivate(ctx context.Context, strategyID, userID string) error
}
```

**PostgreSQL Implementation Highlights:**
```go
func (r *PostgresRepository) Create(ctx context.Context, strategy *models.Strategy) error {
    query := `
        INSERT INTO strategies (
            strategy_id, user_id, strategy_name, description,
            news_config, trade_config, is_active,
            created_at, updated_at, version
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
    `
    
    newsConfigJSON, _ := json.Marshal(strategy.NewsConfig)
    tradeConfigJSON, _ := json.Marshal(strategy.TradeConfig)
    
    _, err := r.db.ExecContext(ctx, query,
        strategy.StrategyID,
        strategy.UserID,
        strategy.StrategyName,
        strategy.Description,
        newsConfigJSON,
        tradeConfigJSON,
        strategy.IsActive,
        strategy.CreatedAt,
        strategy.UpdatedAt,
        strategy.Version,
    )
    
    return err
}
```

**Transaction Support:**
```go
func (r *PostgresRepository) Update(ctx context.Context, strategy *models.Strategy) error {
    tx, err := r.db.BeginTx(ctx, nil)
    if err != nil {
        return err
    }
    defer tx.Rollback()
    
    // Check version for optimistic locking
    var currentVersion int
    err = tx.QueryRowContext(ctx, 
        "SELECT version FROM strategies WHERE strategy_id = $1 FOR UPDATE",
        strategy.StrategyID,
    ).Scan(&currentVersion)
    
    if currentVersion != strategy.Version {
        return ErrVersionMismatch
    }
    
    // Increment version
    strategy.Version++
    strategy.UpdatedAt = time.Now()
    
    // Update strategy
    _, err = tx.ExecContext(ctx, updateQuery, ...)
    if err != nil {
        return err
    }
    
    return tx.Commit()
}
```

### 5. Service Layer (`internal/service/`)

**Purpose:** Business logic and orchestration.

**Key Methods:**
```go
type StrategyService struct {
    repo     repository.StrategyRepository
    producer *kafka.Producer
}

func (s *StrategyService) CreateStrategy(ctx context.Context, req *pb.CreateStrategyRequest) (*pb.CreateStrategyResponse, error) {
    // 1. Validate request
    if err := s.validateCreateRequest(req); err != nil {
        return errorResponse(err), nil
    }
    
    // 2. Create strategy model
    strategy := &models.Strategy{
        StrategyID:   uuid.New().String(),
        UserID:       req.UserId,
        StrategyName: req.StrategyName,
        Description:  req.Description,
        NewsConfig:   transformNewsConfig(req.NewsConfig),
        TradeConfig:  transformTradeConfig(req.TradeConfig),
        IsActive:     false,
        CreatedAt:    time.Now(),
        UpdatedAt:    time.Now(),
        Version:      1,
    }
    
    // 3. Persist to database
    if err := s.repo.Create(ctx, strategy); err != nil {
        return errorResponse(err), nil
    }
    
    // 4. Publish to Kafka
    s.publishStrategyEvent("CREATE", strategy)
    
    // 5. Return response
    return successResponse(strategy), nil
}
```

**Validation Logic:**
```go
func (s *StrategyService) validateCreateRequest(req *pb.CreateStrategyRequest) error {
    if req.UserId == "" {
        return errors.New("user_id is required")
    }
    
    if req.StrategyName == "" {
        return errors.New("strategy_name is required")
    }
    
    if req.NewsConfig == nil {
        return errors.New("news_config is required")
    }
    
    if req.TradeConfig == nil {
        return errors.New("trade_config is required")
    }
    
    // Validate news config
    if len(req.NewsConfig.Keywords) == 0 {
        return errors.New("at least one keyword is required")
    }
    
    if req.NewsConfig.MinImpactScore < 1 || req.NewsConfig.MinImpactScore > 10 {
        return errors.New("impact_score must be between 1 and 10")
    }
    
    // Validate trade config
    validActions := map[string]bool{"BUY": true, "SELL": true}
    if !validActions[req.TradeConfig.ActionType] {
        return errors.New("invalid action_type")
    }
    
    if req.TradeConfig.Quantity <= 0 {
        return errors.New("quantity must be positive")
    }
    
    return nil
}
```

### 6. Kafka Publisher (`internal/service/kafka_publisher.go`)

**Purpose:** Publish strategy events to Kafka.

**Event Format:**
```go
type StrategyEvent struct {
    EventType    string           `json:"event_type"`    // CREATE, UPDATE, DELETE, ACTIVATE, DEACTIVATE
    EventTime    time.Time        `json:"event_time"`
    Strategy     *models.Strategy `json:"strategy"`
}
```

**Publishing Logic:**
```go
func (s *StrategyService) publishStrategyEvent(eventType string, strategy *models.Strategy) {
    event := StrategyEvent{
        EventType: eventType,
        EventTime: time.Now(),
        Strategy:  strategy,
    }
    
    eventJSON, err := json.Marshal(event)
    if err != nil {
        log.Printf("Failed to marshal event: %v", err)
        return
    }
    
    message := &kafka.Message{
        TopicPartition: kafka.TopicPartition{
            Topic:     &s.kafkaTopic,
            Partition: kafka.PartitionAny,
        },
        Key:   []byte(strategy.StrategyID),
        Value: eventJSON,
        Headers: []kafka.Header{
            {Key: "event_type", Value: []byte(eventType)},
            {Key: "user_id", Value: []byte(strategy.UserID)},
        },
    }
    
    s.producer.Produce(message, nil)
}
```

### 7. gRPC Server (`internal/server/grpc_server.go`)

**Purpose:** Handle gRPC requests.

**Server Implementation:**
```go
type GRPCServer struct {
    pb.UnimplementedUserConfigServiceServer
    service *service.StrategyService
}

func (s *GRPCServer) CreateStrategy(ctx context.Context, req *pb.CreateStrategyRequest) (*pb.CreateStrategyResponse, error) {
    return s.service.CreateStrategy(ctx, req)
}

func (s *GRPCServer) GetStrategy(ctx context.Context, req *pb.GetStrategyRequest) (*pb.GetStrategyResponse, error) {
    return s.service.GetStrategy(ctx, req)
}

// ... other RPC methods
```

**Server Startup:**
```go
func StartGRPCServer(port int, svc *service.StrategyService) error {
    listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
    if err != nil {
        return err
    }
    
    grpcServer := grpc.NewServer(
        grpc.UnaryInterceptor(loggingInterceptor),
    )
    
    pb.RegisterUserConfigServiceServer(grpcServer, &GRPCServer{
        service: svc,
    })
    
    log.Printf("gRPC server listening on port %d", port)
    return grpcServer.Serve(listener)
}
```

---

## Database Schema

### Strategies Table

```sql
CREATE TABLE strategies (
    strategy_id      VARCHAR(36) PRIMARY KEY,
    user_id          VARCHAR(50) NOT NULL,
    strategy_name    VARCHAR(255) NOT NULL,
    description      TEXT,
    
    -- Configuration stored as JSONB for flexibility
    news_config      JSONB NOT NULL,
    trade_config     JSONB NOT NULL,
    
    -- Status
    is_active        BOOLEAN DEFAULT FALSE,
    
    -- Metadata
    created_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    version          INTEGER NOT NULL DEFAULT 1,
    
    -- Indexes
    CONSTRAINT strategies_user_id_strategy_name_key UNIQUE (user_id, strategy_name)
);

CREATE INDEX idx_strategies_user_id ON strategies(user_id);
CREATE INDEX idx_strategies_is_active ON strategies(is_active);
CREATE INDEX idx_strategies_created_at ON strategies(created_at DESC);

-- JSONB indexes for efficient querying
CREATE INDEX idx_strategies_news_keywords ON strategies USING GIN ((news_config->'keywords'));
CREATE INDEX idx_strategies_stock_codes ON strategies USING GIN ((news_config->'stock_codes'));
```

### Example Data

```sql
INSERT INTO strategies (
    strategy_id, user_id, strategy_name, description,
    news_config, trade_config, is_active
) VALUES (
    'strat_123abc',
    'IS14415',
    'Apple Earnings Play',
    'Trade on Apple earnings announcements',
    '{
        "keywords": ["Apple", "AAPL", "earnings", "iPhone"],
        "sentiment": "POSITIVE",
        "min_impact_score": 7,
        "categories": ["earnings", "product-launch"],
        "stock_codes": [2885]
    }',
    '{
        "exchange": "NSE",
        "segment": "EQ",
        "action_type": "BUY",
        "order_type": "MARKET",
        "product_type": "INTRADAY",
        "quantity": 100,
        "price_range": {"min": 0, "max": 0},
        "stop_loss": 0,
        "take_profit": 0
    }',
    true
);
```

---

## gRPC API Reference

### Protocol Buffer Definition

```protobuf
syntax = "proto3";

package user_config;

service UserConfigService {
    // Strategy CRUD
    rpc CreateStrategy(CreateStrategyRequest) returns (CreateStrategyResponse);
    rpc GetStrategy(GetStrategyRequest) returns (GetStrategyResponse);
    rpc UpdateStrategy(UpdateStrategyRequest) returns (UpdateStrategyResponse);
    rpc DeleteStrategy(DeleteStrategyRequest) returns (DeleteStrategyResponse);
    
    // Strategy Queries
    rpc ListUserStrategies(ListUserStrategiesRequest) returns (ListUserStrategiesResponse);
    rpc GetStrategiesByIDs(GetStrategiesByIDsRequest) returns (GetStrategiesByIDsResponse);
    
    // Strategy Actions
    rpc ActivateStrategy(ActivateStrategyRequest) returns (ActivateStrategyResponse);
    rpc DeactivateStrategy(DeactivateStrategyRequest) returns (DeactivateStrategyResponse);
    
    // Health
    rpc HealthCheck(HealthCheckRequest) returns (HealthCheckResponse);
}

message CreateStrategyRequest {
    string user_id = 1;
    string strategy_name = 2;
    string description = 3;
    NewsConfig news_config = 4;
    TradeConfig trade_config = 5;
}

message NewsConfig {
    repeated string keywords = 1;
    string sentiment = 2;
    double min_impact_score = 3;
    repeated string categories = 4;
    repeated int32 stock_codes = 5;
}

message TradeConfig {
    string exchange = 1;
    string segment = 2;
    string action_type = 3;
    string order_type = 4;
    string product_type = 5;
    int32 quantity = 6;
    PriceRange price_range = 7;
    double stop_loss = 8;
    double take_profit = 9;
}
```

### API Examples

#### Create Strategy

```bash
grpcurl -plaintext \
  -d '{
    "user_id": "IS14415",
    "strategy_name": "Tech Stock News Trading",
    "description": "Buy on positive tech news",
    "news_config": {
      "keywords": ["Apple", "Microsoft", "Google"],
      "sentiment": "POSITIVE",
      "min_impact_score": 7,
      "categories": ["earnings", "product-launch"]
    },
    "trade_config": {
      "exchange": "NSE",
      "action_type": "BUY",
      "order_type": "MARKET",
      "quantity": 100
    }
  }' \
  localhost:50051 \
  user_config.UserConfigService/CreateStrategy
```

#### List User Strategies

```bash
grpcurl -plaintext \
  -d '{
    "user_id": "IS14415",
    "page": 1,
    "page_size": 10
  }' \
  localhost:50051 \
  user_config.UserConfigService/ListUserStrategies
```

---

## Kafka Integration

### Topic Configuration

**Topic Name:** `strategy.updates`

**Partitions:** 3 (for parallel processing)

**Replication Factor:** 2 (for fault tolerance)

**Retention:** 7 days

### Message Format

```json
{
  "event_type": "CREATE",
  "event_time": "2025-12-12T10:30:00Z",
  "strategy": {
    "strategy_id": "strat_123abc",
    "user_id": "IS14415",
    "strategy_name": "Apple Earnings Play",
    "description": "Trade on Apple earnings",
    "news_config": {
      "keywords": ["Apple", "AAPL", "earnings"],
      "sentiment": "POSITIVE",
      "min_impact_score": 7,
      "categories": ["earnings"],
      "stock_codes": [2885]
    },
    "trade_config": {
      "exchange": "NSE",
      "segment": "EQ",
      "action_type": "BUY",
      "order_type": "MARKET",
      "product_type": "INTRADAY",
      "quantity": 100
    },
    "is_active": false,
    "created_at": "2025-12-12T10:30:00Z",
    "updated_at": "2025-12-12T10:30:00Z",
    "version": 1
  }
}
```

### Event Types

| Event Type | Description | When Published |
|------------|-------------|----------------|
| `CREATE` | New strategy created | After successful insert |
| `UPDATE` | Strategy modified | After successful update |
| `DELETE` | Strategy removed | After successful delete |
| `ACTIVATE` | Strategy activated | When is_active set to true |
| `DEACTIVATE` | Strategy deactivated | When is_active set to false |

### Consumer Integration

**Rules Engine subscribes to this topic to:**
- Update Elasticsearch index with new strategies
- Update Redis cache
- Recompute matching rules

---

## Configuration

### Environment Variables

```bash
# Service Configuration
SERVICE_NAME=user-config-service
GRPC_PORT=50051
LOG_LEVEL=INFO

# Database Configuration
DATABASE_URL=postgresql://trading_user:postgres@localhost:5432/trading_system
DB_MAX_CONNECTIONS=25
DB_CONNECTION_TIMEOUT=30s

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC_STRATEGY=strategy.updates
KAFKA_COMPRESSION=snappy
KAFKA_BATCH_SIZE=16384

# Performance Tuning
WORKER_POOL_SIZE=10
REQUEST_TIMEOUT=30s
```

### Configuration Best Practices

1. **Database Connections**: Set max connections based on expected load (25-50 for small deployments)
2. **Kafka Batching**: Use batching for better throughput
3. **Timeouts**: Set reasonable timeouts (30s for most operations)
4. **Logging**: Use INFO in production, DEBUG only for troubleshooting
5. **Connection Pooling**: Reuse database and Kafka connections

---

## Setup & Deployment

### Prerequisites

```bash
# 1. Install Go
go version  # Should be 1.23+

# 2. Install PostgreSQL
psql --version  # Should be 13+

# 3. Install Protocol Buffers compiler
protoc --version

# 4. Install Kafka (optional)
```

### Development Setup

```bash
# 1. Navigate to service directory
cd services/user-config

# 2. Install dependencies
go mod download
go mod tidy

# 3. Setup database
createdb trading_system
psql -d trading_system -f migrations/001_create_strategies.sql

# 4. Configure environment
cp .env.example .env
# Edit .env with your configuration

# 5. Generate protocol buffers (if changed)
cd ../../api/proto
make generate

# 6. Build and run
cd ../../services/user-config
go build -o bin/user-config cmd/main.go
./bin/user-config
```

### Production Deployment

#### Docker

```dockerfile
FROM golang:1.23-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o user-config cmd/main.go

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/

COPY --from=builder /app/user-config .
COPY --from=builder /app/.env .

EXPOSE 50051
CMD ["./user-config"]
```

#### Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: user-config-service
spec:
  replicas: 3
  selector:
    matchLabels:
      app: user-config
  template:
    metadata:
      labels:
        app: user-config
    spec:
      containers:
      - name: user-config
        image: user-config:latest
        ports:
        - containerPort: 50051
        env:
        - name: GRPC_PORT
          value: "50051"
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: db-credentials
              key: url
        - name: KAFKA_BOOTSTRAP_SERVERS
          value: "kafka:9092"
        livenessProbe:
          exec:
            command: ["grpc_health_probe", "-addr=:50051"]
          initialDelaySeconds: 10
        readinessProbe:
          exec:
            command: ["grpc_health_probe", "-addr=:50051"]
          initialDelaySeconds: 5
---
apiVersion: v1
kind: Service
metadata:
  name: user-config-service
spec:
  selector:
    app: user-config
  ports:
  - protocol: TCP
    port: 50051
    targetPort: 50051
  type: ClusterIP
```

---

## Testing

### Unit Tests

```go
// repository_test.go
func TestCreateStrategy(t *testing.T) {
    // Setup
    db := setupTestDB()
    defer db.Close()
    repo := repository.NewPostgresRepository(db)
    
    strategy := &models.Strategy{
        StrategyID:   "test_123",
        UserID:       "user_456",
        StrategyName: "Test Strategy",
        // ... other fields
    }
    
    // Execute
    err := repo.Create(context.Background(), strategy)
    
    // Assert
    assert.NoError(t, err)
    
    // Verify
    retrieved, err := repo.GetByID(context.Background(), "test_123", "user_456")
    assert.NoError(t, err)
    assert.Equal(t, strategy.StrategyName, retrieved.StrategyName)
}
```

### Integration Tests

```bash
# Start dependencies
docker-compose up -d postgres kafka

# Run integration tests
go test -tags=integration ./...
```

### Load Testing

```bash
# Using ghz (gRPC benchmarking tool)
ghz --insecure \
  --proto api/proto/user_config/user_config.proto \
  --call user_config.UserConfigService/CreateStrategy \
  -d '{"user_id":"IS14415","strategy_name":"Load Test"}' \
  -n 1000 \
  -c 10 \
  localhost:50051
```

---

## Troubleshooting

### Common Issues

#### 1. Database Connection Failed

**Error:** `failed to connect to database`

**Solution:**
```bash
# Check PostgreSQL is running
pg_isready -h localhost -p 5432

# Verify credentials
psql -U trading_user -d trading_system

# Check .env file
cat .env | grep DATABASE_URL
```

#### 2. Kafka Producer Error

**Error:** `Failed to deliver message to Kafka`

**Solution:**
```bash
# Check Kafka is running
kafka-topics.sh --bootstrap-server localhost:9092 --list

# Create topic if missing
kafka-topics.sh --create --topic strategy.updates --bootstrap-server localhost:9092

# Check Kafka logs
tail -f /var/log/kafka/server.log
```

#### 3. gRPC Connection Refused

**Error:** `connection refused on port 50051`

**Solution:**
```bash
# Check if service is running
ps aux | grep user-config

# Check port is not in use
lsof -i :50051

# Check logs
tail -f user_config.log
```

---

## Best Practices

### 1. Error Handling
- Always return meaningful error messages
- Log errors with context (user_id, strategy_id)
- Use error codes for client handling
- Don't expose internal details to clients

### 2. Database Operations
- Use transactions for multi-step operations
- Implement connection pooling
- Use prepared statements
- Add appropriate indexes

### 3. Kafka Publishing
- Make publishing asynchronous (don't block RPC)
- Handle publish failures gracefully
- Include correlation IDs in messages
- Use appropriate partition keys

### 4. Security
- Validate all inputs
- Use parameterized queries (prevent SQL injection)
- Implement rate limiting
- Log security events

### 5. Performance
- Cache frequently accessed data
- Use connection pooling
- Implement pagination for large datasets
- Monitor and optimize slow queries

### 6. Monitoring
- Track RPC latency
- Monitor database connection pool
- Track Kafka publish success/failure rates
- Set up alerts for errors

---

## Additional Resources

- [gRPC Documentation](https://grpc.io/docs/languages/go/)
- [PostgreSQL Best Practices](https://www.postgresql.org/docs/current/index.html)
- [Kafka Producer API](https://kafka.apache.org/documentation/#producerapi)
- [Protocol Buffers Guide](https://developers.google.com/protocol-buffers)

---

**Last Updated:** December 12, 2025  
**Version:** 1.0  
**Maintained by:** Backend Development Team
