# Risk Management Service - Knowledge Transfer Documentation

## Table of Contents
1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Core Components](#core-components)
4. [Risk Validation Logic](#risk-validation-logic)
5. [gRPC API Reference](#grpc-api-reference)
6. [Redis Integration](#redis-integration)
7. [Risk Limits & Profiles](#risk-limits--profiles)
8. [Configuration](#configuration)
9. [Database Schema](#database-schema)
10. [Setup & Deployment](#setup--deployment)
11. [Testing](#testing)
12. [Monitoring & Troubleshooting](#monitoring--troubleshooting)
13. [Best Practices](#best-practices)

---

## 1. Overview

### Purpose
The **Risk Management Service** is a critical component of the algorithmic trading system that ensures all trading operations comply with predefined risk parameters. It performs real-time pre-trade validation and post-trade monitoring to prevent excessive losses and enforce trading discipline.

### Key Responsibilities
- **Pre-Trade Risk Validation**: Validates orders before execution against 8 different risk checks
- **Post-Trade Monitoring**: Tracks executed trades and updates risk metrics
- **Position Tracking**: Maintains real-time position data per user/strategy
- **Risk Limit Enforcement**: Enforces user-specific and strategy-specific risk limits
- **Psychology-Based Adjustments**: Applies risk profile adjustments (Conservative, Moderate, Aggressive)
- **Circuit Breaker**: Implements emergency stop mechanisms for catastrophic losses
- **Duplicate Detection**: Prevents accidental duplicate order submissions
- **Real-Time Metrics**: Provides current risk exposure and usage statistics

### Technology Stack
- **Language**: Go 1.23+
- **Protocol**: gRPC (Port 9005)
- **Storage**: Redis (risk metrics, positions, trade counts)
- **Protobuf**: Service definitions in `api/proto/risk_management/`
- **Logging**: Structured logging to `risk_management.log`

### Integration Points
```
┌─────────────────────┐
│   API Gateway       │ ──► GetRiskMetrics, SetRiskLimits
└─────────────────────┘
           │
           ▼
┌─────────────────────┐
│ Risk Management     │ ──► CheckPreTradeRisk
│ Service (gRPC 9005) │
└─────────────────────┘
           │
           ▼
┌─────────────────────┐
│ Trade Execution     │ ──► Returns approval/rejection
│ Service             │
└─────────────────────┘
           │
           ▼
┌─────────────────────┐
│ Redis (Metrics,     │ ──► Real-time risk data
│ Positions, Counts)  │
└─────────────────────┘
```

---

## 2. Architecture

### High-Level Architecture
```
┌──────────────────────────────────────────────────────────────┐
│                    Risk Management Service                    │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌────────────────┐        ┌────────────────┐              │
│  │  gRPC Server   │────────│  Pre-Trade     │              │
│  │  (Port 9005)   │        │  Checker       │              │
│  └────────────────┘        └────────────────┘              │
│         │                          │                        │
│         │                          ▼                        │
│         │                  ┌────────────────┐              │
│         │                  │ Risk Calculator│              │
│         │                  │ (8 Checks)     │              │
│         │                  └────────────────┘              │
│         │                          │                        │
│         ▼                          ▼                        │
│  ┌────────────────────────────────────────┐                │
│  │        Redis Repository                 │                │
│  │  - Trade Counts                         │                │
│  │  - Daily Loss Tracking                  │                │
│  │  - Position Data                        │                │
│  │  - Risk Profiles                        │                │
│  │  - Duplicate Detection                  │                │
│  └────────────────────────────────────────┘                │
│         │                                                    │
└─────────┼────────────────────────────────────────────────────┘
          │
          ▼
    ┌──────────┐
    │  Redis   │
    │  (Cache) │
    └──────────┘
```

### Design Patterns
1. **Repository Pattern**: Redis access abstracted through repository layer
2. **Validation Chain**: Sequential execution of 8 risk checks with early termination
3. **Profile Strategy Pattern**: Different risk adjustments based on user psychology
4. **Circuit Breaker**: Emergency stop mechanism for catastrophic losses
5. **Idempotency**: Duplicate order detection using fingerprinting

---

## 3. Core Components

### 3.1 Pre-Trade Checker (`internal/checker/pre_trade.go`)

**Purpose**: Validates incoming orders against comprehensive risk rules.

**8 Pre-Trade Risk Checks**:
1. **Daily Trade Limit**: Ensures user hasn't exceeded max daily trades
2. **Daily Loss Limit**: Prevents trading after max daily loss reached
3. **Position Size Limit**: Validates individual position value
4. **Per-Trade Risk Limit**: Ensures single trade risk is within bounds
5. **Portfolio Exposure Limit**: Checks total portfolio exposure percentage
6. **Concentration Risk**: Prevents over-concentration in single stock
7. **Circuit Breaker**: Emergency stop after catastrophic loss percentage
8. **Duplicate Order Detection**: Prevents accidental duplicate submissions

**Key Functions**:
```go
// CheckPreTrade performs all 8 risk validations
func (p *PreTradeChecker) CheckPreTrade(ctx context.Context, req *pb.PreTradeRiskRequest) (*pb.PreTradeRiskResponse, error)

// applyProfileAdjustments modifies limits based on user psychology
func (p *PreTradeChecker) applyProfileAdjustments(profile string, req *pb.PreTradeRiskRequest) *pb.PreTradeRiskRequest

// fingerprintOrder creates unique order signature for duplicate detection
func fingerprintOrder(req *pb.PreTradeRiskRequest) string
```

**Risk Profile Adjustments**:
- **Conservative**: 0.5x multiplier (tighter limits)
- **Moderate**: 1.0x multiplier (default limits)
- **Aggressive**: 1.5x multiplier (looser limits)

**Example Risk Score Calculation**:
```
Violation 1: Daily Trade Limit Exceeded → +15 points
Violation 2: Daily Loss Limit Exceeded  → +20 points
Violation 3: Position Size Too Large    → +10 points
───────────────────────────────────────────────────
Total Risk Score                        → 45 points

Risk Score > 30 → Order REJECTED
```

### 3.2 Risk Calculator (`internal/calculator/`)

**Purpose**: Computes risk metrics and exposure calculations.

**Metrics Computed**:
- Current portfolio value
- Total exposure (long + short positions)
- Available buying power
- Position-wise P&L
- Daily realized P&L
- Unrealized P&L

### 3.3 Models (`internal/models/`)

**RiskLimits** (`limits.go`):
```go
type RiskLimits struct {
    UserID                  string
    StrategyID              string
    MaxDailyTrades          int       // Max trades per day
    MaxDailyLoss            float64   // Max loss per day (INR)
    MaxPositionSize         float64   // Max single position (INR)
    MaxPerTradeRisk         float64   // Max risk per trade (INR)
    MaxPortfolioExposurePct float64   // Max portfolio exposure (%)
    MaxConcentrationPct     float64   // Max concentration in stock (%)
    CircuitBreakerLossPct   float64   // Emergency stop loss (%)
    DuplicateOrderWindowSec int       // Time window for duplicate detection
}
```

**RiskMetrics** (`risk_metrics.go`):
```go
type RiskMetrics struct {
    UserID          string
    StrategyID      string
    TotalExposure   float64   // Current market exposure
    DailyPnL        float64   // Today's realized P&L
    UnrealizedPnL   float64   // Unrealized P&L
    TradeCount      int64     // Trades executed today
    PositionCount   int       // Open positions count
    RiskScore       float64   // Current risk score
    LastUpdated     time.Time
}
```

### 3.4 Redis Repository (`internal/repository/redis.go`)

**Purpose**: Manages all Redis operations for risk data storage and retrieval.

**Key Operations**:
```go
// Trade count management
GetTodayTradeCount(ctx, userID) (int64, error)
IncrementTradeCount(ctx, userID) error

// Loss tracking
GetDailyLoss(ctx, userID) (float64, error)
UpdateDailyLoss(ctx, userID, loss float64) error

// Position management
GetUserPosition(ctx, userID, stockCode) (*Position, error)
UpdatePosition(ctx, position *Position) error

// Risk profile
GetRiskProfile(ctx, userID) (string, error)
SetRiskProfile(ctx, userID, profile string) error

// Duplicate detection
CheckDuplicateOrder(ctx, userID, fingerprint string, windowSec int) (bool, error)
```

**Redis Key Patterns**:
```
risk:trade_count:{userID}:{date}           → Daily trade count
risk:daily_loss:{userID}:{date}            → Daily loss amount
risk:position:{userID}:{stockCode}         → Position data
risk:profile:{userID}                      → Risk profile (conservative/moderate/aggressive)
risk:duplicate:{userID}:{fingerprint}      → Duplicate detection (TTL-based)
risk:metrics:{userID}:{strategyID}         → Real-time risk metrics
risk:exposure:{userID}                     → Portfolio exposure
```

### 3.5 gRPC Server (`internal/server/server.go`)

**Purpose**: Exposes gRPC endpoints for risk validation and management.

**Server Configuration**:
```go
type RiskManagementServer struct {
    pb.UnimplementedRiskManagementServiceServer
    checker    *checker.PreTradeChecker
    calculator *calculator.RiskCalculator
    redisRepo  *repository.RedisRepository
}
```

**gRPC Methods**:
1. `CheckPreTradeRisk`: Validates order before execution
2. `SetRiskLimits`: Configures user/strategy risk limits
3. `GetRiskMetrics`: Retrieves current risk exposure
4. `UpdatePosition`: Records trade execution results
5. `ResetDailyMetrics`: Resets daily counters (scheduled job)

---

## 4. Risk Validation Logic

### Pre-Trade Validation Flow
```
Order Received
    │
    ▼
┌────────────────────────────────┐
│ 1. Fetch Risk Profile (Redis) │
│    (Conservative/Moderate/     │
│     Aggressive)                │
└────────────────────────────────┘
    │
    ▼
┌────────────────────────────────┐
│ 2. Apply Profile Adjustments   │
│    - Conservative: 0.5x limits │
│    - Aggressive: 1.5x limits   │
└────────────────────────────────┘
    │
    ▼
┌────────────────────────────────┐
│ 3. Execute 8 Risk Checks       │
│    ✓ Daily Trade Limit         │
│    ✓ Daily Loss Limit          │
│    ✓ Position Size Limit       │
│    ✓ Per-Trade Risk            │
│    ✓ Portfolio Exposure        │
│    ✓ Concentration Risk        │
│    ✓ Circuit Breaker           │
│    ✓ Duplicate Detection       │
└────────────────────────────────┘
    │
    ▼
┌────────────────────────────────┐
│ 4. Calculate Risk Score        │
│    (Sum of violation weights)  │
└────────────────────────────────┘
    │
    ▼
Risk Score > 30? ──YES──► REJECT Order
    │                     (Return violations)
    NO
    │
    ▼
APPROVE Order
(Return approval response)
```

### Risk Score Weighting
```go
Daily Trade Limit Exceeded      → +15 points
Daily Loss Limit Exceeded       → +20 points
Position Size Too Large         → +10 points
Per-Trade Risk Too High         → +12 points
Portfolio Exposure Exceeded     → +18 points
Concentration Risk High         → +15 points
Circuit Breaker Triggered       → +50 points (instant rejection)
Duplicate Order Detected        → +25 points (instant rejection)
```

### Example: Conservative Profile Effect
```
Original Limits:
    MaxDailyTrades: 10
    MaxPositionSize: ₹100,000
    MaxPerTradeRisk: ₹5,000

After Conservative Profile (0.5x):
    MaxDailyTrades: 5
    MaxPositionSize: ₹50,000
    MaxPerTradeRisk: ₹2,500
```

---

## 5. gRPC API Reference

### Service Definition
**File**: `api/proto/risk_management/risk_management.proto`

### Method 1: CheckPreTradeRisk

**Purpose**: Validates order before execution.

**Request**:
```protobuf
message PreTradeRiskRequest {
    string user_id = 1;
    string strategy_id = 2;
    int64 stock_code = 3;
    string exchange = 4;
    string order_type = 5;
    string order_side = 6;
    int32 quantity = 7;
    double price = 8;
    double stop_loss = 9;
    double take_profit = 10;
    int32 max_daily_trades = 11;
    double max_loss_per_day = 12;
    double max_position_size = 13;
    double max_per_trade_risk = 14;
}
```

**Response**:
```protobuf
message PreTradeRiskResponse {
    bool approved = 1;
    double risk_score = 2;
    repeated RiskViolation violations = 3;
    string message = 4;
}

message RiskViolation {
    RiskViolationType type = 1;
    string message = 2;
    double current_value = 3;
    double limit_value = 4;
}
```

**Example Call**:
```bash
grpcurl -plaintext -d '{
  "user_id": "user123",
  "strategy_id": "strat456",
  "stock_code": 3456,
  "exchange": "NSE",
  "order_type": "LIMIT",
  "order_side": "BUY",
  "quantity": 100,
  "price": 1500.50,
  "max_daily_trades": 10,
  "max_loss_per_day": 50000,
  "max_position_size": 200000,
  "max_per_trade_risk": 10000
}' localhost:9005 risk_management.RiskManagementService/CheckPreTradeRisk
```

**Success Response**:
```json
{
  "approved": true,
  "risk_score": 5.0,
  "violations": [],
  "message": "Order approved"
}
```

**Rejection Response**:
```json
{
  "approved": false,
  "risk_score": 45.0,
  "violations": [
    {
      "type": "RISK_VIOLATION_DAILY_TRADE_LIMIT",
      "message": "Daily trade limit exceeded",
      "current_value": 11,
      "limit_value": 10
    }
  ],
  "message": "Order rejected due to risk violations"
}
```

### Method 2: SetRiskLimits

**Purpose**: Configures risk limits for user/strategy.

**Request**:
```protobuf
message SetRiskLimitsRequest {
    string user_id = 1;
    string strategy_id = 2;
    int32 max_daily_trades = 3;
    double max_daily_loss = 4;
    double max_position_size = 5;
    double max_per_trade_risk = 6;
    double max_portfolio_exposure_pct = 7;
    double max_concentration_pct = 8;
    double circuit_breaker_loss_pct = 9;
    int32 duplicate_order_window_sec = 10;
}
```

**Response**:
```protobuf
message SetRiskLimitsResponse {
    bool success = 1;
    string message = 2;
}
```

### Method 3: GetRiskMetrics

**Purpose**: Retrieves current risk exposure and metrics.

**Request**:
```protobuf
message GetRiskMetricsRequest {
    string user_id = 1;
    string strategy_id = 2;
}
```

**Response**:
```protobuf
message GetRiskMetricsResponse {
    double total_exposure = 1;
    double daily_pnl = 2;
    double unrealized_pnl = 3;
    int64 trade_count = 4;
    int32 position_count = 5;
    double risk_score = 6;
    string last_updated = 7;
}
```

---

## 6. Redis Integration

### Redis Connection
**Configuration** (`config/config.yaml`):
```yaml
redis:
  host: localhost
  port: 6379
  password: ""
  db: 0
  max_retries: 3
  pool_size: 10
  min_idle_conns: 5
```

### Data Structures

#### Trade Count (String with TTL)
```redis
SET risk:trade_count:user123:2025-01-15 10
EXPIRE risk:trade_count:user123:2025-01-15 86400
```

#### Daily Loss (String with TTL)
```redis
SET risk:daily_loss:user123:2025-01-15 25000.50
EXPIRE risk:daily_loss:user123:2025-01-15 86400
```

#### Position Data (Hash)
```redis
HSET risk:position:user123:3456 {
  "stock_code": 3456,
  "quantity": 100,
  "avg_price": 1500.50,
  "current_price": 1520.00,
  "unrealized_pnl": 1950.00
}
```

#### Risk Profile (String)
```redis
SET risk:profile:user123 "conservative"
```

#### Duplicate Detection (String with TTL)
```redis
SET risk:duplicate:user123:3456:BUY:100 "1"
EXPIRE risk:duplicate:user123:3456:BUY:100 60
```

### TTL Strategy
- **Trade Count**: 24 hours (auto-reset daily)
- **Daily Loss**: 24 hours (auto-reset daily)
- **Position Data**: Persistent (manual cleanup)
- **Risk Profile**: Persistent
- **Duplicate Detection**: 60 seconds (configurable)

---

## 7. Risk Limits & Profiles

### Default Risk Limits
```go
DefaultRiskLimits = RiskLimits{
    MaxDailyTrades:          10,
    MaxDailyLoss:            50000.0,  // ₹50,000
    MaxPositionSize:         200000.0, // ₹2,00,000
    MaxPerTradeRisk:         10000.0,  // ₹10,000
    MaxPortfolioExposurePct: 80.0,     // 80%
    MaxConcentrationPct:     25.0,     // 25%
    CircuitBreakerLossPct:   10.0,     // 10%
    DuplicateOrderWindowSec: 60,       // 60 seconds
}
```

### Risk Profiles

#### Conservative Profile
```yaml
Profile: conservative
Multiplier: 0.5x
Use Case: Risk-averse traders, beginners
Characteristics:
  - Lower position sizes
  - Fewer daily trades
  - Tighter stop losses
```

#### Moderate Profile (Default)
```yaml
Profile: moderate
Multiplier: 1.0x
Use Case: Standard traders
Characteristics:
  - Balanced risk/reward
  - Default system limits
  - Standard position sizing
```

#### Aggressive Profile
```yaml
Profile: aggressive
Multiplier: 1.5x
Use Case: Experienced traders, higher risk tolerance
Characteristics:
  - Larger position sizes
  - More daily trades
  - Wider stop losses
```

### Setting Risk Profile
```bash
# Via gRPC (custom method)
grpcurl -plaintext -d '{
  "user_id": "user123",
  "profile": "conservative"
}' localhost:9005 risk_management.RiskManagementService/SetRiskProfile

# Direct Redis command
redis-cli SET risk:profile:user123 "conservative"
```

---

## 8. Configuration

### Service Configuration
**File**: `config/config.yaml`

```yaml
server:
  grpc_port: 9005
  enable_reflection: true

redis:
  host: localhost
  port: 6379
  password: ""
  db: 0
  max_retries: 3
  pool_size: 10
  min_idle_conns: 5

risk:
  default_max_daily_trades: 10
  default_max_daily_loss: 50000.0
  default_max_position_size: 200000.0
  default_max_per_trade_risk: 10000.0
  default_portfolio_exposure_pct: 80.0
  default_concentration_pct: 25.0
  default_circuit_breaker_pct: 10.0
  duplicate_window_sec: 60

logging:
  level: info
  format: json
  output: risk_management.log
```

### Environment Variables
```bash
# Service configuration
GRPC_PORT=9005
ENABLE_GRPC_REFLECTION=true

# Redis configuration
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=""
REDIS_DB=0

# Risk defaults
DEFAULT_MAX_DAILY_TRADES=10
DEFAULT_MAX_DAILY_LOSS=50000
DEFAULT_MAX_POSITION_SIZE=200000

# Logging
LOG_LEVEL=info
LOG_OUTPUT=risk_management.log
```

---

## 9. Database Schema

### Redis Data Model

#### Key Patterns
```
risk:trade_count:{user_id}:{date}          → String (TTL: 24h)
risk:daily_loss:{user_id}:{date}           → String (TTL: 24h)
risk:position:{user_id}:{stock_code}       → Hash
risk:profile:{user_id}                     → String
risk:duplicate:{user_id}:{fingerprint}     → String (TTL: 60s)
risk:metrics:{user_id}:{strategy_id}       → Hash
risk:exposure:{user_id}                    → String
risk:limits:{user_id}:{strategy_id}        → Hash
```

#### Example Data
```redis
# Trade count
risk:trade_count:user123:2025-01-15 → "7"

# Daily loss
risk:daily_loss:user123:2025-01-15 → "12500.50"

# Position
risk:position:user123:3456 → {
  "stock_code": "3456",
  "quantity": "100",
  "avg_price": "1500.50",
  "current_price": "1520.00",
  "unrealized_pnl": "1950.00"
}

# Risk profile
risk:profile:user123 → "conservative"

# Duplicate detection
risk:duplicate:user123:3456:BUY:100 → "1" (TTL: 60s)

# Risk metrics
risk:metrics:user123:strat456 → {
  "total_exposure": "150000.00",
  "daily_pnl": "-5000.00",
  "unrealized_pnl": "2500.00",
  "trade_count": "7",
  "position_count": "3",
  "risk_score": "15.0",
  "last_updated": "2025-01-15T10:30:00Z"
}
```

---

## 10. Setup & Deployment

### Prerequisites
```bash
# Go 1.23+
go version

# Redis 6.0+
redis-server --version

# Protocol Buffers compiler
protoc --version
```

### Installation Steps

#### 1. Clone Repository
```bash
cd /home/stockkask/algo-trading/Algo-Treading/services/risk-management
```

#### 2. Install Dependencies
```bash
go mod download
```

#### 3. Generate Protobuf Code
```bash
cd ../../api/proto
make generate
```

#### 4. Configure Redis
```bash
# Start Redis
redis-server

# Verify connection
redis-cli ping
# Expected: PONG
```

#### 5. Configure Service
```bash
# Copy example config
cp config/config.example.yaml config/config.yaml

# Edit configuration
nano config/config.yaml
```

#### 6. Build Service
```bash
# Using build script
./build.sh

# Or manual build
go build -o bin/risk-management cmd/main.go
```

#### 7. Run Service
```bash
# Using run script
./run.sh

# Or manual run
./bin/risk-management

# Expected output:
# 2025-01-15 10:00:00 INFO Starting Risk Management Service
# 2025-01-15 10:00:00 INFO Connected to Redis at localhost:6379
# 2025-01-15 10:00:00 INFO gRPC server listening on port 9005
```

### Docker Deployment

**Dockerfile**:
```dockerfile
FROM golang:1.23-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o bin/risk-management cmd/main.go

FROM alpine:latest
RUN apk --no-cache add ca-certificates

WORKDIR /root/
COPY --from=builder /app/bin/risk-management .
COPY --from=builder /app/config ./config

EXPOSE 9005

CMD ["./risk-management"]
```

**Build & Run**:
```bash
# Build image
docker build -t risk-management:latest .

# Run container
docker run -d \
  --name risk-management \
  -p 9005:9005 \
  -e REDIS_HOST=redis \
  --network algo-trading-network \
  risk-management:latest
```

### Health Check
```bash
# gRPC health check
grpcurl -plaintext localhost:9005 grpc.health.v1.Health/Check

# Redis connection test
redis-cli -h localhost -p 6379 ping
```

---

## 11. Testing

### Unit Tests

**Run All Tests**:
```bash
go test ./... -v
```

**Test Coverage**:
```bash
go test ./... -cover -coverprofile=coverage.out
go tool cover -html=coverage.out
```

### Integration Tests

**Test Pre-Trade Checker**:
```go
// test_client.go
package main

import (
    "context"
    "log"
    pb "github.com/RohitIndira/Algo-Treading/api/proto/risk_management"
    "google.golang.org/grpc"
)

func main() {
    conn, _ := grpc.Dial("localhost:9005", grpc.WithInsecure())
    defer conn.Close()
    
    client := pb.NewRiskManagementServiceClient(conn)
    
    req := &pb.PreTradeRiskRequest{
        UserId:          "user123",
        StrategyId:      "strat456",
        StockCode:       3456,
        Exchange:        "NSE",
        OrderType:       "LIMIT",
        OrderSide:       "BUY",
        Quantity:        100,
        Price:           1500.50,
        MaxDailyTrades:  10,
        MaxLossPerDay:   50000.0,
        MaxPositionSize: 200000.0,
        MaxPerTradeRisk: 10000.0,
    }
    
    resp, err := client.CheckPreTradeRisk(context.Background(), req)
    if err != nil {
        log.Fatalf("Error: %v", err)
    }
    
    log.Printf("Approved: %v, Risk Score: %.2f", resp.Approved, resp.RiskScore)
    for _, v := range resp.Violations {
        log.Printf("Violation: %s", v.Message)
    }
}
```

**Run Integration Test**:
```bash
go run test_client.go
```

### Test Scenarios

#### Scenario 1: Approve Valid Order
```bash
# Expected: Approved = true, Risk Score < 30
```

#### Scenario 2: Reject Due to Daily Trade Limit
```bash
# Setup: Increment trade count to max
redis-cli SET risk:trade_count:user123:2025-01-15 10

# Expected: Approved = false, Violation = DAILY_TRADE_LIMIT
```

#### Scenario 3: Conservative Profile Test
```bash
# Setup: Set conservative profile
redis-cli SET risk:profile:user123 "conservative"

# Expected: Limits reduced by 50%
```

#### Scenario 4: Duplicate Detection
```bash
# Send same order twice within 60 seconds
# Expected: Second order rejected with DUPLICATE_ORDER violation
```

---

## 12. Monitoring & Troubleshooting

### Logging

**Log Levels**:
- **INFO**: Normal operations, approvals/rejections
- **WARN**: Threshold breaches, retries
- **ERROR**: Service failures, Redis connection issues

**Log Format**:
```json
{
  "timestamp": "2025-01-15T10:30:00Z",
  "level": "INFO",
  "service": "risk-management",
  "message": "Order approved",
  "user_id": "user123",
  "risk_score": 5.0,
  "violations": []
}
```

### Metrics to Monitor

#### Service Health
```bash
# gRPC server status
grpcurl -plaintext localhost:9005 grpc.health.v1.Health/Check

# Redis connection
redis-cli ping
```

#### Business Metrics
```redis
# Total trade count today
redis-cli KEYS "risk:trade_count:*:$(date +%Y-%m-%d)" | wc -l

# Users with daily loss
redis-cli KEYS "risk:daily_loss:*:$(date +%Y-%m-%d)"

# Active positions
redis-cli KEYS "risk:position:*" | wc -l
```

### Common Issues

#### Issue 1: Redis Connection Failure
**Symptom**: Service fails to start, logs show "Failed to connect to Redis"

**Solution**:
```bash
# Check Redis status
systemctl status redis

# Restart Redis
systemctl restart redis

# Verify connection
redis-cli ping
```

#### Issue 2: All Orders Rejected
**Symptom**: Every order rejected with high risk scores

**Solution**:
```bash
# Check if daily counters stuck
redis-cli GET risk:trade_count:user123:$(date +%Y-%m-%d)

# Reset if needed
redis-cli DEL risk:trade_count:user123:$(date +%Y-%m-%d)
redis-cli DEL risk:daily_loss:user123:$(date +%Y-%m-%d)
```

#### Issue 3: Duplicate Detection False Positives
**Symptom**: Legitimate orders flagged as duplicates

**Solution**:
```bash
# Check duplicate window
redis-cli TTL risk:duplicate:user123:3456:BUY:100

# Clear duplicate cache
redis-cli KEYS "risk:duplicate:*" | xargs redis-cli DEL
```

#### Issue 4: Risk Profile Not Applied
**Symptom**: Limits not adjusting per profile

**Solution**:
```bash
# Verify profile setting
redis-cli GET risk:profile:user123

# Set correct profile
redis-cli SET risk:profile:user123 "conservative"
```

### Debugging Commands

```bash
# View all risk keys for user
redis-cli --scan --pattern "risk:*:user123:*"

# Monitor Redis operations
redis-cli MONITOR

# Check service logs
tail -f risk_management.log

# gRPC reflection (list methods)
grpcurl -plaintext localhost:9005 list
```

---

## 13. Best Practices

### Development Best Practices

1. **Always Use Redis TTL**
   - Set TTL for time-sensitive data (trade counts, loss tracking)
   - Prevents stale data accumulation

2. **Implement Idempotent Operations**
   - Use duplicate detection for order submissions
   - Fingerprint orders to detect duplicates

3. **Profile-Based Risk Management**
   - Customize limits per user psychology
   - Conservative profiles for beginners, aggressive for experts

4. **Graceful Degradation**
   - If Redis fails, use in-memory fallback
   - Log failures but don't block critical operations

5. **Comprehensive Logging**
   - Log every risk check result
   - Include user_id, strategy_id, and violation details

### Operational Best Practices

1. **Daily Metric Reset**
   - Schedule cron job to reset daily counters at midnight
   ```bash
   0 0 * * * redis-cli KEYS "risk:trade_count:*" | xargs redis-cli DEL
   0 0 * * * redis-cli KEYS "risk:daily_loss:*" | xargs redis-cli DEL
   ```

2. **Monitor Circuit Breakers**
   - Alert when circuit breaker triggered
   - Investigate catastrophic loss events immediately

3. **Regular Redis Backups**
   ```bash
   # Backup Redis data
   redis-cli BGSAVE
   cp /var/lib/redis/dump.rdb /backup/risk_$(date +%Y%m%d).rdb
   ```

4. **Performance Optimization**
   - Use Redis pipelining for batch operations
   - Cache risk limits to reduce Redis lookups

5. **Security**
   - Enable Redis password authentication
   - Use TLS for gRPC connections in production
   - Encrypt sensitive data in Redis

### Testing Best Practices

1. **Test All 8 Risk Checks**
   - Create test cases for each violation type
   - Verify risk score calculations

2. **Load Testing**
   - Simulate high-volume order validation
   - Ensure Redis can handle concurrent requests

3. **Profile Testing**
   - Verify multipliers applied correctly
   - Test edge cases (profile changes mid-session)

---

## Conclusion

The **Risk Management Service** is the guardian of the trading system, ensuring every trade adheres to strict risk parameters. By implementing 8 comprehensive pre-trade checks, psychology-based profile adjustments, and real-time monitoring via Redis, it prevents catastrophic losses and enforces disciplined trading.

### Quick Reference

**Service Details**:
- Protocol: gRPC
- Port: 9005
- Storage: Redis
- Language: Go 1.23+

**Core Functions**:
- Pre-trade risk validation (8 checks)
- Post-trade monitoring
- Position tracking
- Risk profile management

**Key Files**:
- `internal/checker/pre_trade.go` - 8 risk checks
- `internal/repository/redis.go` - Redis operations
- `api/proto/risk_management/` - gRPC definitions

**Critical Metrics**:
- Trade count per day
- Daily loss tracking
- Position exposure
- Risk score calculation

For detailed guides, refer to:
- [API Gateway KT](./API_GATEWAY_KT.md) - Integration patterns
- [Trade Execution KT](./TRADE_EXECUTION_SERVICE_KT.md) - Order flow integration
- [Master KT Index](./README.md) - Complete documentation index

---

**Document Version**: 1.0  
**Last Updated**: January 2025  
**Maintainer**: Development Team
