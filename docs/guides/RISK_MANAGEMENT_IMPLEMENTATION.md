# Risk Management Service Implementation Guide

## 📋 Overview

The Risk Management Service is a critical microservice in the Algo Trading System that enforces trading limits and monitors portfolio risk in real-time. It performs **pre-trade validation** before orders are submitted and **post-trade monitoring** after execution.

---

## 🏗️ Architecture

### Service Structure

```
services/risk-management/
├── cmd/
│   └── main.go                   # Entry point
├── internal/
│   ├── server/                   # gRPC server implementation
│   │   └── server.go             # Server setup & lifecycle
│   ├── checker/                  # Risk validation logic
│   │   ├── pre_trade.go          # Pre-trade risk checks
│   │   ├── post_trade.go         # Post-trade monitoring
│   │   └── limits.go             # Limit enforcement
│   ├── calculator/               # Risk calculations
│   │   ├── exposure.go           # Position exposure calculations
│   │   ├── drawdown.go           # Drawdown tracking & analysis
│   │   └── var.go                # Value at Risk (VAR)
│   ├── repository/               # Data access layer
│   │   ├── redis.go              # Redis operations (real-time counters)
│   │   └── postgres.go           # PostgreSQL operations (history)
│   └── models/                   # Data models
│       ├── risk_metrics.go       # Risk metrics structure
│       └── limits.go             # Risk limit definitions
├── config/
│   └── config.go                 # Configuration management
├── Dockerfile                    # Docker build
└── README.md                     # Service documentation
```

### Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Language** | Go | High-performance microservice |
| **Communication** | gRPC | Low-latency RPC with Protocol Buffers |
| **Real-time State** | Redis | Fast counters & position tracking |
| **History** | PostgreSQL | Persistent risk audit trail |
| **Deployment** | Docker | Containerized service |

---

## 🔄 Data Flow

### Pre-Trade Risk Check Flow

```
User Strategy Config
    ↓
Trade Execution Service
    ↓
Risk Management Service (CheckPreTradeRisk)
    ├─→ Fetch user risk limits from PostgreSQL
    ├─→ Fetch daily counters from Redis
    ├─→ Validate against all checks
    ├─→ Calculate risk score
    └─→ Return approval/violations
    ↓
If Approved → Execute Trade
If Rejected → Return error to user
```

### Post-Trade Monitoring Flow

```
Trade Executed (Order Filled)
    ↓
Order Status Update from Odin API
    ↓
Risk Management Service (UpdatePostTradeMetrics)
    ├─→ Update position in Redis
    ├─→ Update daily counters
    ├─→ Calculate new exposure/drawdown
    └─→ Store audit entry in PostgreSQL
```

---

## 🛡️ Risk Checks Implemented

### Pre-Trade Checks (Before Order Submission)

#### 1. **Daily Trade Limit Check**
- **Purpose**: Prevent overtrading
- **Check**: `today_trades_count < max_daily_trades`
- **Violation**: `RISK_VIOLATION_DAILY_TRADE_LIMIT`
- **Source**: Redis `user:{user_id}:trades:daily`

#### 2. **Daily Loss Limit Check**
- **Purpose**: Prevent catastrophic daily losses
- **Check**: `cumulative_daily_loss < max_loss_per_day`
- **Violation**: `RISK_VIOLATION_DAILY_LOSS_LIMIT`
- **Source**: Redis `user:{user_id}:loss:daily`

#### 3. **Position Size Limit Check**
- **Purpose**: Limit exposure to single security
- **Check**: `new_position_value < max_position_size`
- **Violation**: `RISK_VIOLATION_POSITION_SIZE_LIMIT`
- **Calculation**: `quantity * current_price`

#### 4. **Per-Trade Risk Limit Check**
- **Purpose**: Limit maximum loss on single trade
- **Check**: `potential_loss < max_per_trade_risk`
- **Violation**: `RISK_VIOLATION_PER_TRADE_RISK_LIMIT`
- **Calculation**: `quantity * (price - stop_loss)`

#### 5. **Duplicate Order Prevention**
- **Purpose**: Prevent accidental duplicate orders
- **Check**: Check if similar order was placed in last X seconds
- **Violation**: `RISK_VIOLATION_DUPLICATE_ORDER`

#### 6. **Insufficient Margin Check**
- **Purpose**: Ensure adequate buying power
- **Check**: `available_margin > required_margin`
- **Violation**: `RISK_VIOLATION_INSUFFICIENT_MARGIN`
- **Source**: User account balance from Redis

#### 7. **Circuit Breaker Check**
- **Purpose**: Stop trading after excessive daily loss
- **Check**: `daily_loss_pct < circuit_breaker_loss_pct`
- **Violation**: `RISK_VIOLATION_CIRCUIT_BREAKER`
- **Default**: Stop trading at 5% daily loss

#### 8. **Concentration Limit Check**
- **Purpose**: Prevent over-concentration in single sector
- **Check**: `largest_position_pct < max_concentration_pct`
- **Violation**: `RISK_VIOLATION_CONCENTRATION_LIMIT`

### Post-Trade Monitoring

#### 1. **Portfolio Exposure Monitoring**
- Track total portfolio exposure percentage
- Alert if exceeds `max_portfolio_exposure_pct`

#### 2. **Drawdown Monitoring**
- Calculate current drawdown from peak
- Track maximum drawdown
- Trigger alerts if exceeds threshold

#### 3. **Position Tracking**
- Update position quantities in Redis
- Track average entry price
- Calculate unrealized P&L

---

## 📊 Key Data Structures

### Redis Data Structures (Real-time)

```go
// Daily trade counter - Sorted Set (timestamp, order_id)
user:{user_id}:trades:daily
  └─ Members: [order_id1, order_id2, ...] (set at current timestamp)

// Daily loss accumulator - String
user:{user_id}:loss:daily
  └─ Value: -1250.50 (cumulative loss)

// User positions - Hash per stock
user:{user_id}:positions:{stock_code}
  ├─ quantity: 100
  ├─ avg_price: 150.50
  ├─ current_price: 152.00
  └─ entry_time: 1699800000

// User account balance - String
user:{user_id}:balance
  └─ Value: 500000.00 (available buying power)
```

### PostgreSQL Tables (Persistent)

```sql
-- Risk Limits Configuration
CREATE TABLE risk_limits (
  risk_limit_id UUID PRIMARY KEY,
  user_id UUID NOT NULL,
  strategy_id UUID NOT NULL,
  max_daily_trades INT,
  max_loss_per_day DECIMAL(15,2),
  max_position_size DECIMAL(15,2),
  max_per_trade_risk DECIMAL(15,2),
  max_portfolio_exposure_pct DECIMAL(5,2),
  max_concentration_pct DECIMAL(5,2),
  circuit_breaker_loss_pct DECIMAL(5,2),
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);

-- Risk Audit Trail
CREATE TABLE risk_audit (
  audit_id UUID PRIMARY KEY,
  user_id UUID NOT NULL,
  order_id UUID NOT NULL,
  check_type VARCHAR(50),        -- PRE_TRADE, POST_TRADE
  violations TEXT,               -- JSON array
  risk_score DECIMAL(5,2),
  approved BOOLEAN,
  timestamp TIMESTAMP
);

-- Position History
CREATE TABLE position_history (
  position_id UUID PRIMARY KEY,
  user_id UUID NOT NULL,
  stock_code BIGINT,
  quantity INT,
  avg_price DECIMAL(10,2),
  current_value DECIMAL(15,2),
  unrealized_pnl DECIMAL(15,2),
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);
```

---

## 💻 Implementation Steps

### Step 1: Set Up Configuration

**File**: `services/risk-management/config/config.go`

```go
package config

import (
	"os"
	"strconv"
)

type Config struct {
	// Server
	GRPCPort string
	
	// Redis
	RedisHost     string
	RedisPort     string
	RedisPassword string
	
	// PostgreSQL
	DBHost     string
	DBPort     string
	DBName     string
	DBUser     string
	DBPassword string
	
	// Risk Defaults
	MaxDailyTrades        int
	MaxDailyLoss          float64
	MaxPositionSize       float64
	CircuitBreakerLossPct float64
}

func LoadConfig() *Config {
	return &Config{
		GRPCPort:              getEnv("GRPC_PORT", "9005"),
		RedisHost:             getEnv("REDIS_HOST", "localhost"),
		RedisPort:             getEnv("REDIS_PORT", "6379"),
		DBHost:                getEnv("DB_HOST", "localhost"),
		DBPort:                getEnv("DB_PORT", "5432"),
		DBName:                getEnv("DB_NAME", "trading_db"),
		DBUser:                getEnv("DB_USER", "postgres"),
		MaxDailyTrades:        getEnvInt("MAX_DAILY_TRADES", 100),
		MaxDailyLoss:          getEnvFloat("MAX_DAILY_LOSS", 10000.0),
		CircuitBreakerLossPct: getEnvFloat("CIRCUIT_BREAKER_LOSS_PCT", 5.0),
	}
}

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value, exists := os.LookupEnv(key); exists {
		if i, err := strconv.Atoi(value); err == nil {
			return i
		}
	}
	return defaultValue
}

func getEnvFloat(key string, defaultValue float64) float64 {
	if value, exists := os.LookupEnv(key); exists {
		if f, err := strconv.ParseFloat(value, 64); err == nil {
			return f
		}
	}
	return defaultValue
}
```

### Step 2: Create Data Models

**File**: `services/risk-management/internal/models/risk_metrics.go`

```go
package models

import "time"

// RiskMetrics holds comprehensive risk information
type RiskMetrics struct {
	UserID             string
	DailyTrades        int
	DailyRealizedPnL   float64
	DailyUnrealizedPnL float64
	CurrentDrawdown    float64
	MaxDrawdown        float64
	PortfolioExposure  float64
	PositionCount      int
	LastUpdated        time.Time
}

// Position represents a user's stock position
type Position struct {
	StockCode      int64
	Quantity       int
	AvgPrice       float64
	CurrentPrice   float64
	UnrealizedPnL  float64
	EntryTime      time.Time
	LastUpdateTime time.Time
}

// RiskViolation represents a rule violation
type RiskViolation struct {
	Type         string
	Message      string
	CurrentValue float64
	LimitValue   float64
}
```

**File**: `services/risk-management/internal/models/limits.go`

```go
package models

// RiskLimits defines limits for a user's trading
type RiskLimits struct {
	UserID                   string
	StrategyID               string
	MaxDailyTrades           int
	MaxDailyLoss             float64
	MaxPositionSize          float64
	MaxPerTradeRisk          float64
	MaxPortfolioExposurePct  float64
	MaxConcentrationPct      float64
	CircuitBreakerLossPct    float64
	DuplicateOrderTimeWindow int // seconds
}
```

### Step 3: Implement Repository Layer

**File**: `services/risk-management/internal/repository/redis.go`

```go
package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisRepository struct {
	client *redis.Client
}

func NewRedisRepository(addr string) *RedisRepository {
	return &RedisRepository{
		client: redis.NewClient(&redis.Options{
			Addr: addr,
		}),
	}
}

// IncrementDailyTradeCount increments daily trade counter
func (r *RedisRepository) IncrementDailyTradeCount(ctx context.Context, userID string, orderID string) error {
	key := fmt.Sprintf("user:%s:trades:daily", userID)
	
	// Add to sorted set with current timestamp as score
	return r.client.ZAdd(ctx, key, redis.Z{
		Score:  float64(time.Now().Unix()),
		Member: orderID,
	}).Err()
}

// GetTodayTradeCount returns count of trades today
func (r *RedisRepository) GetTodayTradeCount(ctx context.Context, userID string) (int64, error) {
	key := fmt.Sprintf("user:%s:trades:daily", userID)
	today := time.Now().Truncate(24 * time.Hour).Unix()
	
	return r.client.ZCountByScore(ctx, key, &redis.ZRangeByScore{
		Min: fmt.Sprintf("%d", today),
		Max: "+inf",
	}).Result()
}

// AddDailyLoss adds loss amount to daily loss counter
func (r *RedisRepository) AddDailyLoss(ctx context.Context, userID string, loss float64) error {
	key := fmt.Sprintf("user:%s:loss:daily", userID)
	return r.client.IncrByFloat(ctx, key, loss).Err()
}

// GetDailyLoss returns cumulative daily loss
func (r *RedisRepository) GetDailyLoss(ctx context.Context, userID string) (float64, error) {
	key := fmt.Sprintf("user:%s:loss:daily", userID)
	val, err := r.client.Get(ctx, key).Float64()
	if err == redis.Nil {
		return 0, nil
	}
	return val, err
}

// SetPosition stores user position
func (r *RedisRepository) SetPosition(ctx context.Context, userID string, stockCode int64, quantity int, avgPrice float64) error {
	key := fmt.Sprintf("user:%s:positions:%d", userID, stockCode)
	
	return r.client.HSet(ctx, key,
		"quantity", quantity,
		"avg_price", avgPrice,
		"last_update", time.Now().Unix(),
	).Err()
}

// GetPosition retrieves user position
func (r *RedisRepository) GetPosition(ctx context.Context, userID string, stockCode int64) (map[string]interface{}, error) {
	key := fmt.Sprintf("user:%s:positions:%d", userID, stockCode)
	return r.client.HGetAll(ctx, key).Result()
}

// ResetDailyCounters resets daily counters at EOD
func (r *RedisRepository) ResetDailyCounters(ctx context.Context, userID string) error {
	tradeKey := fmt.Sprintf("user:%s:trades:daily", userID)
	lossKey := fmt.Sprintf("user:%s:loss:daily", userID)
	
	pipe := r.client.Pipeline()
	pipe.Del(ctx, tradeKey, lossKey)
	_, err := pipe.Exec(ctx)
	return err
}
```

### Step 4: Implement Risk Checker

**File**: `services/risk-management/internal/checker/pre_trade.go`

```go
package checker

import (
	"context"
	"fmt"

	pb "github.com/RohitIndira/Algo-Treading/api/proto/risk_management"
	"github.com/RohitIndira/Algo-Treading/services/risk-management/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/risk-management/internal/repository"
)

type PreTradeChecker struct {
	redisRepo *repository.RedisRepository
}

func NewPreTradeChecker(redisRepo *repository.RedisRepository) *PreTradeChecker {
	return &PreTradeChecker{
		redisRepo: redisRepo,
	}
}

// CheckPreTrade performs all pre-trade risk checks
func (p *PreTradeChecker) CheckPreTrade(ctx context.Context, req *pb.PreTradeRiskRequest) (*pb.PreTradeRiskResponse, error) {
	violations := []*pb.RiskViolation{}
	riskScore := 0.0

	// 1. Daily Trade Limit Check
	tradeCount, err := p.redisRepo.GetTodayTradeCount(ctx, req.UserId)
	if err == nil && tradeCount >= int64(req.MaxDailyTrades) {
		violations = append(violations, &pb.RiskViolation{
			Type:         pb.RiskViolationType_RISK_VIOLATION_DAILY_TRADE_LIMIT.String(),
			Message:      "Daily trade limit exceeded",
			CurrentValue: float64(tradeCount),
			LimitValue:   float64(req.MaxDailyTrades),
		})
		riskScore += 20.0
	}

	// 2. Daily Loss Limit Check
	dailyLoss, err := p.redisRepo.GetDailyLoss(ctx, req.UserId)
	if err == nil && dailyLoss > req.MaxLossPerDay {
		violations = append(violations, &pb.RiskViolation{
			Type:         pb.RiskViolationType_RISK_VIOLATION_DAILY_LOSS_LIMIT.String(),
			Message:      "Daily loss limit exceeded",
			CurrentValue: dailyLoss,
			LimitValue:   req.MaxLossPerDay,
		})
		riskScore += 25.0
	}

	// 3. Position Size Limit Check
	positionValue := float64(req.Quantity) * req.Price
	if positionValue > req.MaxPositionSize {
		violations = append(violations, &pb.RiskViolation{
			Type:         pb.RiskViolationType_RISK_VIOLATION_POSITION_SIZE_LIMIT.String(),
			Message:      "Position size exceeds limit",
			CurrentValue: positionValue,
			LimitValue:   req.MaxPositionSize,
		})
		riskScore += 15.0
	}

	// 4. Per-Trade Risk Limit Check
	potentialLoss := float64(req.Quantity) * (req.Price - req.StopLoss)
	if potentialLoss > req.MaxPerTradeRisk {
		violations = append(violations, &pb.RiskViolation{
			Type:         pb.RiskViolationType_RISK_VIOLATION_PER_TRADE_RISK_LIMIT.String(),
			Message:      "Per-trade risk exceeds limit",
			CurrentValue: potentialLoss,
			LimitValue:   req.MaxPerTradeRisk,
		})
		riskScore += 20.0
	}

	// Determine if approved
	approved := len(violations) == 0

	return &pb.PreTradeRiskResponse{
		Approved:    approved,
		Violations:  violations,
		RiskScore:   riskScore,
		Suggestions: p.generateSuggestions(violations),
	}, nil
}

func (p *PreTradeChecker) generateSuggestions(violations []*pb.RiskViolation) []string {
	suggestions := []string{}
	
	for _, v := range violations {
		switch v.Type {
		case pb.RiskViolationType_RISK_VIOLATION_DAILY_TRADE_LIMIT.String():
			suggestions = append(suggestions, "Reduce trading frequency or increase daily trade limit")
		case pb.RiskViolationType_RISK_VIOLATION_DAILY_LOSS_LIMIT.String():
			suggestions = append(suggestions, "Stop trading for today to avoid exceeding loss limit")
		case pb.RiskViolationType_RISK_VIOLATION_POSITION_SIZE_LIMIT.String():
			suggestions = append(suggestions, "Reduce position size")
		case pb.RiskViolationType_RISK_VIOLATION_PER_TRADE_RISK_LIMIT.String():
			suggestions = append(suggestions, "Increase stop loss distance or reduce quantity")
		}
	}
	
	return suggestions
}
```

### Step 5: Implement gRPC Server

**File**: `services/risk-management/internal/server/server.go`

```go
package server

import (
	"context"
	"fmt"
	"net"

	"google.golang.org/grpc"
	pb "github.com/RohitIndira/Algo-Treading/api/proto/risk_management"
	common "github.com/RohitIndira/Algo-Treading/api/proto/common"
	"github.com/RohitIndira/Algo-Treading/services/risk-management/internal/checker"
	"github.com/RohitIndira/Algo-Treading/services/risk-management/internal/repository"
)

type RiskManagementServer struct {
	pb.UnimplementedRiskManagementServiceServer
	preTradeChecker  *checker.PreTradeChecker
	redisRepo        *repository.RedisRepository
}

func NewRiskManagementServer(redisRepo *repository.RedisRepository) *RiskManagementServer {
	return &RiskManagementServer{
		preTradeChecker: checker.NewPreTradeChecker(redisRepo),
		redisRepo:       redisRepo,
	}
}

func (s *RiskManagementServer) CheckPreTradeRisk(ctx context.Context, req *pb.PreTradeRiskRequest) (*pb.PreTradeRiskResponse, error) {
	return s.preTradeChecker.CheckPreTrade(ctx, req)
}

func (s *RiskManagementServer) UpdatePostTradeMetrics(ctx context.Context, req *pb.PostTradeMetricsRequest) (*pb.PostTradeMetricsResponse, error) {
	// Increment trade counter
	if err := s.redisRepo.IncrementDailyTradeCount(ctx, req.UserId, req.OrderId); err != nil {
		return &pb.PostTradeMetricsResponse{
			Success: false,
			Error: &common.Error{
				Code:    500,
				Message: fmt.Sprintf("Failed to update metrics: %v", err),
			},
		}, nil
	}

	return &pb.PostTradeMetricsResponse{
		Success: true,
	}, nil
}

func (s *RiskManagementServer) HealthCheck(ctx context.Context, req *common.HealthCheckRequest) (*common.HealthCheckResponse, error) {
	return &common.HealthCheckResponse{
		Healthy: true,
		Service: "RiskManagementService",
		Version: "1.0.0",
	}, nil
}

// Start starts the gRPC server
func (s *RiskManagementServer) Start(port string) error {
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	server := grpc.NewServer()
	pb.RegisterRiskManagementServiceServer(server, s)

	fmt.Printf("Risk Management Server listening on port %s\n", port)
	return server.Serve(listener)
}
```

### Step 6: Create Main Entry Point

**File**: `services/risk-management/cmd/main.go`

```go
package main

import (
	"log"

	"github.com/RohitIndira/Algo-Treading/services/risk-management/config"
	"github.com/RohitIndira/Algo-Treading/services/risk-management/internal/repository"
	"github.com/RohitIndira/Algo-Treading/services/risk-management/internal/server"
)

func main() {
	// Load configuration
	cfg := config.LoadConfig()

	// Initialize Redis
	redisAddr := cfg.RedisHost + ":" + cfg.RedisPort
	redisRepo := repository.NewRedisRepository(redisAddr)

	// Create server
	riskServer := server.NewRiskManagementServer(redisRepo)

	// Start server
	if err := riskServer.Start(cfg.GRPCPort); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
```

---

## 🚀 Running the Service

### Prerequisites

```bash
# Ensure Redis is running
docker run -d -p 6379:6379 redis:latest

# Ensure PostgreSQL is running
docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=password postgres:latest
```

### Start Service

```bash
cd services/risk-management

# Install dependencies
go mod download

# Build
go build -o ../../bin/risk-management ./cmd/main.go

# Run
./../../bin/risk-management
```

### Configuration via Environment Variables

```bash
export GRPC_PORT=9005
export REDIS_HOST=localhost
export REDIS_PORT=6379
export DB_HOST=localhost
export MAX_DAILY_TRADES=100
export MAX_DAILY_LOSS=10000.0
export CIRCUIT_BREAKER_LOSS_PCT=5.0

go run cmd/main.go
```

---

## 📡 Usage Examples

### Pre-Trade Risk Check

```go
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
		UserId:           "user123",
		StrategyId:       "strategy456",
		StockCode:        1200, // HDFC
		Quantity:         100,
		Price:            150.50,
		StopLoss:         145.00,
		MaxDailyTrades:   100,
		MaxLossPerDay:    10000,
		MaxPositionSize:  50000,
		MaxPerTradeRisk:  2000,
	}

	resp, err := client.CheckPreTradeRisk(context.Background(), req)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	log.Printf("Approved: %v", resp.Approved)
	log.Printf("Risk Score: %v", resp.RiskScore)
	log.Printf("Violations: %v", resp.Violations)
}
```

---

## 🧪 Testing

### Unit Tests

```go
// services/risk-management/internal/checker/pre_trade_test.go

package checker

import (
	"context"
	"testing"

	pb "github.com/RohitIndira/Algo-Treading/api/proto/risk_management"
	"github.com/RohitIndira/Algo-Treading/services/risk-management/internal/repository"
)

func TestDailyTradeLimit(t *testing.T) {
	redisRepo := repository.NewRedisRepository("localhost:6379")
	checker := NewPreTradeChecker(redisRepo)

	ctx := context.Background()

	req := &pb.PreTradeRiskRequest{
		UserId:         "testuser",
		MaxDailyTrades: 1, // Set limit to 1
		Quantity:       100,
		Price:          150.50,
		StopLoss:       145.00,
	}

	// First call should pass
	resp1, _ := checker.CheckPreTrade(ctx, req)
	if !resp1.Approved {
		t.Fatal("First trade should be approved")
	}

	// Update counter
	redisRepo.IncrementDailyTradeCount(ctx, req.UserId, "order1")

	// Second call should fail
	resp2, _ := checker.CheckPreTrade(ctx, req)
	if resp2.Approved {
		t.Fatal("Second trade should be rejected (limit exceeded)")
	}
}
```

---

## 📊 Monitoring & Observability

### Key Metrics to Track

1. **Pre-Trade Check Latency**: Response time for risk validation
2. **Violation Rate**: Percentage of orders rejected due to risk
3. **Redis Latency**: Response time from Redis operations
4. **Database Latency**: Response time from PostgreSQL queries
5. **Circuit Breaker Triggers**: Count of trading halts

### Prometheus Metrics Example

```go
riskCheckDuration := prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name: "risk_check_duration_seconds",
		Help: "Time taken for pre-trade risk check",
	},
	[]string{"check_type", "result"},
)

violationCounter := prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "risk_violations_total",
		Help: "Total number of risk violations",
	},
	[]string{"violation_type"},
)
```

---

## 🔗 Integration Points

### 1. Trade Execution Service
- **Calls**: `CheckPreTradeRisk` before submitting order
- **Receives**: Approval/rejection with violations

### 2. API Gateway
- **Calls**: `GetRiskMetrics` to display user dashboard
- **Calls**: `SetRiskLimits` to update limits

### 3. Data Ingestion Service
- **Updates**: Position data via Redis when prices change
- **Triggers**: Risk re-evaluation for updated positions

### 4. User Config Service
- **Reads**: Risk limits from PostgreSQL
- **Updates**: Risk limits stored in PostgreSQL

---

## 📝 Best Practices

### ✅ DO:
- ✅ Cache risk limits in memory with TTL
- ✅ Use Redis for real-time counters (fast)
- ✅ Store audit trail in PostgreSQL (persistence)
- ✅ Reset daily counters at market close
- ✅ Monitor latency (target: <10ms per check)
- ✅ Log all violations for compliance

### ❌ DON'T:
- ❌ Query database for every trade check
- ❌ Block on external API calls in risk check
- ❌ Store positions only in Redis (non-persistent)
- ❌ Ignore circuit breaker triggers
- ❌ Allow trades without pre-trade checks

---

## 🐛 Troubleshooting

### Issue: Pre-trade checks always fail

**Solution**: 
1. Verify Redis is running: `redis-cli ping`
2. Check risk limits are set: `redis-cli GET user:{user_id}:limits`
3. Review logs for specific violation type

### Issue: Slow risk checks (>100ms)

**Solution**:
1. Check Redis latency: `redis-cli --latency`
2. Profile gRPC calls with pprof
3. Consider caching limits locally

### Issue: Daily counters not resetting

**Solution**:
1. Ensure EOD job calls `ResetDailyCounters`
2. Check cron job scheduling
3. Verify Redis keys expire correctly

---

## 📚 References

- [Protocol Buffer Definitions](./proto-definitions.md)
- [Trading System Architecture](./trading-system-architecture.md)
- [Directory Structure](./directory-structure.md)
- [gRPC Go Documentation](https://grpc.io/docs/languages/go/)
- [Redis Documentation](https://redis.io/documentation)

---

## ✅ Checklist

- [ ] Create `config.go` with configuration loading
- [ ] Define models in `models/risk_metrics.go` and `models/limits.go`
- [ ] Implement Redis repository in `repository/redis.go`
- [ ] Implement PostgreSQL repository in `repository/postgres.go`
- [ ] Implement pre-trade checker in `checker/pre_trade.go`
- [ ] Implement post-trade checker in `checker/post_trade.go`
- [ ] Implement gRPC server in `server/server.go`
- [ ] Create main entry point in `cmd/main.go`
- [ ] Write unit tests for all checkers
- [ ] Add integration tests
- [ ] Set up monitoring metrics
- [ ] Create Dockerfile for deployment
- [ ] Document API endpoints
- [ ] Test with Trade Execution service

