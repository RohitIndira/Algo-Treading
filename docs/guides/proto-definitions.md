# Protocol Buffer Definitions Guide

This document describes the Protocol Buffer (protobuf) definitions used for gRPC communication between microservices in the trading system.

## Overview

The system uses Protocol Buffers v3 (proto3) for defining service contracts and message types. All `.proto` files are located in the `api/proto/` directory.

## Directory Structure

```
api/proto/
├── Makefile                          # Build script for generating Go code
├── common/
│   └── common.proto                  # Shared types and enums
├── user_config/
│   └── user_config.proto            # User Configuration Service
├── risk_management/
│   └── risk_management.proto        # Risk Management Service
└── trade_execution/
    └── trade_execution.proto        # Trade Execution Service
```

## Common Types (`common.proto`)

Shared message types and enums used across all services.

### Key Types

- **Timestamp**: Consistent time representation across services
- **PaginationRequest/Response**: Standardized pagination
- **Error**: Structured error reporting
- **Stock**: Stock information structure
- **OrderType, OrderStatus, OrderSide**: Trading enums
- **Sentiment, Exchange**: Market data enums
- **HealthCheckRequest/Response**: Service health monitoring

### Usage Example

```protobuf
import "api/proto/common/common.proto";

message MyMessage {
  common.Timestamp created_at = 1;
  common.OrderStatus status = 2;
}
```

## User Configuration Service

**Package**: `user_config`  
**File**: `api/proto/user_config/user_config.proto`

### Purpose
Manages user trading strategies and preferences.

### Service Methods

| Method | Description |
|--------|-------------|
| `CreateStrategy` | Create a new trading strategy |
| `UpdateStrategy` | Update existing strategy |
| `DeleteStrategy` | Delete a strategy |
| `GetStrategy` | Retrieve strategy details |
| `ListUserStrategies` | List all user strategies with pagination |
| `ActivateStrategy` | Activate a strategy |
| `DeactivateStrategy` | Deactivate a strategy |
| `GetStrategiesByIDs` | Bulk fetch strategies (internal use) |
| `HealthCheck` | Service health status |

### Key Message Types

#### Strategy
Complete trading strategy configuration including:
- User identification
- Strategy conditions (impact score, sentiment, categories, stocks)
- Trade configuration (order type, quantity, stop loss/take profit)
- Risk limits (daily trades, max loss, position sizing)

#### StrategyConditions
Filter criteria for matching market events:
- Impact score threshold (1-10)
- Sentiment filters (positive, neutral, negative)
- News categories
- Stock codes and exchanges
- Price range and volume thresholds
- Percentage change threshold

#### TradeConfig
Execution parameters:
- Order type (MARKET, LIMIT)
- Quantity and position sizing
- Stop loss and take profit percentages
- Exchange preference

#### RiskLimits
Risk management constraints:
- Max daily trades
- Max daily loss
- Position sizing strategy
- Portfolio exposure limits

## Risk Management Service

**Package**: `risk_management`  
**File**: `api/proto/risk_management/risk_management.proto`

### Purpose
Provides pre-trade risk checks and post-trade monitoring.

### Service Methods

| Method | Description |
|--------|-------------|
| `CheckPreTradeRisk` | Validate order against risk limits |
| `UpdatePostTradeMetrics` | Update metrics after execution |
| `GetRiskMetrics` | Retrieve current risk metrics |
| `SetRiskLimits` | Configure custom risk limits |
| `ResetDailyCounters` | Reset daily risk counters (EOD) |
| `GetUserPositions` | Fetch current positions |
| `HealthCheck` | Service health status |

### Key Message Types

#### PreTradeRiskRequest
Contains order details for risk validation:
- User and strategy IDs
- Stock and order information
- Stop loss and take profit
- Risk limits to check against

#### PreTradeRiskResponse
Risk check results:
- Approval status
- Violations list (if any)
- Risk score (0-100)
- Suggested actions

#### RiskViolationType
Enum of possible violations:
- Daily trade limit
- Daily loss limit
- Position size limit
- Per-trade risk limit
- Duplicate order
- Insufficient margin
- Circuit breaker
- Concentration limit

#### RiskMetrics
Comprehensive risk statistics:
- Daily counters (trades, P&L)
- Position metrics (count, value, unrealized P&L)
- Portfolio metrics (exposure, concentration)
- Risk indicators (drawdown, Sharpe ratio)
- Recent violations

#### Position
Current position details:
- Stock information
- Quantity and average price
- Unrealized P&L and percentage
- Investment and current value

## Trade Execution Service

**Package**: `trade_execution`  
**File**: `api/proto/trade_execution/trade_execution.proto`

### Purpose
Handles order execution via Odin API and order lifecycle management.

### Service Methods

| Method | Description |
|--------|-------------|
| `GetOrderStatus` | Get order status by ID |
| `GetUserOrders` | List user orders with filters |
| `CancelOrder` | Cancel a pending order |
| `ModifyOrder` | Modify order parameters |
| `GetOrderHistory` | Retrieve historical orders |
| `GetOrderStatistics` | Get order statistics |
| `HealthCheck` | Service health status |

### Key Message Types

#### Order
Complete order information:
- Order identification (internal and Odin IDs)
- User, strategy, and event IDs
- Stock and exchange details
- Order parameters (type, side, quantity, price)
- Stop loss and take profit
- Execution details (filled quantity, price, commission)
- Status and timestamps
- Error information

#### OrderRequest
Message published to RabbitMQ by Rules Engine:
- Request ID for idempotency
- User and strategy context
- Stock and order details
- Risk approval status
- Retry information

#### OrderFilter
Flexible filtering options:
- Status filters
- Exchange filters
- Date range
- Stock codes
- Strategy IDs
- Order side

#### OrderStatistics
Comprehensive trading statistics:
- Order counts by status
- Fill and rejection rates
- Trading volume metrics
- Execution timing (avg, p95)
- Breakdown by exchange, type, and strategy

## Code Generation

### Prerequisites

```bash
# Install protoc compiler
# On Ubuntu/Debian:
sudo apt install -y protobuf-compiler

# On macOS:
brew install protobuf

# Install Go plugins
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

### Generate Go Code

```bash
# Navigate to proto directory
cd api/proto

# Install tools (first time only)
make install-tools

# Generate all proto files
make generate-all

# Or generate specific service
make generate-common
make generate-user-config
make generate-risk-management
make generate-trade-execution

# Clean generated files
make clean
```

### Generated Files

For each `.proto` file, two Go files are generated:
- `*.pb.go` - Message type definitions
- `*_grpc.pb.go` - Service interfaces and client/server stubs

Example:
```
api/proto/user_config/
├── user_config.proto
├── user_config.pb.go          # Generated
└── user_config_grpc.pb.go     # Generated
```

## Usage in Go Services

### Server Implementation

```go
package main

import (
    pb "github.com/yourusername/trading-system/api/proto/user_config"
    "google.golang.org/grpc"
)

type userConfigServer struct {
    pb.UnimplementedUserConfigServiceServer
}

func (s *userConfigServer) CreateStrategy(
    ctx context.Context,
    req *pb.CreateStrategyRequest,
) (*pb.CreateStrategyResponse, error) {
    // Implementation
}

func main() {
    lis, _ := net.Listen("tcp", ":50051")
    grpcServer := grpc.NewServer()
    pb.RegisterUserConfigServiceServer(grpcServer, &userConfigServer{})
    grpcServer.Serve(lis)
}
```

### Client Usage

```go
package main

import (
    pb "github.com/yourusername/trading-system/api/proto/user_config"
    "google.golang.org/grpc"
)

func main() {
    conn, _ := grpc.Dial("localhost:50051", grpc.WithInsecure())
    defer conn.Close()
    
    client := pb.NewUserConfigServiceClient(conn)
    
    resp, err := client.CreateStrategy(context.Background(), &pb.CreateStrategyRequest{
        UserId: "user123",
        StrategyName: "My Strategy",
        // ... other fields
    })
}
```

## Best Practices

### 1. Versioning
- Use semantic versioning for breaking changes
- Consider creating new package versions (e.g., `user_config.v2`) for major changes
- Maintain backward compatibility when possible

### 2. Field Numbering
- Never reuse field numbers
- Reserve field numbers for deprecated fields
- Use field numbers 1-15 for frequently used fields (more efficient encoding)

### 3. Optional Fields
- Use `optional` keyword for truly optional fields in proto3
- Consider using wrapper types for nullable primitives

### 4. Error Handling
- Use the `Error` message type consistently
- Provide meaningful error codes and messages
- Include contextual details in the `details` field

### 5. Naming Conventions
- Use CamelCase for message names
- Use snake_case for field names
- Prefix enum values with the enum name

### 6. Documentation
- Add comments to all messages and fields
- Document service methods with expected behavior
- Include examples where helpful

## Inter-Service Communication Flow

### User Config ↔ Rules Engine
```
Rules Engine → GetStrategiesByIDs → User Config
              ← Strategy list ←
```

### Trade Execution ↔ Risk Management
```
Trade Execution → CheckPreTradeRisk → Risk Management
                ← Risk approval ←
                → UpdatePostTradeMetrics →
                ← Success ←
```

### API Gateway ↔ All Services
```
API Gateway → gRPC methods → Services
            ← Responses ←
```

## Testing

### Unit Testing Generated Code

```go
func TestStrategyCreation(t *testing.T) {
    strategy := &pb.Strategy{
        StrategyId: "test-123",
        UserId: "user-456",
        Active: true,
        // ... other fields
    }
    
    // Test serialization
    data, err := proto.Marshal(strategy)
    assert.NoError(t, err)
    
    // Test deserialization
    var decoded pb.Strategy
    err = proto.Unmarshal(data, &decoded)
    assert.NoError(t, err)
    assert.Equal(t, strategy.StrategyId, decoded.StrategyId)
}
```

## Troubleshooting

### Common Issues

1. **Import not found**
   - Ensure `--proto_path` includes the root directory
   - Check relative import paths

2. **Plugin not found**
   - Verify Go plugins are installed
   - Check `$GOPATH/bin` is in `$PATH`

3. **Version mismatch**
   - Ensure protoc compiler and Go plugins are compatible
   - Update both to latest versions

4. **Generated files not updating**
   - Run `make clean` before regenerating
   - Check file permissions

## Additional Resources

- [Protocol Buffers Documentation](https://protobuf.dev/)
- [gRPC Go Tutorial](https://grpc.io/docs/languages/go/)
- [Protocol Buffers Style Guide](https://protobuf.dev/programming-guides/style/)
- [gRPC Best Practices](https://grpc.io/docs/guides/performance/)

## Next Steps

After generating the proto code:
1. Implement gRPC servers for each service
2. Add business logic in service implementations
3. Configure service endpoints in configuration files
4. Set up service discovery and load balancing
5. Implement comprehensive testing
6. Add monitoring and observability
