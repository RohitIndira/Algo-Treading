# Risk Management Service

## Overview
The Risk Management Service is a critical component of the algorithmic trading system that performs:
- **Pre-trade risk validation** - Checks orders before they're sent to the exchange
- **Post-trade monitoring** - Updates metrics after trade execution
- **Position tracking** - Monitors open positions and portfolio exposure
- **Risk limit enforcement** - Enforces user-defined and system risk limits

## Service Details
- **Port**: 9005 (gRPC)
- **Protocol**: gRPC
- **Storage**: Redis (for real-time metrics)

## What the Service Does

### 1. Pre-Trade Risk Checks
Before any order is submitted to the exchange, the service validates:
- ✓ Daily trade count limits
- ✓ Daily loss limits
- ✓ Position size limits
- ✓ Per-trade risk limits
- ✓ Duplicate order detection
- ✓ Portfolio concentration limits

### 2. Post-Trade Updates
After trade execution:
- Updates daily trade counters
- Tracks realized/unrealized P&L
- Updates position information
- Monitors portfolio exposure

### 3. Risk Metrics Tracking
Real-time monitoring of:
- Daily P&L (profit/loss)
- Open positions count
- Portfolio exposure percentage
- Drawdown metrics
- Risk violations

## How to Test the Service

### Option 1: Run the Test Client (Recommended)
```powershell
# In a new terminal, while the service is running
go run .\services\risk-management\test_client.go
```

This will:
1. Check service health
2. Set risk limits for a test user
3. Perform pre-trade risk checks
4. Simulate trade execution
5. Get risk metrics
6. Get user positions
7. Test a risk violation scenario

### Option 2: Use grpcurl (Command-line gRPC client)

First, install grpcurl:
```powershell
# Using Go
go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest
```

Then test the service:

```powershell
# Health Check
grpcurl -plaintext localhost:9005 risk_management.RiskManagementService/HealthCheck

# Set Risk Limits
grpcurl -plaintext -d '{
  "limits": {
    "user_id": "user-123",
    "max_daily_trades": 10,
    "max_daily_loss": 5000.0,
    "max_position_size": 50000.0
  }
}' localhost:9005 risk_management.RiskManagementService/SetRiskLimits

# Check Pre-Trade Risk
grpcurl -plaintext -d '{
  "user_id": "user-123",
  "strategy_id": "strategy-1",
  "stock_code": 3456,
  "exchange": "EXCHANGE_NSE",
  "order_type": "ORDER_TYPE_LIMIT",
  "order_side": "ORDER_SIDE_BUY",
  "quantity": 10,
  "price": 2500.50,
  "max_daily_trades": 10,
  "max_loss_per_day": 5000.0,
  "max_position_size": 50000.0
}' localhost:9005 risk_management.RiskManagementService/CheckPreTradeRisk

# Get Risk Metrics
grpcurl -plaintext -d '{"user_id": "user-123"}' localhost:9005 risk_management.RiskManagementService/GetRiskMetrics
```

### Option 3: Integration with Other Services

The service is called by:
- **Trade Execution Service** - Before submitting orders
- **Rules Engine** - When evaluating trading rules
- **API Gateway** - For risk dashboard and monitoring

## When Does It Execute?

The Risk Management Service executes:

1. **On Every Order Request** (Pre-Trade)
   - Before any order is sent to the broker/exchange
   - Validates the order against all risk limits
   - Returns approval/rejection with reasons

2. **After Trade Execution** (Post-Trade)
   - When an order is filled
   - Updates position and P&L metrics
   - Recalculates risk exposure

3. **On-Demand Queries**
   - When fetching current risk metrics
   - When retrieving position information
   - During risk limit updates

4. **Scheduled Tasks** (if implemented)
   - Daily counter resets (EOD)
   - Position mark-to-market updates
   - Risk report generation

## Architecture Flow

```
Trading Strategy
       ↓
Rules Engine → [Pre-Trade Check] → Risk Management Service
       ↓                                    ↓
   Approved?                          Check Redis
       ↓                                    ↓
Trade Execution ← [YES/NO + Violations] ← Return
       ↓
   Order Filled
       ↓
[Post-Trade Update] → Risk Management Service → Update Redis
```

## Configuration

Check `services/risk-management/config/config.go` for:
- Redis connection settings
- gRPC port configuration
- Default risk limits

## Dependencies

- **Redis** - For storing real-time risk metrics and positions
- **Protocol Buffers** - For gRPC communication
- **Other Services** - User Config, Trade Execution

## Monitoring

To monitor the service:
1. Check logs for risk violations
2. Monitor Redis for user metrics
3. Track gRPC call success rates
4. Monitor daily reset operations

## Common Use Cases

1. **Risk Violation Alert**: When a user exceeds daily loss limit
2. **Position Limit**: Preventing over-concentration in single stock
3. **Circuit Breaker**: Stopping all trading after severe loss
4. **Daily Limit**: Enforcing maximum number of trades per day
