# Auto Square-Off Implementation Guide

## Current Status

### ✅ What EXISTS in the System

The auto square-off mechanism is **partially implemented** but **NOT ACTIVELY RUNNING**. Here's what's already in place:

#### 1. **Data Model** (User Config Service)
- **File**: `services/user-config/internal/models/strategy.go`
- **Fields in RiskLimits struct**:
  ```go
  EnableAutoSquareOff bool   // Flag to enable/disable feature
  AutoSquareOffTime   string // Time in "HH:MM" format (e.g., "15:05")
  ```
- **Proto Definition**: `api/proto/user_config/user_config.proto`
- **Database**: Fields exist in PostgreSQL `risk_limits` table

#### 2. **Scheduler Implementation** (Trade Execution Service)
- **File**: `services/trade-execution/internal/scheduler/auto_square_off.go`
- **Features**:
  - `AutoSquareOffScheduler` struct manages the scheduling
  - Runs on a 1-minute check interval
  - Only executes on weekdays (Monday-Friday)
  - Parses time in "HH:MM" format
  - Default square-off time: **15:05 (3:05 PM)**
  - Gets all open/partially filled orders (INTRADAY products only)
  - Creates reverse orders with market order type and IOC validity
  - Executes square-off for all clients simultaneously

---

## ❌ What's MISSING / NOT IMPLEMENTED

### Critical Gap: Scheduler is NOT Integrated into Main Service

**The scheduler is DEFINED but NOT INSTANTIATED or STARTED** in the trade execution service.

**File**: `services/trade-execution/cmd/main.go`

Currently initializes:
- ✅ Kafka consumer
- ✅ RabbitMQ consumer  
- ✅ gRPC server
- ❌ **Auto Square-Off Scheduler** (NOT INITIALIZED)

---

## Implementation Steps

### Step 1: Update Main Service to Integrate Scheduler

**File**: `services/trade-execution/cmd/main.go`

Add scheduler initialization in the `main()` function after initializing the order executor:

```go
// Initialize Auto Square-Off Scheduler
import (
    "github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/scheduler"
)

// After line ~52 (after orderExecutor initialization)
autoSquareOffScheduler := scheduler.NewAutoSquareOffScheduler(
    orderRepo,
    credsRepo,
    orderExecutor,
    cfg.AutoSquareOffTime,
)
log.Println("✓ Auto Square-Off scheduler initialized")
```

Add `AutoSquareOffTime` to Config struct:

```go
type Config struct {
    // ... existing fields ...
    AutoSquareOffTime string // Format: "15:05"
}
```

Add to `loadConfig()` function:

```go
func loadConfig() Config {
    // ... existing code ...
    return Config{
        // ... existing fields ...
        AutoSquareOffTime: getEnv("AUTO_SQUARE_OFF_TIME", "15:05"),
    }
}
```

### Step 2: Start the Scheduler

In the `main()` function, start the scheduler as a goroutine with the other services:

```go
// Start Auto Square-Off Scheduler (around line ~145)
go func() {
    log.Println("Starting Auto Square-Off Scheduler...")
    if err := autoSquareOffScheduler.Start(ctx); err != nil {
        log.Printf("Auto Square-Off Scheduler error: %v", err)
    }
}()
```

Add to graceful shutdown section to stop the scheduler:

```go
// Before cancel() call
autoSquareOffScheduler.Stop()
```

### Step 3: Environment Configuration

Add to `.env` file:

```env
# Auto Square-Off Configuration
AUTO_SQUARE_OFF_TIME=15:05
```

### Step 4: Database Verification

Ensure the `risk_limits` table has these columns:

```sql
ALTER TABLE risk_limits ADD COLUMN IF NOT EXISTS enable_auto_square_off BOOLEAN DEFAULT false;
ALTER TABLE risk_limits ADD COLUMN IF NOT EXISTS auto_square_off_time VARCHAR(5) DEFAULT '15:05';
```

---

## How It Works (After Implementation)

### Flow Diagram

```
1. User creates strategy with:
   - enable_auto_square_off = true
   - auto_square_off_time = "15:05"
   ↓
2. AutoSquareOffScheduler runs every minute checking current time
   ↓
3. When current time matches square-off time (15:05):
   - Fetch all INTRADAY orders with FILLED or PARTIALLY_FILLED status
   - For each filled order:
     * Create reverse order (opposite side)
     * Use MARKET order type with IOC validity
     * Bypass risk checks (RiskApproved = true)
   ↓
4. Reverse orders execute immediately for all clients
   ↓
5. All open positions are closed (squared off)
```

### Example Execution

**Scenario**: User has:
- Long 100 shares of INFY at 15:00
- Market close time set to 15:05

**At 15:05**:
1. Scheduler detects time match
2. Finds the 100 INFY long position (FILLED status)
3. Creates reverse order:
   - Side: SELL
   - Quantity: 100
   - Type: MARKET
   - Validity: IOC
4. Order executes immediately at market price
5. Position is now squared (0 quantity)

---

## Client-Specific Configuration

Each client can have **different square-off times** via the risk limits:

```json
{
  "client_id": "CLIENT_001",
  "strategy": {
    "enable_auto_square_off": true,
    "auto_square_off_time": "14:30"  // Client-specific time
  }
}
```

The system queries the database per client and creates reverse orders accordingly.

---

## Required Database Schema

### risk_limits Table

```sql
CREATE TABLE IF NOT EXISTS risk_limits (
    risk_limit_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    strategy_id UUID NOT NULL REFERENCES strategies(strategy_id),
    max_daily_trades INT,
    max_loss_per_day DECIMAL(15,2),
    position_sizing VARCHAR(50),
    max_portfolio_exposure_pct DECIMAL(5,2),
    max_per_trade_risk DECIMAL(15,2),
    enable_risk_checks BOOLEAN DEFAULT true,
    enable_auto_square_off BOOLEAN DEFAULT false,
    auto_square_off_time VARCHAR(5),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (strategy_id) REFERENCES strategies(strategy_id)
);
```

---

## Testing the Implementation

### 1. Enable for a Strategy

```bash
curl -X POST http://localhost:9003/api/strategies \
  -H "Content-Type: application/json" \
  -d '{
    "strategy_name": "Test Square-Off",
    "enable_auto_square_off": true,
    "auto_square_off_time": "15:05"
  }'
```

### 2. Create Test Order

Create an INTRADAY long position before 15:05

### 3. Monitor Logs

```bash
# Watch trade execution service logs
docker logs -f trade-execution-service
```

Expected output at 15:05:
```
Auto Square-Off Time Reached - Initiating square-off for all open positions
Found X open orders to square off
Squaring off order ORDER_ID for user USER_ID (Symbol: SYMBOL, Qty: 100)
Successfully created and executed square-off order
Auto Square-Off: Complete (Success: X, Failed: Y)
```

---

## Configuration Options

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `AUTO_SQUARE_OFF_TIME` | `15:05` | Global default square-off time (HH:MM format) |

### Per-Strategy Override

Via User Config Service, each strategy can override the global time.

---

## Edge Cases & Considerations

### ✅ Handled Cases

1. **Weekends**: Scheduler skips Saturday & Sunday
2. **No Open Orders**: Logs and continues without error
3. **Partial Execution Failure**: Records success count and logs failures
4. **Time Parsing Errors**: Defaults to 15:05 and logs error
5. **Risk Checks Bypass**: Auto square-off orders bypass risk checks (RiskApproved = true)

### ⚠️ To Consider

1. **Market Hours**: Ensure square-off time is within market hours (typically 09:15 - 15:30 IST for Indian markets)
2. **Order Execution Timing**: Market orders execute at best available price at that moment
3. **Slippage**: Price might differ from last trading price due to market conditions
4. **Failed Executions**: Logs failures but doesn't retry automatically (can be enhanced)

---

## Enhancement Opportunities

1. **Per-User Override**: Allow users to modify square-off time via UI
2. **Retry Logic**: Add retry mechanism for failed square-offs
3. **Notifications**: Send email/SMS alerts when auto square-off executes
4. **Audit Trail**: Store square-off events in audit table for compliance
5. **Price Limits**: Allow setting price limits for market orders
6. **Partial Square-Off**: Allow squaring off only a percentage of position
7. **Exclusion Rules**: Allow excluding certain symbols from auto square-off

---

## Files to Modify for Full Implementation

1. **`services/trade-execution/cmd/main.go`**
   - Add scheduler initialization
   - Add to Config struct
   - Start as goroutine
   - Add to shutdown

2. **`.env`** (or config)
   - Add AUTO_SQUARE_OFF_TIME

3. **Database Migrations** (if not already applied)
   - Ensure columns exist in risk_limits table

---

## Summary

| Aspect | Status |
|--------|--------|
| Data Model | ✅ Complete |
| Scheduler Logic | ✅ Complete |
| Integration in Main Service | ❌ **TODO** |
| Database Schema | ✅ Complete |
| Environment Config | ⚠️ Partial |
| Testing | ⏳ Pending |

**Priority**: Integrate scheduler into `main.go` to activate the feature.
