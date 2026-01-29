# Paper Trading System - Implementation Summary

## ✅ What Has Been Fixed

The paper trading system has been completely implemented to handle **52-week breakout strategy** in simulation mode without placing real orders.

---

## 📋 Files Created

### 1. Database Migrations
- **`services/trade-execution/migrations/003_create_paper_positions_table.sql`**
  - Creates `paper_positions` table for tracking open/closed positions
  - Creates `paper_pnl_history` table for realized PnL tracking
  - Creates `user_daily_paper_pnl` view for daily summaries
  - Indexes for efficient queries

### 2. Models
- **`services/trade-execution/internal/models/paper_position.go`**
  - `PaperPosition` struct with all position fields
  - `PaperPnLHistory` struct for closed positions
  - `UserDailyPaperPnL` for daily aggregates
  - Helper methods: `CalculatePnL()`, `ShouldTriggerStopLoss()`, `ShouldTriggerTakeProfit()`

### 3. Repository Layer
- **`services/trade-execution/internal/repository/paper_position_repository.go`**
  - Full CRUD operations for paper positions
  - `Create()`, `Get()`, `GetOpenPositions()`, `GetAllOpenPositions()`
  - `UpdatePrice()` - Updates current price and calculates PnL
  - `ClosePosition()` - Closes position and records PnL history
  - `GetUserDailyPnL()`, `GetTotalRealizedPnL()`

### 4. Position Management
- **`services/trade-execution/internal/paper/position_manager.go`**
  - Background service that monitors all open paper positions
  - Checks SL/TP every 10 seconds (configurable)
  - Automatically closes positions when triggered
  - Creates SELL orders and records PnL
  - Methods: `Start()`, `Stop()`, `CreatePosition()`, `ClosePosition()`, `GetUserPnL()`

### 5. Price Provider
- **`services/trade-execution/internal/paper/redis_price_provider.go`**
  - Gets live market prices from Redis
  - Supports multiple key patterns
  - Batch price fetching for efficiency
  - Fallback mechanisms if primary key pattern not found

### 6. Integration
- **`services/trade-execution/internal/executor/paper_trade_handler.go`**
  - Handles paper order processing
  - Creates positions for BUY orders
  - Integrates with position manager
  - Methods: `ProcessPaperOrder()`, `GetUserPositions()`, `GetUserPnL()`

### 7. Documentation
- **`docs/guides/PAPER_TRADING_GUIDE.md`**
  - Complete setup and usage guide
  - Database schema documentation
  - Testing checklist
  - Troubleshooting guide
  - API endpoints specification

---

## 🔧 Files Modified

### 1. Signal Processor
**`services/trade-execution/internal/executor/signal_processor.go`**
- Added `paperTradeHandler` field
- Modified `ProcessTradeSignal()` to handle PAPER mode properly
- Paper signals now create orders AND positions
- Added paper order processing flow

**Changes:**
```go
// Before: Skipped paper signals completely
if mode == "PAPER" {
    return nil  // ❌ Nothing happened
}

// After: Process paper signals properly
if mode == "PAPER" {
    order := convertSignalToOrder(signal)
    orderRepo.Create(ctx, order)  // ✅ Save to DB
    paperTradeHandler.ProcessPaperOrder(ctx, order)  // ✅ Create position
    return nil
}
```

### 2. Cash 52W Engine
**`services/rules-engine/internal/cash52w/engine.go`**
- Added comment explaining paper position management
- Clarified that trade-execution handles persistent positions
- No breaking changes to existing logic

---

## 🎯 Key Features Implemented

### ✅ 1. Order Storage
- Paper orders saved to `orders` table with `trading_mode='PAPER'`
- Orders marked as `FILLED` immediately (simulated execution)
- No real broker calls for paper mode

### ✅ 2. Position Tracking
- Persistent positions in `paper_positions` table
- Survives service restarts
- Tracks entry price, current price, quantity
- Stop loss and take profit levels stored

### ✅ 3. Automated Stop Loss
- Background monitor checks positions every 10 seconds
- Gets live prices from Redis
- Triggers when: `current_price <= stop_loss`
- Creates SELL order automatically
- Closes position and records PnL

### ✅ 4. Automated Take Profit
- Monitors same as stop loss
- Triggers when: `current_price >= take_profit`
- Creates SELL order automatically
- Closes position with profit

### ✅ 5. Live PnL Calculation
- **Unrealized PnL**: `(current_price - entry_price) × quantity`
- **Unrealized PnL %**: `((current_price - entry_price) / entry_price) × 100`
- Updated every monitoring cycle (10 seconds)
- Stored in database

### ✅ 6. Realized PnL Tracking
- Recorded when position closes
- Stored in `paper_pnl_history` table
- Tracks exit reason (STOP_LOSS, TAKE_PROFIT, MANUAL)
- Daily aggregates available via view

---

## 🏗️ Architecture

```
┌──────────────────┐
│   Market Data    │
│   (52W Breakout) │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  Rules Engine    │
│  (Cash 52W)      │
│  - Check mode    │
│  - Create signal │
└────────┬─────────┘
         │
         ▼ (Kafka: trade-signals)
┌──────────────────┐
│ Trade Execution  │
│ Signal Processor │
│  - Save order    │
│  - Create pos    │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Paper Position   │
│    Manager       │
│ - Monitor SL/TP  │
│ - Update PnL     │
│ - Auto-close     │
└──────────────────┘
         │
         ▼ (Every 10s)
┌──────────────────┐
│      Redis       │
│  (Live Prices)   │
└──────────────────┘
```

---

## 🚀 How to Use

### Step 1: Run Database Migration
```powershell
cd services/trade-execution
psql -U postgres -d algo_trading_db -f migrations/003_create_paper_positions_table.sql
```

### Step 2: Configure User for Paper Trading
```bash
POST /api/v1/strategies/cash52w/configure
{
  "user_id": "user123",
  "enabled": true,
  "capital_per_stock": 20000,
  "trading_mode": "PAPER"  # ← Set to PAPER
}
```

### Step 3: Start Services
Ensure all services are running:
- PostgreSQL
- Redis (with market data)
- Kafka
- Rules Engine
- Trade Execution
- Data Ingestion

### Step 4: Monitor Paper Trades
```sql
-- Check paper orders
SELECT * FROM orders 
WHERE user_id = 'user123' AND trading_mode = 'PAPER';

-- Check open positions
SELECT * FROM paper_positions 
WHERE user_id = 'user123' AND status = 'OPEN';

-- Check PnL history
SELECT * FROM paper_pnl_history 
WHERE user_id = 'user123';
```

---

## 🔍 What's Checked

### When User Sets PAPER Mode:

1. ✅ **No Real Orders**: Orders NOT sent to RabbitMQ → No broker calls
2. ✅ **Orders Saved**: Orders saved to DB with `trading_mode='PAPER'`
3. ✅ **Positions Created**: New entry in `paper_positions` table
4. ✅ **SL/TP Set**: Stop loss and take profit levels stored
5. ✅ **Background Monitoring**: Position manager monitors position
6. ✅ **Live Price Updates**: Current price updated from Redis
7. ✅ **PnL Calculated**: Unrealized PnL calculated and stored
8. ✅ **Auto-Sell on SL**: Position closed when stop loss hit
9. ✅ **Auto-Sell on TP**: Position closed when take profit hit
10. ✅ **PnL Recorded**: Realized PnL saved to history

---

## 📊 Database Tables Summary

### `orders` Table (Existing - Modified)
- Added: `trading_mode` column
- Paper orders: `trading_mode = 'PAPER'`
- Live orders: `trading_mode = 'LIVE'`

### `paper_positions` Table (New)
```
position_id, user_id, strategy_id, symbol, token, exchange
quantity, entry_price, current_price
stop_loss, take_profit
unrealized_pnl, unrealized_pnl_pct
status (OPEN, CLOSED_SL, CLOSED_TP, CLOSED_MANUAL)
entry_order_id, exit_order_id
opened_at, closed_at, last_updated
```

### `paper_pnl_history` Table (New)
```
pnl_id, user_id, strategy_id, position_id
symbol, exchange, quantity
entry_price, exit_price
realized_pnl, realized_pnl_pct
exit_reason (STOP_LOSS, TAKE_PROFIT, MANUAL)
entry_time, exit_time
```

### `user_daily_paper_pnl` View (New)
```
user_id, strategy_id, trade_date
num_trades, daily_pnl, avg_pnl_pct
winning_trades, losing_trades
```

---

## 🧪 Testing

### Manual Test Steps:

1. **Set user to PAPER mode**
   ```bash
   POST /api/v1/strategies/cash52w/configure
   {"user_id": "test_user", "trading_mode": "PAPER"}
   ```

2. **Trigger a 52W breakout** (or wait for natural breakout)
   - Check logs: "52w-high paper trade simulated"

3. **Verify order created**
   ```sql
   SELECT * FROM orders WHERE user_id = 'test_user' AND trading_mode = 'PAPER';
   ```

4. **Verify position created**
   ```sql
   SELECT * FROM paper_positions WHERE user_id = 'test_user' AND status = 'OPEN';
   ```

5. **Wait for price movement**
   - Position manager runs every 10 seconds
   - Check logs: "Monitoring X open paper positions"

6. **Verify PnL updates**
   ```sql
   SELECT symbol, current_price, unrealized_pnl, last_updated 
   FROM paper_positions WHERE user_id = 'test_user';
   ```

7. **Simulate stop loss** (manually update Redis price below SL)
   - Check logs: "🛑 Stop Loss triggered"
   - Verify position closed and PnL recorded

---

## 🎓 Next Steps

### For Live Trading (Not Yet Implemented):
1. Keep same flow but with `trading_mode = 'LIVE'`
2. Orders ARE sent to RabbitMQ
3. Real broker execution via Odin API
4. Positions tracked via broker's position API
5. SL/TP managed by broker (not our service)

### To Implement Live Trading:
- No changes needed to paper trading code
- Just ensure user's strategy has `trading_mode = 'LIVE'`
- Rules engine already handles this distinction
- Trade execution already routes LIVE orders to RabbitMQ

---

## 📝 Summary

**Paper Trading System is Complete!** 🎉

Users can now:
- ✅ Trade in simulation mode (no real money)
- ✅ See their positions persisted
- ✅ Have stop loss auto-executed
- ✅ Have take profit auto-executed
- ✅ Track live PnL in real-time
- ✅ View trade history and analytics
- ✅ Test strategies risk-free

**Next**: Test thoroughly, then move to live trading implementation.

---

## 📞 Support

If you encounter issues:
1. Check service logs (rules-engine, trade-execution)
2. Verify database migrations ran successfully
3. Ensure Redis has live market data
4. Check user's strategy configuration
5. Review [PAPER_TRADING_GUIDE.md](./PAPER_TRADING_GUIDE.md) for troubleshooting

**System Status**: ✅ **Paper Trading READY FOR TESTING**
