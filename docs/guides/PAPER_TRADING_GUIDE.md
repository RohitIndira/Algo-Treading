# Paper Trading System - Implementation Guide

## Overview
The paper trading system allows users to simulate trading with the **Cash 52-Week High** strategy without placing real orders. All paper trades are tracked with positions, stop loss/take profit monitoring, and live PnL calculation.

## Key Features

### ✅ 1. Separate Paper Orders & Positions
- **Paper orders** are stored in the `orders` table with `trading_mode = 'PAPER'`
- **Paper positions** are stored in the `paper_positions` table
- **PnL history** is tracked in the `paper_pnl_history` table

### ✅ 2. Automated Stop Loss & Take Profit
- Position manager monitors all open paper positions every 10 seconds
- Automatically sells positions when:
  - **Stop Loss**: Price falls below SL level
  - **Take Profit**: Price rises above TP level
- Creates SELL orders and records realized PnL

### ✅ 3. Live PnL Tracking
- **Unrealized PnL**: Updated continuously for open positions
- **Realized PnL**: Recorded when positions are closed
- PnL calculated in both absolute (₹) and percentage (%)

### ✅ 4. User Configuration
Each user can set their **Cash 52-Week High** strategy to:
- **LIVE mode**: Real orders placed with broker
- **PAPER mode**: Simulated orders, no real execution

## Database Schema

### Paper Positions Table
```sql
CREATE TABLE paper_positions (
    position_id UUID PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    strategy_id VARCHAR(50) NOT NULL,
    stock_code BIGINT NOT NULL,
    token BIGINT NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    exchange VARCHAR(10) NOT NULL,
    quantity INT NOT NULL,
    entry_price DECIMAL(15,2) NOT NULL,
    current_price DECIMAL(15,2) NOT NULL,
    stop_loss DECIMAL(15,2),
    take_profit DECIMAL(15,2),
    unrealized_pnl DECIMAL(15,2) DEFAULT 0,
    unrealized_pnl_pct DECIMAL(10,4) DEFAULT 0,
    status VARCHAR(20) NOT NULL DEFAULT 'OPEN',
    entry_order_id UUID NOT NULL,
    exit_order_id UUID,
    opened_at TIMESTAMP DEFAULT NOW(),
    closed_at TIMESTAMP,
    last_updated TIMESTAMP DEFAULT NOW()
);
```

### Paper PnL History Table
```sql
CREATE TABLE paper_pnl_history (
    pnl_id UUID PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    strategy_id VARCHAR(50) NOT NULL,
    position_id UUID NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    exchange VARCHAR(10) NOT NULL,
    quantity INT NOT NULL,
    entry_price DECIMAL(15,2) NOT NULL,
    exit_price DECIMAL(15,2) NOT NULL,
    realized_pnl DECIMAL(15,2) NOT NULL,
    realized_pnl_pct DECIMAL(10,4) NOT NULL,
    exit_reason VARCHAR(50) NOT NULL,
    entry_time TIMESTAMP NOT NULL,
    exit_time TIMESTAMP DEFAULT NOW()
);
```

## Setup Instructions

### 1. Run Database Migrations
```powershell
# Navigate to trade-execution service
cd services/trade-execution

# Run migrations (if you have a migration tool)
# Or execute the SQL file directly
psql -U postgres -d algo_trading_db -f migrations/003_create_paper_positions_table.sql
```

### 2. Configure User for Paper Trading

Use the **ConfigureCash52WeekStrategy** API endpoint:

```bash
POST http://localhost:9000/api/v1/strategies/cash52w/configure
Content-Type: application/json

{
  "user_id": "user123",
  "enabled": true,
  "capital_per_stock": 20000,
  "trading_mode": "PAPER"
}
```

**Response:**
```json
{
  "success": true,
  "strategy": {
    "strategy_id": "52525252-5252-5252-5252-525252525252",
    "user_id": "user123",
    "strategy_name": "Cash 52-Week High",
    "active": true,
    "trading_mode": "PAPER"
  }
}
```

### 3. Start Services

Ensure these services are running:
1. **PostgreSQL** (orders & positions)
2. **Redis** (live market data)
3. **Kafka** (trade signals)
4. **RabbitMQ** (order routing)
5. **Rules Engine** (generates 52W signals)
6. **Trade Execution** (position management)
7. **Data Ingestion** (market data)

### 4. Verify Paper Trading is Working

#### Check Paper Orders
```sql
SELECT order_id, user_id, symbol, quantity, price, status, trading_mode
FROM orders
WHERE user_id = 'user123' AND trading_mode = 'PAPER'
ORDER BY created_at DESC
LIMIT 10;
```

#### Check Paper Positions
```sql
SELECT position_id, symbol, quantity, entry_price, current_price, 
       unrealized_pnl, unrealized_pnl_pct, status
FROM paper_positions
WHERE user_id = 'user123' AND status = 'OPEN';
```

#### Check Realized PnL
```sql
SELECT symbol, entry_price, exit_price, realized_pnl, 
       realized_pnl_pct, exit_reason, exit_time
FROM paper_pnl_history
WHERE user_id = 'user123'
ORDER BY exit_time DESC;
```

#### Get Daily Summary
```sql
SELECT * FROM user_daily_paper_pnl
WHERE user_id = 'user123'
ORDER BY trade_date DESC;
```

## How It Works

### Paper Trade Flow

```
1. Market Data (52W Breakout) → Kafka
                ↓
2. Rules Engine (Cash52W) 
   - Checks user's trading_mode
   - If PAPER: Creates order with trading_mode='PAPER'
   - Publishes to Kafka (NOT RabbitMQ)
                ↓
3. Trade Execution (Signal Processor)
   - Receives paper signal from Kafka
   - Saves order to DB with status='FILLED'
   - Calls PaperTradeHandler.ProcessPaperOrder()
                ↓
4. Position Manager
   - Creates new paper_position entry
   - Sets stop_loss and take_profit levels
                ↓
5. Background Monitor (every 10 seconds)
   - Gets live prices from Redis
   - Updates current_price and unrealized_pnl
   - Checks if SL or TP triggered
   - If triggered: Creates SELL order, closes position, records PnL
```

### Stop Loss Example

```
Entry:
- Symbol: RELIANCE
- Entry Price: ₹2,500
- Quantity: 8
- Stop Loss: ₹2,250 (10% below entry)
- Take Profit: ₹3,000 (20% above entry)

Monitoring:
- Current Price drops to ₹2,240
- SL triggered! (2,240 < 2,250)

Auto-Sell:
- Create SELL order at ₹2,240
- Close position
- Realized PnL: (2,240 - 2,500) × 8 = -₹2,080
- Exit Reason: STOP_LOSS
```

## API Endpoints (To Be Added)

The following endpoints should be added to the API Gateway:

### Get User's Paper Positions
```
GET /api/v1/users/{user_id}/paper/positions?status=OPEN
```

### Get User's Paper PnL
```
GET /api/v1/users/{user_id}/paper/pnl?date=2026-01-28
```

### Get Paper PnL History
```
GET /api/v1/users/{user_id}/paper/history?limit=50&offset=0
```

### Close Paper Position Manually
```
POST /api/v1/users/{user_id}/paper/positions/{position_id}/close
```

## Testing Checklist

- [ ] User with PAPER mode does NOT place real orders
- [ ] User with LIVE mode places real orders
- [ ] Paper orders are saved to database
- [ ] Paper positions are created on BUY orders
- [ ] Stop loss triggers correctly
- [ ] Take profit triggers correctly
- [ ] Unrealized PnL updates with live prices
- [ ] Realized PnL is recorded when position closes
- [ ] Multiple users can have different modes (LIVE/PAPER)
- [ ] Positions persist after service restart
- [ ] Daily PnL summary is accurate

## Logs to Monitor

### Rules Engine (Cash 52W)
```
52w-high paper trade simulated (no real order sent)
```

### Trade Execution
```
✓ Paper order saved to database with status FILLED
✓ Paper position created - Symbol: RELIANCE, User: user123
Monitoring 5 open paper positions for SL/TP triggers
🛑 Stop Loss triggered for RELIANCE (User: user123, Price: 2240.00, SL: 2250.00)
✓ Position closed - Symbol: RELIANCE, Reason: STOP_LOSS, PnL: ₹-2,080.00
```

## Troubleshooting

### Paper Orders Not Being Created
1. Check user's strategy: `SELECT * FROM strategies WHERE user_id = 'user123'`
2. Verify `trading_mode = 'PAPER'`
3. Check rules-engine logs for "paper trade simulated"

### Positions Not Being Created
1. Check signal processor logs
2. Verify PaperTradeHandler is initialized
3. Check database for orders with trading_mode='PAPER' and status='FILLED'

### SL/TP Not Triggering
1. Verify Position Manager is running: Check for "Paper Position Manager started" log
2. Check Redis for live prices: `redis-cli GET market:data:NSE:{token}`
3. Verify positions have stop_loss/take_profit values set

### PnL Not Updating
1. Check Position Manager logs for price update errors
2. Verify Redis connection
3. Check last_updated timestamp in paper_positions table

## Performance Considerations

- **Position Monitoring**: Checks every 10 seconds (configurable)
- **Batch Price Updates**: Use Redis pipeline for multiple positions
- **Database Load**: Positions table is relatively small (< 1000 active positions per user)
- **Redis Dependency**: System requires Redis for live prices

## Future Enhancements

1. **WebSocket Updates**: Real-time PnL updates to frontend
2. **Trailing Stop Loss**: Dynamic SL that moves with price
3. **Partial Position Closure**: Close part of position
4. **Position Averaging**: Add to existing position
5. **Portfolio-Level Risk**: Max drawdown, position correlation
6. **Performance Analytics**: Win rate, average hold time, Sharpe ratio

## Summary

The paper trading system is now fully functional with:
- ✅ Database schema for positions and PnL
- ✅ Position lifecycle management
- ✅ Automated SL/TP monitoring
- ✅ Live PnL calculation
- ✅ Integration with Cash 52W strategy

Users can now safely test the 52-Week High strategy in paper mode before switching to live trading!
