# Paper Trading System - Final Integration Checklist

## ✅ Code Integration Status

### 1. Database Layer - ✅ COMPLETE
- [x] `paper_positions` table migration created
- [x] `paper_pnl_history` table migration created
- [x] `user_daily_paper_pnl` view created
- [x] Models created (PaperPosition, PaperPnLHistory)
- [x] Repository implemented with all CRUD operations

### 2. Business Logic - ✅ COMPLETE
- [x] Position Manager created with SL/TP monitoring
- [x] Redis Price Provider implemented
- [x] Paper Trade Handler created
- [x] Signal Processor updated to handle paper trades
- [x] Cash 52W engine already supports paper mode

### 3. Service Integration - ✅ COMPLETE
- [x] main.go updated with Redis initialization
- [x] main.go updated with paper repository initialization
- [x] main.go updated with position manager startup
- [x] main.go updated with paper trade handler
- [x] Signal processor receives paper trade handler
- [x] Environment variables added to .env

### 4. API Gateway - ✅ ALREADY CONFIGURED
- [x] `/api/v1/strategies/cash52w/configure` endpoint exists
- [x] Accepts `trading_mode` parameter
- [x] Routes to user-config service
- [x] Returns strategy configuration

---

## 🔧 Integration Points Verified

### ✅ Rules Engine → Kafka
```go
// Cash 52W engine already checks trading mode
mode := e.effectiveModeForUser(userID)
orderReq.TradingMode = mode

if mode == "PAPER" {
    // Does NOT publish to RabbitMQ ✓
    // Publishes to Kafka trade-signals ✓
}
```

### ✅ Kafka → Trade Execution
```go
// Signal processor now handles paper trades
if strings.ToUpper(signal.TradingMode) == "PAPER" {
    order := convertSignalToOrder(signal)
    orderRepo.Create(ctx, order)  // Save to DB ✓
    paperTradeHandler.ProcessPaperOrder(ctx, order)  // Create position ✓
}
```

### ✅ Position Manager → Redis
```go
// Price provider gets live prices from Redis
currentPrice, err := priceProvider.GetLivePrice(ctx, token, exchange)
// Tries multiple key patterns ✓
// Handles fallbacks ✓
```

### ✅ SL/TP Monitoring → Auto-Sell
```go
// Position manager runs every 10 seconds
if position.ShouldTriggerStopLoss(currentPrice) {
    closePositionOnTrigger(...)  // Creates SELL order ✓
    positionRepo.ClosePosition(...)  // Records PnL ✓
}
```

---

## 📋 Pre-Deployment Checklist

### Database Setup
- [ ] Run migration: `003_create_paper_positions_table.sql`
- [ ] Verify tables created: `paper_positions`, `paper_pnl_history`
- [ ] Verify view created: `user_daily_paper_pnl`
- [ ] Check foreign key constraints

### Service Configuration
- [ ] `.env` has Redis configuration
- [ ] `.env` has `PAPER_POSITION_CHECK_INTERVAL_SEC`
- [ ] Redis is running and accessible
- [ ] Redis has market data (from data-ingestion)

### Code Compilation
- [ ] No import errors
- [ ] `go mod tidy` runs successfully
- [ ] Service compiles without errors
- [ ] All dependencies resolved

### Service Startup
- [ ] PostgreSQL connected
- [ ] Redis connected
- [ ] RabbitMQ connected
- [ ] Kafka consumer started
- [ ] Position manager started
- [ ] gRPC server started

---

## 🧪 Testing Steps

### Test 1: User Configuration
```bash
# Configure user for paper trading
curl -X POST http://localhost:9000/api/v1/strategies/cash52w/configure \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "test_user",
    "enabled": true,
    "capital_per_stock": 20000,
    "trading_mode": "PAPER"
  }'

# Expected: success=true, trading_mode=PAPER
```

### Test 2: Verify Strategy in Database
```sql
SELECT strategy_id, user_id, strategy_name, active, trading_mode
FROM strategies
WHERE user_id = 'test_user' AND strategy_name = 'Cash 52-Week High';

-- Expected: trading_mode = 'PAPER'
```

### Test 3: Trigger Paper Trade
```bash
# Wait for 52W breakout OR manually insert test signal
# Check rules-engine logs for: "52w-high paper trade simulated"
```

### Test 4: Verify Paper Order Created
```sql
SELECT order_id, user_id, symbol, quantity, price, status, trading_mode
FROM orders
WHERE user_id = 'test_user' AND trading_mode = 'PAPER'
ORDER BY created_at DESC
LIMIT 5;

-- Expected: trading_mode = 'PAPER', status = 'FILLED'
```

### Test 5: Verify Paper Position Created
```sql
SELECT position_id, symbol, quantity, entry_price, stop_loss, take_profit, status
FROM paper_positions
WHERE user_id = 'test_user' AND status = 'OPEN';

-- Expected: At least 1 open position with SL/TP values
```

### Test 6: Monitor SL/TP Checking
```bash
# Check trade-execution logs
# Expected every 10 seconds: "Monitoring X open paper positions"
```

### Test 7: Simulate Stop Loss
```bash
# Option A: Wait for price to fall below SL naturally
# Option B: Manually update Redis price below SL for testing

# Set price below stop loss in Redis
redis-cli SET "market:data:NSE:{token}" '{"ltp":2200,"token":123456}'

# Wait 10 seconds for position manager check
# Check logs: "🛑 Stop Loss triggered"
```

### Test 8: Verify Position Closed
```sql
-- Check position is closed
SELECT position_id, symbol, status, closed_at
FROM paper_positions
WHERE user_id = 'test_user' AND status = 'CLOSED_SL';

-- Check PnL recorded
SELECT symbol, entry_price, exit_price, realized_pnl, exit_reason
FROM paper_pnl_history
WHERE user_id = 'test_user'
ORDER BY exit_time DESC;
```

### Test 9: Verify PnL Calculation
```sql
-- Get total PnL for user
SELECT 
    u.user_id,
    u.trade_date,
    u.daily_pnl,
    u.winning_trades,
    u.losing_trades
FROM user_daily_paper_pnl u
WHERE u.user_id = 'test_user';
```

### Test 10: Test Live Mode
```bash
# Configure same user for LIVE trading
curl -X POST http://localhost:9000/api/v1/strategies/cash52w/configure \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "test_user",
    "enabled": true,
    "trading_mode": "LIVE"
  }'

# Trigger trade - verify it DOES go to RabbitMQ (real execution)
# Check that order goes to odin-api-wrapper
```

---

## 🔍 Key Logs to Monitor

### Rules Engine
```
✓ Should see: "52w-high paper trade simulated (no real order sent)"
✓ Should see: "mode=PAPER"
❌ Should NOT see: "failed to publish jobbing order" (for paper trades)
```

### Trade Execution
```
✓ Should see: "✓ Paper Position Manager started"
✓ Should see: "Monitoring X open paper positions"
✓ Should see: "✓ Paper order saved to database"
✓ Should see: "✓ Paper position created"
✓ Should see: "🛑 Stop Loss triggered" (when SL hits)
✓ Should see: "✓ Position closed - Reason: STOP_LOSS"
```

### Redis
```
✓ Should see: "✓ Connected to Redis"
❌ Should NOT see: "Failed to get live price" (indicates Redis issue)
```

---

## ⚠️ Common Issues & Fixes

### Issue: "undefined: paper.NewPositionManager"
**Fix:** Run `go mod tidy` and rebuild

### Issue: "Redis connection failed"
**Fix:** 
1. Start Redis: `docker start redis` or `redis-server`
2. Verify: `redis-cli PING` should return "PONG"
3. Check .env has correct REDIS_HOST and REDIS_PORT

### Issue: "Paper orders created but no positions"
**Fix:**
1. Verify paperTradeHandler passed to NewSignalProcessor
2. Check logs for "ProcessPaperOrder" errors
3. Verify orders have status='FILLED'

### Issue: "SL/TP not triggering"
**Fix:**
1. Verify position manager started (check logs)
2. Check Redis has price data: `redis-cli GET market:data:NSE:{token}`
3. Verify position has stop_loss/take_profit values set
4. Check PAPER_POSITION_CHECK_INTERVAL_SEC in .env

### Issue: "Position manager already running"
**Fix:** Only call Start() once - already handled in main.go

---

## 📊 Expected Behavior Summary

| Scenario | Expected Result |
|----------|----------------|
| User sets PAPER mode | `trading_mode = 'PAPER'` in strategies table |
| 52W breakout occurs | Order with `trading_mode='PAPER'` created |
| Paper order created | Position created in `paper_positions` |
| Price updates | `current_price` and `unrealized_pnl` updated every 10s |
| Price hits SL | Position auto-closed, PnL recorded in history |
| Price hits TP | Position auto-closed, PnL recorded in history |
| User sets LIVE mode | Orders sent to RabbitMQ for real execution |

---

## ✅ Final Verification Commands

```bash
# 1. Check service is running
curl http://localhost:9004/health

# 2. Check Redis connection
redis-cli PING

# 3. Check database tables exist
psql -U postgres -d trading_execution -c "\dt paper_*"

# 4. Check position manager is monitoring
tail -f trade-execution.log | grep "Monitoring"

# 5. Check paper orders exist
psql -U postgres -d trading_execution -c "SELECT COUNT(*) FROM orders WHERE trading_mode='PAPER';"

# 6. Check paper positions exist
psql -U postgres -d trading_execution -c "SELECT COUNT(*) FROM paper_positions WHERE status='OPEN';"
```

---

## 🎓 System Architecture Flow

```
User Config (PAPER mode)
         ↓
52W Breakout Detected
         ↓
Rules Engine: mode="PAPER"
         ↓
Kafka: trade-signals topic
         ↓
Trade Execution: Signal Processor
         ↓
Create Order (trading_mode='PAPER')
         ↓
Paper Trade Handler
         ↓
Create Position in paper_positions
         ↓
Position Manager (Background)
  ├─ Every 10s: Get live price from Redis
  ├─ Update current_price & unrealized_pnl
  ├─ Check if SL triggered
  ├─ Check if TP triggered
  └─ Auto-close if triggered
         ↓
Position Closed
         ↓
PnL recorded in paper_pnl_history
```

---

## 🚀 Deployment Checklist

- [ ] Code reviewed and tested locally
- [ ] Database migration applied
- [ ] Environment variables configured
- [ ] Redis connection verified
- [ ] Service starts without errors
- [ ] Paper trades tested end-to-end
- [ ] SL/TP triggers verified
- [ ] PnL calculations accurate
- [ ] Live mode still works
- [ ] Documentation complete

---

## 📝 Next Steps After Deployment

1. **Monitor for 24 hours** - Check logs for any unexpected errors
2. **Verify SL/TP triggers** - Ensure auto-sell happens correctly
3. **Check PnL accuracy** - Compare calculated PnL with expected values
4. **User feedback** - Get feedback from users testing paper mode
5. **Performance tuning** - Adjust check interval if needed
6. **Add metrics** - Track number of positions, trigger rate, etc.

---

## 🎉 Success Criteria

✅ Users can set PAPER mode via API  
✅ Paper orders are NOT sent to broker  
✅ Paper positions are created and persisted  
✅ Stop loss auto-executes when triggered  
✅ Take profit auto-executes when triggered  
✅ Unrealized PnL updates in real-time  
✅ Realized PnL is recorded accurately  
✅ System survives service restarts  
✅ Live mode still works for real trading  

**When all criteria are met: PAPER TRADING IS PRODUCTION READY! 🚀**
