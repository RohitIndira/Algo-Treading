# 🚀 PAPER TRADING - QUICK START GUIDE

## One-Time Setup (5 minutes)

### 1. Run Database Migration
```bash
psql -U postgres -d trading_execution -f services/trade-execution/migrations/003_create_paper_positions_table.sql
```

### 2. Verify Redis is Running
```bash
redis-cli PING
# Should return: PONG
```

### 3. Start Trade Execution Service
```bash
cd services/trade-execution
go run cmd/main.go
```

**Expected logs:**
```
✓ Connected to PostgreSQL
✓ Connected to Redis
✓ Paper trading system initialized
✓ Paper position manager started
```

---

## Usage (2 minutes)

### Set User to Paper Mode
```bash
curl -X POST http://localhost:9000/api/v1/strategies/cash52w/configure \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "YOUR_USER_ID",
    "enabled": true,
    "capital_per_stock": 20000,
    "trading_mode": "PAPER"
  }'
```

### Check if Working
```sql
-- Check paper orders
SELECT * FROM orders WHERE trading_mode = 'PAPER' ORDER BY created_at DESC LIMIT 5;

-- Check paper positions
SELECT * FROM paper_positions WHERE status = 'OPEN';

-- Check realized PnL
SELECT * FROM paper_pnl_history ORDER BY exit_time DESC LIMIT 10;
```

---

## Key Features

✅ **No Real Orders** - Orders not sent to broker  
✅ **Persistent Positions** - Survives service restart  
✅ **Auto Stop Loss** - Sells when price hits SL  
✅ **Auto Take Profit** - Sells when price hits TP  
✅ **Live PnL** - Updates every 10 seconds  

---

## Switch to Live Trading

```bash
curl -X POST http://localhost:9000/api/v1/strategies/cash52w/configure \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "YOUR_USER_ID",
    "enabled": true,
    "trading_mode": "LIVE"
  }'
```

---

## Troubleshooting

**No positions created?**
- Check logs: `tail -f trade-execution.log | grep "Paper"`
- Verify orders have `status='FILLED'`

**SL/TP not triggering?**
- Verify position manager is running
- Check Redis has prices: `redis-cli GET market:data:NSE:{token}`

**Service won't start?**
- Run migration first
- Check Redis is running
- Verify .env has Redis config

---

## Monitoring Commands

```bash
# Watch position manager
tail -f trade-execution.log | grep "Monitoring"

# Count paper orders
psql -U postgres -d trading_execution -c "SELECT COUNT(*) FROM orders WHERE trading_mode='PAPER';"

# Count open positions
psql -U postgres -d trading_execution -c "SELECT COUNT(*) FROM paper_positions WHERE status='OPEN';"

# Get total PnL
psql -U postgres -d trading_execution -c "SELECT user_id, SUM(realized_pnl) as total_pnl FROM paper_pnl_history GROUP BY user_id;"
```

---

## That's It! 🎉

You now have a fully functional paper trading system.

**For detailed docs:** See [PAPER_TRADING_GUIDE.md](./docs/guides/PAPER_TRADING_GUIDE.md)
