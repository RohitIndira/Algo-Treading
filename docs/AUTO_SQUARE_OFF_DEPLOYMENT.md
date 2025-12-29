# Auto Square-Off System - Deployment & Configuration Guide

## Quick Start (5 Minutes)

### Step 1: Set Environment Variable
```bash
# In .env file or docker-compose.yml
AUTO_SQUARE_OFF_TIME=15:05
```

### Step 2: Verify Database Schema
```bash
# Connect to PostgreSQL and verify columns exist
psql -h localhost -U postgres -d trading_config

SELECT column_name FROM information_schema.columns 
WHERE table_name = 'risk_limits' 
AND column_name IN ('enable_auto_square_off', 'auto_square_off_time');
```

**Expected Output:**
```
 column_name
─────────────────────────
 enable_auto_square_off
 auto_square_off_time
```

### Step 3: Restart Trade Execution Service
```bash
# Via Docker
docker-compose restart trade-execution

# OR manually
cd services/trade-execution
go run cmd/main.go
```

### Step 4: Verify in Logs
```bash
# Should see at startup:
docker logs -f trade-execution | grep -i "auto square"

# Expected output:
# ✓ Auto Square-Off scheduler initialized
# Starting Auto Square-Off Scheduler...
# Auto Square-Off Scheduler (Time: 15:05)
# - Auto Square-Off Time: 15:05
```

**That's it! Auto square-off is now ACTIVE.**

---

## Configuration Reference

### Environment Variables

| Variable | Default | Format | Notes |
|----------|---------|--------|-------|
| `AUTO_SQUARE_OFF_TIME` | `15:05` | `HH:MM` (24-hour) | Global default time |

### Docker Compose Example

```yaml
services:
  trade-execution:
    image: trade-execution:latest
    environment:
      - AUTO_SQUARE_OFF_TIME=15:05
      - SERVICE_PORT=9004
      - RABBITMQ_URL=amqp://admin:admin123@rabbitmq:5672/
      - KAFKA_BROKERS=kafka:9092
      - POSTGRES_HOST=postgres
      - POSTGRES_DB=trading_execution
    ports:
      - "9004:9004"
```

### Kubernetes ConfigMap Example

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: trade-execution-config
data:
  AUTO_SQUARE_OFF_TIME: "15:05"
---
apiVersion: v1
kind: Pod
metadata:
  name: trade-execution
spec:
  containers:
  - name: trade-execution
    image: trade-execution:latest
    envFrom:
    - configMapRef:
        name: trade-execution-config
```

---

## Per-Strategy Configuration

### Enable Auto Square-Off for Strategy

Use User Config Service API to enable:

```bash
curl -X POST http://localhost:9003/api/strategies \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -d '{
    "user_id": "USER_123",
    "strategy_name": "Intraday INFY Trading",
    "description": "Automatic square-off at 15:05",
    "risk_limits": {
      "enable_auto_square_off": true,
      "auto_square_off_time": "15:05"
    }
  }'
```

### Response

```json
{
  "strategy_id": "550e8400-e29b-41d4-a716-446655440000",
  "user_id": "USER_123",
  "strategy_name": "Intraday INFY Trading",
  "risk_limits": {
    "enable_auto_square_off": true,
    "auto_square_off_time": "15:05"
  },
  "created_at": "2025-01-18T10:30:00Z"
}
```

### Override Time for Specific Client

```bash
curl -X PUT http://localhost:9003/api/strategies/STRATEGY_ID \
  -H "Content-Type: application/json" \
  -d '{
    "risk_limits": {
      "enable_auto_square_off": true,
      "auto_square_off_time": "14:30"  # Different time!
    }
  }'
```

---

## Database Configuration

### Verify Table Structure

```sql
-- Connect to trading_config database
\d risk_limits;

-- Check columns
SELECT 
  column_name, 
  data_type, 
  is_nullable 
FROM information_schema.columns 
WHERE table_name = 'risk_limits' 
ORDER BY ordinal_position;
```

**Expected Columns:**
```
Column Name               | Data Type        | Nullable
──────────────────────────┼──────────────────┼──────────
strategy_id              | uuid             | NO
risk_limit_id            | uuid             | NO
enable_auto_square_off   | boolean          | YES
auto_square_off_time     | character varying| YES
enable_risk_checks       | boolean          | NO
max_daily_trades         | integer          | YES
max_loss_per_day         | numeric          | YES
...
```

### Add Columns If Missing

```sql
-- If columns don't exist, add them
ALTER TABLE risk_limits 
ADD COLUMN IF NOT EXISTS enable_auto_square_off BOOLEAN DEFAULT false;

ALTER TABLE risk_limits 
ADD COLUMN IF NOT EXISTS auto_square_off_time VARCHAR(5) DEFAULT '15:05';

-- Verify
SELECT enable_auto_square_off, auto_square_off_time 
FROM risk_limits LIMIT 1;
```

---

## Testing Procedure

### Test 1: Manual Verification

**Time**: Anytime before 15:05

```bash
# 1. Check scheduler is running
docker logs trade-execution-service | grep -i "scheduler initialized"

# 2. Create a test INTRADAY position
curl -X POST http://localhost:9004/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "TEST_USER",
    "symbol": "INFY",
    "order_side": "BUY",
    "quantity": 100,
    "product_type": "INTRADAY",
    "order_type": "MARKET"
  }'

# 3. Verify order is FILLED
SELECT order_id, status, filled_quantity FROM orders 
WHERE user_id = 'TEST_USER' AND symbol = 'INFY';
```

### Test 2: Watch Square-Off Execution

**Time**: Around 15:05

```bash
# Terminal 1: Watch logs
docker logs -f trade-execution-service | grep -i "square"

# Expected at 15:05:
# Auto Square-Off Time Reached - Initiating square-off for all open positions
# Found 1 open orders to square off
# Squaring off order TEST_ORDER_123 for user TEST_USER (Symbol: INFY, Qty: 100)
# Successfully created and executed square-off order
# Auto Square-Off: Complete (Success: 1, Failed: 0)
```

### Test 3: Verify Position Closed

**Time**: After 15:05

```bash
# Check original and square-off orders
SELECT 
  order_id, 
  user_id, 
  symbol, 
  order_side, 
  filled_quantity, 
  status,
  created_at 
FROM orders 
WHERE user_id = 'TEST_USER' 
ORDER BY created_at DESC 
LIMIT 5;

# Expected:
# - Original: BUY 100 INFY (FILLED, 15:04)
# - Reverse: SELL 100 INFY (FILLED, 15:05) ← Created by scheduler
# - Net position: 0 (squared off)
```

---

## Monitoring & Alerts

### Key Metrics to Monitor

```bash
# 1. Scheduler running check (every 5 minutes)
docker logs trade-execution-service | \
  grep "Auto Square-Off Time Reached" | \
  tail -1

# 2. Square-off success rate
docker logs trade-execution-service | \
  grep "Auto Square-Off: Complete" | \
  tail -1

# 3. Any failed square-offs
docker logs trade-execution-service | \
  grep -i "failed to square off"

# 4. Database query count
SELECT COUNT(*) FROM orders 
WHERE source = 'SCHEDULER' 
AND created_at >= NOW() - INTERVAL '1 day';
```

### Alert Rules (Prometheus/Datadog)

```yaml
# Alert if scheduler not running
- alert: AutoSquareOffSchedulerDown
  expr: rate(square_off_checks_total[5m]) == 0
  for: 10m
  annotations:
    summary: "Auto square-off scheduler not running"

# Alert if square-off failures > 10%
- alert: AutoSquareOffFailureRate
  expr: |
    (rate(square_off_failures[5m]) / rate(square_off_attempts[5m])) > 0.1
  annotations:
    summary: "Auto square-off failure rate high"

# Alert if square-off takes > 5 minutes
- alert: AutoSquareOffDelay
  expr: square_off_duration_seconds > 300
  annotations:
    summary: "Auto square-off execution delayed"
```

---

## Troubleshooting

### Issue 1: Scheduler Not Starting

**Symptoms:**
```
- No "Auto Square-Off scheduler initialized" in logs
- Positions not squaring off at 15:05
```

**Diagnosis:**
```bash
# Check if import exists
grep "internal/scheduler" services/trade-execution/cmd/main.go

# Check if function called
grep "NewAutoSquareOffScheduler" services/trade-execution/cmd/main.go

# Rebuild and restart
cd services/trade-execution
go clean -cache
go build -o main cmd/main.go
./main
```

**Solution:**
- Verify scheduler import on line 21
- Verify initialization on lines 67-73
- Check for Go syntax errors
- Rebuild project

---

### Issue 2: Positions Not Closing at 15:05

**Symptoms:**
```
- Scheduler running but no square-off logs
- Positions remain open after 15:05
```

**Diagnosis:**
```bash
# 1. Check time
date  # Is it 15:05 IST?

# 2. Check if any INTRADAY orders exist
SELECT * FROM orders 
WHERE product_type = 'INTRADAY' 
AND status IN ('FILLED', 'PARTIALLY_FILLED')
AND user_id != 'SYSTEM';

# 3. Check if strategy has auto square-off enabled
SELECT enable_auto_square_off, auto_square_off_time 
FROM risk_limits;

# 4. Check scheduler logs
docker logs trade-execution-service | grep -i "should.*square"
```

**Solutions:**
1. Verify current time is 15:05
2. Create test order before 15:05
3. Enable auto square-off for strategy
4. Check that it's a weekday (Mon-Fri)

---

### Issue 3: Failed Square-Off Orders

**Symptoms:**
```
Auto Square-Off: Complete (Success: 2, Failed: 1)
```

**Diagnosis:**
```bash
# Check broker connectivity
curl https://api.broker.com/health

# Check credentials
SELECT * FROM credentials 
WHERE user_id = 'FAILED_USER';

# Check order executor logs
docker logs trade-execution-service | \
  grep -A5 "failed to execute"

# Check database for failed orders
SELECT * FROM orders 
WHERE source = 'SCHEDULER' 
AND status NOT IN ('FILLED', 'PARTIALLY_FILLED');
```

**Solutions:**
1. Check broker API status
2. Verify user credentials
3. Check network connectivity
4. Verify sufficient margin/liquidity

---

### Issue 4: Wrong Square-Off Time

**Symptoms:**
```
Squares off at 14:30 instead of 15:05
```

**Diagnosis:**
```bash
# Check environment variable
echo $AUTO_SQUARE_OFF_TIME

# Check in Config struct
grep -A2 "AutoSquareOffTime" services/trade-execution/cmd/main.go

# Check database
SELECT DISTINCT auto_square_off_time FROM risk_limits;
```

**Solutions:**
1. Update `.env` file
2. Restart service after env change
3. Check per-strategy overrides in database
4. Verify time format (HH:MM, 24-hour)

---

## Performance Tuning

### Optimize Database Query

If scheduling many clients, consider adding index:

```sql
-- Add index on product_type and status for faster queries
CREATE INDEX idx_orders_product_status 
ON orders(product_type, status) 
WHERE status IN ('FILLED', 'PARTIALLY_FILLED');
```

### Batch Execution

For > 1000 orders, consider batch processing:

```go
// In auto_square_off.go, modify squareOffAllPositions
const BATCH_SIZE = 100

for i := 0; i < len(openOrders); i += BATCH_SIZE {
    end := i + BATCH_SIZE
    if end > len(openOrders) {
        end = len(openOrders)
    }
    batch := openOrders[i:end]
    s.executeOrderBatch(ctx, batch)
}
```

### Resource Limits

Set resource constraints in docker-compose:

```yaml
trade-execution:
  resources:
    limits:
      cpus: '0.5'
      memory: 256M
    reservations:
      cpus: '0.25'
      memory: 128M
```

---

## Rollback Procedure

### Quick Rollback

```bash
# If something goes wrong, revert latest commit
git revert HEAD

# Rebuild
cd services/trade-execution
go build -o main cmd/main.go

# Restart
docker-compose restart trade-execution

# Verify reverted
grep "autoSquareOffScheduler" services/trade-execution/cmd/main.go
# (Should have no results)
```

### Manual Rollback

Remove 7 changes from `services/trade-execution/cmd/main.go`:

1. Delete line with `"github.com/.../scheduler"` import
2. Delete AutoSquareOffTime from Config struct
3. Delete AutoSquareOffTime from loadConfig()
4. Delete autoSquareOffScheduler initialization block
5. Delete autoSquareOffScheduler goroutine
6. Delete autoSquareOffScheduler.Stop() call
7. Delete auto square-off status log line

---

## Migration Checklist

Before deploying to production:

- [ ] Code changes committed and tested
- [ ] Environment variable documented
- [ ] `.env` file updated with `AUTO_SQUARE_OFF_TIME=15:05`
- [ ] Database schema verified (columns exist)
- [ ] Test strategy created with auto square-off enabled
- [ ] Service restarted and logs verified
- [ ] Test order created and position verified to close
- [ ] Monitoring and alerts configured
- [ ] Rollback procedure documented
- [ ] Team trained on feature
- [ ] Release notes prepared

---

## Support & Documentation

**Documentation Files**:
- `AUTO_SQUARE_OFF_IMPLEMENTATION_SUMMARY.md` - Overview
- `AUTO_SQUARE_OFF_IMPLEMENTATION_COMPLETE.md` - Detailed guide
- `AUTO_SQUARE_OFF_CODE_CHANGES.md` - Code diff
- `AUTO_SQUARE_OFF_QUICK_REFERENCE.md` - Quick guide
- `AUTO_SQUARE_OFF_ARCHITECTURE.md` - Diagrams
- `AUTO_SQUARE_OFF_DEPLOYMENT.md` - This file

**Contact**: For issues, check logs or review documentation files.

---

## Version History

| Date | Version | Status |
|------|---------|--------|
| 2025-01-18 | 1.0 | ✅ Released |

