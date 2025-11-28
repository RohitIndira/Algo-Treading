# Risk Management Service - Quick Reference

## What is it?
Your Risk Management Service is **currently running on port 9005**. It acts as a gatekeeper for all trades, ensuring they don't violate risk limits before being executed.

## What It Does (In Simple Terms)

### ✓ Before Every Trade (Pre-Trade Check)
- Checks if you've exceeded daily trade limits
- Validates you haven't hit daily loss limits
- Ensures position size is within limits
- Checks if you have sufficient margin
- Prevents portfolio over-concentration

### ✓ After Trade Execution (Post-Trade)
- Updates your daily trade count
- Tracks your profit/loss
- Updates position information
- Monitors portfolio exposure

### ✓ Real-Time Monitoring
- Tracks all open positions
- Calculates unrealized P&L
- Monitors daily performance
- Enforces circuit breakers

## How to Test It (3 Ways)

### 1. Quick Test (Recommended) ✨
```powershell
# Open a NEW terminal while the service is running
go run .\services\risk-management\test_client.go
```

**What you'll see:**
- ✓ Health check
- ✓ Setting risk limits
- ✓ Pre-trade validation (approved/rejected)
- ✓ Risk violations with reasons
- ✓ Current portfolio metrics

### 2. Manual API Testing with grpcurl
```powershell
# Install grpcurl first
go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest

# Health check
grpcurl -plaintext localhost:9005 risk_management.RiskManagementService/HealthCheck

# Check if a trade would be approved
grpcurl -plaintext -d '{
  "user_id": "my-user-123",
  "stock_code": 3456,
  "quantity": 10,
  "price": 2500,
  "max_daily_trades": 10,
  "max_loss_per_day": 5000
}' localhost:9005 risk_management.RiskManagementService/CheckPreTradeRisk
```

### 3. Integration Testing
The service is designed to be called by:
- **Trade Execution Service** - validates every order
- **API Gateway** - provides risk dashboard
- **Rules Engine** - checks strategy orders

## When Does It Execute?

| Trigger | Action | Example |
|---------|--------|---------|
| **Order Submission** | Pre-trade check | User tries to buy 100 shares → Service validates |
| **Order Fill** | Post-trade update | Order executed → Updates P&L and counters |
| **Dashboard Request** | Metrics query | User opens risk dashboard → Returns current stats |
| **Limit Update** | Store new limits | User sets max daily loss → Saves to Redis |
| **EOD Process** | Reset counters | Daily reset → Clears daily trade counts |

## Real Example Flow

```
User Strategy: "Buy 50 shares of RELIANCE at ₹2500"
              ↓
Rules Engine: Generates order request
              ↓
Risk Service: 🛡️ PRE-TRADE CHECK
              ├─ Daily trades: 5/10 ✓
              ├─ Daily loss: ₹2,000/₹5,000 ✓
              ├─ Position size: ₹125,000/₹150,000 ✓
              └─ Margin available: ✓
              ↓
         APPROVED ✅
              ↓
Trade Execution: Sends order to broker
              ↓
Order Filled: 50 shares @ ₹2500
              ↓
Risk Service: 🛡️ POST-TRADE UPDATE
              ├─ Daily trades: 6/10
              ├─ Position value: ₹250,000
              └─ Unrealized P&L: ₹0
```

## Key Features You Tested

From your test run, the service:
- ✅ Health check passed
- ✅ Set risk limits for test user
- ✅ Detected violations (insufficient margin, concentration)
- ✅ Calculated risk scores
- ✅ Provided helpful suggestions
- ✅ Tracked daily metrics

## Risk Violations Detected

Your test showed these violations:
1. **Insufficient Margin** - Not enough funds to cover the trade
2. **Concentration Limit** - Too much exposure in one stock
3. **Position Size Limit** - Order too large for set limits
4. **Per-Trade Risk** - Single trade risk too high

## Monitoring the Service

While it's running, you can:
- Check logs in the terminal for each request
- See approval/rejection decisions in real-time
- Monitor Redis for stored metrics
- Track gRPC calls

## Configuration

Default settings in `services/risk-management/config/config.go`:
- **Port**: 9005
- **Redis**: localhost:6379
- **Timeout**: 10 seconds

## Next Steps

1. **Keep it running** - It needs to be active to protect trades
2. **Test with real data** - Integrate with Trade Execution service
3. **Monitor violations** - Watch for patterns in rejections
4. **Adjust limits** - Fine-tune based on your strategy

## Common Questions

**Q: Why was my test trade rejected?**
A: The service starts with no margin (₹0), so it rejects trades. In production, it would check your actual account balance.

**Q: How do I stop the service?**
A: Press `Ctrl+C` in the terminal where it's running.

**Q: Does it save data?**
A: Yes, in Redis. Data persists until Redis is cleared or EOD reset runs.

**Q: Can I change limits?**
A: Yes, use the `SetRiskLimits` RPC call or modify the config.

---

**Service Status**: ✅ Running on localhost:9005
**Protocol**: gRPC
**Storage**: Redis
**Purpose**: Protect your trading account from excessive risk
