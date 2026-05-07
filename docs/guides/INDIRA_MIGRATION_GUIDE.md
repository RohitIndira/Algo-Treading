# Indira Securities API Migration Guide

## Overview

This guide provides step-by-step instructions for migrating from the multi-layer Odin API wrapper architecture to direct Indira Securities API integration.

---

## Migration Summary

### What's Changing

**Before (Old Architecture)**:
```
Trade-Execution (Go) → pkg/odin (Go) → odin-api-wrapper (Python) → b2c-api-python (Python) → Broker API
```

**After (New Architecture)**:
```
Trade-Execution (Go) → pkg/indira (Go) → Indira Securities API
```

### What's Being Removed

1. `services/odin-api-wrapper/` - Python FastAPI wrapper service
2. `b2c-api-python/` - Python SDK
3. `pkg/odin/` - Go client for odin-wrapper
4. `services/trade-execution/internal/odin/` - Odin integration layer
5. RabbitMQ dependencies (if only used for odin-wrapper communication)

### What's Being Added

1. `pkg/indira/` - New native Go client for Indira Securities API
2. `services/trade-execution/internal/indira/` - Indira integration layer
3. Bearer token management in authentication service

---

## Prerequisites

Before starting the migration, ensure you have:

1. ✅ Access to Indira Securities API documentation
2. ✅ Valid Indira Securities API credentials (userId, appId, source)
3. ✅ Bearer token generation mechanism (authentication endpoint)
4. ✅ Understanding of Indira API symbol format
5. ✅ Test environment for validation

---

## Migration Steps

### Phase 1: Database Migration

#### 1.1 Add New Columns to Orders Table

```sql
-- Add Indira-specific columns to orders table
ALTER TABLE orders ADD COLUMN IF NOT EXISTS indira_order_id VARCHAR(50);
ALTER TABLE orders ADD COLUMN IF NOT EXISTS indira_response TEXT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS product_type VARCHAR(20) DEFAULT 'INTRADAY';
ALTER TABLE orders ADD COLUMN IF NOT EXISTS target_price DECIMAL(18, 2);

-- Create index for Indira order ID lookups
CREATE INDEX IF NOT EXISTS idx_orders_indira_order_id ON orders(indira_order_id);

-- Add comments
COMMENT ON COLUMN orders.indira_order_id IS 'Order ID from Indira Securities API';
COMMENT ON COLUMN orders.indira_response IS 'Raw response from Indira Securities API';
COMMENT ON COLUMN orders.product_type IS 'Product type: INTRADAY, DELIVERY, CASH, MTF';
COMMENT ON COLUMN orders.target_price IS 'Target price for bracket orders';
```

#### 1.2 Add Bearer Token Storage to User Credentials

```sql
-- Add bearer token fields to user_credentials table
ALTER TABLE user_credentials ADD COLUMN IF NOT EXISTS indira_bearer_token TEXT;
ALTER TABLE user_credentials ADD COLUMN IF NOT EXISTS indira_token_expiry TIMESTAMP;
ALTER TABLE user_credentials ADD COLUMN IF NOT EXISTS indira_app_id VARCHAR(100);
ALTER TABLE user_credentials ADD COLUMN IF NOT EXISTS indira_source VARCHAR(20) DEFAULT 'WEB';

-- Add comments
COMMENT ON COLUMN user_credentials.indira_bearer_token IS 'Indira Securities API bearer token';
COMMENT ON COLUMN user_credentials.indira_token_expiry IS 'Bearer token expiry timestamp';
COMMENT ON COLUMN user_credentials.indira_app_id IS 'Indira application ID';
COMMENT ON COLUMN user_credentials.indira_source IS 'Source platform: IOS, AND, WEB';
```

### Phase 2: Update Authentication Service

#### 2.1 Discover Indira Login Endpoint

**Action Required**: Find the Indira Securities authentication/login endpoint that returns a bearer token.

Current known information:
- API requires Bearer token in Authorization header
- Headers: `userId`, `appId`, `source`
- Token has expiry time

**TODO**: Document the login endpoint once discovered.

#### 2.2 Implement Token Management

Update the user-login-service or authentication service to:

1. Call Indira login API
2. Store bearer token in database
3. Monitor token expiry
4. Auto-refresh tokens before expiry
5. Provide token to trade-execution service

Example implementation:

```go
// In user-login-service or auth service
type IndiraAuthService struct {
    db     *sql.DB
    // ... other fields
}

func (s *IndiraAuthService) Login(userId, password string) (*IndiraTokenInfo, error) {
    // Call Indira login API
    // This endpoint needs to be discovered
    resp, err := s.callIndiraLoginAPI(userId, password)
    if err != nil {
        return nil, err
    }
    
    // Store token in database
    _, err = s.db.Exec(`
        UPDATE user_credentials 
        SET indira_bearer_token = $1,
            indira_token_expiry = $2,
            updated_at = NOW()
        WHERE user_id = $3
    `, resp.BearerToken, resp.ExpiryTime, userId)
    
    return &IndiraTokenInfo{
        BearerToken: resp.BearerToken,
        ExpiryTime:  resp.ExpiryTime,
        UserId:      userId,
    }, nil
}

func (s *IndiraAuthService) GetValidToken(userId string) (string, error) {
    var token string
    var expiry time.Time
    
    err := s.db.QueryRow(`
        SELECT indira_bearer_token, indira_token_expiry
        FROM user_credentials
        WHERE user_id = $1
    `, userId).Scan(&token, &expiry)
    
    if err != nil {
        return "", err
    }
    
    // Check if token is expired or about to expire (5 min buffer)
    if time.Now().Add(5 * time.Minute).After(expiry) {
        // Refresh token
        return s.RefreshToken(userId)
    }
    
    return token, nil
}
```

### Phase 3: Update Trade Execution Service

#### 3.1 Update Dependencies

Edit `services/trade-execution/go.mod`:

```go
require (
    github.com/RohitIndira/Algo-Treading/pkg/indira v0.0.0
    // ... other dependencies
)

// Remove or comment out:
// github.com/RohitIndira/Algo-Treading/pkg/odin v0.0.0
```

#### 3.2 Update Configuration

Edit `services/trade-execution/cmd/main.go`:

```go
type Config struct {
    // ... existing fields ...
    
    // Remove Odin-specific config
    // OdinAPIURL   string
    
    // Add Indira-specific config
    IndiraUserId string
    IndiraAppId  string
    IndiraSource string
    
    // Keep or remove RabbitMQ depending on other uses
    // RabbitMQURL  string
}

func loadConfig() Config {
    return Config{
        // ... existing config ...
        
        IndiraUserId: getEnv("INDIRA_USER_ID", ""),
        IndiraAppId:  getEnv("INDIRA_APP_ID", ""),
        IndiraSource: getEnv("INDIRA_SOURCE", "WEB"),
    }
}
```

#### 3.3 Update Order Executor

Replace Odin client initialization with Indira client:

```go
// In cmd/main.go or wherever broker client is initialized

// OLD: Odin client
// odinClient := odin.NewExecutionClient(cfg.OdinAPIURL)

// NEW: Indira client
indiraClient := indira.NewExecutionClient(
    cfg.IndiraUserId,
    cfg.IndiraAppId,
    cfg.IndiraSource,
)

// Create order executor with Indira client
orderExecutor := executor.NewOrderExecutor(
    orderRepo,
    indiraClient, // Use Indira client instead of Odin
    kafkaPublisher,
)
```

#### 3.4 Update Order Executor Implementation

Edit `services/trade-execution/internal/executor/order_executor.go`:

```go
type OrderExecutor struct {
    orderRepo      repository.OrderRepository
    brokerClient   *indira.ExecutionClient // Changed from odin to indira
    kafkaPublisher publisher.KafkaPublisher
}

func (e *OrderExecutor) PlaceOrder(ctx context.Context, order *models.Order) error {
    // Ensure broker client is authenticated
    if !e.brokerClient.IsAuthenticated() {
        // Fetch bearer token from auth service or database
        token, expiry, err := e.getBearerToken(order.UserID)
        if err != nil {
            return fmt.Errorf("failed to get bearer token: %w", err)
        }
        e.brokerClient.SetBearerToken(token, expiry)
    }
    
    // Place order via Indira API
    orderID, err := e.brokerClient.PlaceOrder(ctx, order)
    if err != nil {
        return fmt.Errorf("broker API error: %w", err)
    }
    
    // Update order with Indira order ID
    order.IndiraOrderID = &orderID
    order.Status = models.OrderStatusSubmitted
    
    // Save to database
    if err := e.orderRepo.UpdateOrder(ctx, order); err != nil {
        return fmt.Errorf("failed to update order: %w", err)
    }
    
    // Publish to Kafka
    e.kafkaPublisher.PublishOrderUpdate(order)
    
    return nil
}
```

#### 3.5 Add Bearer Token Retrieval

Add method to retrieve bearer token from database or auth service:

```go
func (e *OrderExecutor) getBearerToken(userID string) (token string, expiry time.Time, err error) {
    // Option 1: Fetch from database
    err = e.db.QueryRow(`
        SELECT indira_bearer_token, indira_token_expiry
        FROM user_credentials
        WHERE user_id = $1
    `, userID).Scan(&token, &expiry)
    
    if err != nil {
        return "", time.Time{}, err
    }
    
    // Check if expired
    if time.Now().After(expiry) {
        return "", time.Time{}, fmt.Errorf("bearer token expired")
    }
    
    return token, expiry, nil
    
    // Option 2: Call auth service via gRPC/HTTP
    // authResp, err := e.authClient.GetBearerToken(ctx, &auth.TokenRequest{
    //     UserId: userID,
    // })
}
```

### Phase 4: Update Symbol Handling

#### 4.1 Update Symbol Format Conversion

Ensure all places that handle symbols use the new Indira format:

```go
// OLD format: Just symbol name
// symbol := "TCS"

// NEW format: Full Indira symbol
symbolBuilder := indira.NewSymbolBuilder()
symbolBuilder.Symbol = "TCS"
symbolBuilder.Exchange = "NSE"
symbolBuilder.Token = "11536"
symbolBuilder.Instrument = "STK"
symbolBuilder.Series = "EQ"
symbol := symbolBuilder.BuildSymbol() // "STK_TCS_EQ_NSE_11536"
```

#### 4.2 Update Database Queries

Update queries that filter or search by symbol to handle both formats during transition:

```go
// Support both old and new symbol formats
WHERE symbol = $1 OR symbol LIKE '%' || $1 || '%'
```

### Phase 5: Environment Configuration

#### 5.1 Update Environment Variables

Add to `.env` or deployment configs:

```bash
# Indira Securities API Configuration
INDIRA_USER_ID=ISPL19122
INDIRA_APP_ID=your-app-id-here
INDIRA_SOURCE=WEB  # or IOS, AND

# Remove old Odin config (or keep for rollback)
# ODIN_API_URL=http://localhost:8080
# ODIN_USER_ID=...
# ODIN_PASSWORD=...
```

#### 5.2 Update Docker Compose

Remove odin-api-wrapper service from `docker-compose.yml`:

```yaml
# REMOVE THIS SECTION:
# odin-api-wrapper:
#   build: ./services/odin-api-wrapper
#   ports:
#     - "8080:8000"
#   environment:
#     - ODIN_API_URL=...
#   depends_on:
#     - rabbitmq
```

### Phase 6: Testing

#### 6.1 Unit Tests

Create unit tests for Indira client:

```go
// pkg/indira/client_test.go
func TestPlaceOrder(t *testing.T) {
    client := indira.NewClient(indira.Config{
        UserId: "TEST_USER",
        AppId:  "TEST_APP",
        Source: "WEB",
    })
    
    // Set mock token
    client.SetBearerToken("mock-token", time.Now().Add(1*time.Hour))
    
    // Test order placement
    req := &indira.PlaceOrderRequest{
        Symbol:      "STK_TCS_EQ_NSE_11536",
        ExcToken:    "11536",
        Exc:         "NSE",
        OrdAction:   "BUY",
        OrdType:     "Market",
        Qty:         1,
        // ... other fields
    }
    
    resp, err := client.PlaceOrder(context.Background(), req)
    assert.NoError(t, err)
    assert.NotEmpty(t, resp.OrderId)
}
```

#### 6.2 Integration Tests

Create integration tests that call real Indira API (in test environment):

```go
func TestIndiraIntegration(t *testing.T) {
    if testing.Short() {
        t.Skip("Skipping integration test")
    }
    
    // Use test credentials
    client := indira.NewClient(indira.Config{
        UserId: os.Getenv("TEST_INDIRA_USER_ID"),
        AppId:  os.Getenv("TEST_INDIRA_APP_ID"),
        Source: "WEB",
    })
    
    // Get real bearer token from auth service
    token, expiry := getTestBearerToken(t)
    client.SetBearerToken(token, expiry)
    
    // Test order placement
    t.Run("PlaceOrder", func(t *testing.T) {
        // ... test with small quantity
    })
    
    // Test order book retrieval
    t.Run("GetOrderBook", func(t *testing.T) {
        orders, err := client.GetOrderBook(context.Background())
        assert.NoError(t, err)
        assert.NotNil(t, orders)
    })
}
```

#### 6.3 End-to-End Testing

Test complete order flow:

1. Create strategy
2. Generate trading signal
3. Place order via Indira API
4. Check order status
5. Verify order appears in order book
6. Cancel/modify order
7. Verify changes

### Phase 7: Deployment

#### 7.1 Gradual Rollout Plan

**Option 1: Feature Flag**

Implement feature flag to toggle between Odin and Indira:

```go
type BrokerClient interface {
    PlaceOrder(ctx context.Context, order *models.Order) (string, error)
    CancelOrder(ctx context.Context, exchange, orderID, symbol string) error
    // ... other methods
}

func NewBrokerClient(useIndira bool) BrokerClient {
    if useIndira {
        return indira.NewExecutionClient(...)
    }
    return odin.NewExecutionClient(...)
}

// In config
UseIndira: getEnvBool("USE_INDIRA_API", false)
```

**Option 2: Parallel Running**

Run both systems in parallel, compare results:

```go
func (e *OrderExecutor) PlaceOrder(ctx context.Context, order *models.Order) error {
    // Place via both APIs
    indiraOrderID, indiraErr := e.indiraClient.PlaceOrder(ctx, order)
    odinOrderID, odinErr := e.odinClient.PlaceOrder(ctx, order)
    
    // Compare results, log discrepancies
    if indiraErr != nil || odinErr != nil {
        log.Printf("API mismatch: Indira=%v, Odin=%v", indiraErr, odinErr)
    }
    
    // Use Indira result
    return indiraErr
}
```

#### 7.2 Deployment Steps

1. **Deploy to Development**
   ```bash
   # Update environment
   kubectl apply -f deployments/k8s/dev/trade-execution.yaml
   
   # Monitor logs
   kubectl logs -f deployment/trade-execution -n dev
   ```

2. **Deploy to Staging**
   ```bash
   # Run smoke tests
   ./scripts/smoke-test.sh staging
   
   # Deploy
   kubectl apply -f deployments/k8s/staging/trade-execution.yaml
   
   # Monitor for 24 hours
   ```

3. **Deploy to Production**
   ```bash
   # Enable for 10% of users
   kubectl set env deployment/trade-execution INDIRA_ROLLOUT_PERCENT=10
   
   # Monitor metrics
   # - Order success rate
   # - API latency
   # - Error rates
   
   # Gradually increase
   kubectl set env deployment/trade-execution INDIRA_ROLLOUT_PERCENT=25
   kubectl set env deployment/trade-execution INDIRA_ROLLOUT_PERCENT=50
   kubectl set env deployment/trade-execution INDIRA_ROLLOUT_PERCENT=100
   ```

#### 7.3 Rollback Plan

If issues are discovered:

```bash
# Quick rollback: revert environment variable
kubectl set env deployment/trade-execution USE_INDIRA_API=false

# Or full rollback: redeploy previous version
kubectl rollout undo deployment/trade-execution
```

### Phase 8: Cleanup

Once Indira API is stable and fully deployed:

#### 8.1 Remove Old Code

```bash
# Remove Odin wrapper service
rm -rf services/odin-api-wrapper/

# Remove Python SDK
rm -rf b2c-api-python/

# Remove Odin Go client
rm -rf pkg/odin/

# Remove Odin integration from trade-execution
rm -rf services/trade-execution/internal/odin/
```

#### 8.2 Remove Old Database Columns

```sql
-- After confirming no longer needed
ALTER TABLE orders DROP COLUMN IF EXISTS odin_order_id;
ALTER TABLE orders DROP COLUMN IF EXISTS odin_response;
```

#### 8.3 Remove Old Environment Variables

```bash
# Remove from .env and deployment configs
# ODIN_API_URL
# ODIN_USER_ID
# ODIN_PASSWORD
# RABBITMQ_URL (if only used for Odin)
```

#### 8.4 Update Documentation

- Update architecture diagrams
- Update API documentation
- Update deployment guides
- Archive migration documentation

---

## Monitoring & Validation

### Key Metrics to Monitor

1. **Order Success Rate**
   - Target: >99%
   - Alert if drops below 95%

2. **API Latency**
   - Target: <500ms for order placement
   - Alert if exceeds 1000ms

3. **Error Rates**
   - Target: <1% error rate
   - Alert on sudden spikes

4. **Token Refresh Success**
   - Target: 100% success on token refresh
   - Alert on any failures

### Validation Checklist

- [ ] Order placement working
- [ ] Order modification working
- [ ] Order cancellation working
- [ ] Order book retrieval working
- [ ] Position retrieval working
- [ ] Holdings retrieval working
- [ ] Bearer token management working
- [ ] Symbol format conversion working
- [ ] Error handling working
- [ ] Logging working
- [ ] Metrics collection working

---

## Troubleshooting

### Common Issues

#### 1. Authentication Failures

**Symptom**: "not authenticated" errors

**Solution**:
- Check bearer token is set correctly
- Verify token has not expired
- Check userId, appId, source are correct
- Verify authentication service is returning valid tokens

#### 2. Symbol Format Errors

**Symptom**: "invalid symbol" errors from API

**Solution**:
- Verify symbol format: `STK_TCS_EQ_NSE_11536`
- Check exchange token is correct
- Ensure exchange name is cleaned (no "EXCHANGE_" prefix)

#### 3. Order Placement Failures

**Symptom**: Orders rejected by broker

**Solution**:
- Check order parameters (qty, price, etc.)
- Verify product type is valid
- Check order type mapping
- Review broker error message in response

#### 4. Token Expiry Issues

**Symptom**: Intermittent authentication failures

**Solution**:
- Implement auto-refresh before expiry
- Add token expiry monitoring
- Set up alerts for token issues

---

## Support & Resources

### Documentation
- Indira Securities API docs (if available)
- Internal wiki: `/docs/guides/INDIRA_API_MIGRATION_ANALYSIS.md`
- pkg/indira README: `/pkg/indira/README.md`

### Code References
- New client: `pkg/indira/`
- Integration layer: `services/trade-execution/internal/indira/`
- Old code (for reference): `pkg/odin/`, `services/odin-api-wrapper/`

### Contact
- Backend team: backend-team@company.com
- DevOps team: devops@company.com
- Broker support: support@indiratrade.com

---

## Appendix

### A. API Endpoint Reference

| Function | Old Endpoint (Odin) | New Endpoint (Indira) |
|----------|---------------------|----------------------|
| Place Order | POST /orders/place | POST /order-services/api/order/v1/place-order |
| Modify Order | PUT /orders/modify | POST /order-services/api/order/v1/modify-order |
| Cancel Order | DELETE /orders/cancel | POST /order-services/api/order/v1/cancel-order |
| Order Book | GET /orders/book | GET /portfolio-services/api/order/v1/order-book |
| Positions | GET /portfolio/positions | GET /portfolio-services/api/portfolio/v1/position-book |
| Holdings | GET /portfolio/holdings | GET /portfolio-services/api/portfolio/v2/holdings |

### B. Field Mapping Reference

| Internal Field | Odin Field | Indira Field |
|---------------|------------|--------------|
| Symbol | symbol | symbol (full format) |
| Exchange | exchange | exc |
| OrderType | order_type | ordType |
| OrderSide | transaction_type | ordAction |
| Quantity | quantity | qty |
| Price | price | limitPrice |
| StopLoss | trigger_price | triggerPrice |
| ProductType | product_type | prdType |
| Validity | validity | ordValidity |

### C. Migration Timeline (Example)

| Week | Activity | Owner |
|------|----------|-------|
| 1 | Database migrations, auth service updates | Backend Team |
| 2 | Trade-execution service updates | Backend Team |
| 3 | Testing in dev environment | QA Team |
| 4 | Staging deployment & testing | DevOps + QA |
| 5 | Production deployment (10% rollout) | DevOps |
| 6 | Production deployment (50% rollout) | DevOps |
| 7 | Production deployment (100% rollout) | DevOps |
| 8 | Monitoring & validation | All Teams |
| 9 | Cleanup old code | Backend Team |

---

## Conclusion

This migration will significantly simplify the architecture, improve performance, and make the system easier to maintain. Follow each phase carefully, test thoroughly, and monitor closely during rollout.

For questions or issues during migration, consult this guide or reach out to the team leads.

**Good luck with the migration! 🚀**
