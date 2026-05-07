# Complete Algorithmic Trading System Analysis (Code-Based)

**Analysis Date:** November 18, 2025  
**Branch:** staging  
**Analysis Method:** Direct code inspection

---

## 📊 SYSTEM ARCHITECTURE & DATA FLOW

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         ALGORITHMIC TRADING SYSTEM                              │
└─────────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────────┐
│                          EXTERNAL DATA SOURCE                                    │
│                                                                                  │
│   MongoDB Cloud (StockGPT.fryqpbi.mongodb.net)                                 │
│   Database: trading_system                                                         │
│   Collection: news_impact_dashboard                                               │
│   - Stock news & events                                                         │
│   - Sentiment analysis                                                          │
│   - Impact scores (0-10)                                                        │
│   - Real-time insertions                                                        │
└────────────────────────┬─────────────────────────────────────────────────────────┘
                         │
                         │ Change Stream (watches inserts only)
                         ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│ 1. DATA INGESTION SERVICE                                                       │
│                                                                                  │
│ Purpose: Entry point for all market data                                        │
│                                                                                  │
│ What It Does:                                                                   │
│ ✓ Watches MongoDB collection via Change Streams                                │
│ ✓ Detects new news insertions in real-time                                     │
│ ✓ Extracts fullDocument from change event                                      │
│ ✓ Converts to Extended JSON format (BSON → JSON)                               │
│ ✓ Publishes to Kafka topic: "news-events"                                      │
│ ✓ Uses _id field as message key                                                │
│                                                                                  │
│ What It Cannot Do:                                                              │
│ ✗ Filter/validate news quality                                                  │
│ ✗ Process historical data (only real-time)                                      │
│ ✗ Retry failed Kafka publishes                                                  │
│ ✗ Batch multiple news items                                                     │
│ ✗ Handle MongoDB disconnections gracefully                                      │
│                                                                                  │
│ Configuration:                                                                   │
│ - Kafka Brokers: localhost:9092 (default)                                      │
│ - Topic: news-events                                                            │
│ - Batch size: 100 messages                                                      │
│ - Max attempts: 3                                                               │
│ - Publish timeout: 5 seconds per message                                        │
└────────────────────────┬─────────────────────────────────────────────────────────┘
                         │
                         │ Kafka Topic: "news-events"
                         │ Format: Extended JSON (BSON serialized)
                         │ Key: MongoDB _id
                         ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│ 2. USER CONFIG SERVICE (gRPC Port 50051)                                        │
│                                                                                  │
│ Purpose: Manage trading strategies & user configurations                        │
│                                                                                  │
│ What It Does:                                                                   │
│ ✓ Full CRUD operations for trading strategies (gRPC API)                       │
│ ✓ Stores strategies in PostgreSQL with ACID compliance                         │
│ ✓ Validates strategy schema before saving                                      │
│ ✓ Publishes strategy changes to Kafka topic: "strategy-config"                 │
│ ✓ Event types: CREATE, UPDATE, DELETE, ACTIVATE, DEACTIVATE                    │
│ ✓ Optimistic locking with version control                                      │
│ ✓ Pagination support (max 100, default 20)                                     │
│ ✓ Bulk operations by strategy IDs                                              │
│                                                                                  │
│ What It Cannot Do:                                                              │
│ ✗ Validate strategy logic correctness (only schema)                            │
│ ✗ Backtest strategies                                                           │
│ ✗ Clone or template strategies                                                  │
│ ✗ Version history or rollback                                                   │
│ ✗ Import/export bulk strategies                                                 │
│ ✗ Multi-user collaboration                                                      │
│ ✗ Performance analytics                                                         │
│                                                                                  │
│ Strategy Configuration Fields:                                                  │
│ - News Filters: impact_score_threshold, sentiments[], categories[]             │
│ - Stock Filters: stocks[] (empty = all stocks)                                 │
│ - Price Filters: min_price, max_price (both 0 = no filter)                     │
│ - Volume: min_volume                                                            │
│ - Exchange: NSE, BSE, or both                                                   │
│ - Trade Config: order_type, quantity, stop_loss_pct, take_profit_pct           │
│ - Special: match_all_news (bypasses most filters)                              │
└────────────────────────┬─────────────────────────────────────────────────────────┘
                         │
                         │ Kafka Topic: "strategy-config"
                         │ Consumed by Rules Engine for cache updates
                         ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│ 3. RULES ENGINE SERVICE (gRPC Port 50053)                                       │
│                                                                                  │
│ Purpose: Core intelligence - match news events to trading strategies            │
│                                                                                  │
│ What It Does:                                                                   │
│ ✓ Consumes from Kafka topic: "news-events"                                     │
│ ✓ Converts MongoDB events to MarketEvent objects                               │
│ ✓ Loads strategies from PostgreSQL and caches in Redis                         │
│ ✓ Uses Redis cache for strategy data (TTL-based)                               │
│ ✓ Concurrent strategy evaluation (worker pool: 50 workers)                     │
│ ✓ Max concurrent matches: 100 strategies simultaneously                        │
│ ✓ Condition evaluation with weighted scoring:                                  │
│   - impact_score: 25% weight (most important)                                  │
│   - stock: 20% weight                                                           │
│   - sentiment: 15% weight                                                       │
│   - category: 15% weight                                                        │
│   - price_range: 10% weight                                                     │
│   - volume: 7.5% weight                                                         │
│   - pct_change: 5% weight                                                       │
│   - exchange: 2.5% weight                                                       │
│ ✓ Min match score threshold: 80.0 (configurable)                               │
│ ✓ Full match bonus: 10% (capped at 100)                                        │
│ ✓ Failure penalty: up to 10% if >50% conditions fail                           │
│ ✓ Publishes matched orders to RabbitMQ                                         │
│ ✓ Circuit breaker for RabbitMQ (5 failures, 60s timeout)                       │
│ ✓ Auto-reconnection for Kafka & RabbitMQ                                       │
│                                                                                  │
│ Special Features:                                                               │
│ • Match-all strategy: if match_all_news=true, auto-passes all conditions       │
│   (only impact_score still checked)                                            │
│ • Empty arrays mean "accept all" (e.g., stocks=[] accepts all stocks)          │
│                                                                                  │
│ What It Cannot Do:                                                              │
│ ✗ Publish orders if RabbitMQ unavailable (circuit breaker opens)               │
│ ✗ Fallback if Redis cache fails (degrades to database only)                    │
│ ✗ Reprocess failed events automatically                                        │
│ ✗ Dead-letter queue for unmatched events                                       │
│ ✗ Modify strategies at runtime (needs cache expiry)                            │
│                                                                                  │
│ Performance Metrics:                                                            │
│ - Throughput: 1000+ events/hour                                                │
│ - Latency: <100ms (p95)                                                        │
│ - Batch size: 100 messages                                                      │
│ - Commit interval: configurable                                                 │
└────────────────────────┬─────────────────────────────────────────────────────────┘
                         │
                         │ RabbitMQ Exchange: "order.execution.exchange"
                         │ Queue: "order.execution.queue"
                         │ Routing Key: "order.execution"
                         │ Format: JSON OrderRequest
                         ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│ 4. RISK MANAGEMENT SERVICE (gRPC Port 50055)                                    │
│                                                                                  │
│ Purpose: Pre-trade risk validation & post-trade monitoring                      │
│                                                                                  │
│ What It Does:                                                                   │
│ ✓ Pre-trade risk checks (8 validations):                                       │
│   1. Daily Trade Limit - max trades per day                                    │
│   2. Daily Loss Limit - max loss threshold                                     │
│   3. Position Size Limit - max position value                                  │
│   4. Per-Trade Risk Limit - max loss per trade                                 │
│   5. Duplicate Order Prevention - fingerprint-based (stock+side+qty)           │
│   6. Insufficient Margin Check - balance vs required margin                    │
│   7. Circuit Breaker - daily loss % threshold (5% default)                     │
│   8. Portfolio Concentration - max exposure per stock                          │
│                                                                                  │
│ ✓ Risk profile adjustments (user psychology):                                  │
│   - Conservative: 50% tighter limits                                           │
│   - Moderate: no adjustment                                                     │
│   - Aggressive: 50% looser limits                                              │
│                                                                                  │
│ ✓ Post-trade updates:                                                           │
│   - Increment daily trade counter                                              │
│   - Update positions in Redis                                                   │
│   - Track realized/unrealized P&L                                              │
│                                                                                  │
│ ✓ Risk scoring (cumulative):                                                   │
│   - Daily trade limit violation: +15 points                                    │
│   - Daily loss limit: +20 points                                               │
│   - Position size: +10 points                                                   │
│   - Per-trade risk: +15 points                                                  │
│   - Duplicate order: +10 points                                                │
│   - Insufficient margin: +20 points                                            │
│   - Circuit breaker: +25 points                                                │
│                                                                                  │
│ What It Cannot Do:                                                              │
│ ✗ Block orders after submission to broker                                      │
│ ✗ Real-time market data integration                                            │
│ ✗ Complex derivatives or multi-leg strategies                                  │
│ ✗ Historical risk analytics                                                     │
│ ✗ Firm-wide risk limits (only user-level)                                      │
│ ✗ External risk system integration                                             │
│ ✗ Operate without Redis (complete failure)                                     │
│                                                                                  │
│ Storage: Redis (all metrics in-memory)                                          │
└────────────────────────┬─────────────────────────────────────────────────────────┘
                         │
                         │ gRPC calls from Trade Execution Service
                         │ Returns: approved=true/false + violations[]
                         ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│ 5. TRADE EXECUTION SERVICE (gRPC Port 50054)                                    │
│                                                                                  │
│ Purpose: Execute approved orders via broker API                                 │
│                                                                                  │
│ What It Does:                                                                   │
│ ✓ Consumes from RabbitMQ queue: "order.execution.queue"                        │
│ ✓ Worker pool: 10 concurrent workers (configurable)                            │
│ ✓ Prefetch count: 10 messages per worker                                       │
│ ✓ Validates order request (user_id, stock_code, quantity)                      │
│ ✓ Converts OrderRequest → internal Order model                                 │
│ ✓ Saves order to PostgreSQL with status transitions:                           │
│   RECEIVED → VALIDATED → PENDING → SUBMITTED → FILLED/REJECTED/FAILED          │
│                                                                                  │
│ ✓ Retry logic with exponential backoff:                                        │
│   - Max retries: 3 (configurable)                                              │
│   - Base delay: 1 second                                                        │
│   - Exponential: delay = base * attempt (1s, 2s, 3s)                           │
│                                                                                  │
│ ✓ Order execution flow:                                                         │
│   1. Check risk approval status                                                │
│   2. Update status to PENDING                                                   │
│   3. Call Indira Securities API to place order                                 │
│   4. Store broker order ID (indira_order_id)                                   │
│   5. Update status to SUBMITTED                                                 │
│   6. Record execution events                                                    │
│                                                                                  │
│ ✓ Message handling:                                                             │
│   - Valid order → Ack                                                           │
│   - Retryable error (retry < 3) → Nack + requeue                              │
│   - Max retries reached → Nack + DLQ                                           │
│   - Invalid request → Nack + DLQ                                               │
│                                                                                  │
│ ✓ Database operations:                                                          │
│   - Create order                                                                │
│   - Update order status                                                         │
│   - Record execution events                                                     │
│   - Get user orders                                                             │
│   - Get order by ID                                                             │
│                                                                                  │
│ ✓ gRPC API endpoints:                                                           │
│   - GetOrder, GetUserOrders, CancelOrder, GetOrderStatus, GetOrderStatistics   │
│                                                                                  │
│ What It Cannot Do:                                                              │
│ ✗ Modify orders once submitted (TODO in code)                                  │
│ ✗ Filter logic in GetUserOrders (TODO)                                         │
│ ✗ Statistics aggregation (TODO)                                                 │
│ ✗ Handle complex order types (basket, iceberg)                                 │
│ ✗ Advanced order routing                                                        │
│ ✗ Function if Indira API is down                                               │
│ ✗ Partial fill handling                                                         │
│ ✗ Order cancellation cascade                                                    │
│                                                                                  │
│ Database Schema:                                                                 │
│ - orders table: full order details + status tracking                           │
│ - execution_events table: audit log of all state changes                       │
│ - 8 performance indexes                                                         │
└────────────────────────┬─────────────────────────────────────────────────────────┘
                         │
                         │ Direct API calls to Indira Securities
                         ▼
                    Indira Securities API (broker)

```

---

## 🔄 COMPLETE DATA FLOW SEQUENCE

1. **News Insertion** → MongoDB Cloud (external)
2. **Change Stream Detection** → Data Ingestion Service watches insert
3. **Extract & Transform** → fullDocument → Extended JSON
4. **Kafka Publish** → Topic: "news-events"
5. **Kafka Consume** → Rules Engine receives event
6. **MongoDB Event → MarketEvent** → Convert to internal format
7. **Strategy Lookup** → Load from PostgreSQL / Redis cache
8. **Redis Cache Lookup** → Get full strategy details
9. **Concurrent Evaluation** → 50 workers evaluate conditions
10. **Scoring** → Weighted scoring (impact: 25%, stock: 20%, etc.)
11. **Filter** → Match score >= 80.0
12. **Order Request Creation** → Convert match to order
13. **RabbitMQ Publish** → Queue: "order.execution.queue"
14. **RabbitMQ Consume** → Trade Execution worker picks up
15. **Validate** → Check user_id, stock_code, quantity
16. **Database Save** → PostgreSQL with status: RECEIVED
17. **Risk Check** → gRPC call to Risk Management Service
18. **Risk Evaluation** → 8 checks + risk profile adjustment
19. **Approval/Rejection** → Returns approved + violations
20. **Execute** → If approved, call Indira Securities API
21. **Broker Submission** → Place order via Indira client
22. **Broker Response** → Returns indira_order_id
23. **Database Update** → Status: SUBMITTED, store indira_order_id
25. **Post-Trade Update** → Update Redis metrics
26. **Event Recording** → Execution events table

---

## ⚙️ HARDCODED VALUES & DEFAULT CONFIGURATIONS

### 🚨 Critical Issues - MUST BE USER-CONFIGURABLE

#### 1. **Service Ports** (Should be configurable via env)
```
API Gateway:     8081 (HTTP)
User Config:     50051 (gRPC)
Data Ingestion:  50052 (gRPC)
Rules Engine:    50053 (gRPC)
Trade Execution: 50054 (gRPC)
Risk Management: 50055 (gRPC)
```

#### 2. **Worker/Concurrency Settings** (Should be per-deployment)
```go
// Rules Engine - config/config.go
WorkerCount:             50    // Should vary by server capacity
MaxBatchSize:            100   // Should vary by load
MaxConcurrentMatches:    100   // Should be configurable
MinMatchScore:           80.0  // Should be user-adjustable

// Trade Execution - cmd/main.go
WorkerCount:   10   // Default, should be based on load
PrefetchCount: 10   // Default, should tune per RabbitMQ performance
MaxRetries:    3    // Should be configurable per environment
RetryDelay:    1s   // Should be configurable
```

#### 3. **Connection Pool Settings** (Should be environment-specific)
```go
// Trade Execution - cmd/main.go:171
db.SetMaxOpenConns(25)  // Hardcoded, should be configurable
db.SetMaxIdleConns(5)   // Hardcoded
db.SetConnMaxLifetime(5 * time.Minute)  // Hardcoded
```

#### 4. **Kafka Configuration** (Defaults should be removed)
```go
// All services have these defaults:
KAFKA_BROKERS=localhost:9092  // Should REQUIRE explicit config
Topic names are hardcoded in code, should be env vars
```

#### 5. **Risk Management Thresholds** (Should be user-configurable)
```go
// services/risk-management/internal/checker/pre_trade.go:141
circuitBreakerThreshold := 5.0  // Hardcoded 5% daily loss limit
// Should be per-user configurable

// Risk profile adjustments are hardcoded:
Conservative: 0.5x limits
Moderate: 1.0x limits  
Aggressive: 1.5x limits
// Should be configurable multipliers
```

#### 6. **Scoring Weights** (Should be strategy-level configurable)
```go
// services/rules-engine/internal/matcher/scorer.go:11-18
weights: map[string]float64{
    "impact_score": 25.0,  // Hardcoded weights
    "stock":        20.0,
    "sentiment":    15.0,
    "category":     15.0,
    "price_range":  10.0,
    "volume":       7.5,
    "pct_change":   5.0,
    "exchange":     2.5,
}
// Should be configurable per strategy or globally
```

#### 7. **Timeouts** (Should be configurable)
```go
// Data Ingestion - internal/watcher/mongo_watcher.go:72
context.WithTimeout(ctx, 5*time.Second)  // Hardcoded publish timeout

// RabbitMQ Circuit Breaker - rules-engine/internal/publisher/publisher.go:95
5 max failures, 60*time.Second timeout  // Hardcoded
```

#### 8. **Pagination Defaults** (Should be configurable)
```go
// User Config Service - internal/service/strategy_service.go:103-104
if limit <= 0 || limit > 100 {
    limit = 20  // Hardcoded default and max
}
```

#### 9. **Queue/Exchange Names** (Should be env vars)
```go
// Trade Execution - cmd/main.go
QueueName:     "order.execution.queue"       // Hardcoded
Exchange:      "order.execution.exchange"    // Hardcoded
RoutingKey:    "order.execution"             // Hardcoded
```

#### 10. **Database Defaults** (Should fail if not configured)
```go
// All services:
DB_HOST=localhost      // Should REQUIRE explicit config
POSTGRES_PORT=5432     // Should REQUIRE explicit config
REDIS_HOST=localhost   // Should REQUIRE explicit config
```

---

## 🔧 FIELDS THAT SHOULD BE USER-CONFIGURABLE (NOT DEFAULTS)

### Environment-Level Configuration
```yaml
# Should be REQUIRED (no defaults):
- All database hosts/ports
- All message broker URLs
- All service URLs
- API keys and secrets

# Should be configurable per environment:
- Worker counts
- Connection pool sizes
- Retry counts and delays
- Timeouts
- Batch sizes
- Cache TTLs
- Circuit breaker thresholds
```

### User-Level Configuration
```yaml
# Should be in database, not hardcoded:
- Risk limits (per user or per strategy)
- Risk profile multipliers
- Circuit breaker thresholds
- Daily trade limits
- Position size limits
- Match score thresholds

# Should be strategy-level:
- Scoring weights for conditions
- Match score minimum
- Order execution parameters
```

### System-Level Configuration
```yaml
# Should be configurable via config service:
- Service discovery endpoints
- Queue/topic names
- Retry policies
- Logging levels
- Monitoring endpoints
```

---

## 🚫 INCOMPLETE IMPLEMENTATIONS (TODOs Found in Code)

### Trade Execution Service
```go
// internal/server/grpc_server.go:186
// TODO: Implement order modification logic

// internal/server/grpc_server.go:200
// TODO: Add filtering logic based on req.Filter

// internal/server/grpc_server.go:241
// TODO: Implement statistics aggregation

// internal/executor/executor.go:150
// TODO: Parse response and update order based on status
```

---

## 🎯 SERVICE CAPABILITIES MATRIX

| Service | Can Do | Cannot Do | Critical Dependencies |
|---------|--------|-----------|----------------------|
| **Data Ingestion** | Watch MongoDB changes, Publish to Kafka | Filter quality, Batch process, Retry | MongoDB Cloud, Kafka |
| **User Config** | CRUD strategies, Kafka events | Validate logic, Backtest, Version control | PostgreSQL, Kafka (optional) |
| **Rules Engine** | Match events, Score strategies, Concurrent eval | Reprocess failures, DLQ | PostgreSQL, Redis, Kafka, RabbitMQ |
| **Risk Management** | 8 pre-trade checks, Profile adjustments | Post-submission blocking, Complex derivatives | Redis (CRITICAL) |
| **Trade Execution** | Order execution, Retry logic, Status tracking | Modify orders, Complex orders, Partial fills | PostgreSQL, RabbitMQ, Indira API |

---

## 🔒 SINGLE POINTS OF FAILURE

1. **MongoDB Cloud** - No fallback if unavailable
2. **PostgreSQL** - Strategy loading and order storage fails
3. **Redis** - Risk Management completely fails
4. **RabbitMQ** - Circuit breaker opens but no DLQ
5. **PostgreSQL** - Services fail to start
6. **Indira API** - No order execution possible

---

## 📈 RECOMMENDATIONS

### High Priority
1. ✅ Remove ALL `localhost` defaults - force explicit configuration
2. ✅ Make all worker counts, pool sizes environment-specific
3. ✅ Implement health checks for all services
4. ✅ Add circuit breakers to ALL external dependencies
5. ✅ Complete TODO items in Trade Execution
6. ✅ Implement dead-letter queues for failed messages
7. ✅ Make risk thresholds user-configurable in database

### Medium Priority
8. ✅ Centralize configuration (Consul/etcd)
9. ✅ Add distributed tracing (Jaeger/Zipkin)
10. ✅ Implement service mesh (Istio/Linkerd)
11. ✅ Add comprehensive monitoring and alerting
12. ✅ Make scoring weights configurable per strategy

### Low Priority
13. ✅ Add rate limiting to all APIs
14. ✅ Implement graceful degradation strategies
15. ✅ Add caching layers where appropriate

---

## 📊 SYSTEM METRICS

| Metric | Value | Configurable? |
|--------|-------|---------------|
| Rules Engine Throughput | 1000+ events/hour | ❌ No (capacity-based) |
| Match Latency (p95) | <100ms | ❌ No |
| Min Match Score | 80.0 | ✅ Via env var |
| Worker Count (Rules) | 50 | ✅ Via env var |
| Worker Count (Execution) | 10 | ✅ Via env var |
| Max Concurrent Matches | 100 | ✅ Via env var |
| Retry Max Attempts | 3 | ✅ Via env var |
| Circuit Breaker Failures | 5 | ❌ Hardcoded |
| Circuit Breaker Timeout | 60s | ❌ Hardcoded |

---

**End of Analysis**
