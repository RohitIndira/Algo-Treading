# Algorithmic Trading System Architecture

> ⚠️ **This is the original DESIGN document. Several parts do not match the code as built.**
> Verified against the codebase on 2026-07-13. Key as-built corrections (authoritative source: the code, and [`docs/SERVICE_DEPENDENCIES.md`](../SERVICE_DEPENDENCIES.md)):
>
> | This doc says | As built (code) |
> |---|---|
> | Order execution consumes from **RabbitMQ** `order.execution.queue` | **Kafka** `trade-signals` topic. RabbitMQ config exists but is **not used** on the live path |
> | Ingestion topic `market.data.news` | `news-events` |
> | User Config stores configs in **MongoDB** + Redis hot cache | **PostgreSQL** `trading_db`; **no Redis** in user-config |
> | Rules Engine loads strategies from **PostgreSQL (cached in Redis)** | **In-memory** config store, seeded by gRPC `BulkLoad` from user-config and kept in sync via `user-config-events` Kafka topic; PostgreSQL only for trade-signal auditing |
> | Trade Execution does pre/post-trade risk via **Risk Management gRPC** | Trade Execution **never calls** risk-management; risk is checked in **rules-engine** (and is **fail-open**) |
> | Risk Management uses **PostgreSQL** for history | **Redis only**; also excluded from the PM2 deployment |
> | API Gateway exposes gRPC for order status | Gateway speaks gRPC to **user-config only**; orders/AMN are HTTP proxies |
> | "Monitoring & Observability Service" (#7) as a service | Not a separate service; each Go service exposes Prometheus `/metrics` |
>
> The current, code-derived component/flow view lives in [`docs/ARCHITECTURE.drawio`](../ARCHITECTURE.drawio).

## System Overview

A high-performance, event-driven microservices-based algorithmic trading system for Indian stock markets, supporting 10,000+ concurrent users with personalized trading strategies based on real-time news sentiment and impact scores.

---

## Architecture Principles

1. **Microservices Pattern**: Independent, scalable services communicating via gRPC
2. **Event-Driven Architecture**: Asynchronous processing using Kafka and RabbitMQ
3. **Polyglot Persistence**: Right database for each use case
4. **High Throughput**: Designed for 10K users with real-time processing
5. **Loose Coupling**: Services communicate through well-defined APIs

---

## System Components

### 1. API Gateway Service
**Purpose**: Single entry point for all client requests

**Technology Stack**:
- Go with Gin/Fiber framework
- JWT authentication
- Rate limiting (Redis-based)
- Request validation

**Responsibilities**:
- User authentication and authorization
- Request routing to appropriate microservices
- API rate limiting
- Response aggregation

**gRPC Endpoints**:
- User management calls
- Configuration management calls
- Order status queries

---

### 2. User Configuration Service
**Purpose**: Manage user trading preferences and strategies

**Technology Stack**:
- Go microservice
- MongoDB for config storage
- Redis for hot cache (active users)
- gRPC server

**Data Model**:
```json
{
  "user_id": "string",
  "strategy_name": "string",
  "active": true,
  "conditions": {
    "impact_score_threshold": 5,
    "sentiment": ["positive", "neutral"],
    "categories": ["Financial Results", "Dividends"],
    "stocks": ["NSE:RELIANCE", "BSE:500325"],
    "price_range": {
      "min_price": 100,
      "max_price": 5000
    },
    "volume_threshold": 100000,
    "pct_change_threshold": 2.0
  },
  "trade_config": {
    "order_type": "MARKET/LIMIT",
    "quantity": 1000,
    "max_position_size": 100000,
    "stop_loss_pct": 2.0,
    "take_profit_pct": 5.0,
    "exchange": "NSE/BSE"
  },
  "risk_limits": {
    "max_daily_trades": 10,
    "max_loss_per_day": 50000,
    "position_sizing": "FIXED/PERCENTAGE"
  },
  "created_at": "timestamp",
  "updated_at": "timestamp"
}
```

**gRPC Methods**:
```protobuf
service UserConfigService {
  rpc CreateStrategy(CreateStrategyRequest) returns (Strategy);
  rpc UpdateStrategy(UpdateStrategyRequest) returns (Strategy);
  rpc DeleteStrategy(DeleteStrategyRequest) returns (Empty);
  rpc GetStrategy(GetStrategyRequest) returns (Strategy);
  rpc ListUserStrategies(ListStrategiesRequest) returns (StrategyList);
  rpc ActivateStrategy(ActivateRequest) returns (Strategy);
  rpc DeactivateStrategy(DeactivateRequest) returns (Strategy);
}
```

**Scalability Features**:
- Redis caching for active user configs (10K hot cache)
- MongoDB indexed queries on user_id, active status
- Config versioning for audit trail

---

### 3. Data Ingestion Service
**Purpose**: Monitor MongoDB change streams and publish events

**Technology Stack**:
- Go microservice
- MongoDB Change Streams
- Kafka Producer
- Goroutines for concurrent processing

**Responsibilities**:
- Watch MongoDB collection for new/updated documents
- Validate and enrich data
- Publish to Kafka topic: `market.data.news`
- Handle backpressure and buffering

**Event Schema** (Published to Kafka):
```json
{
  "event_id": "uuid",
  "event_type": "NEWS_UPDATE",
  "timestamp": "2025-09-10T13:26:07.125Z",
  "stock_data": {
    "stock": 517170,
    "exchange": "BSE",
    "symbol": "EDVENSWA",
    "company": "INE125G01014",
    "company_name": "Edvenswa Enterprises Ltd"
  },
  "news_data": {
    "news_id": "a0e732d7-a0d9-4bf6-bc74-5625e6e2d2a9",
    "news_link": "https://...",
    "category": "General / Other",
    "short_summary": "...",
    "document_date": "2025-09-09 00:02:13"
  },
  "analysis": {
    "sentiment": "Neutral",
    "impact": "Routine procedural update...",
    "impact_score": 2
  },
  "market_data": {
    "last_traded_price": 47.75,
    "pct_change": 5.96,
    "news_first_price": 47,
    "news_pct_change": 1.6,
    "price_map": {
      "open": 45.45,
      "high": 47.99,
      "low": 45.21,
      "volume": 41504
    }
  }
}
```

**Performance Optimization**:
- Batch processing (100 events/batch)
- Goroutine pool (100 workers)
- Kafka partitioning by stock symbol
- Circuit breaker for Kafka failures

---

### 4. Rules Processing Engine
**Purpose**: Match incoming market events against user strategies

**Technology Stack**:
- Go microservice
- Kafka Consumer (news-events, user-config-events topics)
- PostgreSQL for strategy storage
- Redis for strategy caching and LTP lookup
- RabbitMQ Producer (order.execution.queue)

**Matching Algorithm**:
```
1. Receive event from Kafka
2. Load active strategies from PostgreSQL (cached in Redis)
3. For each strategy:
   - Evaluate all conditions (impact score, sentiment, price, volume, etc.)
   - Weighted scoring (impact: 25%, stock: 20%, sentiment: 15%, etc.)
   - Min match score threshold: 80.0
4. If match found:
   - Generate trade signal
   - Publish to Kafka (trade-signals topic)
5. Update metrics and logs
```

**Scalability Features**:
- Kafka consumer group with partitioned topics
- Parallel processing (50 goroutines per instance)
- Redis caching for strategy data
- Exactly-once semantics via Kafka transactions

**gRPC Methods** (Internal):
```protobuf
service RulesEngine {
  rpc EvaluateConditions(EvaluateRequest) returns (EvaluateResponse);
  rpc ReloadUserRules(ReloadRequest) returns (Empty);
}
```

---

### 5. Trade Execution Service
**Purpose**: Execute trades via Indira Securities API

**Technology Stack**:
- Go microservice
- RabbitMQ Consumer (order.execution.queue)
- PostgreSQL for order management
- gRPC server for order status
- Indira Securities API client

**Responsibilities**:
- Consume trade signals from Kafka
- Pre-trade risk checks
- Call Indira Securities API for order placement
- Update order status in PostgreSQL
- Publish execution confirmations

**Order Lifecycle**:
```
RECEIVED -> VALIDATED -> PENDING -> SUBMITTED -> FILLED/REJECTED/CANCELLED
```

**PostgreSQL Schema**:
```sql
CREATE TABLE orders (
  order_id UUID PRIMARY KEY,
  user_id VARCHAR(50) NOT NULL,
  strategy_id VARCHAR(50) NOT NULL,
  event_id UUID NOT NULL,
  stock_code BIGINT NOT NULL,
  exchange VARCHAR(10) NOT NULL,
  order_type VARCHAR(10) NOT NULL,
  quantity INT NOT NULL,
  price DECIMAL(10,2),
  status VARCHAR(20) NOT NULL,
  indira_order_id VARCHAR(50),
  filled_quantity INT DEFAULT 0,
  filled_price DECIMAL(10,2),
  commission DECIMAL(10,2),
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW(),
  executed_at TIMESTAMP,
  error_message TEXT
);

CREATE INDEX idx_user_orders ON orders(user_id, created_at DESC);
CREATE INDEX idx_status_orders ON orders(status, created_at DESC);
CREATE INDEX idx_event_orders ON orders(event_id);
```

**RabbitMQ Message Schema**:
```json
{
  "order_id": "uuid",
  "user_id": "string",
  "strategy_id": "string",
  "event_id": "uuid",
  "stock_code": 517170,
  "exchange": "BSE",
  "order_type": "MARKET",
  "quantity": 1000,
  "price": 47.75,
  "stop_loss": 46.80,
  "take_profit": 50.14,
  "timestamp": "2025-09-10T13:26:07.125Z"
}
```

**Indira Securities API Integration**:
- Order placement endpoint
- Order status check via WebSocket
- Order modification/cancellation
- Error handling and retry logic
- Per-user credential management

**gRPC Methods**:
```protobuf
service TradeExecutionService {
  rpc GetOrderStatus(OrderStatusRequest) returns (OrderStatus);
  rpc GetUserOrders(UserOrdersRequest) returns (OrderList);
  rpc CancelOrder(CancelOrderRequest) returns (CancelResponse);
  rpc GetOrderHistory(OrderHistoryRequest) returns (OrderHistoryList);
}
```

**Error Handling**:
- Retry logic with exponential backoff
- Dead letter queue for failed orders
- Circuit breaker for Indira API
- Idempotency checks

---

### 6. Risk Management Service
**Purpose**: Real-time risk monitoring and enforcement

**Technology Stack**:
- Go microservice
- Redis for real-time counters
- PostgreSQL for risk history
- gRPC server

**Risk Checks**:
1. **Pre-Trade Checks**:
   - Daily trade count limit
   - Daily loss limit
   - Position size limits
   - Margin requirements
   - Duplicate order prevention

2. **Post-Trade Monitoring**:
   - Portfolio exposure
   - Concentration risk
   - Drawdown monitoring
   - Circuit breaker triggers

**Redis Data Structures**:
```
user:{user_id}:trades:daily -> Sorted Set (timestamp, order_id)
user:{user_id}:loss:daily -> String (cumulative loss)
user:{user_id}:positions:{stock} -> Hash (quantity, avg_price, pnl)
```

**gRPC Methods**:
```protobuf
service RiskManagementService {
  rpc CheckPreTradeRisk(RiskCheckRequest) returns (RiskCheckResponse);
  rpc UpdatePostTradeMetrics(UpdateMetricsRequest) returns (Empty);
  rpc GetRiskMetrics(RiskMetricsRequest) returns (RiskMetrics);
  rpc SetRiskLimits(SetLimitsRequest) returns (Empty);
}
```

---

### 7. Monitoring & Observability Service
**Purpose**: System health monitoring and metrics

**Technology Stack**:
- Prometheus for metrics collection
- Grafana for visualization
- ELK Stack for log aggregation
- Go metrics exporters

**Metrics Tracked**:
- Events processed per second
- Matching latency (p50, p95, p99)
- Order execution time
- Success/failure rates
- System resource usage
- Queue depths

**Dashboards**:
- System health overview
- User activity metrics
- Trading performance
- Error rate tracking
- Latency heat maps

---

## Data Flow Architecture

### Flow 1: Market Data Ingestion  *(as built)*
```
MongoDB (CAG_CHATBOT.NewsImpactDashboard)
  -> Change Stream
  -> Data Ingestion Service (+ Redis company-master enrichment)
  -> Kafka Topic (news-events)
  -> Rules Processing Engine
```

### Flow 2: Strategy Matching  *(as built)*
```
Kafka Consumer (Rules Engine: news-events)
  -> Match against in-memory strategies (seeded via gRPC BulkLoad from user-config,
     kept in sync via user-config-events)
  -> Pre-trade risk check via Risk Management gRPC (fail-open)
  -> Generate Trade Signal
  -> Publish to Kafka (trade-signals)
```

### Flow 3: Order Execution  *(as built — Kafka, not RabbitMQ; no risk gRPC here)*
```
Kafka Consumer (Trade Execution Service: trade-signals)
  -> Route live vs paper; live -> Indira Securities REST API
  -> Update Order Status (PostgreSQL trading_execution)
  -> Per-user Indira order-status WebSocket -> in-process broadcast to frontend WS
  -> Publish execution events (trade-executions, order-updates)
```

### Flow 4: User Configuration Update
```
User Dashboard (Web/API)
  -> API Gateway
  -> User Config Service (gRPC)
  -> Update PostgreSQL
  -> Publish event to Kafka (user-config-events)
  -> Invalidate Redis Cache
```

---

## Message Queue Strategy

### Kafka (Data Ingestion)
**Topic**: `news-events`  *(as built — the design-time name `market.data.news` is not used)*
- **Consumer Group**: `rule-engine-news-processor` (default; see rules-engine config)
- Additional live topics: `user-config-events`, `trade-signals`, `trade-executions`, `order-updates`
- Note: topics auto-create with 1 partition by default (`EnsureTopicExists(..., 1, 1)`); partition/replication targets below are aspirational

**Why Kafka**:
- High throughput for market data stream
- Replay capability for backtesting
- Persistent storage for audit
- Exactly-once semantics

### RabbitMQ (Order Execution)
**Queue**: `order.execution.queue`
- **Type**: Durable, Quorum Queue
- **Prefetch**: 10 messages per consumer
- **Dead Letter Exchange**: `order.execution.dlx`
- **Priority**: Support for order priority

**Why RabbitMQ**:
- Strong guarantees for critical order messages
- Advanced routing capabilities
- Priority queue support
- Easy retry and DLQ handling
- Lower latency for order execution

---

## Database Strategy

### MongoDB
**Collections**:
1. `market_data` - News and sentiment data (external team writes)
2. `user_configs` - User strategy configurations
3. `strategy_history` - Config change audit log

**Indexes**:
- `market_data`: (stock, dt_tm), (impact_score, sentiment)
- `user_configs`: (user_id), (active), (user_id, active)

**Change Streams**:
- Watch `market_data` collection for inserts/updates

### PostgreSQL
**Tables**:
- `orders` - Order lifecycle management
- `executions` - Trade execution details
- `risk_events` - Risk violation logs

**Why PostgreSQL**:
- ACID compliance for financial data
- Complex queries for reporting
- Reliable transaction handling

### Redis
**Use Cases**:
1. **User Config Cache** - Hot cache for 10K active users
2. **Risk Counters** - Real-time trade counts, loss tracking
3. **Rate Limiting** - API gateway rate limits
4. **Session Management** - JWT token storage

**Structure**:
- Redis Cluster (6 nodes: 3 masters, 3 replicas)
- TTL-based cache invalidation
- Pub/Sub for cache invalidation notifications

### PostgreSQL (Strategies)
**Tables**: strategies, strategy_conditions, trade_configs, risk_limits
**Purpose**: Strategy storage and matching for 10K users

---

## Scalability Analysis

### Target Performance
- **Users**: 10,000 concurrent active strategies
- **Events**: ~1000 news events/hour (peak)
- **Matching Latency**: < 100ms (p95)
- **Order Execution**: < 500ms (p95)

### Horizontal Scaling

| Service | Instances | Reasoning |
|---------|-----------|-----------|
| API Gateway | 3-5 | Load balanced, stateless |
| User Config Service | 2-3 | Read-heavy, cached |
| Data Ingestion | 2-3 | Change stream fan-out |
| Rules Engine | 10-20 | CPU-intensive matching |
| Trade Execution | 5-10 | I/O bound (Indira API) |
| Risk Management | 3-5 | Redis-backed, fast |

### Bottleneck Mitigation
1. **Rules Engine**: Redis caching reduces DB load
2. **Order Queue**: RabbitMQ with multiple consumers
3. **Database**: MongoDB sharding, PostgreSQL connection pooling
4. **Network**: gRPC connection pooling, HTTP/2 multiplexing

---

## Technology Stack Summary

| Component | Technology | Purpose |
|-----------|------------|---------|
| Language | Go 1.21+ | All microservices |
| RPC | gRPC + Protocol Buffers | Inter-service communication |
| Event Streaming | Apache Kafka | Market data ingestion |
| Message Queue | RabbitMQ | Order execution queue |
| Databases | MongoDB, PostgreSQL, Redis | Polyglot persistence |
| API Framework | Gin/Fiber | REST API Gateway |
| Authentication | JWT | User authentication |
| Monitoring | Prometheus + Grafana | Metrics and dashboards |
| Logging | ELK Stack | Centralized logging |
| Container | Docker | Service containerization |

---

## Security Considerations

1. **Authentication**: JWT tokens with 24-hour expiry
2. **Authorization**: Role-based access control (RBAC)
3. **Data Encryption**: TLS 1.3 for all gRPC communication
4. **API Security**: Rate limiting, input validation
5. **Secrets Management**: Environment variables, external secret store
6. **Audit Logging**: All configuration changes logged
7. **Database Security**: Encrypted connections, least privilege access

---

## Development Workflow

1. **Proto-First Design**: Define gRPC services with .proto files
2. **Code Generation**: Generate Go stubs from proto files
3. **Service Implementation**: Implement business logic
4. **Unit Testing**: Test individual service methods
5. **Integration Testing**: Test service interactions
6. **Load Testing**: Verify scalability targets

---

## Next Steps

1. ✅ Architecture design complete
2. ⏭️ Define directory structure
3. ⏭️ Create Protocol Buffer definitions
4. ⏭️ Implement core services (User Config, Data Ingestion)
5. ⏭️ Implement Rules Engine with PostgreSQL + Redis
6. ⏭️ Implement Trade Execution with Indira Securities API
7. ⏭️ Add monitoring and observability
8. ⏭️ Load testing and optimization

---

## Glossary

- **Impact Score**: Numerical rating (1-10) of news impact on stock price
- **Sentiment**: Emotional tone of news (Positive, Neutral, Negative)
- **Strategy**: User-defined trading rules and conditions
- **Trade Signal**: Matched condition triggering an order
- **Indira Securities API**: Indian stock market trading API (broker)
- **Change Stream**: MongoDB real-time data change notification
