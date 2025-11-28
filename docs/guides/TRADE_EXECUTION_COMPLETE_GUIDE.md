# Trade Execution Service - Complete Implementation Guide

## 📋 Table of Contents

1. [Executive Summary](#executive-summary)
2. [Prerequisites Checklist](#prerequisites-checklist)
3. [Architecture Overview](#architecture-overview)
4. [Implementation Roadmap](#implementation-roadmap)
5. [Quick Start Guide](#quick-start-guide)
6. [Documentation Index](#documentation-index)

---

## 🎯 Executive Summary

### What is the Trade Execution Service?

The **Trade Execution Service** is a critical microservice in the algorithmic trading system that:

- **Consumes** validated order requests from RabbitMQ
- **Executes** trades via the Odin Trading API (broker)
- **Tracks** complete order lifecycle from submission to fulfillment
- **Provides** gRPC APIs for order status queries and management
- **Manages** order persistence in PostgreSQL database

### Key Capabilities

✅ **Automated Order Processing** - Consumes orders from message queue  
✅ **Broker Integration** - Places orders via Odin API  
✅ **Order Lifecycle Management** - Tracks from received to filled/rejected  
✅ **Retry Logic** - Handles failures with exponential backoff  
✅ **Status Polling** - Monitors order execution status  
✅ **gRPC APIs** - Query and manage orders programmatically  
✅ **Risk Integration** - Validates risk approval before execution  
✅ **Error Handling** - Dead letter queue for failed orders  

### System Position in Trading Flow

```
Market Data → Rules Engine → RabbitMQ → [TRADE EXECUTION] → Odin API → Exchange
                                               ↓
                                          PostgreSQL
                                          (Order Records)
```

---

## 📦 Prerequisites Checklist

### Infrastructure Requirements

#### 1. PostgreSQL Database ✓
```bash
Version: PostgreSQL 13+
Database: trading_execution
Purpose: Order persistence and history

# Setup Command
createdb trading_execution
psql -d trading_execution -f migrations/001_create_orders_table.sql
```

#### 2. RabbitMQ Message Queue ✓
```bash
Version: RabbitMQ 3.9+
Queue: order.execution.queue
Exchange: order.execution.exchange
DLQ: order.execution.dlq

# Docker Setup
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

#### 3. Redis Cache ✓
```bash
Version: Redis 6+
Purpose: Duplicate detection, rate limiting, status cache

# Docker Setup
docker run -d --name redis -p 6379:6379 redis:6-alpine
```

#### 4. Odin API Access ✓
```bash
Required Credentials:
- ODIN_BASE_URL (Broker API endpoint)
- ODIN_API_KEY
- ODIN_X_API_KEY
- User credentials for authentication

# Test connectivity
curl -X POST $ODIN_BASE_URL/api/v1/login
```

### Development Tools

```bash
# Go Programming Language
go version  # Requires 1.21+

# Protocol Buffers
protoc --version  # Requires 3.x+
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# Database Migration Tool (Optional)
go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@latest

# gRPC Testing Tool
go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest
```

### Environment Configuration

Create `.env` file:

```bash
# Service
SERVICE_NAME=trade-execution-service
SERVICE_PORT=9004
ENVIRONMENT=development

# PostgreSQL
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=trading_user
POSTGRES_PASSWORD=your_secure_password
POSTGRES_DB=trading_execution
POSTGRES_SSL_MODE=disable

# RabbitMQ
RABBITMQ_URL=amqp://guest:guest@localhost:5672/
RABBITMQ_QUEUE=order.execution.queue
RABBITMQ_EXCHANGE=order.execution.exchange
RABBITMQ_PREFETCH=10
WORKER_COUNT=10

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=
REDIS_DB=0

# Odin API
ODIN_BASE_URL=https://api.odin.example.com
ODIN_API_KEY=your_api_key
ODIN_X_API_KEY=your_x_api_key
ODIN_TIMEOUT=30s
MAX_RETRIES=3
RETRY_DELAY_SEC=1

# Logging
LOG_LEVEL=info
LOG_FORMAT=json
```

---

## 🏗️ Architecture Overview

### Service Components

```
┌─────────────────────────────────────────────────────────┐
│           TRADE EXECUTION SERVICE                       │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  RabbitMQ Consumer ──▶ Order Validator                 │
│         │                    │                          │
│         │                    ▼                          │
│         │            Order Executor ──▶ Odin Client    │
│         │                    │                          │
│         │                    ▼                          │
│         │            Repository Layer                   │
│         │                    │                          │
│         └────────────────────┴──────────▶ PostgreSQL   │
│                                                         │
│  gRPC Server ────────────────────────────▶ Queries     │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

### Data Flow

**Order Execution Flow:**
1. **Receive**: RabbitMQ consumer receives order request
2. **Validate**: Check request fields and risk approval
3. **Persist**: Save to PostgreSQL (status: RECEIVED)
4. **Execute**: Call Odin API to place order
5. **Track**: Poll Odin API for order status updates
6. **Update**: Update database with execution details
7. **Notify**: Inform Risk Management service

**Query Flow:**
1. Client calls gRPC API (e.g., GetOrderStatus)
2. Service queries PostgreSQL
3. Returns order details to client

### Order Lifecycle States

```
RECEIVED → VALIDATED → PENDING → SUBMITTED → FILLED
                           ↓           ↓
                       REJECTED    CANCELLED
                           ↓
                       FAILED
```

---

## 🗺️ Implementation Roadmap

### Phase 1: Foundation (Week 1)
- [ ] Setup project structure
- [ ] Configure PostgreSQL database
- [ ] Create database migrations
- [ ] Define data models
- [ ] Setup configuration management

### Phase 2: Core Components (Week 2)
- [ ] Implement repository layer (CRUD operations)
- [ ] Integrate Odin API client
- [ ] Build order executor with retry logic
- [ ] Implement order validator

### Phase 3: Message Queue Integration (Week 3)
- [ ] Setup RabbitMQ consumer
- [ ] Implement worker pool
- [ ] Add error handling and DLQ
- [ ] Test end-to-end order processing

### Phase 4: gRPC Server (Week 4)
- [ ] Implement gRPC service methods
- [ ] Add authentication and authorization
- [ ] Build query endpoints
- [ ] Test gRPC APIs

### Phase 5: Advanced Features (Week 5)
- [ ] Add Redis caching layer
- [ ] Implement duplicate detection
- [ ] Build status polling worker
- [ ] Add circuit breaker pattern

### Phase 6: Testing & Monitoring (Week 6)
- [ ] Write unit tests (80%+ coverage)
- [ ] Create integration tests
- [ ] Setup Prometheus metrics
- [ ] Configure logging and alerts
- [ ] Load testing

### Phase 7: Production Readiness (Week 7)
- [ ] Security audit
- [ ] Performance optimization
- [ ] Documentation completion
- [ ] Deployment configuration
- [ ] Disaster recovery plan

---

## 🚀 Quick Start Guide

### Step 1: Clone and Setup

```powershell
# Navigate to service directory
cd services/trade-execution

# Create necessary directories
mkdir cmd, config, internal, migrations

# Install Go dependencies
go mod init trade-execution
go get github.com/jmoiron/sqlx
go get github.com/lib/pq
go get github.com/google/uuid
go get github.com/rabbitmq/amqp091-go
go get google.golang.org/grpc
```

### Step 2: Database Setup

```powershell
# Create database
createdb trading_execution

# Run migrations
psql -d trading_execution -f migrations/001_create_orders_table.sql

# Verify tables
psql -d trading_execution -c "\dt"
```

### Step 3: Configure Environment

```powershell
# Copy environment template
cp .env.example .env

# Edit configuration
notepad .env  # Update with your credentials
```

### Step 4: Build and Run

```powershell
# Build service
go build -o ../../bin/trade-execution.exe ./cmd/main.go

# Run service
../../bin/trade-execution.exe

# Expected output:
# Starting Trade Execution Service...
# Connected to PostgreSQL
# RabbitMQ consumer started
# gRPC server listening on port 9004
# Service started successfully
```

### Step 5: Test the Service

```powershell
# Test health check
grpcurl -plaintext localhost:9004 trade_execution.TradeExecutionService/HealthCheck

# Expected output:
# {
#   "status": "healthy",
#   "service": "trade-execution-service"
# }
```

### Step 6: Send Test Order (via RabbitMQ)

```powershell
# Use the Rules Engine to publish an order
# Or manually publish to RabbitMQ queue using management UI
# http://localhost:15672 (guest/guest)

# Example order message:
{
  "request_id": "req-123",
  "user_id": "user123",
  "strategy_id": "strategy-1",
  "event_id": "event-456",
  "stock_code": 517170,
  "exchange": "BSE",
  "symbol": "EDVENSWA",
  "order_type": "MARKET",
  "order_side": "BUY",
  "quantity": 10,
  "risk_approved": true,
  "risk_score": 3.5,
  "timestamp": "2025-11-13T10:00:00Z"
}
```

### Step 7: Query Order Status

```powershell
# Using grpcurl
grpcurl -plaintext -d '{
  "order_id": "<order-uuid>",
  "user_id": "user123"
}' localhost:9004 trade_execution.TradeExecutionService/GetOrderStatus

# Using test client
go run test_client.go
```

---

## 📚 Documentation Index

### Core Documentation

1. **[TRADE_EXECUTION_IMPLEMENTATION.md](./TRADE_EXECUTION_IMPLEMENTATION.md)**
   - Complete implementation guide (Part 1)
   - Prerequisites and setup
   - Architecture and design
   - Data models and repository layer
   - Odin API integration
   - Order executor implementation

2. **[TRADE_EXECUTION_IMPLEMENTATION_PART2.md](./TRADE_EXECUTION_IMPLEMENTATION_PART2.md)**
   - Implementation guide (Part 2)
   - RabbitMQ consumer
   - gRPC server
   - Main service entry point
   - Testing strategy
   - Monitoring and observability

3. **[TRADE_EXECUTION_ARCHITECTURE_VISUAL.md](./TRADE_EXECUTION_ARCHITECTURE_VISUAL.md)**
   - Visual architecture diagrams
   - System flow charts
   - State machine diagrams
   - Database schema visualization
   - Integration points
   - Monitoring dashboards

### Related Documentation

4. **[trading-system-architecture.md](./trading-system-architecture.md)**
   - Overall system architecture
   - All microservices overview
   - Message queue strategy
   - Database strategy
   - Technology stack

5. **[odin-api-sdk-integration.md](./odin-api-sdk-integration.md)**
   - Odin API documentation
   - Authentication and session management
   - Order placement methods
   - Order status queries
   - API best practices

6. **[proto-definitions.md](./proto-definitions.md)**
   - Protocol Buffer definitions
   - gRPC service contracts
   - Message formats
   - Common types

7. **[Risk Management Service README](../../services/risk-management/README.md)**
   - Risk validation integration
   - Pre-trade checks
   - Post-trade updates

---

## 🎓 Key Concepts

### Order Types

| Type | Description | Use Case |
|------|-------------|----------|
| MARKET | Execute at best available price | Quick execution, uncertain price |
| LIMIT | Execute at specified price or better | Price control, may not fill |
| STOP_LOSS | Trigger when price reaches stop level | Risk management, downside protection |

### Order Status Flow

```
RECEIVED      ← Initial state when order arrives
    ↓
VALIDATED     ← Request passes validation
    ↓
PENDING       ← Awaiting execution
    ↓
SUBMITTED     ← Sent to Odin API
    ↓
FILLED        ← Order fully executed
REJECTED      ← Broker rejected order
CANCELLED     ← User or system cancelled
FAILED        ← System error occurred
```

### Retry Strategy

```
Attempt 1: Immediate
Attempt 2: Wait 1 second
Attempt 3: Wait 2 seconds
Attempt 4: Wait 4 seconds (final)
    ↓
  Failed → Send to DLQ
```

### Error Handling

| Error Type | Action | Requeue |
|------------|--------|---------|
| Validation Error | Reject, send to DLQ | No |
| Risk Not Approved | Reject order | No |
| Database Error | Retry with backoff | Yes |
| Odin API 4xx | Reject order | No |
| Odin API 5xx | Retry up to 3 times | Yes |
| Network Timeout | Retry with backoff | Yes |

---

## 🔍 Common Use Cases

### Use Case 1: Query Order Status

```go
// Client code (API Gateway)
ctx := context.Background()
req := &pb.GetOrderStatusRequest{
    OrderId: "123e4567-e89b-12d3-a456-426614174000",
    UserId:  "user123",
}

resp, err := client.GetOrderStatus(ctx, req)
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Order Status: %s\n", resp.Order.Status)
fmt.Printf("Filled: %d/%d\n", resp.Order.FilledQuantity, resp.Order.Quantity)
```

### Use Case 2: Cancel Pending Order

```go
req := &pb.CancelOrderRequest{
    OrderId: "123e4567-e89b-12d3-a456-426614174000",
    UserId:  "user123",
    Reason:  "User requested cancellation",
}

resp, err := client.CancelOrder(ctx, req)
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Cancel Success: %v\n", resp.Success)
```

### Use Case 3: Get User Order History

```go
req := &pb.GetUserOrdersRequest{
    UserId: "user123",
    Pagination: &pb.PaginationRequest{
        Page:     1,
        PageSize: 20,
    },
    Filter: &pb.OrderFilter{
        Statuses: []pb.OrderStatus{
            pb.OrderStatus_ORDER_STATUS_FILLED,
            pb.OrderStatus_ORDER_STATUS_REJECTED,
        },
    },
}

resp, err := client.GetUserOrders(ctx, req)
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Found %d orders\n", len(resp.Orders))
```

---

## 🛠️ Troubleshooting

### Problem: Service won't start

**Symptoms**: Service exits immediately with connection errors

**Solutions**:
```powershell
# Check PostgreSQL
psql -U postgres -d trading_execution -c "SELECT 1;"

# Check RabbitMQ
curl http://localhost:15672/api/overview

# Check environment variables
echo $POSTGRES_HOST
echo $RABBITMQ_URL

# Verify port availability
netstat -an | findstr "9004"
```

### Problem: Orders stuck in PENDING

**Symptoms**: Orders saved to database but not submitted to Odin

**Solutions**:
```powershell
# Check Odin API connectivity
curl -X POST $ODIN_BASE_URL/api/v1/health

# Check executor logs
tail -f logs/trade-execution.log | grep "ERROR"

# Verify risk approval
psql -d trading_execution -c "SELECT order_id, status, risk_approved FROM orders WHERE status='PENDING';"
```

### Problem: High RabbitMQ queue depth

**Symptoms**: Queue growing, orders not processing fast enough

**Solutions**:
```powershell
# Check worker count
# Increase WORKER_COUNT in .env
WORKER_COUNT=20  # Increase from 10

# Check database connection pool
# Increase MAX_OPEN_CONNS
POSTGRES_MAX_OPEN_CONNS=50

# Scale service horizontally
docker-compose up -d --scale trade-execution=3
```

### Problem: Odin API rate limiting

**Symptoms**: 429 errors from Odin API

**Solutions**:
```go
// Implement rate limiting in executor
import "golang.org/x/time/rate"

limiter := rate.NewLimiter(rate.Every(time.Second), 10) // 10 req/sec
limiter.Wait(ctx) // Wait for token before API call
```

---

## 📈 Performance Optimization

### Database Optimization

```sql
-- Add covering indexes for common queries
CREATE INDEX idx_orders_user_status_created 
ON orders(user_id, status, created_at DESC)
INCLUDE (order_id, stock_code, quantity, filled_quantity);

-- Analyze query performance
EXPLAIN ANALYZE 
SELECT * FROM orders 
WHERE user_id = 'user123' 
AND status = 'FILLED' 
ORDER BY created_at DESC 
LIMIT 20;

-- Vacuum and analyze regularly
VACUUM ANALYZE orders;
```

### Connection Pooling

```go
// Optimize PostgreSQL connection pool
db.SetMaxOpenConns(50)           // Increase for high load
db.SetMaxIdleConns(10)            // Keep warm connections
db.SetConnMaxLifetime(5 * time.Minute)
db.SetConnMaxIdleTime(2 * time.Minute)
```

### Worker Tuning

```yaml
# Optimize based on load
worker_count: 20              # Increase for high throughput
rabbitmq_prefetch: 5          # Decrease to reduce memory
batch_size: 50                # Process orders in batches
```

---

## 🔒 Security Best Practices

### 1. API Key Management
```bash
# Never commit credentials
# Use environment variables or secret management
export ODIN_API_KEY=$(vault read -field=api_key secret/odin)
```

### 2. Database Security
```sql
-- Create dedicated user with minimal permissions
CREATE USER trade_exec_svc WITH PASSWORD 'strong_password';
GRANT SELECT, INSERT, UPDATE ON orders TO trade_exec_svc;
GRANT SELECT, INSERT ON execution_events TO trade_exec_svc;
```

### 3. gRPC Security
```go
// Enable TLS for production
creds, _ := credentials.NewServerTLSFromFile(certFile, keyFile)
grpcServer := grpc.NewServer(grpc.Creds(creds))
```

### 4. Input Validation
```go
// Always validate user inputs
if order.Quantity <= 0 {
    return errors.New("quantity must be positive")
}
if order.Price != nil && *order.Price <= 0 {
    return errors.New("price must be positive")
}
```

---

## 📞 Support and Resources

### Getting Help

- **Documentation**: Check all markdown files in `docs/guides/`
- **Code Examples**: See `test_client.go` and integration tests
- **Logs**: Check service logs for detailed error messages
- **Monitoring**: Use Prometheus metrics and Grafana dashboards

### Useful Commands

```powershell
# View service logs
docker logs -f trade-execution-service

# Check RabbitMQ queue status
rabbitmqctl list_queues name messages consumers

# Monitor PostgreSQL connections
psql -d trading_execution -c "SELECT * FROM pg_stat_activity WHERE datname='trading_execution';"

# Check Redis cache
redis-cli KEYS "order:*"
```

### Additional Resources

- [gRPC Go Documentation](https://grpc.io/docs/languages/go/)
- [RabbitMQ Go Client](https://github.com/rabbitmq/amqp091-go)
- [PostgreSQL Best Practices](https://wiki.postgresql.org/wiki/Don't_Do_This)
- [Odin API Documentation](./odin-api-sdk-integration.md)

---

## ✅ Checklist: Ready for Production

### Before Deployment

- [ ] All unit tests passing (80%+ coverage)
- [ ] Integration tests successful
- [ ] Load testing completed (target: 1000 orders/sec)
- [ ] Security audit completed
- [ ] Error handling verified
- [ ] Monitoring dashboards configured
- [ ] Alerts setup for critical errors
- [ ] Database indexes optimized
- [ ] Connection pools tuned
- [ ] Retry logic tested
- [ ] Circuit breaker working
- [ ] Dead letter queue configured
- [ ] Documentation complete
- [ ] Disaster recovery plan in place
- [ ] Rollback procedure documented

### Post-Deployment Monitoring

- [ ] Check order success rate (target: >98%)
- [ ] Monitor average latency (target: <500ms p95)
- [ ] Watch queue depth (target: <1000)
- [ ] Track Odin API errors (target: <1%)
- [ ] Verify database performance
- [ ] Check log errors
- [ ] Monitor resource utilization
- [ ] Review DLQ messages

---

## 🎉 Conclusion

You now have everything needed to implement the Trade Execution Service:

✅ **Complete Architecture** - Understanding of system design  
✅ **Step-by-Step Guide** - Implementation instructions  
✅ **Code Examples** - Working code samples  
✅ **Testing Strategy** - Comprehensive testing approach  
✅ **Deployment Guide** - Production readiness  
✅ **Troubleshooting** - Common issues and solutions  

**Next Steps:**
1. Review all documentation files
2. Setup local development environment
3. Follow implementation roadmap
4. Build components incrementally
5. Test thoroughly
6. Deploy to production

Good luck with your implementation! 🚀
