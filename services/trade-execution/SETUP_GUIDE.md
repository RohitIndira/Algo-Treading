# Trade Execution Service - Setup Guide

## ✅ Implementation Complete

The trade execution service has been **fully implemented** with all components:

- ✅ Go module with all dependencies
- ✅ Configuration files
- ✅ Database migration scripts
- ✅ Order models and enums
- ✅ Database repository layer
- ✅ Odin API client wrapper
- ✅ Order executor with retry logic
- ✅ RabbitMQ consumer with worker pool
- ✅ gRPC server (port 9004)
- ✅ Main service entry point
- ✅ Test client

## 📋 Prerequisites

Before running the service, ensure you have:

1. **Go 1.21+** installed
2. **PostgreSQL 13+** running
3. **RabbitMQ 3.9+** running
4. **Odin API credentials** (API key and secret)

## 🚀 Quick Start

### Step 1: Database Setup

```powershell
# Connect to PostgreSQL
psql -U postgres

# Create database
CREATE DATABASE trading_execution;

# Exit psql
\q

# Run migrations
cd services/trade-execution
psql -U postgres -d trading_execution -f migrations/001_create_orders_table.sql
```

### Step 2: Environment Configuration

Create `.env` file in `services/trade-execution/`:

```env
# PostgreSQL Configuration
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password
POSTGRES_DB=trading_execution

# RabbitMQ Configuration
RABBITMQ_URL=amqp://guest:guest@localhost:5672/

# Odin API Configuration
ODIN_BASE_URL=https://api.odin.com
ODIN_API_KEY=your_api_key_here
ODIN_API_SECRET=your_api_secret_here

# Service Configuration
GRPC_PORT=9004
ENVIRONMENT=development
LOG_LEVEL=debug
```

### Step 3: Build the Service

```powershell
cd services/trade-execution
go build -o ../../bin/trade-execution.exe ./cmd/main.go
```

### Step 4: Run the Service

```powershell
cd services/trade-execution
../../bin/trade-execution.exe
```

You should see:
```
2024-01-15T10:30:00Z    INFO    Trade Execution Service starting...
2024-01-15T10:30:00Z    INFO    Connected to PostgreSQL database: trading_execution
2024-01-15T10:30:00Z    INFO    Connected to RabbitMQ: amqp://localhost:5672/
2024-01-15T10:30:00Z    INFO    Started 10 RabbitMQ consumer workers
2024-01-15T10:30:00Z    INFO    gRPC server listening on :9004
2024-01-15T10:30:00Z    INFO    Service is ready
```

## 🧪 Testing

### Test gRPC Endpoints

```powershell
cd services/trade-execution
go run test_client.go
```

This will test all endpoints:
- HealthCheck
- GetOrderStatus
- GetUserOrders
- GetOrderHistory
- GetOrderStatistics
- CancelOrder

### Manual Testing with grpcurl

```powershell
# Install grpcurl (if not installed)
go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest

# Test health check
grpcurl -plaintext localhost:9004 trade_execution.TradeExecutionService/HealthCheck

# Get order status
grpcurl -plaintext -d '{\"order_id\": \"123e4567-e89b-12d3-a456-426614174000\"}' localhost:9004 trade_execution.TradeExecutionService/GetOrderStatus

# Get user orders
grpcurl -plaintext -d '{\"user_id\": \"user_123\", \"limit\": 10}' localhost:9004 trade_execution.TradeExecutionService/GetUserOrders
```

## 📊 Architecture Overview

```
┌─────────────────┐
│  Risk Mgmt      │
│  Service        │
└────────┬────────┘
         │ Publishes order
         ▼
┌─────────────────┐
│   RabbitMQ      │
│   Queue         │◄── Consumer (10 workers)
└────────┬────────┘
         │
         ▼
┌─────────────────┐      ┌──────────────┐
│ Order Executor  │─────▶│  Odin API    │
│ (Retry Logic)   │      │  (Broker)    │
└────────┬────────┘      └──────────────┘
         │
         ▼
┌─────────────────┐
│  PostgreSQL     │
│  (Orders DB)    │
└─────────────────┘
         ▲
         │
┌─────────────────┐
│  gRPC Server    │
│  (Port 9004)    │
└─────────────────┘
```

## 🔧 Configuration

Edit `config/config.yaml` for service settings:

```yaml
server:
  grpc_port: "9004"

database:
  host: "${POSTGRES_HOST}"
  port: 5432
  user: "${POSTGRES_USER}"
  password: "${POSTGRES_PASSWORD}"
  dbname: "${POSTGRES_DB}"

rabbitmq:
  url: "${RABBITMQ_URL}"
  exchange: "orders"
  queue: "trade_execution"
  workers: 10
  prefetch_count: 10

odin:
  base_url: "${ODIN_BASE_URL}"
  api_key: "${ODIN_API_KEY}"
  api_secret: "${ODIN_API_SECRET}"
  timeout: 30s

executor:
  max_retries: 3
  initial_backoff: 1s
  max_backoff: 30s
  poll_interval: 2s
  poll_timeout: 5m

logger:
  environment: "${ENVIRONMENT}"
  level: "${LOG_LEVEL}"
  service_name: "trade-execution"
```

## 🔄 Order Flow

### 1. Order Submission
```
Risk Management Service → RabbitMQ Queue
```

### 2. Order Processing
```
RabbitMQ → Consumer Worker → Validate → Save to DB (RECEIVED)
```

### 3. Order Execution
```
Order Executor → Submit to Odin API → Update DB (SUBMITTED)
              ↓ (on failure)
           Retry 3 times → Update DB (REJECTED/FAILED)
```

### 4. Order Polling
```
Poll Odin API (every 2s) → Check status → Update DB (FILLED/CANCELLED)
```

### 5. Query Orders
```
gRPC Client → gRPC Server → Repository → PostgreSQL → Response
```

## 📝 Order States

```
RECEIVED → VALIDATED → PENDING → SUBMITTED → FILLED
                                            → CANCELLED
                                            → FAILED
                                            → REJECTED
```

## 🛠️ Troubleshooting

### Service won't start

1. **Check PostgreSQL connection:**
   ```powershell
   psql -U postgres -d trading_execution -c "SELECT 1;"
   ```

2. **Check RabbitMQ:**
   ```powershell
   # Check if RabbitMQ is running
   rabbitmqctl status
   ```

3. **Check ports:**
   ```powershell
   netstat -ano | findstr :9004
   netstat -ano | findstr :5672
   netstat -ano | findstr :5432
   ```

### Orders not being processed

1. **Check RabbitMQ queue:**
   - Open RabbitMQ Management: http://localhost:15672
   - Check `trade_execution` queue for messages

2. **Check logs:**
   ```powershell
   # Service logs will show worker activity
   # Look for: "Processing order request"
   ```

3. **Verify consumer workers:**
   - Check logs for: "Started 10 RabbitMQ consumer workers"

### Orders failing to submit to Odin

1. **Verify Odin API credentials:**
   ```powershell
   # Check .env file has correct credentials
   cat .env | findstr ODIN
   ```

2. **Test Odin API connectivity:**
   ```powershell
   curl -X GET https://api.odin.com/health
   ```

3. **Check retry logs:**
   - Look for: "Failed to submit order, attempt X/3"

### Database issues

1. **Check migrations:**
   ```powershell
   psql -U postgres -d trading_execution -c "\dt"
   # Should show: orders, execution_events
   ```

2. **Check indexes:**
   ```powershell
   psql -U postgres -d trading_execution -c "\di"
   # Should show 8 indexes
   ```

3. **View recent orders:**
   ```powershell
   psql -U postgres -d trading_execution -c "SELECT id, user_id, status, created_at FROM orders ORDER BY created_at DESC LIMIT 5;"
   ```

## 🔍 Monitoring

### Check Service Health

```powershell
grpcurl -plaintext localhost:9004 trade_execution.TradeExecutionService/HealthCheck
```

### View Order Statistics

```powershell
grpcurl -plaintext -d '{"user_id": "user_123"}' localhost:9004 trade_execution.TradeExecutionService/GetOrderStatistics
```

### Database Queries

```sql
-- Orders by status
SELECT status, COUNT(*) FROM orders GROUP BY status;

-- Recent execution events
SELECT order_id, event_type, details, created_at 
FROM execution_events 
ORDER BY created_at DESC 
LIMIT 10;

-- Average execution time
SELECT AVG(EXTRACT(EPOCH FROM (updated_at - created_at))) as avg_seconds
FROM orders 
WHERE status = 'FILLED';

-- Orders pending for more than 5 minutes
SELECT id, user_id, symbol, status, created_at
FROM orders
WHERE status IN ('PENDING', 'SUBMITTED')
  AND created_at < NOW() - INTERVAL '5 minutes';
```

## 📚 API Reference

### gRPC Methods

#### GetOrderStatus
```protobuf
rpc GetOrderStatus(GetOrderStatusRequest) returns (GetOrderStatusResponse);

message GetOrderStatusRequest {
  string order_id = 1;
}
```

#### GetUserOrders
```protobuf
rpc GetUserOrders(GetUserOrdersRequest) returns (GetUserOrdersResponse);

message GetUserOrdersRequest {
  string user_id = 1;
  int32 limit = 2;
  int32 offset = 3;
}
```

#### CancelOrder
```protobuf
rpc CancelOrder(CancelOrderRequest) returns (CancelOrderResponse);

message CancelOrderRequest {
  string order_id = 1;
  string user_id = 2;
}
```

#### GetOrderHistory
```protobuf
rpc GetOrderHistory(GetOrderHistoryRequest) returns (GetOrderHistoryResponse);

message GetOrderHistoryRequest {
  string user_id = 1;
  int64 start_time = 2;
  int64 end_time = 3;
  int32 limit = 4;
  int32 offset = 5;
}
```

## 🚦 Next Steps

1. **Set up monitoring:**
   - Integrate with Prometheus for metrics
   - Set up Grafana dashboards
   - Configure alerts for failed orders

2. **Load testing:**
   - Use `tests/load/` scripts to test throughput
   - Verify worker pool scales correctly
   - Test retry logic under high load

3. **Integration:**
   - Connect risk management service
   - Set up order flow pipeline
   - Test end-to-end order placement

4. **Production readiness:**
   - Configure SSL/TLS for gRPC
   - Set up database backups
   - Implement circuit breaker for Odin API
   - Add rate limiting

## 📖 Additional Documentation

- **Complete Implementation Guide:** `docs/guides/TRADE_EXECUTION_COMPLETE_GUIDE.md`
- **Architecture Visual:** `docs/guides/TRADE_EXECUTION_ARCHITECTURE_VISUAL.md`
- **Implementation Details Part 1:** `docs/guides/TRADE_EXECUTION_IMPLEMENTATION.md`
- **Implementation Details Part 2:** `docs/guides/TRADE_EXECUTION_IMPLEMENTATION_PART2.md`

## 💡 Tips

1. **Development:** Use `ENVIRONMENT=development` for verbose logging
2. **Production:** Use `ENVIRONMENT=production` for JSON structured logs
3. **Testing:** Start with RabbitMQ management UI to manually publish test orders
4. **Debugging:** Check `execution_events` table for detailed order processing history

## 📞 Support

For issues or questions:
1. Check the troubleshooting section above
2. Review the complete implementation guides
3. Check service logs for error details
4. Verify all prerequisites are running

---

**Status:** ✅ Implementation Complete - Ready for Testing
**Version:** 1.0.0
**Last Updated:** 2024-01-15
