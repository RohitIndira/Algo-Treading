# Quick Start Guide - Running the Algo Trading System

## Current System Status (November 13, 2025)

### ✅ Infrastructure Services - ALL RUNNING

```
Service          | Status    | Port  | Access URL
-----------------|-----------|-------|----------------------------------
PostgreSQL       | Running   | 5432  | localhost:5432
RabbitMQ         | Running   | 5672  | amqp://localhost:5672
RabbitMQ UI      | Running   | 15672 | http://localhost:15672
Kafka            | Running   | 9092  | localhost:9092
Kafka UI         | Running   | 8080  | http://localhost:8080
Zookeeper        | Running   | 2181  | localhost:2181
Redis            | Running   | 6379  | localhost:6379
```

**Credentials:**
- PostgreSQL: `postgres` / `password` (Database: `orders`)
- RabbitMQ: `guest` / `guest`

### ✅ Trade Execution Service - RUNNING

```
Status: ✓ Active and Healthy
Port: 9004
Endpoint: localhost:9004
Workers: 10 RabbitMQ consumers active
Database: Connected to PostgreSQL
```

### ⚠️ User Config Service - Needs Start

**Status:** Ready to start (requires environment variables)

---

## How to Start Services

### 1. Infrastructure (Already Running)

All infrastructure services are running via Docker. No action needed.

To verify:
```powershell
docker ps
```

### 2. Trade Execution Service (Already Running)

Currently running with 10 workers processing orders from RabbitMQ.

**To restart if needed:**
```powershell
cd services\trade-execution
$env:POSTGRES_USER="postgres"
$env:POSTGRES_PASSWORD="password"
$env:POSTGRES_DB="orders"
$env:POSTGRES_HOST="localhost"
$env:POSTGRES_PORT="5432"
$env:POSTGRES_SSL_MODE="disable"
$env:GRPC_PORT="9004"
$env:SERVICE_PORT="9004"
go run cmd/main.go
```

### 3. User Config Service (Start This)

```powershell
cd services\user-config
$env:DB_HOST="localhost"
$env:DB_PORT="5432"
$env:DB_USER="postgres"
$env:DB_PASSWORD="password"
$env:DB_NAME="orders"
$env:DB_SSLMODE="disable"
$env:GRPC_PORT="9001"
$env:KAFKA_ENABLED="true"
$env:KAFKA_BROKERS="localhost:9092"
$env:KAFKA_TOPIC="strategy.events"
go run cmd/main.go
```

### 4. Optional Services (Not Required for Basic Frontend)

- **Risk Management Service** (Port 9005)
- **Rules Engine Service** (Port 9003)
- **Data Ingestion Service** (Port 9002)

These can be started later when needed.

---

## Database Migrations

✅ **Already Applied:**
- Trade Execution: `001_create_orders_table.sql`
- User Config: `001_create_strategies_table.sql`

**Tables Created:**
- `orders` - Stores all order information
- `strategies` - Stores user trading strategies
- `strategy_conditions` - Strategy filter conditions
- `trade_config` - Trade execution configuration
- `risk_limits` - Risk management limits

---

## Frontend Integration

### API Endpoints Available

#### User Config Service (Port 9001)
- `CreateStrategy` - Create new trading strategy
- `UpdateStrategy` - Update existing strategy
- `GetStrategy` - Retrieve strategy details
- `ListUserStrategies` - List all user strategies
- `ActivateStrategy` - Activate a strategy
- `DeactivateStrategy` - Deactivate a strategy
- `DeleteStrategy` - Delete a strategy

#### Trade Execution Service (Port 9004)
- `GetOrderStatus` - Get order status by ID
- `GetUserOrders` - Get all user orders with filters
- `CancelOrder` - Cancel pending order
- `ModifyOrder` - Modify pending order
- `GetOrderHistory` - Get historical orders
- `GetOrderStatistics` - Get trading statistics

### Protocol: gRPC

**Important:** The services use gRPC protocol. Frontend needs either:
1. **gRPC-Web adapter** (recommended for browser)
2. **REST API Gateway** (to be implemented)
3. **Direct gRPC client** (Node.js backend)

### Sample Test User

For development testing:
- User ID: `user_123` or `test_user_001`
- No authentication required currently

---

## Testing the System

### 1. Check Service Health

```powershell
# Trade Execution Service
curl -X POST http://localhost:9004/health

# User Config Service (once started)
curl -X POST http://localhost:9001/health
```

### 2. Test with grpcurl

```powershell
# List available services
grpcurl -plaintext localhost:9004 list

# Get order status (example)
grpcurl -plaintext -d '{
  "order_id": "test_order_001",
  "user_id": "user_123"
}' localhost:9004 trade_execution.TradeExecutionService/GetOrderStatus
```

### 3. View Databases

```powershell
# Connect to PostgreSQL
docker exec -it trading-postgres psql -U postgres -d orders

# Sample queries
\dt                                    # List tables
SELECT * FROM strategies LIMIT 5;      # View strategies
SELECT * FROM orders LIMIT 5;          # View orders
```

### 4. Monitor Queues

**RabbitMQ UI:**
- URL: http://localhost:15672
- Username: `guest`
- Password: `guest`
- Check: `order.execution.queue`

**Kafka UI:**
- URL: http://localhost:8080
- Topics: `market.data.news`, `strategy.events`

---

## Common Issues & Solutions

### Issue: Service can't connect to PostgreSQL
**Solution:** Ensure PostgreSQL container is running and credentials match:
```powershell
docker ps | grep postgres
```

### Issue: Port already in use
**Solution:** Check what's using the port:
```powershell
netstat -ano | findstr :9004
```

### Issue: go mod issues
**Solution:** Run go mod tidy:
```powershell
cd services\<service-name>
go mod tidy
```

### Issue: Module not found errors
**Solution:** The project uses workspace modules. Ensure you're running from the service directory with proper environment variables.

---

## What to Send to Frontend Developers

### Essential Documents
1. **FRONTEND_API_DOCUMENTATION.md** - Complete API reference
2. **This Quick Start Guide** - How to run services
3. **Proto Files:**
   - `api/proto/user_config/user_config.proto`
   - `api/proto/trade_execution/trade_execution.proto`
   - `api/proto/common/common.proto`

### Key Information

**Service Endpoints:**
- User Config: `localhost:9001` (gRPC)
- Trade Execution: `localhost:9004` (gRPC)

**Protocol:** gRPC (need gRPC-Web adapter for browser)

**Test User IDs:** `user_123`, `test_user_001`

**Authentication:** Not implemented yet - use placeholder user IDs

**Sample Stock Codes:**
- RELIANCE: 15124
- TCS: 11536
- INFY: 10940
- HDFC BANK: 1333
- ICICI BANK: 4963

---

## Next Steps

### For Backend Team
1. ✅ Infrastructure services running
2. ✅ Trade Execution Service running
3. ⏳ Start User Config Service
4. ⏳ Implement REST API Gateway (optional)
5. ⏳ Implement WebSocket for real-time updates
6. ⏳ Add authentication layer

### For Frontend Team
1. Review `FRONTEND_API_DOCUMENTATION.md`
2. Set up gRPC-Web client or wait for REST gateway
3. Implement strategy management UI
4. Implement order tracking UI
5. Add real-time order status updates
6. Implement authentication flow (when ready)

---

## Support & Documentation

**Full Documentation:**
- `docs/FRONTEND_API_DOCUMENTATION.md` - Complete API reference
- `docs/guides/TRADE_EXECUTION_COMPLETE_GUIDE.md` - Trade execution details
- `docs/guides/odin-api-sdk-integration.md` - Broker integration
- `services/*/README.md` - Service-specific guides

**Contact:** Backend Team
**Last Updated:** November 13, 2025
