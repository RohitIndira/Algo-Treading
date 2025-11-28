# Trade Execution Service - Quick Reference

## 🚀 Start Commands

```powershell
# Start PostgreSQL (if using local)
pg_ctl -D "C:\Program Files\PostgreSQL\13\data" start

# Start RabbitMQ (if using local)
rabbitmq-server

# Run database migrations
cd services/trade-execution
psql -U postgres -d trading_execution -f migrations/001_create_orders_table.sql

# Build service
go build -o ../../bin/trade-execution.exe ./cmd/main.go

# Run service
../../bin/trade-execution.exe
```

## 🧪 Test Commands

```powershell
# Run test client
go run test_client.go

# Test with grpcurl
grpcurl -plaintext localhost:9004 list
grpcurl -plaintext localhost:9004 trade_execution.TradeExecutionService/HealthCheck
```

## 📊 Database Queries

```sql
-- Check order counts by status
SELECT status, COUNT(*) FROM orders GROUP BY status;

-- Recent orders
SELECT id, user_id, symbol, status, created_at FROM orders ORDER BY created_at DESC LIMIT 10;

-- Failed orders
SELECT id, user_id, symbol, status, error_message FROM orders WHERE status IN ('FAILED', 'REJECTED');

-- Execution events
SELECT order_id, event_type, details, created_at FROM execution_events ORDER BY created_at DESC LIMIT 20;
```

## 🔧 Troubleshooting

```powershell
# Check if ports are in use
netstat -ano | findstr :9004    # gRPC server
netstat -ano | findstr :5672    # RabbitMQ
netstat -ano | findstr :5432    # PostgreSQL

# Check PostgreSQL connection
psql -U postgres -d trading_execution -c "SELECT 1;"

# Check RabbitMQ status
rabbitmqctl status

# View RabbitMQ queues
rabbitmqctl list_queues name messages consumers
```

## 📦 Dependencies

```powershell
# Update dependencies
go mod tidy

# Vendor dependencies
go mod vendor

# Verify dependencies
go mod verify
```

## 🌐 Endpoints

- **gRPC Server:** `localhost:9004`
- **PostgreSQL:** `localhost:5432`
- **RabbitMQ:** `localhost:5672`
- **RabbitMQ Management:** `http://localhost:15672`

## 🔐 Required Environment Variables

```
POSTGRES_PASSWORD     # PostgreSQL password
ODIN_API_KEY         # Odin API key
ODIN_API_SECRET      # Odin API secret
```

## 📝 Order Status Flow

```
RECEIVED → VALIDATED → PENDING → SUBMITTED → FILLED
                                           → CANCELLED
                                           → FAILED
                                           → REJECTED
```

## 🛠️ Common Issues

**Service won't start:**
- Check `.env` file exists with correct values
- Verify PostgreSQL is running
- Verify RabbitMQ is running

**Orders not processing:**
- Check RabbitMQ queue has messages
- Check service logs for errors
- Verify worker pool started (log: "Started 10 RabbitMQ consumer workers")

**Can't connect to gRPC:**
- Verify port 9004 is not in use
- Check firewall settings
- Test with: `grpcurl -plaintext localhost:9004 list`

## 📚 Documentation Files

- `SETUP_GUIDE.md` - Complete setup instructions
- `docs/guides/TRADE_EXECUTION_COMPLETE_GUIDE.md` - Full guide
- `docs/guides/TRADE_EXECUTION_ARCHITECTURE_VISUAL.md` - Architecture diagrams
- `docs/guides/TRADE_EXECUTION_IMPLEMENTATION.md` - Implementation details

## 🎯 Key Files

```
services/trade-execution/
├── cmd/main.go                          # Service entry point
├── internal/
│   ├── models/order.go                  # Data models
│   ├── repository/order_repository.go   # Database layer
│   ├── odin/client.go                   # Odin API wrapper
│   ├── executor/executor.go             # Order execution logic
│   ├── consumer/rabbitmq_consumer.go    # RabbitMQ consumer
│   └── server/grpc_server.go            # gRPC server
├── config/config.yaml                   # Service configuration
├── migrations/001_create_orders_table.sql # Database schema
└── test_client.go                       # Test client
```

## 🔄 Development Workflow

1. Make changes to code
2. Run `go build -o ../../bin/trade-execution.exe ./cmd/main.go`
3. Stop running service (Ctrl+C)
4. Start service: `../../bin/trade-execution.exe`
5. Test with `go run test_client.go`

## 📊 Monitoring Queries

```sql
-- Performance metrics
SELECT 
    status,
    COUNT(*) as count,
    AVG(EXTRACT(EPOCH FROM (updated_at - created_at))) as avg_time_seconds
FROM orders 
GROUP BY status;

-- Hourly order volume
SELECT 
    DATE_TRUNC('hour', created_at) as hour,
    COUNT(*) as orders
FROM orders 
GROUP BY hour 
ORDER BY hour DESC;

-- Error rate
SELECT 
    COUNT(CASE WHEN status IN ('FAILED', 'REJECTED') THEN 1 END)::float / 
    COUNT(*)::float * 100 as error_rate_percentage
FROM orders
WHERE created_at > NOW() - INTERVAL '1 hour';
```

---

**Quick Start:** Create `.env` → Run migrations → Build → Start service → Test
