# Trade Execution Service - Deployment Checklist

## ✅ Pre-Deployment Checklist

### 1. Environment Setup
- [ ] Go 1.21+ installed
- [ ] PostgreSQL 13+ installed and running
- [ ] RabbitMQ 3.9+ installed and running
- [ ] Odin API credentials obtained

### 2. Code Status
- [x] All source files created
- [x] Go modules initialized (`go mod tidy` completed)
- [x] No compilation errors
- [x] All dependencies resolved

### 3. Database Setup
- [ ] PostgreSQL database created (`trading_execution`)
- [ ] Database user created with appropriate permissions
- [ ] Migration scripts executed (`001_create_orders_table.sql`)
- [ ] Database tables verified (`orders`, `execution_events`)
- [ ] Indexes created (8 indexes on orders table)
- [ ] Database connection tested

### 4. RabbitMQ Setup
- [ ] RabbitMQ server running
- [ ] RabbitMQ management plugin enabled (optional, for monitoring)
- [ ] Exchange created: `orders`
- [ ] Queue created: `trade_execution`
- [ ] Queue bound to exchange
- [ ] Connection credentials configured

### 5. Configuration
- [ ] `.env` file created (copied from `.env.example`)
- [ ] PostgreSQL credentials filled in
- [ ] RabbitMQ URL configured
- [ ] Odin API credentials configured
- [ ] `config/config.yaml` reviewed and adjusted
- [ ] Log level set appropriately (`debug` for dev, `info` for prod)

### 6. Network & Security
- [ ] Port 9004 available for gRPC server
- [ ] Firewall rules configured (if needed)
- [ ] PostgreSQL port (5432) accessible
- [ ] RabbitMQ ports (5672, 15672) accessible
- [ ] Odin API endpoint reachable
- [ ] SSL/TLS certificates configured (for production)

### 7. Build & Test
- [ ] Service builds successfully: `go build -o ../../bin/trade-execution.exe ./cmd/main.go`
- [ ] Service starts without errors
- [ ] Health check responds: `grpcurl -plaintext localhost:9004 trade_execution.TradeExecutionService/HealthCheck`
- [ ] Test client runs successfully: `go run test_client.go`
- [ ] Sample order can be submitted and processed

---

## 🚀 Deployment Steps

### Step 1: Database Migration
```powershell
cd services/trade-execution

# Connect to PostgreSQL
psql -U postgres

# Create database
CREATE DATABASE trading_execution;
\q

# Run migration
psql -U postgres -d trading_execution -f migrations/001_create_orders_table.sql

# Verify tables
psql -U postgres -d trading_execution -c "\dt"
```

**Expected Output:**
```
         List of relations
 Schema |        Name        | Type  |  Owner
--------+--------------------+-------+----------
 public | execution_events   | table | postgres
 public | orders            | table | postgres
```

### Step 2: Environment Configuration
```powershell
# Copy template
cp .env.example .env

# Edit .env with your values
notepad .env
```

**Required Variables:**
- `POSTGRES_PASSWORD` - Your PostgreSQL password
- `ODIN_API_KEY` - Your Odin API key
- `ODIN_API_SECRET` - Your Odin API secret

### Step 3: Build Service
```powershell
cd services/trade-execution

# Build
go build -o ../../bin/trade-execution.exe ./cmd/main.go

# Verify build
../../bin/trade-execution.exe --version  # (if version flag implemented)
```

### Step 4: Start Service
```powershell
cd services/trade-execution

# Run service
../../bin/trade-execution.exe
```

**Expected Logs:**
```
2024-01-15T10:30:00Z    INFO    Trade Execution Service starting...
2024-01-15T10:30:00Z    INFO    Connected to PostgreSQL database: trading_execution
2024-01-15T10:30:00Z    INFO    Connected to RabbitMQ: amqp://localhost:5672/
2024-01-15T10:30:00Z    INFO    Started 10 RabbitMQ consumer workers
2024-01-15T10:30:00Z    INFO    gRPC server listening on :9004
2024-01-15T10:30:00Z    INFO    Service is ready
```

### Step 5: Verify Service
```powershell
# Test health check
grpcurl -plaintext localhost:9004 trade_execution.TradeExecutionService/HealthCheck

# Run test client
go run test_client.go
```

**Expected Health Check Response:**
```json
{
  "healthy": true,
  "service": "trade-execution-service",
  "version": "1.0.0"
}
```

---

## 🔍 Post-Deployment Verification

### 1. Service Health
- [ ] Service process is running
- [ ] gRPC server responds on port 9004
- [ ] Health check returns healthy status
- [ ] No error logs in service output

### 2. Database Connectivity
```sql
-- Verify tables exist
\dt

-- Check for any test orders
SELECT COUNT(*) FROM orders;

-- Verify indexes
\di
```

### 3. RabbitMQ Connectivity
- [ ] Service connected to RabbitMQ (check logs)
- [ ] 10 worker goroutines started
- [ ] Queue `trade_execution` exists
- [ ] No connection errors in logs

### 4. Odin API Connectivity
- [ ] Test order submission (if possible)
- [ ] No authentication errors in logs
- [ ] API response times acceptable

### 5. Performance
```powershell
# Check CPU usage
Get-Process trade-execution | Select-Object CPU

# Check memory usage
Get-Process trade-execution | Select-Object WorkingSet
```

---

## 📊 Monitoring Setup

### 1. Database Monitoring Queries
```sql
-- Order status distribution
SELECT status, COUNT(*) FROM orders GROUP BY status;

-- Orders processed in last hour
SELECT COUNT(*) FROM orders WHERE created_at > NOW() - INTERVAL '1 hour';

-- Average processing time
SELECT AVG(EXTRACT(EPOCH FROM (updated_at - created_at))) as avg_seconds
FROM orders WHERE status = 'FILLED';

-- Error rate
SELECT 
    COUNT(CASE WHEN status IN ('FAILED', 'REJECTED') THEN 1 END)::float / 
    NULLIF(COUNT(*), 0)::float * 100 as error_rate_percentage
FROM orders
WHERE created_at > NOW() - INTERVAL '1 hour';
```

### 2. RabbitMQ Monitoring
- [ ] Access RabbitMQ Management UI: http://localhost:15672
- [ ] Monitor `trade_execution` queue depth
- [ ] Check consumer count (should be 10)
- [ ] Monitor message rates

### 3. Service Logs
- [ ] Configure log rotation
- [ ] Set up log aggregation (optional: ELK stack)
- [ ] Configure log retention policy

### 4. Alerts (Recommended)
- [ ] Order processing failures > 10%
- [ ] Queue depth > 1000 messages
- [ ] Service downtime
- [ ] Database connection failures
- [ ] Odin API errors

---

## 🐛 Troubleshooting

### Issue: Service won't start

**Check:**
1. PostgreSQL is running: `psql -U postgres -c "SELECT 1;"`
2. RabbitMQ is running: `rabbitmqctl status`
3. Port 9004 is available: `netstat -ano | findstr :9004`
4. `.env` file exists with correct values

### Issue: Can't connect to database

**Check:**
1. Database exists: `psql -U postgres -l | findstr trading_execution`
2. Credentials are correct in `.env`
3. PostgreSQL is accepting connections
4. Firewall allows connection to port 5432

### Issue: RabbitMQ connection fails

**Check:**
1. RabbitMQ is running: `rabbitmqctl status`
2. Queue exists: `rabbitmqctl list_queues`
3. URL format in `.env`: `amqp://user:pass@host:port/`
4. Firewall allows connection to port 5672

### Issue: Orders not processing

**Check:**
1. RabbitMQ queue has messages
2. Consumer workers started (log: "Started 10 RabbitMQ consumer workers")
3. No errors in service logs
4. Database is accepting writes

### Issue: Odin API errors

**Check:**
1. API credentials are correct in `.env`
2. Odin API endpoint is reachable: `curl https://api.odin.com/health`
3. API key has necessary permissions
4. No rate limiting from Odin API

---

## 📈 Performance Tuning

### 1. Worker Pool Size
- Default: 10 workers
- Adjust in `config/config.yaml`: `rabbitmq.workers`
- Consider: CPU cores, Odin API rate limits

### 2. Database Connection Pool
- Default: 25 max connections, 5 idle
- Adjust in `config/config.yaml`: `database.max_connections`
- Monitor with: `SELECT count(*) FROM pg_stat_activity WHERE datname='trading_execution';`

### 3. RabbitMQ Prefetch
- Default: 10 messages per worker
- Adjust in `config/config.yaml`: `rabbitmq.prefetch_count`
- Balance: throughput vs memory usage

### 4. Retry Configuration
- Max retries: 3 (adjust: `executor.max_retries`)
- Backoff: 1s → 30s (adjust: `executor.initial_backoff`, `executor.max_backoff`)

---

## 🚦 Production Checklist

### Before Going to Production
- [ ] Load testing completed (100+ orders/second)
- [ ] Stress testing completed (1000+ orders/second for 1 hour)
- [ ] Failover testing completed (database restart, RabbitMQ restart)
- [ ] Security audit completed
- [ ] SSL/TLS enabled for gRPC
- [ ] Database backups configured
- [ ] Log aggregation configured
- [ ] Monitoring and alerting configured
- [ ] Disaster recovery plan documented
- [ ] Circuit breaker for Odin API implemented
- [ ] Rate limiting configured
- [ ] Documentation reviewed and updated

### Production Configuration Changes
```yaml
# config/config.yaml

# Increase connection pool
database:
  max_connections: 50
  max_idle_connections: 10

# Adjust timeouts
odin:
  timeout: 60s

# Set production log level
logger:
  environment: "production"
  level: "info"
```

---

## 📞 Support Contacts

- **Database Issues:** DBA Team
- **RabbitMQ Issues:** Infrastructure Team
- **Odin API Issues:** Odin Support (support@odin.com)
- **Application Issues:** Development Team

---

## 📚 Additional Resources

- **Setup Guide:** `SETUP_GUIDE.md`
- **Quick Reference:** `QUICK_REFERENCE.md`
- **Implementation Summary:** `IMPLEMENTATION_SUMMARY.md`
- **Architecture Docs:** `docs/guides/`

---

**Deployment Checklist Version:** 1.0.0  
**Last Updated:** 2024-01-15  
**Status:** ✅ Ready for Deployment
