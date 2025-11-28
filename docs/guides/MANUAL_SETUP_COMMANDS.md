# Trading System - Manual Setup Commands

**Copy and paste these commands one by one in your terminal**

---

## 📋 Part 1: Infrastructure Setup

### Step 1.1: Check Prerequisites
```bash
# Check Docker
docker --version

# Check Docker Compose
docker-compose --version

# Check PostgreSQL
psql --version

# Check Go
go version

# Check Python
python3 --version
```

---

### Step 1.2: Setup RabbitMQ (Docker)
```bash
# Pull RabbitMQ image
docker pull rabbitmq:3-management

# Run RabbitMQ
docker run -d --name trading-rabbitmq \
  -p 5672:5672 \
  -p 15672:15672 \
  -e RABBITMQ_DEFAULT_USER=guest \
  -e RABBITMQ_DEFAULT_PASS=guest \
  rabbitmq:3-management

# Verify RabbitMQ is running
docker ps | grep trading-rabbitmq

# Wait 10 seconds for RabbitMQ to start
sleep 10

# Check RabbitMQ Management UI (optional)
echo "RabbitMQ UI: http://localhost:15672 (guest/guest)"
```

---

### Step 1.3: Setup Kafka (Docker)
```bash
# Navigate to docker directory
cd /home/rohitt/Desktop/trading-system/deployments/docker

# Start Kafka and Zookeeper
docker-compose -f docker-compose-kafka.yml up -d

# Wait for Kafka to start
sleep 15

# Verify Kafka is running
docker ps | grep kafka

# Kafka UI available at: http://localhost:8080
```

---

### Step 1.4: Check Redis (Already Running)
```bash
# Check if Redis is running
redis-cli ping
# Should return: PONG

# If not running, start Redis
# redis-server &
```

---

### Step 1.5: Check PostgreSQL (Already Running)
```bash
# Check PostgreSQL
psql -U postgres -c "SELECT version();"
```

---

## 📋 Part 2: Database Setup

### Step 2.1: Create Trading Execution Database
```bash
# Create database
psql -U postgres -c "CREATE DATABASE trading_execution;" 2>/dev/null || echo "Database already exists"

# Run migrations
cd /home/rohitt/Desktop/trading-system/services/trade-execution
psql -U postgres -d trading_execution -f migrations/001_create_orders_table.sql
```

---

### Step 2.2: Check User Config Database (Already Created)
```bash
# Verify user config database
psql -U postgres -d trading_config -c "SELECT COUNT(*) FROM strategies;"
```

---

## 📋 Part 3: Generate Proto Files (ONE TIME)

```bash
# Navigate to proto directory
cd /home/rohitt/Desktop/trading-system/api/proto

# Generate all proto files
make generate-all

# Verify generated files
ls -la common/*.pb.go
ls -la user_config/*.pb.go
ls -la risk_management/*.pb.go
ls -la trade_execution/*.pb.go
ls -la rules_engine/*.pb.go
```

---

## 📋 Part 4: Start Services (Each in Separate Terminal)

### Terminal 1: Odin API Wrapper (Python)
```bash
cd /home/rohitt/Desktop/trading-system/services/odin-api-wrapper
python3 main.py

# Expected output:
# * Running on http://0.0.0.0:8000
```

---

### Terminal 2: User Config Service
```bash
cd /home/rohitt/Desktop/trading-system/services/user-config
go run cmd/main.go

# Expected output:
# gRPC server listening on :9001
```

---

### Terminal 3: Risk Management Service
```bash
cd /home/rohitt/Desktop/trading-system/services/risk-management
go run cmd/main.go

# Expected output:
# Risk Management Server listening on :9005
```

---

### Terminal 4: Trade Execution Service
```bash
cd /home/rohitt/Desktop/trading-system/services/trade-execution
go run cmd/main.go

# Expected output:
# ✓ Trade Execution Service Started
# - gRPC Server: localhost:9004
```

---

### Terminal 5: Rules Engine Service
```bash
cd /home/rohitt/Desktop/trading-system/services/rules-engine
go run cmd/main.go

# Expected output:
# Rules Engine Service started
# Kafka consumer listening...
```

---

### Terminal 6: Data Ingestion Service
```bash
cd /home/rohitt/Desktop/trading-system/services/data-ingestion
go run cmd/main.go

# Expected output:
# Starting data-ingestion service
# Connected to MongoDB
# Connected to Kafka
```

---

## 📋 Part 5: Verify Services

### Step 5.1: Check All Ports
```bash
# Check if all services are running
echo "=== Service Ports Status ==="
echo "RabbitMQ (5672):"
netstat -an | grep 5672 || echo "Not running"

echo "RabbitMQ UI (15672):"
netstat -an | grep 15672 || echo "Not running"

echo "Kafka (9092):"
netstat -an | grep 9092 || echo "Not running"

echo "Kafka UI (8080):"
netstat -an | grep 8080 || echo "Not running"

echo "Redis (6379):"
netstat -an | grep 6379 || echo "Not running"

echo "PostgreSQL (5432):"
netstat -an | grep 5432 || echo "Not running"

echo "Odin API (8000):"
netstat -an | grep 8000 || echo "Not running"

echo "User Config (9001):"
netstat -an | grep 9001 || echo "Not running"

echo "Rules Engine (9003):"
netstat -an | grep 9003 || echo "Not running"

echo "Trade Execution (9004):"
netstat -an | grep 9004 || echo "Not running"

echo "Risk Management (9005):"
netstat -an | grep 9005 || echo "Not running"
```

---

### Step 5.2: Test Health Checks
```bash
# Test Odin API Wrapper
curl http://localhost:8000/health

# Test User Config Service
grpcurl -plaintext localhost:9001 user_config.UserConfigService/HealthCheck

# Test Trade Execution Service
grpcurl -plaintext localhost:9004 trade_execution.TradeExecutionService/HealthCheck

# Test Risk Management Service
grpcurl -plaintext localhost:9005 risk_management.RiskManagementService/HealthCheck
```

---

### Step 5.3: Check RabbitMQ Queues
```bash
# List queues
docker exec trading-rabbitmq rabbitmqctl list_queues

# Should show: order.execution.queue
```

---

### Step 5.4: Check Kafka Topics
```bash
# List topics
docker exec trading-kafka kafka-topics.sh --list --bootstrap-server localhost:9092

# Should show: market.data.news
```

---

## 📋 Part 6: Test Complete Pipeline (Optional)

### Step 6.1: Insert Test News in MongoDB
```bash
# This would be done by external team
# You can manually test by inserting a document in MongoDB
mongosh --eval '
use trading_system
db.news_impact_dashboard.insertOne({
  "headline": "Test News",
  "sentiment": "positive",
  "impact_score": 0.8,
  "timestamp": new Date()
})
'
```

---

### Step 6.2: Monitor Service Logs
```bash
# Watch each service terminal for activity
# Data flow: MongoDB → Kafka → Rules Engine → RabbitMQ → Trade Execution → Odin API
```

---

## 📋 Part 7: Stop Services

### Stop Go Services
```bash
# In each service terminal, press: Ctrl+C
```

---

### Stop Docker Containers
```bash
# Stop RabbitMQ
docker stop trading-rabbitmq
docker rm trading-rabbitmq

# Stop Kafka
cd /home/rohitt/Desktop/trading-system/deployments/docker
docker-compose -f docker-compose-kafka.yml down

# Verify all stopped
docker ps
```

---

## 📋 Part 8: Restart Everything

### Quick Restart Commands
```bash
# 1. Start RabbitMQ
docker start trading-rabbitmq || docker run -d --name trading-rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management

# 2. Start Kafka
cd /home/rohitt/Desktop/trading-system/deployments/docker && docker-compose -f docker-compose-kafka.yml up -d

# 3. Wait for containers to start
sleep 15

# 4. Start services in separate terminals (see Part 4)
```

---

## 📊 Service URLs & Ports

| Service | URL/Port | Credentials |
|---------|----------|-------------|
| RabbitMQ Management | http://localhost:15672 | guest/guest |
| Kafka UI | http://localhost:8080 | - |
| Redis | localhost:6379 | - |
| PostgreSQL | localhost:5432 | postgres/postgres |
| Odin API Wrapper | http://localhost:8000 | - |
| User Config (gRPC) | localhost:9001 | - |
| Rules Engine (gRPC) | localhost:9003 | - |
| Trade Execution (gRPC) | localhost:9004 | - |
| Risk Management (gRPC) | localhost:9005 | - |

---

## 🐛 Troubleshooting

### If RabbitMQ fails to start:
```bash
docker logs trading-rabbitmq
docker rm trading-rabbitmq
docker run -d --name trading-rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

### If Kafka fails to start:
```bash
cd /home/rohitt/Desktop/trading-system/deployments/docker
docker-compose -f docker-compose-kafka.yml logs
docker-compose -f docker-compose-kafka.yml down
docker-compose -f docker-compose-kafka.yml up -d
```

### If service won't start:
```bash
# Check if port is already in use
netstat -an | grep <PORT>

# Kill process using port
lsof -ti:<PORT> | xargs kill -9
```

---

## ✅ System is Ready When:

- [x] RabbitMQ running on port 5672
- [x] Kafka running on port 9092  
- [x] Redis responding to PING
- [x] PostgreSQL accessible
- [x] All 6 services running
- [x] Health checks passing

**Your algorithmic trading system is now operational!** 🚀
