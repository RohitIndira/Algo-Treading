# Quick Start Guide - Algo Trading System

## Infrastructure Setup

Start all infrastructure services with a single command:

```bash
# Linux/Mac
cd deployments/docker && ./setup.sh

# Windows PowerShell
cd deployments\docker; .\setup.ps1
```

Or use Docker Compose directly:
```bash
docker compose -f deployments/docker/docker-compose.yml up -d
```

### Infrastructure Services

```
Service          | Port  | Access
-----------------|-------|----------------------------------
PostgreSQL       | 5432  | localhost:5432
Redis            | 6379  | localhost:6379
Kafka            | 9092  | localhost:9092
Zookeeper        | 2181  | localhost:2181
RabbitMQ         | 5672  | amqp://localhost:5672
RabbitMQ UI      | 15672 | http://localhost:15672
```

**With UI profile** (`./setup.sh ui`):
```
Kafka UI         | 8082  | http://localhost:8082
Redis Commander  | 8081  | http://localhost:8081
```

**Credentials:**
- PostgreSQL: `postgres` / `postgres` (Database: `trading_db`)
- RabbitMQ: `guest` / `guest`

---

## Running Services

### All-in-One (Docker Compose)
```bash
docker compose up -d    # from project root
```

### Individual Services (Development)

```powershell
# API Gateway (REST - Port 8081)
cd api\gateway && go run cmd/main.go

# User Config Service (gRPC - Port 50051)
cd services\user-config
$env:DATABASE_HOST="localhost"; $env:DATABASE_PORT="5432"
$env:DATABASE_USER="postgres"; $env:DATABASE_PASSWORD="postgres"
$env:DATABASE_NAME="trading_db"; $env:GRPC_PORT="50051"
$env:KAFKA_BROKERS="localhost:9092"
go run cmd/main.go

# Data Ingestion Service (gRPC - Port 50052)
cd services\data-ingestion && go run cmd/main.go

# Rules Engine Service (gRPC - Port 50053)
cd services\rules-engine && go run cmd/main.go

# Trade Execution Service (gRPC - Port 50054)
cd services\trade-execution
$env:DATABASE_HOST="localhost"; $env:DATABASE_PORT="5432"
$env:DATABASE_USER="postgres"; $env:DATABASE_PASSWORD="postgres"
$env:DATABASE_NAME="trading_db"; $env:GRPC_PORT="50054"
go run cmd/main.go

# Risk Management Service (gRPC - Port 50055)
cd services\risk-management && go run cmd/main.go
```

### Service Ports

```
Service            | Port  | Protocol
-------------------|-------|----------
API Gateway        | 8081  | HTTP/REST
User Config        | 50051 | gRPC
Data Ingestion     | 50052 | gRPC
Rules Engine       | 50053 | gRPC
Trade Execution    | 50054 | gRPC
Risk Management    | 50055 | gRPC
```

---

## Database

All tables are auto-created when PostgreSQL starts via `init_all_schemas.sql`.

**Tables:** strategies, strategy_conditions, trade_configs, risk_limits, execution_outbox, orders, execution_events, user_credentials, trade_signals

```powershell
# Connect to database
docker exec -it trading-postgres psql -U postgres -d trading_db

# Sample queries
\dt                                    # List tables
SELECT * FROM strategies LIMIT 5;
SELECT * FROM orders LIMIT 5;
```

---

## Testing

### Via REST API Gateway (Port 8081)

```bash
# Health check
curl http://localhost:8081/health

# List strategies
curl -H "userId: user_123" http://localhost:8081/api/v1/strategies

# Create strategy (see docs/api/CREATE_STRATEGY_API.md for full schema)
curl -X POST http://localhost:8081/api/v1/strategies \
  -H "Content-Type: application/json" \
  -H "userId: user_123" \
  -H "appId: test_app" \
  -H "source: WEB" \
  -d '{"strategy_name": "Test Strategy", ...}'
```

### Via gRPC (Direct)

```bash
# List gRPC services
grpcurl -plaintext localhost:50051 list
grpcurl -plaintext localhost:50054 list
```

### Monitor Queues

- **RabbitMQ UI**: http://localhost:15672 (guest/guest)
- **Kafka UI**: http://localhost:8082 (with `./setup.sh ui`)

---

## Test User IDs

- `user_123`
- `test_user_001`

No authentication required in development mode.

**Sample Stock Codes (BSE):**
- RELIANCE: 15124
- TCS: 11536
- INFY: 10940
- HDFC BANK: 1333
- ICICI BANK: 4963

---

## Documentation

- [API cURL Examples](../api/API_CURL.md)
- [Create Strategy API](../api/CREATE_STRATEGY_API.md)
- [Indira Broker Integration](INDIRA_MIGRATION_GUIDE.md)
- [Kafka Topics & Message Formats](KAFKA_TOPICS_GUIDE.md)
- Service-specific: `services/*/README.md`
