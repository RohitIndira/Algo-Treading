# User Config Service

A gRPC-based microservice for managing user trading strategies and configurations in the news-driven algorithmic trading system.

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Running the Service](#running-the-service)
- [API Endpoints](#api-endpoints)
- [Kafka Integration](#kafka-integration)
- [Database Schema](#database-schema)
- [Development](#development)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)

## 🎯 Overview

The User Config Service is responsible for storing and managing user trading strategies. It provides a gRPC API for CRUD operations on strategies and publishes configuration changes to Kafka for consumption by other services (like the Rules Engine).

### Key Responsibilities

- **Strategy Management**: Create, read, update, delete trading strategies
- **Configuration Storage**: Persist strategy configurations in PostgreSQL
- **Event Publishing**: Publish strategy changes to Kafka for real-time updates
- **Validation**: Validate strategy configurations before saving

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   User Config Service                   │
│                                                         │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────┐ │
│  │   gRPC       │───▶│   Service    │───▶│  Kafka   │ │
│  │   Server     │    │   Layer      │    │ Producer │ │
│  └──────────────┘    └──────────────┘    └──────────┘ │
│         │                    │                          │
│         │                    ▼                          │
│         │            ┌──────────────┐                   │
│         │            │  Repository  │                   │
│         │            │    Layer     │                   │
│         │            └──────────────┘                   │
│         │                    │                          │
│         ▼                    ▼                          │
│  ┌──────────────────────────────────┐                  │
│  │         PostgreSQL Database        │                  │
│  └──────────────────────────────────┘                  │
└─────────────────────────────────────────────────────────┘
```

## ✨ Features

### Core Features
- ✅ **Full CRUD Operations** - Create, Read, Update, Delete strategies
- ✅ **Strategy Activation/Deactivation** - Enable/disable strategies dynamically
- ✅ **Bulk Operations** - Retrieve multiple strategies by IDs
- ✅ **Pagination Support** - Efficient listing with pagination
- ✅ **Optimistic Locking** - Version-based concurrency control
- ✅ **Transactional Operations** - ACID compliance for data integrity

### Strategy Configuration
- **News Filters**: Impact score, sentiment, categories
- **Exchange Selection**: NSE, BSE, or both
- **Price Filters**: Min/max price range
- **Volume Thresholds**: Minimum trading volume
- **Stock Selection**: Specific stock codes to monitor

### Trade Configuration
- **Order Types**: Market, Limit, Stop Loss
- **Position Sizing**: Fixed quantity or percentage-based
- **Risk Management**: Stop loss, take profit, max position size
- **Exchange Preference**: Primary exchange for execution

### Kafka Integration
- **Real-time Events**: Publishes all strategy changes to Kafka
- **Event Types**: CREATE, UPDATE, DELETE, ACTIVATE, DEACTIVATE
- **Message Format**: JSON with full strategy details
- **Headers**: event_type, user_id for filtering

## 📦 Prerequisites

### Required
- **Go**: 1.21 or later
- **PostgreSQL**: 13 or later
- **Protocol Buffers Compiler**: protoc 3.x
- **protoc-gen-go**: Latest version
- **protoc-gen-go-grpc**: Latest version

### Optional (for full functionality)
- **Kafka**: 2.8 or later (for event streaming)
- **grpcurl**: For testing gRPC endpoints
- **Docker**: For containerized deployment

## 🚀 Installation

### 1. Generate Protocol Buffer Code

```bash
cd /home/rohitt/Desktop/trading-system/api/proto

# Install protoc generators (first time only)
make install-tools

# Generate Go code from .proto files
make generate-all
```

### 2. Install Go Dependencies

```bash
cd /home/rohitt/Desktop/trading-system/services/user-config

# Download dependencies
go mod tidy
go mod download
```

### 3. Setup Database

```bash
# Create database
createdb trading_db

# Run migrations
psql -U postgres -d trading_db < migrations/001_create_strategies_table.sql
```

### 4. Configure Environment

```bash
# Copy example environment file
cp .env.example .env

# Edit configuration (see Configuration section)
nano .env
```

## ⚙️ Configuration

### Environment Variables

Create a `.env` file in the service directory:

```bash
# Server Configuration
GRPC_PORT=50051

# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=your_password
DB_NAME=trading_db
DB_SSLMODE=disable

# Kafka Configuration
KAFKA_ENABLED=true
KAFKA_BROKERS=localhost:9092
KAFKA_TOPIC=user-config-events

# Credential encryption (AES-128/192/256 — must be 16, 24, or 32 bytes)
ENCRYPTION_KEY=change-me-to-a-real-32-byte-secret
# ALLOW_INSECURE_ENCRYPTION_KEY=true  # local dev only (see note below)

# Logging
LOG_LEVEL=INFO  # DEBUG, INFO, WARN, ERROR
```

> **⚠️ ENCRYPTION_KEY is mandatory and validated at boot.** The service
> **refuses to start** if `ENCRYPTION_KEY` is not exactly 16, 24, or 32 bytes,
> or if it is left as the built-in placeholder `0123456789abcdef0123456789abcdef`.
> For local development you may keep the placeholder by explicitly setting
> `ALLOW_INSECURE_ENCRYPTION_KEY=true` — the service then boots but logs a loud
> `WARN` on every start. **Never set that flag in staging or production.**

### Configuration Options

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `GRPC_PORT` | gRPC server port | 50051 | Yes |
| `DB_HOST` | PostgreSQL host | localhost | Yes |
| `DB_PORT` | PostgreSQL port | 5432 | Yes |
| `DB_USER` | Database user | postgres | Yes |
| `DB_PASSWORD` | Database password | - | Yes |
| `DB_NAME` | Database name | trading_db | Yes |
| `DB_SSLMODE` | SSL mode | disable | No |
| `EXECUTION_DB_*` | execution_db connection (broker credentials) | localhost/execution_db | For credentials |
| `KAFKA_ENABLED` | Enable Kafka publishing | true | No |
| `KAFKA_BROKERS` | Kafka broker addresses | localhost:9092 | If Kafka enabled |
| `KAFKA_TOPIC` | Kafka topic name | user-config-events | If Kafka enabled |
| `ENCRYPTION_KEY` | AES key for broker tokens (16/24/32 bytes; placeholder rejected at boot) | _(placeholder — rejected)_ | **Yes** |
| `ALLOW_INSECURE_ENCRYPTION_KEY` | Permit the placeholder key (local dev only; WARNs on boot) | false | No |
| `LOG_LEVEL` | Logging level | INFO | No |

## 🏃 Running the Service

### Development Mode

```bash
cd /home/rohitt/Desktop/trading-system/services/user-config

# Run directly
go run cmd/main.go

# Or build and run
go build -o bin/user-config cmd/main.go
./bin/user-config
```

### Production Mode

```bash
# Build optimized binary
go build -o bin/user-config -ldflags="-s -w" cmd/main.go

# Run with environment file
./bin/user-config
```

### Using Docker (TODO)

```bash
docker build -t user-config-service .
docker run -p 50051:50051 --env-file .env user-config-service
```

## 📡 API Endpoints

### gRPC Service: `UserConfigService`

#### 1. CreateStrategy
Creates a new trading strategy.

```protobuf
rpc CreateStrategy(CreateStrategyRequest) returns (CreateStrategyResponse)
```

**Example Request:**
```json
{
  "user_id": "IS14415",
  "strategy_name": "High Impact News Trader",
  "description": "Trades on high-impact news with positive sentiment",
  "activate_immediately": true,
  "conditions": {
    "impact_score_threshold": 7,
    "sentiments": ["POSITIVE", "NEUTRAL"],
    "categories": ["Results", "Board Meeting"],
    "exchanges": ["NSE", "BSE"],
    "price_range": {"min": 10.0, "max": 1000.0},
    "volume_threshold": 100000,
    "pct_change_threshold": 2.0
  },
  "trade_config": {
    "order_type": "MARKET",
    "quantity": 100,
    "exchange": "NSE",
    "order_side": "BUY",
    "validity": "DAY",
    "max_position_size": 50000.0,
    "stop_loss_pct": 2.0,
    "take_profit_pct": 5.0
  },
  "risk_limits": {
    "max_daily_trades": 10,
    "max_loss_per_day": 10000.0,
    "position_sizing": "FIXED",
    "max_portfolio_exposure_pct": 25.0,
    "enable_risk_checks": true
  }
}
```

#### 2. UpdateStrategy
Updates an existing strategy.

```protobuf
rpc UpdateStrategy(UpdateStrategyRequest) returns (UpdateStrategyResponse)
```

#### 3. DeleteStrategy
Deletes a strategy.

```protobuf
rpc DeleteStrategy(DeleteStrategyRequest) returns (DeleteStrategyResponse)
```

#### 4. GetStrategy
Retrieves a specific strategy.

```protobuf
rpc GetStrategy(GetStrategyRequest) returns (GetStrategyResponse)
```

#### 5. ListUserStrategies
Lists all strategies for a user with pagination.

```protobuf
rpc ListUserStrategies(ListUserStrategiesRequest) returns (ListUserStrategiesResponse)
```

#### 6. ActivateStrategy
Activates a strategy.

```protobuf
rpc ActivateStrategy(ActivateStrategyRequest) returns (ActivateStrategyResponse)
```

#### 7. DeactivateStrategy
Deactivates a strategy.

```protobuf
rpc DeactivateStrategy(DeactivateStrategyRequest) returns (DeactivateStrategyResponse)
```

#### 8. GetStrategiesByIDs
Retrieves multiple strategies by their IDs (internal use).

```protobuf
rpc GetStrategiesByIDs(GetStrategiesByIDsRequest) returns (GetStrategiesByIDsResponse)
```

#### 9. HealthCheck
Health check endpoint.

```protobuf
rpc HealthCheck(HealthCheckRequest) returns (HealthCheckResponse)
```

## 📨 Kafka Integration

### Published Events

The service publishes strategy configuration events to Kafka for consumption by other services.

#### Event Format

```json
{
  "event_type": "CREATE|UPDATE|DELETE|ACTIVATE|DEACTIVATE",
  "strategy": {
    "strategy_id": "uuid",
    "user_id": "IS14415",
    "strategy_name": "High Impact News Trader",
    "active": true,
    "conditions": { ... },
    "trade_config": { ... },
    "risk_limits": { ... }
  },
  "timestamp": 1699564800
}
```

#### Event Types

- **CREATE**: New strategy created
- **UPDATE**: Strategy configuration updated
- **DELETE**: Strategy deleted
- **ACTIVATE**: Strategy activated
- **DEACTIVATE**: Strategy deactivated

#### Kafka Topic

Default topic: `user-config-events`

#### Consumer Integration

Other services (like Rules Engine) subscribe to this topic to:
1. Cache active strategy configurations
2. Match incoming news against user filters
3. Generate trade signals

## 🗄️ Database Schema

### Tables

#### `strategies`
Main strategy table.

| Column | Type | Description |
|--------|------|-------------|
| strategy_id | UUID | Primary key |
| user_id | VARCHAR(100) | User identifier |
| strategy_name | VARCHAR(255) | Strategy name |
| description | TEXT | Description |
| active | BOOLEAN | Active status |
| version | INTEGER | Version (optimistic locking) |
| created_at | TIMESTAMP | Creation time |
| updated_at | TIMESTAMP | Last update time |

#### `strategy_conditions`
Strategy trigger conditions.

| Column | Type | Description |
|--------|------|-------------|
| condition_id | UUID | Primary key |
| strategy_id | UUID | Foreign key |
| impact_score_threshold | INTEGER | Min impact score (1-10) |
| sentiments | VARCHAR[] | Sentiment filters |
| categories | VARCHAR[] | News categories |
| stock_codes | BIGINT[] | Specific stocks |
| price_range_min | DECIMAL | Min price |
| price_range_max | DECIMAL | Max price |
| volume_threshold | BIGINT | Min volume |
| pct_change_threshold | DECIMAL | Price change % |
| exchanges | VARCHAR[] | NSE, BSE |

#### `trade_configs`
Trade execution configuration.

| Column | Type | Description |
|--------|------|-------------|
| trade_config_id | UUID | Primary key |
| strategy_id | UUID | Foreign key |
| order_type | VARCHAR(50) | MARKET, LIMIT, etc. |
| quantity | INTEGER | Trade quantity |
| max_position_size | DECIMAL | Max position |
| stop_loss_pct | DECIMAL | Stop loss % |
| take_profit_pct | DECIMAL | Take profit % |
| exchange | VARCHAR(50) | Primary exchange |
| order_side | VARCHAR(10) | BUY/SELL |
| limit_price | DECIMAL | Limit price |
| validity | VARCHAR(50) | DAY, IOC, etc. |

#### `risk_limits`
Risk management limits.

| Column | Type | Description |
|--------|------|-------------|
| risk_limit_id | UUID | Primary key |
| strategy_id | UUID | Foreign key |
| max_daily_trades | INTEGER | Max trades per day |
| max_loss_per_day | DECIMAL | Max daily loss |
| position_sizing | VARCHAR(50) | Sizing strategy |
| max_portfolio_exposure_pct | DECIMAL | Max exposure % |
| max_per_trade_risk | DECIMAL | Max risk per trade |
| enable_risk_checks | BOOLEAN | Enable checks |

## 🛠️ Development

### Project Structure

```
services/user-config/
├── cmd/
│   └── main.go              # Application entry point
├── config/
│   └── config.go            # Configuration management
├── internal/
│   ├── models/
│   │   └── strategy.go      # Domain models
│   ├── repository/
│   │   └── strategy_repository.go  # Database operations
│   ├── service/
│   │   └── strategy_service.go     # Business logic
│   └── server/
│       └── grpc_server.go   # gRPC server implementation
├── migrations/
│   └── 001_create_strategies_table.sql
├── .env.example
├── go.mod
└── README.md
```

### Adding New Features

1. **Update Proto Definition**: Modify `api/proto/user_config/user_config.proto`
2. **Regenerate Code**: Run `make generate-all` in `api/proto/`
3. **Update Models**: Add/modify structs in `internal/models/`
4. **Update Repository**: Add database operations in `internal/repository/`
5. **Update Service**: Add business logic in `internal/service/`
6. **Update gRPC Server**: Implement new endpoints in `internal/server/`

### Code Style

- Follow standard Go conventions
- Use `go fmt` for formatting
- Run `go vet` for static analysis
- Write tests for new features

## 🧪 Testing

### Unit Tests (TODO)

```bash
# Run all tests
go test ./...

# Run with coverage
go test -cover ./...

# Run specific package
go test ./internal/service
```

### Integration Tests (TODO)

```bash
# Run integration tests
go test -tags=integration ./...
```

### Manual Testing with grpcurl

```bash
# Health check
grpcurl -plaintext localhost:50051 user_config.UserConfigService/HealthCheck

# Create strategy
grpcurl -plaintext -d @ localhost:50051 user_config.UserConfigService/CreateStrategy <<EOF
{
  "user_id": "IS14415",
  "strategy_name": "Test Strategy",
  ...
}
EOF

# List strategies
grpcurl -plaintext -d '{"user_id":"IS14415","active_only":true}' localhost:50051 user_config.UserConfigService/ListUserStrategies
```

## 🐛 Troubleshooting

### Common Issues

#### 1. Database Connection Failed

```
Error: Failed to connect to database
```

**Solution:**
- Verify PostgreSQL is running: `pg_isready`
- Check connection details in `.env`
- Ensure database exists: `psql -l | grep trading_db`

#### 2. Port Already in Use

```
Error: Failed to listen: address already in use
```

**Solution:**
- Check if port 50051 is in use: `lsof -i :50051`
- Change `GRPC_PORT` in `.env`
- Kill conflicting process

#### 3. Kafka Connection Failed

```
Warning: failed to publish to kafka
```

**Solution:**
- Check if Kafka is running
- Verify `KAFKA_BROKERS` in `.env`
- Set `KAFKA_ENABLED=false` to disable Kafka

#### 4. Proto Generation Failed

```
Error: protoc-gen-go: program not found
```

**Solution:**
```bash
cd api/proto
make install-tools
```

### Logs

Check logs for detailed error messages:

```bash
# Set debug logging
export LOG_LEVEL=DEBUG

# Run service
go run cmd/main.go
```

## 📚 Additional Resources

- [Protocol Buffers Documentation](https://protobuf.dev/)
- [gRPC Go Quick Start](https://grpc.io/docs/languages/go/quickstart/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Kafka Documentation](https://kafka.apache.org/documentation/)

## 📝 License

This service is part of the Algo Trading System project.

## 👥 Contributing

1. Create a feature branch
2. Make your changes
3. Write/update tests
4. Submit a pull request

## 🔗 Related Services

- **Rules Engine**: Consumes strategy configs from Kafka
- **Risk Management**: Validates trades against risk limits
- **Trade Execution**: Executes trades based on strategies
- **Data Ingestion**: Provides market data for matching
