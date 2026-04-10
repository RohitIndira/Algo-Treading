# Algorithmic Trading System

A high-performance, event-driven microservices-based algorithmic trading system for Indian stock markets (NSE/BSE), supporting 10,000+ concurrent users with personalized trading strategies based on real-time news sentiment and impact scores.

## Overview

Users define custom trading strategies that automatically execute trades when market conditions match their criteria. The system processes real-time news and market data, evaluates user-defined rules, and executes trades via the Indira Securities API.

### Key Features

- **Personalized Strategies**: 10,000+ users with unique trading rules
- **Real-Time Processing**: MongoDB change streams for instant data ingestion
- **Event-Driven Architecture**: Kafka and RabbitMQ for reliable message processing
- **Risk Management**: Pre-trade risk checks and real-time monitoring
- **Paper & Live Trading**: Test strategies risk-free before going live
- **OCO Orders**: One-Cancels-the-Other bracket order support
- **gRPC Communication**: Fast inter-service communication
- **REST API Gateway**: Frontend-friendly HTTP/JSON API

---

## Architecture

```
News Update (MongoDB)
  -> Data Ingestion Service
  -> Kafka (news-events)
  -> Rules Engine (strategy matching)
    | (Match found)
  -> Kafka (trade-signals)
  -> Trade Execution Service
  -> Risk Management (pre-trade check)
  -> Indira Securities API (order placement)
  -> PostgreSQL (order record)
```

### Services

| Service | Port | Description |
|---------|------|-------------|
| API Gateway | 8081 (HTTP) | REST API for frontend |
| User Config | 50051 (gRPC) | Strategy CRUD, Kafka event publishing |
| Data Ingestion | 50052 (gRPC) | MongoDB change streams -> Kafka |
| Rules Engine | 50053 (gRPC) | Strategy matching, trade signal generation |
| Trade Execution | 50054 (gRPC) | Order lifecycle, broker integration |
| Risk Management | 50055 (gRPC) | Pre-trade risk checks, risk scoring |

### Infrastructure

| Component | Purpose |
|-----------|---------|
| PostgreSQL | Strategies, orders, credentials, trade signals |
| MongoDB | News/market data (watched via change streams) |
| Redis | Caching, price data, risk counters |
| Kafka | Event streaming (configs, signals, news) |
| RabbitMQ | Order queue for broker integration |

---

## Quick Start

### Prerequisites

- Go 1.21+
- Docker & Docker Compose

### Setup

```bash
# Start all infrastructure (PostgreSQL, Redis, Kafka, RabbitMQ)
cd deployments/docker
./setup.sh          # Linux/Mac
.\setup.ps1         # Windows

# Or use the all-in-one compose (infrastructure + app services)
docker compose up -d
```

### Run Individual Services

```bash
cd services/user-config && go run cmd/main.go
cd services/data-ingestion && go run cmd/main.go
cd services/rules-engine && go run cmd/main.go
cd services/trade-execution && go run cmd/main.go
cd services/risk-management && go run cmd/main.go
cd api/gateway && go run cmd/main.go
```

### Build Docker Images

```bash
./scripts/docker-build-push.sh          # Linux/Mac
.\scripts\docker-build-push.ps1         # Windows
```

---

## Project Structure

```
Algo-Treading/
├── api/gateway/              # REST API Gateway
├── services/
│   ├── user-config/          # Strategy management
│   ├── data-ingestion/       # MongoDB -> Kafka pipeline
│   ├── rules-engine/         # Strategy matching engine
│   ├── trade-execution/      # Order execution & broker integration
│   └── risk-management/      # Risk checks & monitoring
├── pkg/                      # Shared packages (database, kafka, rabbitmq, crypto)
├── deployments/docker/       # Docker Compose & setup scripts
├── scripts/                  # Build & utility scripts
├── docs/                     # Documentation
│   ├── api/                  # API reference & cURL examples
│   ├── guides/               # Architecture, Kafka topics, Indira API
│   └── KT/                   # Knowledge transfer docs per service
└── docker-compose.yml        # All-in-one compose file
```

---

## API Reference

### REST API (API Gateway - port 8081)

```
POST   /api/v1/strategies              Create strategy
GET    /api/v1/strategies              List user strategies
GET    /api/v1/strategies/:id          Get strategy
PUT    /api/v1/strategies/:id          Update strategy
DELETE /api/v1/strategies/:id          Delete strategy
POST   /api/v1/strategies/:id/activate   Activate
POST   /api/v1/strategies/:id/deactivate Deactivate
```

Headers: `Authorization`, `userId`, `appId`, `source`

See [docs/api/](docs/api/) for detailed examples with cURL commands.

---

## Documentation

| Document | Description |
|----------|-------------|
| [Quick Start Guide](docs/guides/QUICK_START_GUIDE.md) | Setup and first run |
| [System Analysis](docs/guides/COMPLETE_SYSTEM_ANALYSIS.md) | Deep architecture analysis |
| [Architecture](docs/guides/trading-system-architecture.md) | System design overview |
| [Kafka Topics](docs/guides/KAFKA_TOPICS_GUIDE.md) | Message formats and topics |
| [Proto Definitions](docs/guides/proto-definitions.md) | gRPC API contracts |
| [Indira API Guide](docs/guides/INDIRA_MIGRATION_GUIDE.md) | Broker integration |
| [Indira API Reference](docs/guides/INDIRA_API_QUICK_REFERENCE.md) | Broker API endpoints |
| [API cURL Examples](docs/api/API_CURL.md) | Testing commands |
| [Strategy API](docs/api/CREATE_STRATEGY_API.md) | Strategy creation reference |
| [KT Documents](docs/KT/) | Knowledge transfer per service |
| [Requirements](docs/Project_Requirements_Document.md) | Business requirements |
| [QA Testing](docs/QA_Feature_Testing_Document.md) | Test cases |

---

## Database

All tables are created automatically via `deployments/docker/init_all_schemas.sql` when PostgreSQL starts.

**Tables**: strategies, strategy_conditions, trade_configs, risk_limits, execution_outbox, orders, execution_events, user_credentials, trade_signals

To recreate from scratch:
```bash
./scripts/create_db_from_scratch.sh     # Linux/Mac
scripts\create_db_from_scratch.bat      # Windows
```
