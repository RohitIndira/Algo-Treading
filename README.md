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

- Go 1.25+ (the workspace `go.work` requires it; rules-engine declares `go 1.25`)
- Docker & Docker Compose v2

### Setup

**PostgreSQL runs natively, not in Docker.** This machine uses the Windows
service `postgresql-x64-18`, which holds the real `trading_db` and
`trading_execution` data. Docker provides everything else.

> Do not run a Postgres container alongside it. Both bind port 5432 on
> Windows, but `localhost:5432` resolves to the native server — so the
> container looks healthy while silently receiving no traffic.

From the repo root:

```bash
# Start infrastructure: Redis, Kafka (KRaft), RabbitMQ
docker compose up -d

# Create the Kafka topics (once, after Kafka reports healthy)
docker cp deployments/docker/init_kafka_topics.sh trading-kafka:/tmp/t.sh
docker exec trading-kafka bash -c "tr -d '\r' </tmp/t.sh >/tmp/k.sh; KAFKA_BROKER=kafka:29092 bash /tmp/k.sh"
```

Compose profiles select what else comes up:

| Command | Adds |
|---------|------|
| `docker compose up -d` | Redis, Kafka, RabbitMQ (**default**) |
| `docker compose --profile ui up -d` | Kafka UI :8085, Redis Commander :8086, pgAdmin :8087 |
| `docker compose --profile apps up -d` | the six Go services, containerised |
| `docker compose --profile mongo up -d` | local MongoDB (Atlas is the default) |
| `docker compose --profile pgdocker up -d` | containerised Postgres — **only** on a machine with no native install |

Containerised services reach the native database at
`host.docker.internal:5432`; host-run services use `localhost:5432`.

### Generate Protobuf Code (required after cloning)

The generated `*.pb.go` files are **gitignored**, so a fresh clone has none —
and an edited `.proto` leaves the ones on disk stale. Either way the build
fails with errors like `undefined: pb.AMNActivation`.

```bash
./scripts/proto-gen.sh          # all protos
./scripts/proto-gen.sh user_config
make proto                      # same thing
```

No local `protoc` needed — it runs a pinned toolchain container
(`deployments/docker/protoc.Dockerfile`: protoc 33.0, protoc-gen-go v1.36.10,
protoc-gen-go-grpc v1.5.1). Set `USE_LOCAL_PROTOC=1` to use a host protoc.
**Rerun this after every `.proto` change.**

### Run Individual Services

The `.env` files point at `localhost`, so services run on the host against the
containerised infrastructure with no edits:

```bash
cd services/user-config && go run cmd/main.go
cd services/data-ingestion && go run cmd/main.go
cd services/rules-engine && go run cmd/main.go
cd services/trade-execution && go run cmd/main.go
cd services/risk-management && go run cmd/main.go
cd api/gateway && go run cmd/main.go
```

### Build Docker Images

Every image builds from the **repo root** — the service `go.mod` files use
relative `replace` directives and the `go.work` workspace, so a per-service
build context cannot resolve them:

```bash
docker compose --profile apps build            # all services
docker build -f services/user-config/Dockerfile -t algo-trading/user-config .
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

The system uses **two PostgreSQL databases** on the same instance:

| Database | Owned by | Contents |
|----------|----------|----------|
| `trading_db` | user-config, rules-engine | strategies, strategy_conditions, trade_configs, risk_limits, execution_outbox, amn_activations, trade_signals, signal_decisions |
| `trading_execution` | trade-execution | orders, fills, positions, broker_accounts, order_groups, daily_pnl_summary, signal_metrics |

Both already exist on the native server. To recreate them from scratch
(needs `psql` on PATH — it ships at `C:\Program Files\PostgreSQL\18\bin`):

```bash
./scripts/create_db_from_scratch.sh     # Linux/Mac
scripts\create_db_from_scratch.bat      # Windows
```

The schema is assembled from, in order:

1. `deployments/docker/init_all_schemas.sql` — base schema for `trading_db`
2. `services/user-config/migrations/*.sql` and `services/rules-engine/migrations/*.sql` → `trading_db`
3. `services/trade-execution/migrations/*.sql` → `trading_execution` (`001_init.sql` is the authoritative order/fill schema)

`deployments/docker/init/` contains the same sequence as two shell scripts.
They run automatically only under the `pgdocker` profile, but they also serve
as the executable reference for what a correct two-database bootstrap looks
like.
