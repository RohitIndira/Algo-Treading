# Trading System Directory Structure

## Root Directory Layout

```
trading-system/
├── api/                          # API Gateway & Protocol Buffers
│   ├── gateway/                  # API Gateway service
│   └── proto/                    # gRPC Protocol Buffer definitions
├── services/                     # Microservices
│   ├── user-config/              # User Configuration Service
│   ├── data-ingestion/           # Data Ingestion Service
│   ├── rules-engine/             # Rules Processing Engine
│   ├── trade-execution/          # Trade Execution Service
│   ├── risk-management/          # Risk Management Service
│   └── notification/             # Notification Service (optional)
├── pkg/                          # Shared packages
│   ├── auth/                     # JWT authentication
│   ├── database/                 # Database clients
│   ├── kafka/                    # Kafka producer/consumer
│   ├── rabbitmq/                 # RabbitMQ client
│   ├── redis/                    # Redis client
│   ├── logger/                   # Structured logging
│   ├── metrics/                  # Prometheus metrics
│   └── odin/                     # Odin API client
├── internal/                     # Private application code
│   ├── models/                   # Shared data models
│   └── utils/                    # Utility functions
├── deployments/                  # Deployment configurations
│   ├── docker/                   # Dockerfiles
│   ├── kubernetes/               # K8s manifests (future)
│   └── docker-compose.yml        # Local development setup
├── scripts/                      # Build and utility scripts
│   ├── build.sh                  # Build all services
│   ├── proto-gen.sh              # Generate proto code
│   └── db-migrate.sh             # Database migrations
├── configs/                      # Configuration files
│   ├── development/              # Dev environment configs
│   ├── staging/                  # Staging configs
│   └── production/               # Production configs
├── docs/                         # Documentation
│   ├── api/                      # API documentation
│   ├── architecture/             # Architecture diagrams
│   └── guides/                   # Setup and development guides
├── tests/                        # Integration and E2E tests
│   ├── integration/              # Integration tests
│   └── load/                     # Load testing scripts
├── monitoring/                   # Monitoring configurations
│   ├── prometheus/               # Prometheus config
│   ├── grafana/                  # Grafana dashboards
│   └── elk/                      # ELK stack config
├── .env.example                  # Environment variables template
├── .gitignore                    # Git ignore file
├── go.mod                        # Go modules file
├── go.sum                        # Go dependencies checksum
├── Makefile                      # Build automation
└── README.md                     # Project overview
```

---

## Detailed Service Structure

### 1. API Gateway (`api/gateway/`)

```
api/gateway/
├── cmd/
│   └── main.go                   # Entry point
├── internal/
│   ├── handlers/                 # HTTP handlers
│   │   ├── auth.go               # Authentication endpoints
│   │   ├── config.go             # User config endpoints
│   │   ├── orders.go             # Order management endpoints
│   │   └── health.go             # Health check endpoint
│   ├── middleware/               # HTTP middleware
│   │   ├── auth.go               # JWT validation
│   │   ├── ratelimit.go          # Rate limiting
│   │   ├── logger.go             # Request logging
│   │   └── cors.go               # CORS handling
│   ├── router/                   # Route definitions
│   │   └── router.go
│   └── grpc_clients/             # gRPC client connections
│       ├── config_client.go      # User Config Service client
│       ├── execution_client.go   # Trade Execution client
│       └── risk_client.go        # Risk Management client
├── config/
│   └── config.go                 # Configuration loading
├── Dockerfile                    # Docker build file
└── README.md                     # Service documentation
```

### 2. Protocol Buffers (`api/proto/`)

```
api/proto/
├── common/                       # Shared message types
│   ├── common.proto              # Common types (timestamps, money, etc.)
│   └── errors.proto              # Error definitions
├── user_config/                  # User Configuration Service
│   └── user_config.proto         # Strategy, conditions, trade config
├── trade_execution/              # Trade Execution Service
│   └── trade_execution.proto     # Order, execution, status
├── risk_management/              # Risk Management Service
│   └── risk_management.proto     # Risk checks, limits, metrics
└── rules_engine/                 # Rules Processing Engine
    └── rules_engine.proto        # Evaluation, matching
```

### 3. User Configuration Service (`services/user-config/`)

```
services/user-config/
├── cmd/
│   └── main.go                   # Entry point
├── internal/
│   ├── server/                   # gRPC server
│   │   └── server.go             # Server implementation
│   ├── service/                  # Business logic
│   │   ├── strategy.go           # Strategy CRUD operations
│   │   ├── validation.go         # Config validation
│   │   └── cache.go              # Redis caching logic
│   ├── repository/               # Data access layer
│   │   ├── mongodb.go            # MongoDB operations
│   │   └── redis.go              # Redis operations
│   └── models/                   # Service-specific models
│       └── strategy.go           # Strategy data structure
├── config/
│   └── config.go                 # Configuration loading
├── migrations/                   # Database migrations
│   └── 001_init.js               # Initial MongoDB setup
├── Dockerfile
└── README.md
```

### 4. Data Ingestion Service (`services/data-ingestion/`)

```
services/data-ingestion/
├── cmd/
│   └── main.go                   # Entry point
├── internal/
│   ├── watcher/                  # MongoDB change stream watcher
│   │   ├── watcher.go            # Change stream listener
│   │   └── handler.go            # Event handler
│   ├── processor/                # Data processing
│   │   ├── transformer.go        # Transform MongoDB to Kafka format
│   │   ├── validator.go          # Data validation
│   │   └── enricher.go           # Data enrichment
│   ├── publisher/                # Kafka publisher
│   │   ├── kafka.go              # Kafka producer
│   │   └── batcher.go            # Batch processing
│   └── models/                   # Data models
│       ├── market_data.go        # Market data structure
│       └── event.go              # Kafka event structure
├── config/
│   └── config.go
├── Dockerfile
└── README.md
```

### 5. Rules Processing Engine (`services/rules-engine/`)

```
services/rules-engine/
├── cmd/
│   └── main.go                   # Entry point
├── internal/
│   ├── consumer/                 # Kafka consumer
│   │   ├── consumer.go           # Kafka consumer setup
│   │   └── handler.go            # Message handler
│   ├── matcher/                  # Rule matching logic
│   │   ├── matcher.go            # Main matching engine
│   │   ├── evaluator.go          # Condition evaluation
│   │   └── scorer.go             # Match scoring
│   ├── index/                    # Elasticsearch indexing
│   │   ├── indexer.go            # Index management
│   │   ├── query.go              # Query builder
│   │   └── sync.go               # Index synchronization
│   ├── cache/                    # Config caching
│   │   └── cache.go              # Redis cache operations
│   ├── publisher/                # RabbitMQ publisher
│   │   └── rabbitmq.go           # Order signal publisher
│   └── models/                   # Models
│       ├── rule.go               # Rule structure
│       └── signal.go             # Trade signal structure
├── config/
│   └── config.go
├── Dockerfile
└── README.md
```

### 6. Trade Execution Service (`services/trade-execution/`)

```
services/trade-execution/
├── cmd/
│   └── main.go                   # Entry point
├── internal/
│   ├── server/                   # gRPC server
│   │   └── server.go             # Server implementation
│   ├── consumer/                 # RabbitMQ consumer
│   │   ├── consumer.go           # Order queue consumer
│   │   └── handler.go            # Order processing
│   ├── executor/                 # Trade execution
│   │   ├── executor.go           # Main execution logic
│   │   ├── validator.go          # Pre-trade validation
│   │   └── retry.go              # Retry logic
│   ├── odin/                     # Odin API integration
│   │   ├── client.go             # Odin API client
│   │   ├── orders.go             # Order placement
│   │   └── status.go             # Order status tracking
│   ├── repository/               # Data access
│   │   └── postgres.go           # PostgreSQL operations
│   └── models/                   # Models
│       ├── order.go              # Order structure
│       └── execution.go          # Execution details
├── config/
│   └── config.go
├── migrations/                   # PostgreSQL migrations
│   ├── 001_create_orders.sql    # Orders table
│   └── 002_create_executions.sql # Executions table
├── Dockerfile
└── README.md
```

### 7. Risk Management Service (`services/risk-management/`)

```
services/risk-management/
├── cmd/
│   └── main.go                   # Entry point
├── internal/
│   ├── server/                   # gRPC server
│   │   └── server.go
│   ├── checker/                  # Risk checking
│   │   ├── pre_trade.go          # Pre-trade checks
│   │   ├── post_trade.go         # Post-trade monitoring
│   │   └── limits.go             # Limit enforcement
│   ├── calculator/               # Risk calculations
│   │   ├── exposure.go           # Position exposure
│   │   ├── drawdown.go           # Drawdown tracking
│   │   └── var.go                # Value at Risk
│   ├── repository/               # Data access
│   │   ├── redis.go              # Redis counters
│   │   └── postgres.go           # Risk history
│   └── models/                   # Models
│       ├── risk_metrics.go       # Risk metrics structure
│       └── limits.go             # Limit definitions
├── config/
│   └── config.go
├── Dockerfile
└── README.md
```

---

## Shared Packages (`pkg/`)

### Authentication (`pkg/auth/`)

```
pkg/auth/
├── jwt.go                        # JWT token generation/validation
├── claims.go                     # Custom JWT claims
└── middleware.go                 # Auth middleware
```

### Database Clients (`pkg/database/`)

```
pkg/database/
├── mongodb/
│   ├── client.go                 # MongoDB client
│   ├── changestream.go           # Change stream utilities
│   └── repository.go             # Base repository pattern
├── postgres/
│   ├── client.go                 # PostgreSQL client
│   ├── transaction.go            # Transaction handling
│   └── migration.go              # Migration utilities
├── redis/
│   ├── client.go                 # Redis client
│   ├── cache.go                  # Caching utilities
│   └── pubsub.go                 # Pub/Sub utilities
└── elasticsearch/
    ├── client.go                 # Elasticsearch client
    ├── indexer.go                # Index management
    └── query.go                  # Query builder
```

### Kafka (`pkg/kafka/`)

```
pkg/kafka/
├── producer.go                   # Kafka producer
├── consumer.go                   # Kafka consumer
├── config.go                     # Kafka configuration
└── serializer.go                 # Message serialization
```

### RabbitMQ (`pkg/rabbitmq/`)

```
pkg/rabbitmq/
├── producer.go                   # RabbitMQ producer
├── consumer.go                   # RabbitMQ consumer
├── config.go                     # RabbitMQ configuration
└── retry.go                      # Retry logic with DLQ
```

### Logger (`pkg/logger/`)

```
pkg/logger/
├── logger.go                     # Structured logging (zap/zerolog)
├── context.go                    # Context-aware logging
└── fields.go                     # Standard log fields
```

### Metrics (`pkg/metrics/`)

```
pkg/metrics/
├── prometheus.go                 # Prometheus metrics
├── collectors.go                 # Custom collectors
└── middleware.go                 # Metrics middleware
```

### Odin API Client (`pkg/odin/`)

```
pkg/odin/
├── client.go                     # Odin API client
├── orders.go                     # Order operations
├── market_data.go                # Market data retrieval
├── auth.go                       # Authentication
└── models.go                     # API request/response models
```

---

## Internal Shared Code (`internal/`)

```
internal/
├── models/
│   ├── user.go                   # User model
│   ├── strategy.go               # Strategy model
│   ├── order.go                  # Order model
│   └── event.go                  # Event model
└── utils/
    ├── validator.go              # Input validation
    ├── converter.go              # Type conversions
    └── generator.go              # ID generation (UUID, etc.)
```

---

## Deployment Configurations (`deployments/`)

### Docker (`deployments/docker/`)

```
deployments/docker/
├── api-gateway.Dockerfile
├── user-config.Dockerfile
├── data-ingestion.Dockerfile
├── rules-engine.Dockerfile
├── trade-execution.Dockerfile
└── risk-management.Dockerfile
```

### Docker Compose (`deployments/docker-compose.yml`)

```yaml
version: '3.8'
services:
  # Databases
  mongodb:
  postgres:
  redis:
  elasticsearch:
  
  # Message Brokers
  kafka:
  zookeeper:
  rabbitmq:
  
  # Services
  api-gateway:
  user-config:
  data-ingestion:
  rules-engine:
  trade-execution:
  risk-management:
  
  # Monitoring
  prometheus:
  grafana:
```

---

## Configuration Files (`configs/`)

```
configs/
├── development/
│   ├── api-gateway.yaml
│   ├── user-config.yaml
│   ├── data-ingestion.yaml
│   ├── rules-engine.yaml
│   ├── trade-execution.yaml
│   └── risk-management.yaml
├── staging/
│   └── [same structure]
└── production/
    └── [same structure]
```

**Config Structure Example** (YAML):
```yaml
server:
  host: "0.0.0.0"
  port: 8080
  grpc_port: 9090

database:
  mongodb:
    uri: "mongodb://localhost:27017"
    database: "trading_system"
  postgres:
    host: "localhost"
    port: 5432
    database: "orders"
    username: "${DB_USER}"
    password: "${DB_PASSWORD}"
  redis:
    host: "localhost"
    port: 6379
    password: ""
    db: 0
  elasticsearch:
    url: "http://localhost:9200"

kafka:
  brokers: ["localhost:9092"]
  topic: "market.data.news"
  consumer_group: "rules-engine-group"

rabbitmq:
  url: "amqp://guest:guest@localhost:5672/"
  exchange: "orders"
  queue: "order.execution.queue"

odin:
  api_url: "https://api.odin.example.com"
  api_key: "${ODIN_API_KEY}"
  secret_key: "${ODIN_SECRET_KEY}"

logging:
  level: "info"
  format: "json"

metrics:
  enabled: true
  port: 9100
```

---

## Scripts (`scripts/`)

### Build Script (`scripts/build.sh`)

```bash
#!/bin/bash
# Build all microservices

services=(
  "api-gateway"
  "user-config"
  "data-ingestion"
  "rules-engine"
  "trade-execution"
  "risk-management"
)

for service in "${services[@]}"; do
  echo "Building $service..."
  cd "services/$service" || exit
  go build -o "../../bin/$service" ./cmd/main.go
  cd ../..
done
```

### Proto Generation Script (`scripts/proto-gen.sh`)

```bash
#!/bin/bash
# Generate Go code from proto files

protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       api/proto/**/*.proto
```

---

## Documentation (`docs/`)

```
docs/
├── api/
│   ├── rest-api.md               # REST API documentation
│   ├── grpc-api.md               # gRPC API documentation
│   └── postman/                  # Postman collections
├── architecture/
│   ├── overview.md               # Architecture overview
│   ├── data-flow.md              # Data flow diagrams
│   └── scalability.md            # Scalability strategy
└── guides/
    ├── setup.md                  # Local setup guide
    ├── development.md            # Development guide
    └── deployment.md             # Deployment guide
```

---

## Testing (`tests/`)

```
tests/
├── integration/
│   ├── user_config_test.go       # User config integration tests
│   ├── trade_execution_test.go   # Trade execution tests
│   └── end_to_end_test.go        # Full flow tests
└── load/
    ├── k6/                       # K6 load testing scripts
    │   ├── rules_engine.js
    │   └── trade_execution.js
    └── scenarios/                # Test scenarios
        └── 10k_users.yaml
```

---

## Monitoring (`monitoring/`)

### Prometheus (`monitoring/prometheus/`)

```
monitoring/prometheus/
├── prometheus.yml                # Prometheus config
├── alerts.yml                    # Alert rules
└── recording_rules.yml           # Recording rules
```

### Grafana (`monitoring/grafana/`)

```
monitoring/grafana/
├── dashboards/
│   ├── system-overview.json      # System overview dashboard
│   ├── service-health.json       # Service health dashboard
│   └── trading-metrics.json      # Trading metrics dashboard
└── datasources/
    └── prometheus.yaml           # Prometheus datasource
```

---

## Root Files

### Makefile

```makefile
.PHONY: proto build test docker-up docker-down

proto:
	@./scripts/proto-gen.sh

build:
	@./scripts/build.sh

test:
	@go test -v ./...

test-integration:
	@go test -v ./tests/integration/...

docker-up:
	@docker-compose -f deployments/docker-compose.yml up -d

docker-down:
	@docker-compose -f deployments/docker-compose.yml down

lint:
	@golangci-lint run ./...

migrate-up:
	@./scripts/db-migrate.sh up

migrate-down:
	@./scripts/db-migrate.sh down
```

### .gitignore

```
# Binaries
bin/
*.exe
*.dll
*.so
*.dylib

# Go
*.test
*.out
vendor/

# IDEs
.vscode/
.idea/
*.swp
*.swo

# Environment
.env
*.env.local

# Logs
logs/
*.log

# Generated code
api/proto/**/*.pb.go

# OS
.DS_Store
Thumbs.db
```

### .env.example

```env
# MongoDB
MONGO_URI=mongodb://localhost:27017
MONGO_DATABASE=trading_system

# PostgreSQL
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=orders
POSTGRES_USER=postgres
POSTGRES_PASSWORD=password

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=

# Elasticsearch
ELASTICSEARCH_URL=http://localhost:9200

# Kafka
KAFKA_BROKERS=localhost:9092

# RabbitMQ
RABBITMQ_URL=amqp://guest:guest@localhost:5672/

# Odin API
ODIN_API_URL=https://api.odin.example.com
ODIN_API_KEY=your_api_key
ODIN_SECRET_KEY=your_secret_key

# JWT
JWT_SECRET=your_jwt_secret
JWT_EXPIRY=24h

# Logging
LOG_LEVEL=info
```

---

## Summary

This directory structure follows:

1. **Clear Separation of Concerns**: Each service is independent
2. **Shared Code Reusability**: Common packages in `pkg/`
3. **Proto-First Approach**: All APIs defined in `api/proto/`
4. **Configuration Management**: Environment-specific configs
5. **Testing Strategy**: Unit, integration, and load tests
6. **Deployment Ready**: Docker and compose configurations
7. **Observability**: Built-in monitoring and logging

The structure supports:
- Independent service development and deployment
- Easy testing and debugging
- Clear API contracts via Protocol Buffers
- Scalable microservices architecture
- Production-ready observability
