<<<<<<< HEAD
# Algo-Treading
=======
# Algorithmic Trading System

A high-performance, event-driven microservices-based algorithmic trading system for Indian stock markets, supporting 10,000+ concurrent users with personalized trading strategies based on real-time news sentiment and impact scores.

## 🎯 Overview

This system enables users to define custom trading strategies that automatically execute trades when market conditions match their specified criteria. It processes real-time news and market data, evaluates user-defined rules, and executes trades via the Odin API.

### Key Features

- **Personalized Strategies**: 10,000+ users can define unique trading rules
- **Real-Time Processing**: MongoDB change streams for instant data ingestion
- **Event-Driven Architecture**: Kafka and RabbitMQ for reliable message processing
- **Risk Management**: Pre-trade risk checks and real-time monitoring
- **High Performance**: Designed for <100ms matching latency (p95)
- **gRPC Communication**: Fast, efficient inter-service communication
- **Scalable Design**: Horizontal scaling for all services

---

## 🏗️ Architecture

The system follows a microservices architecture with the following layers:

### **Ingestion Layer**
- **Data Ingestion Service**: Monitors MongoDB for news updates, publishes to Kafka

### **Processing Layer**
- **Rules Processing Engine**: Matches events against 10K user strategies using Elasticsearch
- **Kafka Event Bus**: High-throughput message streaming

### **Execution Layer**
- **Trade Execution Service**: Executes trades via Odin API
- **Risk Management Service**: Pre/post-trade risk enforcement
- **RabbitMQ**: Reliable order queue with retry logic

### **User Interface Layer**
- **API Gateway**: REST API for user interactions
- **User Configuration Service**: Manage trading strategies

### **Storage Layer**
- **MongoDB**: Market data, user configs
- **PostgreSQL**: Order management, executions
- **Redis**: Caching, rate limiting, risk counters
- **Elasticsearch**: Fast rule indexing for matching

---

## 📊 System Flow

```
News Update (MongoDB) 
  → Data Ingestion Service 
  → Kafka (market.data.news)
  → Rules Processing Engine
    ↓ (Match found)
  → RabbitMQ (order.execution.queue)
  → Risk Management (Pre-trade check)
  → Trade Execution Service
  → Odin API (Order placement)
  → PostgreSQL (Order record)
```

---

## 🚀 Quick Start

### Prerequisites

- Go 1.21+
- Docker & Docker Compose
- Protocol Buffer Compiler
- MongoDB, PostgreSQL, Redis, Elasticsearch
- Kafka, RabbitMQ

### Setup

```bash
# Clone repository
git clone <repository-url>
cd trading-system

# Install dependencies
go mod download

# Generate protobuf code
make proto

# Start infrastructure (Docker Compose)
make docker-up

# Build all services
make build

# Run tests
make test
```

### Environment Configuration

```bash
# Copy environment template
cp .env.example .env

# Edit configuration
nano .env
```

Required environment variables:
- MongoDB connection URI
- PostgreSQL credentials
- Redis connection
- Kafka brokers
- RabbitMQ URL
- Odin API credentials
- JWT secret

---

## 📦 Project Structure

```
trading-system/
├── api/                      # API Gateway & Protocol Buffers
├── services/                 # Microservices
│   ├── user-config/
│   ├── data-ingestion/
│   ├── rules-engine/
│   ├── trade-execution/
│   └── risk-management/
├── pkg/                      # Shared packages
├── internal/                 # Private application code
├── deployments/              # Docker & K8s configs
├── scripts/                  # Build & utility scripts
├── configs/                  # Configuration files
├── docs/                     # Documentation
├── tests/                    # Integration tests
└── monitoring/               # Prometheus & Grafana
```

See [Directory Structure](directory-structure.md) for detailed layout.

---

## 🔧 Microservices

### 1. User Configuration Service
**Port**: 9001 (gRPC)

Manages user trading strategies and preferences.

**Key Operations**:
- Create/Update/Delete strategies
- Activate/Deactivate strategies
- List user strategies

### 2. Data Ingestion Service
**Port**: 9002 (gRPC - metrics only)

Monitors MongoDB change streams and publishes events to Kafka.

**Features**:
- Real-time change detection
- Data validation and enrichment
- Batch processing
- Backpressure handling

### 3. Rules Processing Engine
**Port**: 9003 (gRPC)

Matches incoming events against 10,000 user strategies.

**Features**:
- Elasticsearch-based rule indexing
- Parallel condition evaluation
- Redis caching for hot configs
- Trade signal generation

### 4. Trade Execution Service
**Port**: 9004 (gRPC)

Executes trades via Odin API.

**Features**:
- Order lifecycle management
- Retry logic with exponential backoff
- Order status tracking
- Commission calculation

### 5. Risk Management Service
**Port**: 9005 (gRPC)

Enforces trading limits and monitors risk.

**Features**:
- Pre-trade risk checks
- Daily trade/loss limits
- Position size enforcement
- Real-time portfolio monitoring

### 6. API Gateway
**Port**: 8080 (HTTP/REST)

Single entry point for all client requests.

**Features**:
- JWT authentication
- Rate limiting
- Request routing
- Response aggregation

---

## 🔌 API Reference

### REST API (API Gateway)

#### Authentication
```http
POST /api/v1/auth/login
POST /api/v1/auth/refresh
```

#### Strategy Management
```http
GET    /api/v1/strategies
POST   /api/v1/strategies
GET    /api/v1/strategies/:id
PUT    /api/v1/strategies/:id
DELETE /api/v1/strategies/:id
POST   /api/v1/strategies/:id/activate
POST   /api/v1/strategies/:id/deactivate
```

#### Order Management
```http
GET    /api/v1/orders
GET    /api/v1/orders/:id
POST   /api/v1/orders/:id/cancel
GET    /api/v1/orders/history
```

#### Risk Management
```http
GET    /api/v1/risk/metrics
GET    /api/v1/risk/limits
PUT    /api/v1/risk/limits
GET    /api/v1/risk/events
```

### gRPC APIs

See [Protocol Buffer Definitions](proto-definitions.md) for complete gRPC API specifications.

---

## 📖 Documentation

| Document | Description |
|----------|-------------|
| [Architecture](trading-system-architecture.md) | Comprehensive architecture documentation |
| [Directory Structure](directory-structure.md) | Project directory layout and organization |
| [Protocol Buffers](proto-definitions.md) | gRPC service definitions |
| [API Documentation](docs/api/) | REST and gRPC API reference |
| [Setup Guide](docs/guides/setup.md) | Development environment setup |
| [Development Guide](docs/guides/development.md) | Development workflow and best practices |

---

## 🛠️ Development

### Building Services

```bash
# Build all services
make build

# Build specific service
cd services/user-config
go build -o ../../bin/user-config ./cmd/main.go
```

### Running Tests

```bash
# Unit tests
make test

# Integration tests
make test-integration

# Load tests
cd tests/load
k6 run rules_engine.js
```

### Code Generation

```bash
# Generate protobuf code
make proto

# Run linter
make lint
```

### Local Development

```bash
# Start infrastructure
make docker-up

# Run service locally
cd services/user-config
go run cmd/main.go --config ../../configs/development/user-config.yaml

# View logs
docker-compose -f deployments/docker-compose.yml logs -f
```

---

## 🔒 Security

- **Authentication**: JWT tokens with 24-hour expiry
- **Authorization**: Role-based access control (RBAC)
- **Encryption**: TLS 1.3 for all gRPC communication
- **API Security**: Rate limiting, input validation
- **Secrets Management**: Environment variables, external secret store
- **Audit Logging**: All configuration changes logged

---

## 📊 Monitoring

### Metrics (Prometheus)

Access Prometheus at http://localhost:9090

**Key Metrics**:
- `trading_events_processed_total` - Total events processed
- `trading_matches_found_total` - Total strategy matches
- `trading_orders_placed_total` - Total orders placed
- `trading_matching_latency_ms` - Matching latency histogram
- `trading_execution_latency_ms` - Order execution time

### Dashboards (Grafana)

Access Grafana at http://localhost:3000

**Dashboards**:
- System Overview
- Service Health
- Trading Metrics
- Error Tracking

### Logging (ELK Stack)

Access Kibana at http://localhost:5601

**Log Levels**: ERROR, WARN, INFO, DEBUG

---

## 🧪 Testing

### Unit Tests

```bash
# Run all unit tests
go test ./...

# Run with coverage
go test -cover ./...

# Generate coverage report
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

### Integration Tests

```bash
# Run integration tests
go test -v ./tests/integration/...
```

### Load Tests

```bash
# Test with 10K users
k6 run tests/load/scenarios/10k_users.js
```

---

## 🚀 Deployment

### Docker

```bash
# Build Docker images
docker-compose -f deployments/docker-compose.yml build

# Start all services
docker-compose -f deployments/docker-compose.yml up -d

# Scale specific service
docker-compose -f deployments/docker-compose.yml up -d --scale rules-engine=3
```

### Kubernetes (Future)

```bash
# Apply configurations
kubectl apply -f deployments/kubernetes/

# Check status
kubectl get pods -n trading-system

# View logs
kubectl logs -f deployment/rules-engine -n trading-system
```

---

## 🎯 Performance Targets

| Metric | Target | Current |
|--------|--------|---------|
| Matching Latency (p95) | < 100ms | TBD |
| Order Execution (p95) | < 500ms | TBD |
| Events/Hour | 1000+ | TBD |
| Concurrent Users | 10,000 | TBD |
| System Uptime | 99.9% | TBD |

---

## 🗺️ Roadmap

### Phase 1: Core Development ✅
- [x] Architecture design
- [x] Protocol Buffer definitions
- [x] Directory structure
- [ ] Core service implementations
- [ ] Basic integration tests

### Phase 2: Integration
- [ ] Odin API integration
- [ ] MongoDB change streams
- [ ] Kafka/RabbitMQ setup
- [ ] Elasticsearch indexing
- [ ] End-to-end testing

### Phase 3: Optimization
- [ ] Performance tuning
- [ ] Load testing
- [ ] Caching optimization
- [ ] Database indexing

### Phase 4: Production Readiness
- [ ] Monitoring setup
- [ ] Alerting configuration
- [ ] Disaster recovery
- [ ] Documentation completion
- [ ] Security audit

### Phase 5: Advanced Features
- [ ] Machine learning integration
- [ ] Advanced risk models
- [ ] Portfolio optimization
- [ ] Backtesting engine
- [ ] WebSocket notifications

---

## 🤝 Contributing

### Development Workflow

1. Create feature branch
2. Implement changes
3. Write tests
4. Run linter
5. Submit pull request

### Code Standards

- Follow Go best practices
- Write unit tests (>80% coverage)
- Document public APIs
- Use meaningful commit messages
- Update documentation

---

## 📝 Database Schemas

### MongoDB Collections

**market_data** (Watched by Data Ingestion)
```javascript
{
  _id: ObjectId,
  stock: Int,
  news_id: String,
  sentiment: String,
  impact_score: Int,
  category: String,
  LastTradedPrice: Double,
  pct_change: Double,
  dt_tm: Date,
  // ... other fields
}
```

**user_configs** (User Configuration Service)
```javascript
{
  _id: ObjectId,
  user_id: String,
  strategy_name: String,
  active: Boolean,
  conditions: Object,
  trade_config: Object,
  risk_limits: Object,
  created_at: Date,
  updated_at: Date
}
```

### PostgreSQL Tables

**orders** (Trade Execution Service)
```sql
CREATE TABLE orders (
  order_id UUID PRIMARY KEY,
  user_id VARCHAR(50),
  strategy_id VARCHAR(50),
  event_id UUID,
  stock_code BIGINT,
  exchange VARCHAR(10),
  order_type VARCHAR(10),
  quantity INT,
  status VARCHAR(20),
  created_at TIMESTAMP,
  -- ... other fields
);
```

---

## 🐛 Troubleshooting

### Common Issues

**Problem**: Services can't connect to database
```bash
# Check database connectivity
docker-compose ps
docker-compose logs mongodb

# Verify connection string
echo $MONGO_URI
```

**Problem**: Kafka consumer lag
```bash
# Check consumer group status
docker-compose exec kafka kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group rules-engine-group
```

**Problem**: High matching latency
```bash
# Check Elasticsearch health
curl http://localhost:9200/_cluster/health

# Check Redis cache hit rate
redis-cli info stats | grep hit
```

---

## 📞 Support

- **Documentation**: [docs/](docs/)
- **Issues**: GitHub Issues
- **Email**: support@trading-system.com

---

## 📄 License

Copyright (c) 2025 Trading System

---

## 🙏 Acknowledgments

- **Odin API**: Indian stock market trading
- **MongoDB**: Real-time change streams
- **Kafka**: High-throughput event streaming
- **gRPC**: Efficient inter-service communication

---

## 📚 Additional Resources

- [Go gRPC Documentation](https://grpc.io/docs/languages/go/)
- [MongoDB Change Streams](https://docs.mongodb.com/manual/changeStreams/)
- [Apache Kafka](https://kafka.apache.org/documentation/)
- [Elasticsearch](https://www.elastic.co/guide/index.html)
- [Protocol Buffers](https://developers.google.com/protocol-buffers)

---

**Built with ❤️ for algorithmic traders**
>>>>>>> f7de43992cd89829d3f2af1b88bda0ae858d5edb
