# Rules Engine Service

A high-performance, production-ready rules processing engine for matching market events against 10,000+ user trading strategies.

## 🎯 Overview

The Rules Engine Service is the core intelligence of the algorithmic trading system. It:
- Consumes market events from Kafka in real-time
- Matches events against user-defined trading strategies using Elasticsearch
- Evaluates complex conditions with configurable weights
- Publishes matched orders to RabbitMQ for execution
- Provides comprehensive metrics and monitoring

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────┐
│              Rules Engine Service (Port 9003)           │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  Kafka Consumer ──→ Event Handler ──→ Matcher Engine   │
│         │                                    │          │
│         │                                    ↓          │
│         │                          Elasticsearch Query │
│         │                                    │          │
│         │                                    ↓          │
│         │                          Strategy Cache      │
│         │                          (Redis)             │
│         │                                    │          │
│         │                                    ↓          │
│         │                          Condition Evaluator │
│         │                                    │          │
│         │                                    ↓          │
│         │                          Match Scorer        │
│         │                                    │          │
│         │                                    ↓          │
│         └────────────────────────→  RabbitMQ Publisher │
│                                                         │
│  gRPC Server (Health, Stats, Admin APIs)               │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

## 🚀 Key Features

### Performance
- **High Throughput**: Processes 1000+ events/hour
- **Low Latency**: <100ms matching latency (p95)
- **Concurrent Processing**: Configurable worker pool (default 50)
- **Efficient Caching**: Redis-backed strategy caching with TTL

### Scalability
- **Horizontal Scaling**: Kafka consumer groups for parallel processing
- **Elasticsearch Sharding**: Distributed strategy indexing
- **Connection Pooling**: Optimized database and gRPC connections
- **Circuit Breaker**: Prevents cascade failures

### Reliability
- **Graceful Shutdown**: Proper cleanup of all resources
- **Auto-Reconnection**: Automatic reconnection for Kafka and RabbitMQ
- **Error Handling**: Comprehensive error handling and retry logic
- **Health Checks**: Built-in health check endpoints

### Observability
- **Metrics**: Real-time performance metrics
- **Structured Logging**: JSON-formatted logs with context
- **Statistics**: Detailed matching statistics and analytics
- **Tracing**: Event flow tracking

## 📦 Components

### 1. **Matcher Engine** (`internal/matcher/`)
- **Matcher**: Orchestrates matching process
- **Evaluator**: Evaluates strategy conditions
- **Scorer**: Calculates match scores with configurable weights

### 2. **Index Layer** (`internal/index/`)
- **Indexer**: Manages Elasticsearch indexing
- **QueryEngine**: Builds and executes ES queries

### 3. **Cache Layer** (`internal/cache/`)
- **RedisCache**: Generic Redis cache implementation
- **StrategyCache**: Strategy-specific caching logic

### 4. **Consumer Layer** (`internal/consumer/`)
- **Consumer**: Kafka message consumer
- **Handler**: Event processing logic

### 5. **Publisher Layer** (`internal/publisher/`)
- **Publisher**: RabbitMQ publisher with circuit breaker

## ⚙️ Configuration

All configuration is environment-based (no hardcoded values):

### Core Service
```bash
SERVICE_NAME=rules-engine
SERVICE_VERSION=1.0.0
ENVIRONMENT=production
GRPC_PORT=9003
METRICS_PORT=9103
```

### Kafka
```bash
KAFKA_BROKERS=kafka1:9092,kafka2:9092,kafka3:9092
KAFKA_TOPIC=market.data.news
KAFKA_CONSUMER_GROUP=rules-engine-group
KAFKA_START_OFFSET=latest
KAFKA_COMMIT_INTERVAL=1s
KAFKA_MAX_BYTES=10485760
```

### Elasticsearch
```bash
ELASTICSEARCH_URLS=http://es1:9200,http://es2:9200
ELASTICSEARCH_USERNAME=elastic
ELASTICSEARCH_PASSWORD=password
ELASTICSEARCH_INDEX=user_strategies
ELASTICSEARCH_MAX_RETRIES=3
ELASTICSEARCH_TIMEOUT=5s
```

### Redis
```bash
REDIS_ADDRS=redis1:6379,redis2:6379,redis3:6379
REDIS_PASSWORD=password
REDIS_DB=0
REDIS_POOL_SIZE=100
REDIS_CACHE_TTL=5m
REDIS_CLUSTER_MODE=true
```

### RabbitMQ
```bash
RABBITMQ_URL=amqp://user:password@rabbitmq:5672/
RABBITMQ_EXCHANGE=orders
RABBITMQ_QUEUE=order.execution.queue
RABBITMQ_ROUTING_KEY=order.execution
RABBITMQ_PREFETCH_COUNT=10
```

### gRPC Clients
```bash
USER_CONFIG_SERVICE_ADDR=user-config:9001
RISK_MANAGEMENT_SERVICE_ADDR=risk-management:9005
```

### Performance Tuning
```bash
WORKER_COUNT=50
MAX_BATCH_SIZE=100
MAX_CONCURRENT_MATCHES=100
MIN_MATCH_SCORE=80.0
CIRCUIT_BREAKER_THRESHOLD=5
```

## 🔧 Prerequisites

### Infrastructure
- **Kafka**: v3.0+ (50 partitions for parallel processing)
- **Elasticsearch**: v8.0+ (for strategy indexing)
- **Redis**: v7.0+ (cluster mode recommended)
- **RabbitMQ**: v3.9+ (quorum queues)

### Dependent Services
- **User Config Service**: Port 9001 (strategy management)
- **Risk Management Service**: Port 9005 (risk checks)

### Development Tools
- **Go**: 1.21+
- **Protocol Buffers**: v3.21+
- **Docker**: For containerization

## 📥 Installation

### 1. Clone Repository
```bash
git clone https://github.com/RohitIndira/Algo-Treading.git
cd Algo-Treading/services/rules-engine
```

### 2. Install Dependencies
```bash
go mod download
```

### 3. Generate Proto Files
```bash
cd ../../api/proto
make generate
```

### 4. Set Environment Variables
```bash
cp .env.example .env
# Edit .env with your configuration
```

### 5. Build
```bash
go build -o rules-engine ./cmd/main.go
```

### 6. Run
```bash
./rules-engine
```

## 🐳 Docker Deployment

### Build Image
```bash
docker build -t rules-engine:latest .
```

### Run Container
```bash
docker run -d \
  --name rules-engine \
  -p 9003:9003 \
  -p 9103:9103 \
  --env-file .env \
  rules-engine:latest
```

### Docker Compose
```yaml
version: '3.8'
services:
  rules-engine:
    build: .
    ports:
      - "9003:9003"
      - "9103:9103"
    environment:
      - KAFKA_BROKERS=kafka:9092
      - ELASTICSEARCH_URLS=http://elasticsearch:9200
      - REDIS_ADDRS=redis:6379
      - RABBITMQ_URL=amqp://rabbitmq:5672/
    depends_on:
      - kafka
      - elasticsearch
      - redis
      - rabbitmq
```

## 🧪 Testing

### Unit Tests
```bash
go test ./... -v
```

### Integration Tests
```bash
go test ./... -tags=integration -v
```

### Load Testing
```bash
# Simulate 1000 events
go test ./tests/load -bench=. -benchtime=1000x
```

## 📊 Monitoring

### gRPC Endpoints

#### Health Check
```bash
grpcurl -plaintext localhost:9003 rules_engine.RulesEngineService/HealthCheck
```

#### Get Matching Stats
```bash
grpcurl -plaintext -d '{"top_n": 10}' \
  localhost:9003 rules_engine.RulesEngineService/GetMatchingStats
```

#### Reload User Rules
```bash
grpcurl -plaintext -d '{"user_ids": ["USER123"], "force_refresh": true}' \
  localhost:9003 rules_engine.RulesEngineService/ReloadUserRules
```

### Metrics (Prometheus)
```
# Endpoint
http://localhost:9103/metrics

# Key Metrics
trading_events_processed_total
trading_matches_found_total
trading_orders_generated_total
trading_matching_latency_ms
trading_cache_hit_rate
trading_elasticsearch_query_time_ms
```

### Logs
```bash
# View logs (JSON format)
tail -f /var/log/rules-engine/app.log

# Filter errors
tail -f /var/log/rules-engine/app.log | jq 'select(.level=="error")'
```

## 🎯 Matching Algorithm

### Strategy Evaluation Process

1. **Candidate Selection** (Elasticsearch)
   - Query active strategies
   - Filter by impact score threshold
   - Boost by sentiment, category, stock match

2. **Cache Lookup** (Redis)
   - Fetch full strategy details from cache
   - Fallback to User Config Service on miss

3. **Condition Evaluation**
   - ✓ Impact Score (≥ threshold)
   - ✓ Sentiment (in allowed list)
   - ✓ Category (in allowed list)
   - ✓ Stock (in watchlist or all)
   - ✓ Price Range (within min/max)
   - ✓ Volume (≥ threshold)
   - ✓ Percent Change (≥ threshold)
   - ✓ Exchange (matches preference)

4. **Score Calculation**
   ```
   Weighted Score = Σ (condition_score × weight / 100)
   
   Weights (default):
   - Impact Score: 25%
   - Stock Match: 20%
   - Sentiment: 15%
   - Category: 15%
   - Price Range: 10%
   - Volume: 7.5%
   - Pct Change: 5%
   - Exchange: 2.5%
   ```

5. **Threshold Check**
   - Min score: 80.0 (configurable)
   - Full match bonus: +10%

6. **Order Generation**
   - Create OrderRequest
   - Publish to RabbitMQ

## 🔍 Troubleshooting

### High Latency
```bash
# Check Elasticsearch query time
grpcurl -plaintext localhost:9003 rules_engine.RulesEngineService/GetMatchingStats

# Check cache hit rate
redis-cli info stats | grep hit_rate
```

### No Matches Found
```bash
# Check active strategies count
grpcurl -plaintext localhost:9003 rules_engine.RulesEngineService/GetActiveRulesCount

# Verify Elasticsearch index
curl http://localhost:9200/user_strategies/_count
```

### Consumer Lag
```bash
# Check Kafka consumer lag
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group rules-engine-group
```

### Circuit Breaker Open
```bash
# Check publisher health
curl http://localhost:9103/health

# View RabbitMQ connection status
rabbitmqctl list_connections
```

## 📈 Performance Tuning

### Increase Throughput
```bash
WORKER_COUNT=100
MAX_CONCURRENT_MATCHES=200
KAFKA_MAX_BYTES=20971520  # 20MB
```

### Reduce Latency
```bash
ES_QUERY_TIMEOUT=1s
REDIS_CACHE_TTL=10m
MAX_CONCURRENT_MATCHES=50
```

### Memory Optimization
```bash
REDIS_POOL_SIZE=50
WORKER_COUNT=25
MAX_BATCH_SIZE=50
```

## 🛡️ Security

- **No Hardcoded Credentials**: All credentials via environment
- **TLS Support**: gRPC with TLS encryption
- **Input Validation**: All events and strategies validated
- **Rate Limiting**: Circuit breaker prevents overload
- **Audit Logging**: All matches logged with context

## 🤝 Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit changes (`git commit -m 'Add AmazingFeature'`)
4. Push to branch (`git push origin feature/AmazingFeature`)
5. Open Pull Request

## 📄 License

Copyright (c) 2025 Algo Trading System

## 🙏 Acknowledgments

- **Kafka**: High-throughput event streaming
- **Elasticsearch**: Fast strategy indexing and querying
- **Redis**: High-performance caching
- **RabbitMQ**: Reliable message queuing
- **gRPC**: Efficient inter-service communication

---

**Built with ❤️ for high-frequency algorithmic trading**
