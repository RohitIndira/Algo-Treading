# Knowledge Transfer Documentation

Per-service documentation for onboarding and deep reference.

## Available KT Documents

### [API Gateway](./API_GATEWAY_KT.md)
REST to gRPC translation, CORS, WebSocket support, error handling.

### [User Config Service](./USER_CONFIG_SERVICE_KT.md)
Strategy CRUD, PostgreSQL schema, Kafka event publishing, optimistic locking.

### [Rules Engine Service](./RULES_ENGINE_SERVICE_KT.md)
Event matching algorithm, Redis caching, Kafka consumer/publisher, scoring logic.

### [Data Ingestion Service](./DATA_INGESTION_SERVICE_KT.md)
MongoDB change streams, Kafka publishing, data validation.

### [Trade Execution Service](./TRADE_EXECUTION_SERVICE_KT.md)
Order lifecycle, Kafka consumer (trade signals), broker integration (Indira Securities), paper/live trading, OCO orders, credential management.

### [Risk Management Service](./RISK_MANAGEMENT_SERVICE_KT.md)
Pre-trade risk validation (8 checks), risk profiles, Redis metrics, circuit breaker.

---

## Reading Order by Role

**Frontend Developer:**
1. API Gateway KT
2. User Config Service KT

**Backend Developer:**
1. Data Ingestion Service KT (data pipeline)
2. Rules Engine Service KT (matching logic)
3. Trade Execution Service KT (order processing)
4. Risk Management Service KT (risk checks)

**New Team Member:**
1. API Gateway KT (system entry point)
2. Data Ingestion -> Rules Engine -> Trade Execution (follow the data flow)
3. Risk Management and User Config as needed

---

## Architecture

```
Frontend -> API Gateway (8081 HTTP)
              |
              v
         User Config (50051 gRPC) -> PostgreSQL, Kafka
              |
         Data Ingestion (50052 gRPC) -> MongoDB, Kafka
              |
         Rules Engine (50053 gRPC) -> Redis, Kafka
              |
         Trade Execution (50054 gRPC) -> PostgreSQL, Kafka, RabbitMQ
              |
         Risk Management (50055 gRPC) -> Redis
              |
         Indira Securities API (broker)
```

## Tech Stack

- **Go**: All services
- **PostgreSQL**: Strategies, orders, credentials, trade signals
- **MongoDB**: News/market data (external, watched via change streams)
- **Redis**: Caching, price data, risk counters
- **Kafka**: Event streaming (news, config changes, trade signals)
- **RabbitMQ**: Order execution queue
- **gRPC**: Inter-service communication
- **Protocol Buffers**: API contracts (`api/proto/`)
