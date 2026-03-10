# Knowledge Transfer Documentation - Master Index

## 📚 Overview

This directory contains comprehensive Knowledge Transfer (KT) documentation for all components of the Algorithmic Trading System. Each document provides detailed information about architecture, implementation, configuration, and best practices.

---

## 🗂️ Available Documentation

### 1. **API Gateway KT** 
**File:** [`API_GATEWAY_KT.md`](./API_GATEWAY_KT.md)

**What's Covered:**
- REST to gRPC translation architecture
- HTTP proxying for authentication services
- WebSocket implementation for real-time events
- CORS configuration and management
- Error handling strategies
- Deployment and testing

**Use When:**
- Setting up the API Gateway
- Integrating frontend applications
- Implementing new REST endpoints
- Troubleshooting CORS issues
- Adding WebSocket features

---

### 2. **User Config Service KT**
**File:** [`USER_CONFIG_SERVICE_KT.md`](./USER_CONFIG_SERVICE_KT.md)

**What's Covered:**
- gRPC service implementation
- Strategy CRUD operations
- PostgreSQL database schema
- Kafka event publishing
- Optimistic locking and concurrency
- API examples and testing

**Use When:**
- Managing trading strategies
- Implementing strategy operations
- Understanding data models
- Setting up database
- Troubleshooting strategy issues

---

### 3. **Rules Engine Service KT**
**File:** [`RULES_ENGINE_SERVICE_KT.md`](./RULES_ENGINE_SERVICE_KT.md)

**What's Covered:**
- Event matching algorithm
- Elasticsearch integration
- Redis caching strategy
- Kafka consumer implementation
- RabbitMQ publisher
- Performance optimization
- Scoring and evaluation logic

**Use When:**
- Understanding matching logic
- Optimizing performance
- Debugging match issues
- Scaling the engine
- Implementing new matching rules

---

### 4. **Data Ingestion Service KT**
**File:** [`DATA_INGESTION_SERVICE_KT.md`](./DATA_INGESTION_SERVICE_KT.md)

**What's Covered:**
- MongoDB Change Streams
- Real-time event detection
- Kafka publishing
- Extended JSON format
- Connection management
- Error handling

**Use When:**
- Setting up data pipeline
- Monitoring news events
- Troubleshooting ingestion
- Understanding data flow
- Implementing new data sources

---

### 5. **Odin API Wrapper Service KT**
**File:** [`ODIN_API_WRAPPER_SERVICE_KT.md`](./ODIN_API_WRAPPER_SERVICE_KT.md)

**What's Covered:**
- RabbitMQ consumer implementation
- Dynamic user authentication
- Order translation logic
- Database credential management
- TOTP generation
- Broker integration

**Use When:**
- Executing orders via Odin API
- Managing user credentials
- Troubleshooting order placement
- Understanding broker integration
- Implementing security features

---

### 6. **User Login Service KT**
**File:** [`USER_LOGIN_SERVICE_KT.md`](./USER_LOGIN_SERVICE_KT.md)

**What's Covered:**
- Multi-method authentication
- Session management
- Credential encryption
- Login history tracking
- ODIN API authentication
- FastAPI implementation

**Use When:**
- Implementing authentication
- Managing user sessions
- Storing credentials securely
- Troubleshooting login issues
- Understanding security features

---

### 7. **Risk Management Service KT**
**File:** [`RISK_MANAGEMENT_SERVICE_KT.md`](./RISK_MANAGEMENT_SERVICE_KT.md)

**What's Covered:**
- Pre-trade risk validation (8 checks)
- Post-trade monitoring
- Redis integration for real-time metrics
- Risk profile management (Conservative, Moderate, Aggressive)
- Position tracking and exposure limits
- Circuit breaker implementation
- Duplicate order detection

**Use When:**
- Implementing risk validation
- Understanding risk checks
- Managing risk limits
- Troubleshooting order rejections
- Configuring user risk profiles
- Setting up circuit breakers

---

### 8. **Trade Execution Service KT**
**File:** [`TRADE_EXECUTION_SERVICE_KT.md`](./TRADE_EXECUTION_SERVICE_KT.md)

**What's Covered:**
- Order lifecycle management
- Kafka consumer (trade signals)
- RabbitMQ consumer (order execution)
- Odin API integration with retry logic
- PostgreSQL order storage
- Credential management
- Execution event tracking
- Worker pool architecture

**Use When:**
- Processing trade signals
- Executing orders via Odin API
- Understanding order flow
- Troubleshooting execution failures
- Managing retry mechanisms
- Tracking order status

---

## 🎯 Quick Navigation Guide

### By Role

#### **Frontend Developer**
Start with:
1. API Gateway KT - Understanding REST endpoints
2. User Login Service KT - Authentication flow
3. User Config Service KT - Strategy management APIs

#### **Backend Developer**
Start with:
1. Rules Engine Service KT - Core matching logic
2. Data Ingestion Service KT - Event pipeline
3. Risk Management Service KT - Risk validation
4. Trade Execution Service KT - Order processing
5. Odin API Wrapper Service KT - Broker integration

#### **DevOps Engineer**
Start with:
1. API Gateway KT - Deployment and scaling
2. Rules Engine Service KT - Performance optimization
3. All services - Configuration sections

#### **New Team Member**
Start with:
1. API Gateway KT - System entry point
2. Data Ingestion Service KT - Data flow
3. Rules Engine Service KT - Core matching logic
4. Risk Management Service KT - Risk checks
5. Trade Execution Service KT - Order lifecycle
6. User Config Service KT - Strategy management

---

## 🏗️ System Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                      Frontend Application                    │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│                    API Gateway (Port 8081)                   │
│  - REST to gRPC translation                                  │
│  - WebSocket support                                         │
│  - CORS management                                           │
└────────┬────────────────────────────────────┬───────────────┘
         │                                     │
         ▼                                     ▼
┌─────────────────────┐            ┌─────────────────────────┐
│  User Config Service│            │ User Login Service      │
│    (Port 50051)     │            │   (Port 8002)           │
│  - Strategy CRUD    │            │  - Authentication       │
│  - Kafka Publisher  │            │  - Session Management   │
└──────────┬──────────┘            └─────────────────────────┘
           │
           ▼
    ┌──────────────┐
    │  Kafka Topic │
    │strategy.updates
    └──────┬───────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────┐
│                  Rules Engine (Port 9003)                    │
│  - Elasticsearch indexing                                    │
│  - Redis caching                                             │
│  - Event matching                                            │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│                 Data Ingestion (Port 9001)                   │
│  - MongoDB Change Streams                                    │
│  - Kafka Publisher                                           │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
            ┌────────────────┐
            │  Kafka Topic   │
            │  news-events   │
            └────────┬───────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│              Rules Engine - Matcher                          │
│  (Consumes news, matches strategies, publishes signals)      │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
            ┌────────────────┐
            │  Kafka Topic   │
            │ trade-signals  │
            └────────┬───────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│           Trade Execution Service (Port 9004)                │
│  - Kafka consumer (signals)                                  │
│  - Risk check via Risk Management Service                    │
│  - RabbitMQ publisher (approved orders)                      │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
            ┌────────────────┐
            │ RabbitMQ Queue │
            │trade.executions│
            └────────┬───────┘
                     │
                     ├──► Trade Execution Service (RabbitMQ consumer)
                     │    - Fetches credentials
                     │    - Calls Odin API with retry
                     │
                     └──► Odin API Wrapper (Legacy consumer)
                          - Alternative execution path
                          - Dynamic authentication
```
```

---

## 📊 Technology Stack Reference

### Languages
- **Go**: User Config, Rules Engine, Data Ingestion
- **Python**: Odin API Wrapper, User Login Service
- **Protocol Buffers**: gRPC service definitions

### Databases
- **PostgreSQL**: User credentials, strategies, sessions
- **MongoDB**: News events (external)
- **Elasticsearch**: Strategy indexing and search
- **Redis**: Caching and pub/sub

### Message Queues
- **Apache Kafka**: Event streaming (news, strategies)
- **RabbitMQ**: Order execution queue

### Frameworks
- **gRPC**: Inter-service communication
- **FastAPI**: User Login Service
- **Gorilla Mux**: API Gateway routing

---

## 🔧 Common Development Tasks

### Adding a New API Endpoint
1. Read: API Gateway KT - Core Components
2. Implement: Handler in appropriate service
3. Test: API Gateway KT - Testing section

### Modifying Matching Logic
1. Read: Rules Engine Service KT - Matching Algorithm
2. Update: Evaluator and Scorer
3. Test: Rules Engine Service KT - Testing section

### Adding New Strategy Field
1. Read: User Config Service KT - Database Schema
2. Update: Protocol Buffers definition
3. Update: Database migration
4. Update: Rules Engine indexing

### Debugging Order Placement
1. Read: Trade Execution Service KT - Order Execution Flow
2. Read: Risk Management Service KT - Risk Validation Logic
3. Check: Kafka consumer lag (trade-signals topic)
4. Check: RabbitMQ queue status (trade.executions)
5. Review: PostgreSQL orders table for order status
6. Verify: Odin API credentials in database
7. Check: Risk Management service logs for rejections

---

## 🐛 Troubleshooting Quick Links

### Service Won't Start
- API Gateway KT - Troubleshooting Section
- Risk Management Service KT - Redis Connection Issues
- Trade Execution Service KT - Database/Kafka/RabbitMQ Setup
- Check dependencies (Kafka, PostgreSQL, Redis, RabbitMQ, MongoDB)
- Verify environment variables

### High Latency
- Rules Engine Service KT - Performance Optimization
- Check Elasticsearch performance
- Verify Redis cache hit rate

### Order Not Executing
- Trade Execution Service KT - Monitoring & Troubleshooting
- Risk Management Service KT - Risk Validation Issues
- Odin API Wrapper Service KT - Troubleshooting
- Check Kafka consumer lag (trade-signals topic)
- Check RabbitMQ queue depth (trade.executions)
- Verify risk approval in orders table
- Verify user credentials in database

### Risk Check Failing
- Risk Management Service KT - Common Issues
- Check Redis keys for trade counts and daily loss
- Verify risk profile settings
- Review risk limits configuration
- Check for circuit breaker triggers

### WebSocket Connection Issues
- API Gateway KT - WebSocket Implementation
- Check Redis Pub/Sub
- Verify CORS configuration

---

## 📝 Document Conventions

All KT documents follow this structure:

1. **Overview** - Purpose and responsibilities
2. **Architecture** - High-level design
3. **Project Structure** - File organization
4. **Core Components** - Detailed implementation
5. **Configuration** - Environment setup
6. **Setup & Deployment** - Installation guide
7. **Testing** - Test strategies
8. **Troubleshooting** - Common issues
9. **Best Practices** - Recommendations

---

## 🔄 Keeping Documentation Updated

### When to Update
- New features added
- Architecture changes
- Configuration changes
- Bug fixes that affect understanding
- Performance optimizations

### How to Update
1. Edit the relevant KT markdown file
2. Update version number and date at bottom
3. Update this index if new sections added
4. Commit with descriptive message

---

## 📞 Support & Resources

### Internal Resources
- **Slack**: #trading-system-dev
- **Wiki**: [Internal Wiki Link]
- **Code Review**: GitHub Pull Requests

### External Resources
- [Go Documentation](https://go.dev/doc/)
- [gRPC Go Tutorial](https://grpc.io/docs/languages/go/)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [Elasticsearch Guide](https://www.elastic.co/guide/)
- [Kafka Documentation](https://kafka.apache.org/documentation/)

---

## 📅 Document History

| Version | Date | Changes | Author |
|---------|------|---------|--------|
| 1.0 | 2025-12-12 | Initial KT documentation created | Backend Team |

---

## 🎓 Learning Path

### Week 1: Foundation
- Day 1-2: API Gateway KT
- Day 3-4: User Config Service KT
- Day 5: System architecture overview

### Week 2: Core Logic
- Day 1-3: Rules Engine Service KT
- Day 4: Data Ingestion Service KT
- Day 5: Integration testing

### Week 3: Trading Infrastructure
- Day 1-2: Risk Management Service KT
- Day 3-4: Trade Execution Service KT
- Day 5: Order flow testing

### Week 4: Integration & Authentication
- Day 1-2: Odin API Wrapper Service KT
- Day 3-4: User Login Service KT
- Day 5: End-to-end testing

---

## 🔗 Service Dependency Map

```
User Login Service → PostgreSQL (credentials, sessions)
                 ↓
           API Gateway → All gRPC Services
                 ↓
User Config Service → PostgreSQL (strategies)
                   → Kafka (strategy.updates)
                   → Elasticsearch (indexing via Rules Engine)
                 ↓
         Rules Engine → Elasticsearch (strategy search)
                     → Redis (caching)
                     → Kafka consumer (news-events)
                     → Kafka publisher (trade-signals)
                 ↓
  Data Ingestion → MongoDB (news events)
                → Kafka (news-events)
                 ↓
Trade Execution → Kafka consumer (trade-signals)
               → Risk Management (gRPC check)
               → PostgreSQL (orders)
               → RabbitMQ publisher (trade.executions)
               → RabbitMQ consumer (execution)
               → Odin API (order placement)
                 ↓
Risk Management → Redis (metrics, positions, limits)
               → gRPC (pre-trade checks)
                 ↓
Odin API Wrapper → RabbitMQ (trade.executions)
                → PostgreSQL (credentials)
                → Odin API (broker orders)
```

---

## 📋 Service Quick Reference

| Service | Port | Protocol | Database | Message Queue | Purpose |
|---------|------|----------|----------|---------------|---------|
| API Gateway | 8081 | REST/WebSocket | - | - | Frontend interface |
| User Config | 50051 | gRPC | PostgreSQL | Kafka (producer) | Strategy management |
| Rules Engine | 9003 | gRPC | Elasticsearch, Redis | Kafka (consumer/producer) | Event matching |
| Data Ingestion | 9001 | gRPC | MongoDB | Kafka (producer) | News event streaming |
| User Login | 8002 | REST | PostgreSQL | - | Authentication |
| Risk Management | 9005 | gRPC | Redis | - | Risk validation |
| Trade Execution | 9004 | gRPC | PostgreSQL | Kafka + RabbitMQ | Order processing |
| Odin API Wrapper | - | RabbitMQ | PostgreSQL | RabbitMQ (consumer) | Broker integration |

---

**Last Updated:** January 2025  
**Maintainer:** Development Team  
**Version:** 2.0

### Week 4: Advanced Topics
- Performance optimization
- Monitoring and observability
- Production deployment
- Troubleshooting exercises

---

**Last Updated:** December 12, 2025  
**Maintained by:** Backend Development Team  
**Next Review:** March 12, 2026
