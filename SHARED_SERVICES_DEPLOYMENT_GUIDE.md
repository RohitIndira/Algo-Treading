# Shared vs Custom Services Deployment Guide
## For Multi-Algo Team Collaboration

---

## 📊 Executive Summary

Your system has **5 core microservices**. When sharing with another algo team using different strategies:

- **Services to SHARE** (Build once, use by all): `3 services`
- **Services to CUSTOMIZE** (Each team modifies): `2 services`

---

## 🔍 Service Classification

### 1️⃣ **DATA INGESTION SERVICE** ✅ SHARED
**Status**: Universal Infrastructure Service

#### What It Does:
- Watches MongoDB for new stock news/events
- Publishes to Kafka `news-events` topic
- Real-time data source for the entire system

#### Why Share It:
- **Independent of strategy**: Works with ANY trading strategy
- **Universal data feed**: Both teams consume same market data from MongoDB
- **No business logic**: Pure data pipeline with no algorithmic logic
- **One source of truth**: Ensures both teams get identical market events

#### Customization Needed: ❌ NONE
- Just deploy binary, no code changes needed

#### How to Share:
```bash
# Build once
cd services/data-ingestion
go build -o data-ingestion-service ./cmd/main.go

# Both teams use same binary
docker build -f Dockerfile -t data-ingestion:v1.0.0 .
# Share with other team or push to registry
```

---

### 2️⃣ **RULES ENGINE SERVICE** ❌ CUSTOMIZE
**Status**: Strategy-Specific Intelligence Engine

#### What It Does:
- Consumes market events from Kafka
- Matches events against user-defined trading strategies
- Evaluates complex conditions with rule weights
- Publishes matched orders to RabbitMQ

#### Why Each Team Must Customize:
- **Strategy-dependent**: Matching logic tied to YOUR specific strategy rules
- **Different match scoring**: Your team's strategy may have different:
  - Condition weights
  - Stock filtering logic
  - Price/volume filters
  - Sentiment analysis weights
  - Market timing conditions
- **Different event sources**: Other team might use live market price events (not just news)

#### Customization Areas:
```
/internal/matcher/
  - evaluator.go         [CUSTOMIZE] Score calculation logic
  - condition_evaluator  [CUSTOMIZE] How conditions are evaluated
  - matcher.go           [REVIEW] Matching orchestration

/internal/index/
  - query_engine.go      [REVIEW] Elasticsearch queries (mostly ok)

/config/config.go        [CUSTOMIZE] Strategy matching parameters
```

#### Code Location to Give:
```bash
services/rules-engine/
├── cmd/                 # Give full code
├── internal/matcher/    # Give with customization guide
├── internal/
│   ├── cache/          # Give (minimal customization)
│   ├── consumer/       # Give (can reuse)
│   ├── publisher/      # Give (can reuse)
│   └── index/          # Give (may need tweaks)
├── config/             # Give (needs customization)
└── migrations/         # Give
```

---

### 3️⃣ **RISK MANAGEMENT SERVICE** ✅ SHARED
**Status**: Universal Risk Control Layer

#### What It Does:
- Pre-trade risk validation (before order execution)
- Post-trade metrics tracking
- Risk limit enforcement
- Position monitoring

#### Why Share It:
- **Framework-agnostic**: Validates based on limits, not strategy type
- **User-level controls**: Risk limits defined per user, independent of algo
- **Safety mechanism**: Both teams benefit from same risk guardrails
- **Multi-strategy ready**: Supports multiple strategies per user seamlessly

#### How It Works:
```
Team A Algo Strategy → Order Request
                          ↓
                    Risk Management Service
                    (checks Redis counters)
                          ↓
                    Risk Check: Pass/Fail
                          ↓
                    Updates daily P&L, position count
```

Both teams use the same Redis-backed risk tracking.

#### Customization Needed: ❌ NONE (unless you have custom risk rules)

#### How to Share:
```bash
# Build once
cd services/risk-management
go build -o risk-management-service ./cmd/main.go

docker build -f Dockerfile -t risk-management:v1.0.0 .
# Share binary, no code customization
```

---

### 4️⃣ **TRADE EXECUTION SERVICE** ❌ CUSTOMIZE
**Status**: Execution Handler (May Vary)

#### What It Does:
- Consumes orders from RabbitMQ
- Executes trades via Odin API (broker)
- Updates PostgreSQL order records
- Handles order retries and failures

#### Why You May Need Different Versions:
- **Order placement strategy**: 
  - Team A (news-based): Market orders, immediate execution
  - Team B (price-based): Limit orders with specific price levels
- **Execution timing**:
  - Market conditions (open, close, pre-market)
  - Order timing preferences
- **Broker API usage**: Different order types, parameters

#### Customization Areas:
```
/internal/executor/
  - executor.go           [REVIEW] Core execution logic
  - order_executor.go     [CUSTOMIZE] How orders are placed with Odin API
  - retry_logic.go        [CUSTOMIZE] Retry strategy

/internal/models/
  - trade_order.go        [MAYBE] If order structure differs
```

#### Decision: 
**Option A**: Share base code, each team customizes `executor.go`
**Option B**: Share compiled binary if both teams use same execution params

---

### 5️⃣ **USER CONFIG SERVICE** ✅ SHARED (but need multi-team setup)
**Status**: Strategy Configuration Manager

#### What It Does:
- CRUD operations for trading strategies
- Validates strategy schema
- Stores in PostgreSQL
- Publishes strategy changes to Kafka

#### Why Share It:
- **Strategy storage layer**: Works for ANY strategy schema
- **Multi-user support**: Already handles 10K+ users
- **Can support multi-algo**: Different users can use different strategies

#### Customization Needed: ⚠️ MAYBE (Schema Update)

**If both teams' strategies have different field structure:**
- Team A (news-based): `sentiment_threshold`, `impact_score_min`
- Team B (price-based): `price_target`, `atr_multiplier`

Then you need to:
1. Update PostgreSQL schema to support flexible JSON columns
2. Update validation logic in `config/validation.go`

**If they use same strategy structure**: No changes needed

#### How to Share:
```bash
# Option 1: Share as-is if same strategy schema
cd services/user-config
docker build -f Dockerfile -t user-config:v1.0.0 .

# Option 2: Add flexible schema support first (recommended)
# Modify: services/user-config/internal/models/strategy.go
# Add: strategies JSON field instead of fixed columns
```

---

## 📦 Recommended Deployment Architecture

```
┌─────────────────────────────────────────────────────────┐
│           SHARED INFRASTRUCTURE (Deploy Once)           │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ✅ Data Ingestion Service (v1.0.0)                    │
│  ✅ Risk Management Service (v1.0.0)                   │
│  ✅ User Config Service (v1.0.0 + flexible schema)     │
│                                                         │
│  Infrastructure:                                        │
│  - MongoDB (news events - shared)                       │
│  - Kafka (shared topics)                                │
│  - Redis (risk metrics - shared namespace)              │
│  - PostgreSQL (shared, separate strategy tables)        │
│                                                         │
└─────────────────────────────────────────────────────────┘

┌──────────────────────┐  ┌──────────────────────┐
│   TEAM A (Your Algo) │  │  TEAM B (Price Algo) │
├──────────────────────┤  ├──────────────────────┤
│                      │  │                      │
│ Rules Engine v1.0    │  │ Rules Engine v2.0    │
│ (news-based logic)   │  │ (price-based logic)  │
│                      │  │                      │
│ Trade Execution v1.0 │  │ Trade Execution v2.0 │
│ (order strategy A)   │  │ (order strategy B)   │
│                      │  │                      │
│ Docker: team-a-       │  │ Docker: team-b-      │
│ rules-engine:v1.0     │  │ rules-engine:v2.0    │
│ team-a-               │  │ team-b-              │
│ trade-exec:v1.0       │  │ trade-exec:v2.0      │
│                      │  │                      │
└──────────────────────┘  └──────────────────────┘
```

---

## 🚀 How to Give Source Code to Other Team

### **For SHARED Services** (3 services):
Give them **COMPILED BINARIES** + Docker images only:

```bash
# Build all shared services
cd services/data-ingestion && docker build -t data-ingestion:v1.0.0 .
cd services/risk-management && docker build -t risk-management:v1.0.0 .
cd services/user-config && docker build -t user-config:v1.0.0 .

# Export images
docker save -o shared-services.tar \
  data-ingestion:v1.0.0 \
  risk-management:v1.0.0 \
  user-config:v1.0.0

# Share: shared-services.tar + docker-compose with these services pre-configured
```

### **For CUSTOM Services** (2 services):
Give them **FULL SOURCE CODE** + build instructions:

```bash
# Create source code bundle
mkdir team-b-services
cp -r services/rules-engine team-b-services/
cp -r services/trade-execution team-b-services/
cp go.mod go.sum team-b-services/

# Add customization guide
cat > team-b-services/CUSTOMIZATION_GUIDE.md << 'EOF'
# Customization Guide for Team B

## Rules Engine Customization
File: services/rules-engine/internal/matcher/evaluator.go

This file contains the strategy matching logic. Customize:
1. scoreStrategy() - Adjust weights for your price-based algo
2. evaluateConditions() - Add price/volume specific conditions
3. Rebuild: go build -o rules-engine ./cmd/main.go

## Trade Execution Customization  
File: services/trade-execution/internal/executor/executor.go

Customize for your order placement strategy:
1. placeOrder() - Use limit orders vs market orders
2. preOrderValidation() - Add your execution checks
3. Rebuild: go build -o trade-execution ./cmd/main.go

## Testing
Run: go test ./...

## Docker Build
docker build -f Dockerfile -t team-b-rules-engine:v2.0.0 .
docker build -f Dockerfile -t team-b-trade-execution:v2.0.0 .
EOF

# Share the bundle
tar -czf team-b-source-code.tar.gz team-b-services/
```

---

## 📋 Files to Share - Detailed Breakdown

### **SHARED SERVICES - Binary Only**

```
data-ingestion/
├── Dockerfile                ✅ Share for binary
├── docker-compose.yml        ✅ Share (for reference)
├── .env.example             ✅ Share (config template)
└── (source code)            ❌ Do NOT share (if keeping as binary service)

risk-management/
├── Dockerfile                ✅ Share for binary
├── .env.example             ✅ Share
└── (source code)            ❌ Do NOT share

user-config/
├── Dockerfile                ✅ Share for binary
├── .env.example             ✅ Share
├── migrations/               ✅ Share (DB schema)
└── (source code)            ❌ Do NOT share
```

### **CUSTOM SERVICES - Full Source**

```
rules-engine/
├── ✅ All source code (cmd/, internal/)
├── ✅ config/config.go (for customization)
├── ✅ go.mod, go.sum
├── ✅ migrations/ (if any)
├── ✅ Dockerfile (for their builds)
├── ✅ README.md
└── ✅ .env.example

trade-execution/
├── ✅ All source code (cmd/, internal/)
├── ✅ config/config.go
├── ✅ go.mod, go.sum
├── ✅ migrations/
├── ✅ Dockerfile
├── ✅ README.md
└── ✅ .env.example
```

### **SHARED UTILITIES - Source Code**

```
api/gateway/                   ✅ Share (REST API layer)
api/proto/                     ✅ Share (gRPC definitions)
pkg/                          ✅ Share (shared libraries)
  ├── database/               ✅ 
  ├── logger/                 ✅
  ├── metrics/                ✅
  ├── kafka/                  ✅
  ├── rabbitmq/               ✅
  └── indira/                 ✅
internal/models/              ✅ Share (data structures)
configs/                       ✅ Share (config templates)
```

---

## 🔧 Deployment Strategy

### **Step 1: Setup Shared Infrastructure (One-time)**
```bash
# Both teams use same shared services deployed once
docker-compose up -d data-ingestion risk-management user-config
docker-compose up -d postgres redis kafka rabbitmq elasticsearch mongodb
```

### **Step 2: Team A Deploys Their Services**
```bash
docker-compose up -d team-a-rules-engine team-a-trade-execution
```

### **Step 3: Team B Customizes & Deploys**
```bash
# They modify source code
cd services/rules-engine
# Edit: internal/matcher/evaluator.go for price-based logic
go build -o rules-engine-team-b ./cmd/main.go

# Build & deploy
docker build -t team-b-rules-engine:v1.0.0 .
docker-compose up -d team-b-rules-engine team-b-trade-execution
```

---

## 📊 Data Flow - Both Teams

```
News Event in MongoDB
        ↓
   [SHARED] Data Ingestion Service
        ↓
   Kafka: news-events topic
        ↓
   ┌─────────────────────────┐
   │ Team A Rules Engine     │  Team A: Matches if sentiment > 0.7
   │ (custom evaluator)      │
   └─────────────────────────┘
        ↓
   RabbitMQ: order.execution.queue
        ↓
   [SHARED] Risk Management
        ↓ (if risk check passes)
   ┌─────────────────────────┐
   │ Team A Trade Executor   │  Team A: Market order immediately
   └─────────────────────────┘
        ↓
   Odin API → Exchange → Execution


Live Price Update in Kafka
        ↓
   ┌─────────────────────────┐
   │ Team B Rules Engine     │  Team B: Matches if price > MA(20) + 2*ATR
   │ (custom evaluator)      │
   └─────────────────────────┘
        ↓
   RabbitMQ: order.execution.queue
        ↓
   [SHARED] Risk Management
        ↓ (if risk check passes)
   ┌─────────────────────────┐
   │ Team B Trade Executor   │  Team B: Limit order with TP/SL
   └─────────────────────────┘
        ↓
   Odin API → Exchange → Execution
```

---

## 📝 Summary Table

| Service | Share? | What to Give | Customization | Location |
|---------|--------|--------------|---------------|----------|
| **Data Ingestion** | ✅ YES | Binary + Docker | None | 1 Instance |
| **Risk Management** | ✅ YES | Binary + Docker | None* | 1 Instance |
| **User Config** | ✅ YES | Binary + Docker | Maybe† | 1 Instance |
| **Rules Engine** | ❌ NO | Full Source | YES | Per Team |
| **Trade Execution** | ❌ NO | Full Source | YES | Per Team |

- *None: Unless you have custom risk rules beyond what's implemented
- †Maybe: Only if strategies have different JSON schemas

---

## 🎯 Next Steps

1. **Decide on shared infrastructure**: Single deployment or separate?
2. **Update User Config Service**: Ensure it supports multiple strategy schemas
3. **Document customization guide**: What Team B should change in Rules Engine & Trade Execution
4. **Build shared binaries**: One-time build of the 3 shared services
5. **Create docker-compose**: Single file that spins up everything
6. **Share code repository**: Give Team B access to full repo, they modify only 2 services

---

## 📞 Quick Reference for Team B

**Files they MUST customize:**
1. `services/rules-engine/internal/matcher/evaluator.go` - Matching logic
2. `services/trade-execution/internal/executor/executor.go` - Order placement

**Files they DON'T touch:**
1. Shared service binaries (risk-management, data-ingestion, user-config)
2. `pkg/` shared libraries (unless bug fixes)
3. `api/proto/` (unless adding new gRPC methods)

**Setup commands:**
```bash
# Clone or receive code
git clone <your-repo>

# Install shared services
docker-compose up -d data-ingestion risk-management user-config

# Customize their services
cd services/rules-engine
# Edit evaluator.go
go build -o rules-engine-v2 ./cmd/main.go

# Deploy
docker build -t team-b-rules-engine:v2.0.0 .
docker-compose up -d team-b-rules-engine team-b-trade-execution
```

