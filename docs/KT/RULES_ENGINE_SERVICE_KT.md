# Rules Engine Service - Knowledge Transfer Document

## 📋 Table of Contents

1. [Overview](#overview)
2. [Architecture & Design](#architecture--design)
3. [Project Structure](#project-structure)
4. [Core Components](#core-components)
5. [Matching Algorithm](#matching-algorithm)
6. [Strategy Loading & Caching](#strategy-loading--caching)
7. [Redis Caching](#redis-caching)
8. [Message Processing](#message-processing)
9. [Configuration](#configuration)
10. [Setup & Deployment](#setup--deployment)
11. [Performance Optimization](#performance-optimization)
12. [Monitoring & Metrics](#monitoring--metrics)
13. [Troubleshooting](#troubleshooting)

---

## Overview

### Purpose
The Rules Engine Service is the **core intelligence** of the algorithmic trading system. It matches incoming market events (news, announcements) against thousands of user-defined trading strategies in real-time and generates trading signals when matches are found.

### Key Responsibilities
- **Real-time Event Processing**: Consume market events from Kafka
- **Strategy Matching**: Match events against 10,000+ user strategies using in-memory matching against cached strategies
- **Condition Evaluation**: Evaluate complex matching conditions with configurable weights
- **Order Generation**: Create order requests when strategies match
- **Signal Publishing**: Publish matched orders to RabbitMQ for execution
- **Performance Optimization**: High-throughput, low-latency processing

### Technology Stack
- **Language**: Go 1.23+
- **Message Queue**: Apache Kafka (input), RabbitMQ (output)
- **Cache**: Redis
- **Database**: PostgreSQL
- **RPC**: gRPC

### Performance Characteristics
- **Throughput**: 1000+ events/hour
- **Latency**: <100ms matching (p95)
- **Concurrency**: 50+ worker goroutines
- **Strategy Capacity**: 10,000+ active strategies

---

## Architecture & Design

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                  Rules Engine Service                        │
│                     (Port 50053)                             │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌──────────────┐    ┌────────────────┐    ┌────────────┐  │
│  │   Kafka      │───▶│  Event Handler │───▶│  Matcher   │  │
│  │  Consumer    │    │                │    │  Engine    │  │
│  └──────────────┘    └────────────────┘    └──────┬─────┘  │
│         ↑                                          │         │
│         │                                          ▼         │
│    news-events                            ┌────────────────┐│
│      topic                                │  In-Memory     ││
│                                           │  Strategy Match││
│                                           └────────┬───────┘│
│                                                    │         │
│                                                    ▼         │
│                                           ┌────────────────┐│
│                                           │  Strategy      ││
│                                           │  Cache (Redis) ││
│                                           └────────┬───────┘│
│                                                    │         │
│                                                    ▼         │
│                                           ┌────────────────┐│
│                                           │  Condition     ││
│                                           │  Evaluator     ││
│                                           └────────┬───────┘│
│                                                    │         │
│                                                    ▼         │
│                                           ┌────────────────┐│
│  ┌──────────────┐                        │  Match Scorer  ││
│  │  RabbitMQ    │◀───────────────────────┤                ││
│  │  Publisher   │    Order Requests      └────────────────┘│
│  └──────────────┘                                           │
│         │                                                    │
└─────────┼────────────────────────────────────────────────────┘
          │
          ▼
  trade.executions queue
          │
          ▼
  ┌──────────────────┐
  │  Trade Execution │
  │     Service      │
  └──────────────────┘
```

### Data Flow

```
1. Market Event (Kafka) → Rules Engine
2. Parse Event → Extract key fields
3. Load strategies from Redis cache (PostgreSQL fallback)
4. In-memory matching against cached strategies
5. For each matched strategy:
   a. Evaluate conditions (keywords, sentiment, impact score)
   b. Calculate match score
   c. If score >= threshold → Generate order
6. Publish Order to RabbitMQ
7. Update metrics and logs
```

### Component Interaction

```
┌─────────────┐
│ Kafka Event │
└──────┬──────┘
       │
       ▼
┌────────────────┐
│ Event Handler  │  ← Validates, parses event
└──────┬─────────┘
       │
       ▼
┌────────────────┐
│ Matcher Engine │  ← Core matching logic
└──────┬─────────┘
       │
       ├─→ Redis Cache    ← Fast strategy lookup
       │
       ├─→ PostgreSQL     ← Fallback, full strategy data
       │
       └─→ Evaluator      ← In-memory matching, score matches
              │
              ▼
       ┌──────────────┐
       │ RabbitMQ     │  ← Publish orders
       └──────────────┘
```

---

## Project Structure

```
services/rules-engine/
├── cmd/
│   └── main.go                          # Entry point
├── config/
│   └── config.go                        # Configuration loader
├── internal/
│   ├── consumer/
│   │   ├── consumer.go                  # Kafka consumer
│   │   └── handler.go                   # Event handler
│   ├── matcher/
│   │   ├── matcher.go                   # Core matching engine
│   │   ├── evaluator.go                 # Condition evaluator
│   │   └── scorer.go                    # Match scorer
│   ├── cache/
│   │   ├── redis_cache.go               # Redis cache
│   │   └── strategy_cache.go            # Strategy caching
│   ├── publisher/
│   │   ├── publisher.go                 # RabbitMQ publisher
│   │   └── circuit_breaker.go           # Circuit breaker
│   ├── repository/
│   │   └── strategy_repository.go       # Database access
│   ├── userconfig/
│   │   └── grpc_client.go               # User Config gRPC client
│   ├── sync/
│   │   └── strategy_sync.go             # Strategy synchronization
│   └── models/
│       ├── event.go                     # Event models
│       ├── strategy.go                  # Strategy models
│       └── match.go                     # Match result models
├── migrations/
│   └── 001_create_trade_signals.sql     # Database schema
├── .env                                 # Environment config
├── go.mod                               # Dependencies
└── README.md                            # Documentation
```

---

## Core Components

### 1. Main Application (`cmd/main.go`)

**Purpose:** Bootstrap and dependency injection.

```go
func main() {
    // 1. Load configuration
    cfg := config.Load()
    
    // 2. Initialize dependencies
    // Rules Engine no longer uses Elasticsearch.
    db := initPostgreSQL(cfg)
    redisClient := initRedis(cfg)
    rabbitMQ := initRabbitMQ(cfg)
    
    // 3. Create components
    cache := cache.NewStrategyCache(redisClient)
    repo := repository.NewStrategyRepository(db)
    publisher := publisher.NewRabbitMQPublisher(rabbitMQ)
    
    // 4. Create matcher
    matcher := matcher.NewMatcher(cache, repo, publisher)
    
    // 5. Create event handler
    handler := consumer.NewEventHandler(matcher)
    
    // 6. Start Kafka consumer
    kafkaConsumer := consumer.NewKafkaConsumer(cfg, handler)
    kafkaConsumer.Start()
    
    // 7. Start gRPC server (for health checks, admin)
    grpcServer := server.NewGRPCServer(matcher, cache)
    go grpcServer.Start(cfg.GRPCPort)
    
    // 8. Wait for shutdown
    <-shutdown
    gracefulShutdown(kafkaConsumer, grpcServer, db)
}
```

### 2. Kafka Consumer (`internal/consumer/consumer.go`)

**Purpose:** Consume market events from Kafka.

```go
type KafkaConsumer struct {
    consumer *kafka.Consumer
    handler  *EventHandler
    config   *config.Config
}

func (c *KafkaConsumer) Start() {
    // Subscribe to topic
    c.consumer.SubscribeTopics([]string{c.config.KafkaTopicNews}, nil)
    
    // Start worker pool
    workers := make(chan *kafka.Message, c.config.WorkerPoolSize)
    for i := 0; i < c.config.WorkerPoolSize; i++ {
        go c.worker(workers)
    }
    
    // Consume messages
    for {
        msg, err := c.consumer.ReadMessage(-1)
        if err != nil {
            log.Printf("Consumer error: %v", err)
            continue
        }
        
        workers <- msg
    }
}

func (c *KafkaConsumer) worker(messages <-chan *kafka.Message) {
    for msg := range messages {
        // Process message
        if err := c.handler.HandleEvent(msg.Value); err != nil {
            log.Printf("Handler error: %v", err)
            continue
        }
        
        // Commit offset
        c.consumer.CommitMessage(msg)
    }
}
```

### 3. Event Handler (`internal/consumer/handler.go`)

**Purpose:** Parse and validate incoming events.

```go
type EventHandler struct {
    matcher *matcher.Matcher
}

func (h *EventHandler) HandleEvent(data []byte) error {
    // 1. Parse event
    var event models.NewsEvent
    if err := json.Unmarshal(data, &event); err != nil {
        return fmt.Errorf("failed to parse event: %w", err)
    }
    
    // 2. Validate event
    if err := h.validateEvent(&event); err != nil {
        return fmt.Errorf("invalid event: %w", err)
    }
    
    // 3. Extract Extended JSON MongoDB document
    extJSON := event.FullDocument
    if extJSON == nil {
        return errors.New("missing full_document")
    }
    
    // 4. Send to matcher
    matches, err := h.matcher.MatchEvent(extJSON)
    if err != nil {
        return fmt.Errorf("matching error: %w", err)
    }
    
    log.Printf("Event matched %d strategies", len(matches))
    return nil
}

func (h *EventHandler) validateEvent(event *models.NewsEvent) error {
    if event.Title == "" {
        return errors.New("missing title")
    }
    if event.Timestamp.IsZero() {
        return errors.New("missing timestamp")
    }
    return nil
}
```

### 4. Matcher Engine (`internal/matcher/matcher.go`)

**Purpose:** Core matching logic.

```go
type Matcher struct {
    cache     *cache.StrategyCache
    repo      *repository.StrategyRepository
    publisher *publisher.RabbitMQPublisher
    evaluator *Evaluator
}

func (m *Matcher) MatchEvent(event map[string]interface{}) ([]models.Match, error) {
    // 1. Extract key fields from event
    keywords := extractKeywords(event)
    stockCodes := extractStockCodes(event)
    sentiment := extractSentiment(event)
    impactScore := extractImpactScore(event)
    
    // 2. Load all active strategies from cache (PostgreSQL fallback)
    strategies := m.loadAllActiveStrategies()
    
    log.Printf("Loaded %d active strategies for in-memory matching", len(strategies))
    
    // 3. Evaluate each strategy via in-memory matching
    var matches []models.Match
    for _, strategy := range strategies {
        score := m.evaluator.Evaluate(strategy, event)
        
        // 4. Check if match threshold met
        if score >= m.config.MatchThreshold {
            match := models.Match{
                StrategyID:  strategy.StrategyID,
                UserID:      strategy.UserID,
                Event:       event,
                Score:       score,
                MatchedAt:   time.Now(),
            }
            
            matches = append(matches, match)
            
            // 5. Generate and publish order
            m.publishOrder(strategy, event, match)
        }
    }
    
    return matches, nil
}

func (m *Matcher) loadStrategies(strategyIDs []string) []*models.Strategy {
    var strategies []*models.Strategy
    
    for _, id := range strategyIDs {
        // Try cache first
        strategy, err := m.cache.Get(id)
        if err == nil {
            strategies = append(strategies, strategy)
            continue
        }
        
        // Fallback to database
        strategy, err = m.repo.GetByID(context.Background(), id)
        if err != nil {
            log.Printf("Failed to load strategy %s: %v", id, err)
            continue
        }
        
        // Update cache
        m.cache.Set(id, strategy, 1*time.Hour)
        strategies = append(strategies, strategy)
    }
    
    return strategies
}
```

### 5. Condition Evaluator (`internal/matcher/evaluator.go`)

**Purpose:** Evaluate matching conditions and calculate scores.

```go
type Evaluator struct {
    weights MatchWeights
}

type MatchWeights struct {
    KeywordMatch    float64  // 0.3
    SentimentMatch  float64  // 0.2
    ImpactScore     float64  // 0.3
    StockCodeMatch  float64  // 0.2
}

func (e *Evaluator) Evaluate(strategy *models.Strategy, event map[string]interface{}) float64 {
    var score float64
    
    // 1. Keyword matching
    keywordScore := e.evaluateKeywords(strategy.NewsConfig.Keywords, event)
    score += keywordScore * e.weights.KeywordMatch
    
    // 2. Sentiment matching
    sentimentScore := e.evaluateSentiment(strategy.NewsConfig.Sentiment, event)
    score += sentimentScore * e.weights.SentimentMatch
    
    // 3. Impact score matching
    impactScoreMatch := e.evaluateImpactScore(strategy.NewsConfig.MinImpactScore, event)
    score += impactScoreMatch * e.weights.ImpactScore
    
    // 4. Stock code matching
    stockCodeScore := e.evaluateStockCodes(strategy.NewsConfig.StockCodes, event)
    score += stockCodeScore * e.weights.StockCodeMatch
    
    return score
}

func (e *Evaluator) evaluateKeywords(keywords []string, event map[string]interface{}) float64 {
    title, _ := event["title"].(string)
    content, _ := event["content"].(string)
    combined := strings.ToLower(title + " " + content)
    
    matchedCount := 0
    for _, keyword := range keywords {
        if strings.Contains(combined, strings.ToLower(keyword)) {
            matchedCount++
        }
    }
    
    if len(keywords) == 0 {
        return 0
    }
    
    return float64(matchedCount) / float64(len(keywords))
}

func (e *Evaluator) evaluateSentiment(expectedSentiment string, event map[string]interface{}) float64 {
    actualSentiment, _ := event["sentiment"].(string)
    
    if strings.ToUpper(expectedSentiment) == strings.ToUpper(actualSentiment) {
        return 1.0
    }
    return 0.0
}

func (e *Evaluator) evaluateImpactScore(minScore float64, event map[string]interface{}) float64 {
    impactScore, _ := event["impact_score"].(float64)
    
    if impactScore >= minScore {
        // Normalize to 0-1 range
        return (impactScore - minScore) / (10.0 - minScore)
    }
    return 0.0
}
```

### 6. Strategy Loading from PostgreSQL

**Note:** The Rules Engine no longer uses Elasticsearch. Strategies are loaded from PostgreSQL and cached in Redis. Matching is performed in-memory against cached strategies.

```go
func (m *Matcher) loadAllActiveStrategies() []*models.Strategy {
    // Try loading from Redis cache first
    strategies, err := m.cache.GetAllActive()
    if err == nil && len(strategies) > 0 {
        return strategies
    }
    
    // Fallback: load from PostgreSQL
    strategies, err = m.repo.GetAllActive(context.Background())
    if err != nil {
        log.Printf("Failed to load strategies from DB: %v", err)
        return nil
    }
    
    // Warm cache with loaded strategies
    for _, s := range strategies {
        m.cache.Set(s.StrategyID, s, 1*time.Hour)
    }
    
    return strategies
}
```

### 7. Redis Cache (`internal/cache/strategy_cache.go`)

**Purpose:** Cache frequently accessed strategies.

```go
type StrategyCache struct {
    client *redis.Client
    ttl    time.Duration
}

func (c *StrategyCache) Get(strategyID string) (*models.Strategy, error) {
    key := fmt.Sprintf("strategy:%s", strategyID)
    
    data, err := c.client.Get(context.Background(), key).Bytes()
    if err != nil {
        return nil, err
    }
    
    var strategy models.Strategy
    if err := json.Unmarshal(data, &strategy); err != nil {
        return nil, err
    }
    
    return &strategy, nil
}

func (c *StrategyCache) Set(strategyID string, strategy *models.Strategy, ttl time.Duration) error {
    key := fmt.Sprintf("strategy:%s", strategyID)
    
    data, err := json.Marshal(strategy)
    if err != nil {
        return err
    }
    
    return c.client.Set(context.Background(), key, data, ttl).Err()
}

func (c *StrategyCache) Delete(strategyID string) error {
    key := fmt.Sprintf("strategy:%s", strategyID)
    return c.client.Del(context.Background(), key).Err()
}

func (c *StrategyCache) Clear() error {
    // Clear all strategy keys
    keys, err := c.client.Keys(context.Background(), "strategy:*").Result()
    if err != nil {
        return err
    }
    
    if len(keys) > 0 {
        return c.client.Del(context.Background(), keys...).Err()
    }
    
    return nil
}
```

### 8. RabbitMQ Publisher (`internal/publisher/publisher.go`)

**Purpose:** Publish order requests to RabbitMQ.

```go
type RabbitMQPublisher struct {
    conn          *amqp.Connection
    channel       *amqp.Channel
    exchange      string
    routingKey    string
    circuitBreaker *CircuitBreaker
}

func (p *RabbitMQPublisher) PublishOrder(order *models.OrderRequest) error {
    // Check circuit breaker
    if !p.circuitBreaker.Allow() {
        return errors.New("circuit breaker open")
    }
    
    // Marshal order
    orderJSON, err := json.Marshal(order)
    if err != nil {
        return err
    }
    
    // Publish message
    err = p.channel.Publish(
        p.exchange,    // exchange
        p.routingKey,  // routing key
        false,         // mandatory
        false,         // immediate
        amqp.Publishing{
            ContentType:  "application/json",
            DeliveryMode: amqp.Persistent,
            Body:         orderJSON,
            Headers: amqp.Table{
                "user_id":     order.UserID,
                "strategy_id": order.StrategyID,
                "event_id":    order.EventID,
            },
        },
    )
    
    // Update circuit breaker
    if err != nil {
        p.circuitBreaker.RecordFailure()
        return err
    }
    
    p.circuitBreaker.RecordSuccess()
    return nil
}
```

---

## Matching Algorithm

### Algorithm Steps

1. **Event Ingestion**
   - Receive event from Kafka
   - Parse Extended JSON document
   - Extract key fields (title, keywords, sentiment, impact_score, stock_codes)

2. **Strategy Loading (PostgreSQL + Redis)**
   - Load all active strategies from Redis cache
   - If not in cache, load from PostgreSQL and warm cache
   - Strategies are matched in-memory against event fields

4. **Detailed Evaluation**
   - For each candidate strategy:
     - Calculate keyword match score
     - Calculate sentiment match score
     - Calculate impact score match
     - Calculate stock code match score
     - Weighted sum = total score

5. **Threshold Check**
   - If total score >= threshold (e.g., 0.7)
   - Generate order request

6. **Order Publishing**
   - Create OrderRequest message
   - Publish to RabbitMQ
   - Publish match event to Redis (for WebSocket)

### Scoring Example

```
Strategy: "Apple Earnings Play"
- Keywords: ["Apple", "AAPL", "earnings"]
- Sentiment: POSITIVE
- Min Impact Score: 7
- Stock Codes: [2885]

Event: "Apple reports record Q4 earnings, stock surges"
- Title/Content contains: Apple, earnings
- Sentiment: POSITIVE
- Impact Score: 9
- Stock Codes: [2885]

Calculation:
- Keyword Match: 2/3 = 0.67 → 0.67 * 0.3 = 0.20
- Sentiment Match: 1.0 → 1.0 * 0.2 = 0.20
- Impact Score: (9-7)/(10-7) = 0.67 → 0.67 * 0.3 = 0.20
- Stock Code Match: 1.0 → 1.0 * 0.2 = 0.20

Total Score: 0.20 + 0.20 + 0.20 + 0.20 = 0.80

Result: MATCH (score 0.80 >= threshold 0.70)
```

---

## Configuration

### Environment Variables

```bash
# Service Configuration
SERVICE_NAME=rules-engine
SERVICE_VERSION=1.0.0
GRPC_PORT=50053
ENVIRONMENT=production

# Kafka Consumer
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_GROUP_ID=rules-engine-group
KAFKA_TOPIC_NEWS=news-events
KAFKA_AUTO_OFFSET_RESET=earliest

# Redis Cache
REDIS_ADDR=localhost:6379
REDIS_PASSWORD=
REDIS_DB=0
REDIS_CACHE_TTL=3600

# RabbitMQ Publisher
RABBITMQ_URL=amqp://guest:guest@localhost:5672/
RABBITMQ_EXCHANGE=trade.execution
RABBITMQ_ROUTING_KEY=order.new
RABBITMQ_QUEUE=trade.executions

# PostgreSQL
DATABASE_URL=postgresql://trading_user:postgres@localhost:5432/trading_system

# Performance
WORKER_POOL_SIZE=50
MATCH_THRESHOLD=0.70
BATCH_SIZE=100

# Logging
LOG_LEVEL=INFO
LOG_FORMAT=json
```

---

## Setup & Deployment

### Prerequisites

```bash
# 1. Kafka
docker run -d --name kafka -p 9092:9092 apache/kafka:latest

# 2. Redis
docker run -d --name redis -p 6379:6379 redis:latest

# 3. RabbitMQ
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management

# 4. PostgreSQL
docker run -d --name postgres -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres:15
```

### Running the Service

```bash
# 1. Navigate to service
cd services/rules-engine

# 2. Install dependencies
go mod download

# 3. Setup database
psql -d trading_system -f migrations/001_create_trade_signals.sql

# 4. Configure environment
cp .env.example .env
# Edit .env

# 5. Build and run
go build -o bin/rules-engine cmd/main.go
./bin/rules-engine
```

---

## Performance Optimization

### 1. PostgreSQL + In-Memory Optimization
- Load strategies from PostgreSQL on startup and cache in Redis
- Perform in-memory matching against cached strategies
- Use periodic sync to keep cache up to date

### 2. Redis Caching
- Cache hot strategies (frequently matched)
- Use TTL to prevent stale data
- Implement cache warming

### 3. Worker Pool
- Adjust pool size based on CPU cores
- Monitor queue depth
- Use bounded channels

### 4. Circuit Breaker
- Prevent cascade failures
- Auto-recovery after timeout
- Monitor failure rates

---

## Monitoring & Metrics

### Key Metrics

```go
type Metrics struct {
    EventsProcessed     prometheus.Counter
    EventsMatched       prometheus.Counter
    MatchingLatency     prometheus.Histogram
    StrategyLoadErrors  prometheus.Counter
    CacheHitRate        prometheus.Gauge
    ActiveWorkers       prometheus.Gauge
}
```

### Health Checks

```bash
# gRPC health check
grpcurl -plaintext localhost:50053 grpc.health.v1.Health/Check

# Metrics endpoint
curl http://localhost:50053/metrics
```

---

## Troubleshooting

### Common Issues

#### 1. High Latency
- Check PostgreSQL query performance
- Verify Redis cache hit rate
- Review worker pool size

#### 2. Memory Issues
- Reduce cache TTL
- Lower worker pool size
- Check for memory leaks

#### 3. Kafka Lag
- Increase worker pool
- Optimize matching logic
- Scale horizontally

---

**Last Updated:** December 12, 2025  
**Version:** 1.0  
**Maintained by:** Backend Development Team
