# Data Ingestion Service - Knowledge Transfer Document

## 📋 Table of Contents

1. [Overview](#overview)
2. [Architecture & Design](#architecture--design)
3. [Project Structure](#project-structure)
4. [Core Components](#core-components)
5. [MongoDB Change Streams](#mongodb-change-streams)
6. [Kafka Integration](#kafka-integration)
7. [Configuration](#configuration)
8. [Setup & Deployment](#setup--deployment)
9. [Monitoring](#monitoring)
10. [Troubleshooting](#troubleshooting)

---

## Overview

### Purpose
The Data Ingestion Service is the **entry point** for all market data into the trading system. It watches an external MongoDB collection for new stock news/events and publishes them to Kafka in real-time, triggering the entire trading pipeline.

### Key Responsibilities
- **MongoDB Monitoring**: Watch MongoDB collection using Change Streams
- **Real-time Detection**: Detect new document insertions instantly
- **Data Transformation**: Transform MongoDB documents to standardized format
- **Kafka Publishing**: Publish events to Kafka `news-events` topic
- **Pipeline Trigger**: Initiate the trading workflow

### Technology Stack
- **Language**: Go 1.23+
- **Database**: MongoDB (Change Streams)
- **Message Queue**: Apache Kafka
- **Serialization**: Extended JSON (MongoDB format)

---

## Architecture & Design

### High-Level Architecture

```
┌──────────────────────────────────────────────────────────┐
│           External MongoDB (StockGPT)                     │
│         NewsImpactDashboard Collection                    │
└────────────────────┬─────────────────────────────────────┘
                     │ Change Stream
                     ▼
┌──────────────────────────────────────────────────────────┐
│          Data Ingestion Service (Port 9001)               │
├──────────────────────────────────────────────────────────┤
│                                                           │
│  ┌──────────────┐    ┌────────────────┐    ┌─────────┐  │
│  │   MongoDB    │───▶│   Watcher      │───▶│  Kafka  │  │
│  │   Change     │    │   Handler      │    │Producer │  │
│  │   Stream     │    │                │    │         │  │
│  └──────────────┘    └────────────────┘    └────┬────┘  │
│         ↑                    │                    │       │
│         │                    ▼                    │       │
│    News Inserts      Transform to JSON           │       │
│                                                   │       │
└───────────────────────────────────────────────────┼───────┘
                                                    │
                                                    ▼
                                            ┌───────────────┐
                                            │ Kafka Topic:  │
                                            │ news-events   │
                                            └───────┬───────┘
                                                    │
                                                    ▼
                                            ┌───────────────┐
                                            │ Rules Engine  │
                                            │  (Consumer)   │
                                            └───────────────┘
```

### Data Flow

```
1. News Article → MongoDB Insert
2. Change Stream → Detect Insert
3. Extract Document → Full document from change event
4. Transform → Convert to Extended JSON
5. Publish → Send to Kafka topic
6. Rules Engine → Consumes and processes
```

### Event Flow Sequence

```
┌─────────────┐
│ News Source │ (External system)
└──────┬──────┘
       │
       ▼
┌──────────────────┐
│ MongoDB Insert   │ CAG_CHATBOT.NewsImpactDashboard
└──────┬───────────┘
       │
       ▼
┌──────────────────┐
│ Change Stream    │ Real-time notification
│ Event Triggered  │
└──────┬───────────┘
       │
       ▼
┌──────────────────┐
│ Watcher Receives │ fullDocument field extracted
│ Change Event     │
└──────┬───────────┘
       │
       ▼
┌──────────────────┐
│ Transform to     │ Convert BSON to Extended JSON
│ Extended JSON    │
└──────┬───────────┘
       │
       ▼
┌──────────────────┐
│ Publish to Kafka │ Topic: news-events
└──────┬───────────┘
       │
       ▼
┌──────────────────┐
│ Rules Engine     │ Matches against strategies
│ Processes Event  │
└──────────────────┘
```

---

## Project Structure

```
services/data-ingestion/
├── cmd/
│   └── main.go                    # Application entry point
├── config/
│   └── config.go                  # Configuration management
├── internal/
│   ├── watcher/
│   │   └── mongo_watcher.go       # MongoDB Change Stream watcher
│   ├── publisher/
│   │   └── publisher.go           # Kafka publisher interface
│   └── models/
│       ├── event.go               # Event models
│       └── news.go                # News document models
├── .env                           # Environment configuration
├── .env.example                   # Example configuration
├── go.mod                         # Go module dependencies
├── go.sum                         # Dependency checksums
├── build.sh                       # Build script
├── run.sh                         # Run script
└── README.md                      # Service documentation
```

---

## Core Components

### 1. Main Application (`cmd/main.go`)

**Purpose:** Application bootstrap and lifecycle management.

```go
package main

import (
    "context"
    "log"
    "os"
    "os/signal"
    "syscall"
    
    "github.com/RohitIndira/Algo-Treading/services/data-ingestion/config"
    "github.com/RohitIndira/Algo-Treading/services/data-ingestion/internal/publisher"
    "github.com/RohitIndira/Algo-Treading/services/data-ingestion/internal/watcher"
)

func main() {
    log.Println("=== Data Ingestion Service ===")
    
    // 1. Load configuration
    cfg, err := config.Load()
    if err != nil {
        log.Fatalf("Failed to load config: %v", err)
    }
    
    // 2. Initialize Kafka producer
    kafkaProducer, err := publisher.NewKafkaPublisher(cfg)
    if err != nil {
        log.Fatalf("Failed to create Kafka producer: %v", err)
    }
    defer kafkaProducer.Close()
    
    // 3. Initialize MongoDB watcher
    mongoWatcher, err := watcher.NewMongoWatcher(cfg, kafkaProducer)
    if err != nil {
        log.Fatalf("Failed to create MongoDB watcher: %v", err)
    }
    defer mongoWatcher.Close()
    
    // 4. Start watching
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    
    go func() {
        if err := mongoWatcher.Watch(ctx); err != nil {
            log.Fatalf("Watcher error: %v", err)
        }
    }()
    
    log.Println("✓ Data Ingestion Service started")
    log.Println("✓ Watching MongoDB for new events...")
    
    // 5. Wait for shutdown signal
    quit := make(chan os.Signal, 1)
    signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
    <-quit
    
    log.Println("Shutting down gracefully...")
    cancel()
}
```

### 2. Configuration (`config/config.go`)

**Purpose:** Centralized configuration management.

```go
package config

import (
    "os"
    "github.com/joho/godotenv"
)

type Config struct {
    // MongoDB
    MongoURI        string
    MongoDatabase   string
    MongoCollection string
    
    // Kafka
    KafkaBrokers    string
    KafkaTopicNews  string
    
    // Service
    LogLevel        string
}

func Load() (*Config, error) {
    // Load .env file
    _ = godotenv.Load()
    
    cfg := &Config{
        // MongoDB Configuration
        MongoURI:        getEnv("MONGO_URI", ""),
        MongoDatabase:   getEnv("MONGO_DATABASE", "CAG_CHATBOT"),
        MongoCollection: getEnv("MONGO_NEWS_COLLECTION", "NewsImpactDashboard"),
        
        // Kafka Configuration
        KafkaBrokers:    getEnv("KAFKA_BROKERS", "localhost:9092"),
        KafkaTopicNews:  getEnv("KAFKA_TOPIC_NEWS", "news-events"),
        
        // Service Configuration
        LogLevel:        getEnv("LOG_LEVEL", "INFO"),
    }
    
    return cfg, nil
}

func getEnv(key, defaultValue string) string {
    if value := os.Getenv(key); value != "" {
        return value
    }
    return defaultValue
}
```

### 3. MongoDB Watcher (`internal/watcher/mongo_watcher.go`)

**Purpose:** Watch MongoDB collection for changes using Change Streams.

```go
package watcher

import (
    "context"
    "encoding/json"
    "log"
    "time"
    
    "go.mongodb.org/mongo-driver/bson"
    "go.mongodb.org/mongo-driver/mongo"
    "go.mongodb.org/mongo-driver/mongo/options"
    
    "github.com/RohitIndira/Algo-Treading/services/data-ingestion/config"
    "github.com/RohitIndira/Algo-Treading/services/data-ingestion/internal/publisher"
)

type MongoWatcher struct {
    client     *mongo.Client
    collection *mongo.Collection
    publisher  publisher.Publisher
    config     *config.Config
}

func NewMongoWatcher(cfg *config.Config, pub publisher.Publisher) (*MongoWatcher, error) {
    // 1. Create MongoDB client
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()
    
    clientOptions := options.Client().ApplyURI(cfg.MongoURI)
    client, err := mongo.Connect(ctx, clientOptions)
    if err != nil {
        return nil, err
    }
    
    // 2. Ping to verify connection
    if err := client.Ping(ctx, nil); err != nil {
        return nil, err
    }
    
    log.Printf("✓ Connected to MongoDB: %s/%s", cfg.MongoDatabase, cfg.MongoCollection)
    
    // 3. Get collection
    collection := client.Database(cfg.MongoDatabase).Collection(cfg.MongoCollection)
    
    return &MongoWatcher{
        client:     client,
        collection: collection,
        publisher:  pub,
        config:     cfg,
    }, nil
}

func (w *MongoWatcher) Watch(ctx context.Context) error {
    // 1. Create change stream pipeline
    // Only watch for insert operations
    pipeline := mongo.Pipeline{
        {{Key: "$match", Value: bson.D{
            {Key: "operationType", Value: "insert"},
        }}},
    }
    
    // 2. Open change stream
    changeStream, err := w.collection.Watch(ctx, pipeline)
    if err != nil {
        return err
    }
    defer changeStream.Close(ctx)
    
    log.Println("✓ Change Stream opened, watching for inserts...")
    
    // 3. Iterate over change events
    for changeStream.Next(ctx) {
        var changeEvent bson.M
        if err := changeStream.Decode(&changeEvent); err != nil {
            log.Printf("❌ Error decoding change event: %v", err)
            continue
        }
        
        // 4. Process change event
        if err := w.handleChangeEvent(changeEvent); err != nil {
            log.Printf("❌ Error handling change event: %v", err)
            continue
        }
    }
    
    // Check for errors
    if err := changeStream.Err(); err != nil {
        return err
    }
    
    return nil
}

func (w *MongoWatcher) handleChangeEvent(changeEvent bson.M) error {
    // 1. Extract full document from change event
    fullDocument, ok := changeEvent["fullDocument"].(bson.M)
    if !ok {
        log.Println("⚠️ No fullDocument in change event")
        return nil
    }
    
    // 2. Extract key fields for logging
    title, _ := fullDocument["title"].(string)
    sentiment, _ := fullDocument["sentiment"].(string)
    impactScore, _ := fullDocument["impact_score"].(float64)
    
    log.Printf("📰 New news event detected:")
    log.Printf("   Title: %s", title)
    log.Printf("   Sentiment: %s", sentiment)
    log.Printf("   Impact Score: %.1f", impactScore)
    
    // 3. Convert to Extended JSON (MongoDB format)
    // This preserves MongoDB types like ObjectId, ISODate, etc.
    extJSON, err := bson.MarshalExtJSON(fullDocument, true, false)
    if err != nil {
        return err
    }
    
    // 4. Create event message
    event := map[string]interface{}{
        "operation_type": "insert",
        "timestamp":      time.Now(),
        "database":       w.config.MongoDatabase,
        "collection":     w.config.MongoCollection,
        "full_document":  json.RawMessage(extJSON),
    }
    
    // 5. Publish to Kafka
    if err := w.publisher.Publish(event); err != nil {
        return err
    }
    
    log.Printf("✓ Published to Kafka topic: %s", w.config.KafkaTopicNews)
    return nil
}

func (w *MongoWatcher) Close() error {
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    
    return w.client.Disconnect(ctx)
}
```

### 4. Kafka Publisher (`internal/publisher/publisher.go`)

**Purpose:** Publish events to Kafka.

```go
package publisher

import (
    "encoding/json"
    "log"
    
    "github.com/confluentinc/confluent-kafka-go/v2/kafka"
    "github.com/RohitIndira/Algo-Treading/services/data-ingestion/config"
)

type Publisher interface {
    Publish(event interface{}) error
    Close()
}

type KafkaPublisher struct {
    producer *kafka.Producer
    topic    string
}

func NewKafkaPublisher(cfg *config.Config) (*KafkaPublisher, error) {
    // 1. Create Kafka producer config
    producerConfig := &kafka.ConfigMap{
        "bootstrap.servers": cfg.KafkaBrokers,
        "client.id":         "data-ingestion-service",
        "acks":              "all",
        "compression.type":  "snappy",
    }
    
    // 2. Create producer
    producer, err := kafka.NewProducer(producerConfig)
    if err != nil {
        return nil, err
    }
    
    log.Printf("✓ Kafka producer connected to: %s", cfg.KafkaBrokers)
    
    // 3. Start delivery report goroutine
    go func() {
        for e := range producer.Events() {
            switch ev := e.(type) {
            case *kafka.Message:
                if ev.TopicPartition.Error != nil {
                    log.Printf("❌ Delivery failed: %v", ev.TopicPartition.Error)
                } else {
                    log.Printf("✓ Message delivered to %v", ev.TopicPartition)
                }
            }
        }
    }()
    
    return &KafkaPublisher{
        producer: producer,
        topic:    cfg.KafkaTopicNews,
    }, nil
}

func (p *KafkaPublisher) Publish(event interface{}) error {
    // 1. Marshal event to JSON
    eventJSON, err := json.Marshal(event)
    if err != nil {
        return err
    }
    
    // 2. Create Kafka message
    message := &kafka.Message{
        TopicPartition: kafka.TopicPartition{
            Topic:     &p.topic,
            Partition: kafka.PartitionAny,
        },
        Value: eventJSON,
    }
    
    // 3. Publish to Kafka
    if err := p.producer.Produce(message, nil); err != nil {
        return err
    }
    
    return nil
}

func (p *KafkaPublisher) Close() {
    // Wait for all messages to be delivered
    p.producer.Flush(15 * 1000) // 15 seconds
    p.producer.Close()
    log.Println("✓ Kafka producer closed")
}
```

---

## MongoDB Change Streams

### What are Change Streams?

Change Streams allow applications to access real-time data changes without the complexity and risk of tailing the oplog. They provide:

- **Real-time notifications** when documents are inserted, updated, or deleted
- **Resume capability** to continue from where you left off after disconnection
- **Filtering** to watch specific operations or documents

### Change Stream Pipeline

```go
// Watch only insert operations
pipeline := mongo.Pipeline{
    {{Key: "$match", Value: bson.D{
        {Key: "operationType", Value: "insert"},
    }}},
}

changeStream, err := collection.Watch(ctx, pipeline)
```

### Change Event Structure

```json
{
  "operationType": "insert",
  "fullDocument": {
    "_id": {"$oid": "656..."},
    "title": "Apple announces new iPhone",
    "content": "Apple Inc. announced...",
    "sentiment": "positive",
    "impact_score": 8,
    "stock_codes": [2885],
    "category": "product-launch",
    "timestamp": {"$date": "2025-12-12T10:00:00Z"}
  },
  "ns": {
    "db": "CAG_CHATBOT",
    "coll": "NewsImpactDashboard"
  },
  "documentKey": {
    "_id": {"$oid": "656..."}
  }
}
```

### Extended JSON Format

MongoDB Extended JSON preserves type information:

```json
{
  "_id": {"$oid": "656abc123def"},
  "timestamp": {"$date": "2025-12-12T10:00:00.000Z"},
  "impact_score": {"$numberDouble": "8.5"},
  "stock_codes": [
    {"$numberInt": "2885"},
    {"$numberInt": "1234"}
  ]
}
```

---

## Kafka Integration

### Topic Configuration

**Topic Name:** `news-events`

**Configuration:**
```bash
kafka-topics.sh --create \
  --topic news-events \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 2 \
  --config retention.ms=604800000  # 7 days
```

### Message Format

```json
{
  "operation_type": "insert",
  "timestamp": "2025-12-12T10:30:00Z",
  "database": "CAG_CHATBOT",
  "collection": "NewsImpactDashboard",
  "full_document": {
    "_id": {"$oid": "656..."},
    "title": "Apple announces record earnings",
    "content": "Apple Inc. reported...",
    "sentiment": "POSITIVE",
    "impact_score": {"$numberDouble": "8.5"},
    "stock_codes": [{"$numberInt": "2885"}],
    "category": "earnings",
    "exchange": "NSE",
    "timestamp": {"$date": "2025-12-12T10:00:00.000Z"},
    "source": "Economic Times",
    "tags": ["Apple", "AAPL", "earnings"]
  }
}
```

### Producer Configuration

```go
producerConfig := &kafka.ConfigMap{
    "bootstrap.servers": "localhost:9092",
    "client.id":         "data-ingestion-service",
    "acks":              "all",           // Wait for all replicas
    "compression.type":  "snappy",        // Compress messages
    "batch.size":        16384,           // Batch size in bytes
    "linger.ms":         10,              // Wait 10ms before sending
}
```

---

## Configuration

### Environment Variables

```bash
# MongoDB Configuration
MONGO_URI=mongodb+srv://username:password@cluster.mongodb.net/
MONGO_DATABASE=CAG_CHATBOT
MONGO_NEWS_COLLECTION=NewsImpactDashboard

# Kafka Configuration
KAFKA_BROKERS=localhost:9092
KAFKA_TOPIC_NEWS=news-events

# Service Configuration
LOG_LEVEL=INFO
```

### MongoDB Collection Schema

Expected document structure:

```javascript
{
  "_id": ObjectId("..."),
  "title": String,           // Required: News headline
  "content": String,         // Required: Full article content
  "sentiment": String,       // Required: POSITIVE, NEGATIVE, NEUTRAL
  "impact_score": Number,    // Required: 1-10
  "stock_codes": [Number],   // Required: Array of stock tokens
  "category": String,        // Optional: earnings, merger, etc.
  "exchange": String,        // Optional: NSE, BSE
  "timestamp": ISODate,      // Required: News publish time
  "source": String,          // Optional: News source
  "tags": [String]           // Optional: Additional tags
}
```

---

## Setup & Deployment

### Prerequisites

```bash
# 1. MongoDB Access
# Ensure you have credentials for external MongoDB

# 2. Kafka Running
docker run -d --name kafka -p 9092:9092 apache/kafka:latest

# 3. Go 1.23+
go version
```

### Development Setup

```bash
# 1. Navigate to service directory
cd services/data-ingestion

# 2. Install dependencies
go mod download
go mod tidy

# 3. Configure environment
cp .env.example .env
# Edit .env with your MongoDB and Kafka details

# 4. Test MongoDB connection
go run cmd/main.go --test-connection

# 5. Build and run
go build -o bin/data-ingestion cmd/main.go
./bin/data-ingestion
```

### Production Deployment

#### Docker

```dockerfile
FROM golang:1.23-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o data-ingestion cmd/main.go

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/

COPY --from=builder /app/data-ingestion .
COPY --from=builder /app/.env .

CMD ["./data-ingestion"]
```

#### Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: data-ingestion
spec:
  replicas: 1  # Single instance (Change Stream resume token)
  selector:
    matchLabels:
      app: data-ingestion
  template:
    metadata:
      labels:
        app: data-ingestion
    spec:
      containers:
      - name: data-ingestion
        image: data-ingestion:latest
        env:
        - name: MONGO_URI
          valueFrom:
            secretKeyRef:
              name: mongo-credentials
              key: uri
        - name: KAFKA_BROKERS
          value: "kafka:9092"
        resources:
          limits:
            memory: "256Mi"
            cpu: "500m"
```

---

## Monitoring

### Logs

```bash
# View logs
tail -f data_ingestion.log

# Search for specific events
grep "New news event" data_ingestion.log

# Monitor errors
grep "ERROR" data_ingestion.log
```

### Metrics

```go
type Metrics struct {
    EventsDetected  int64  // Total events detected
    EventsPublished int64  // Successfully published
    PublishErrors   int64  // Failed publishes
    LastEventTime   time.Time
}
```

### Health Checks

```bash
# Check MongoDB connection
mongosh "mongodb+srv://..." --eval "db.adminCommand('ping')"

# Check Kafka
kafka-topics.sh --bootstrap-server localhost:9092 --list

# Check service status
ps aux | grep data-ingestion
```

---

## Troubleshooting

### Common Issues

#### 1. MongoDB Connection Failed

**Error:** `failed to connect to MongoDB`

**Solution:**
```bash
# Verify URI
echo $MONGO_URI

# Test connection
mongosh "$MONGO_URI" --eval "db.adminCommand('ping')"

# Check network/firewall
telnet cluster.mongodb.net 27017
```

#### 2. Change Stream Not Working

**Error:** `change stream error`

**Solution:**
```bash
# Ensure MongoDB is replica set
# Change Streams require replica set or sharded cluster

# Check MongoDB version (requires 3.6+)
mongosh "$MONGO_URI" --eval "db.version()"
```

#### 3. Kafka Publish Failed

**Error:** `failed to publish to Kafka`

**Solution:**
```bash
# Check Kafka is running
kafka-broker-api-versions.sh --bootstrap-server localhost:9092

# Verify topic exists
kafka-topics.sh --list --bootstrap-server localhost:9092

# Create topic if missing
kafka-topics.sh --create --topic news-events --bootstrap-server localhost:9092
```

---

**Last Updated:** December 12, 2025  
**Version:** 1.0  
**Maintained by:** Backend Development Team
