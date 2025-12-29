# Auto Square-Off Implementation - Code Changes

## Summary of Changes

Auto Square-Off scheduler has been **fully integrated** into the Trade Execution Service. The scheduler was already implemented but not being used. This integration activates it.

---

## File: `services/trade-execution/cmd/main.go`

### Change 1: Added Import (Line 21)

```diff
+ "github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/scheduler"
```

**Full imports section**:
```go
import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"go.uber.org/zap"

	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/consumer"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/executor"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/indira"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/publisher"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/repository"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/scheduler"  // ← NEW
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/server"
)
```

---

### Change 2: Added Scheduler Initialization (Lines 67-73)

**Before**:
```go
	orderExecutor := executor.NewOrderExecutor(
		orderRepo,
		credsRepo,
		indiraClient,
		cfg.MaxRetries,
		cfg.RetryDelay,
	)
	log.Println("✓ Order executor initialized")

	// Initialize RabbitMQ consumer
```

**After**:
```go
	orderExecutor := executor.NewOrderExecutor(
		orderRepo,
		credsRepo,
		indiraClient,
		cfg.MaxRetries,
		cfg.RetryDelay,
	)
	log.Println("✓ Order executor initialized")

	// Initialize Auto Square-Off Scheduler              ← NEW
	autoSquareOffScheduler := scheduler.NewAutoSquareOffScheduler(  ← NEW
		orderRepo,                                        ← NEW
		credsRepo,                                        ← NEW
		orderExecutor,                                    ← NEW
		cfg.AutoSquareOffTime,                            ← NEW
	)                                                     ← NEW
	log.Println("✓ Auto Square-Off scheduler initialized")  ← NEW

	// Initialize RabbitMQ consumer
```

---

### Change 3: Started Scheduler as Goroutine (Lines 145-150)

**Before**:
```go
	// Start RabbitMQ consumer
	go func() {
		log.Println("Starting RabbitMQ consumer...")
		if err := rabbitConsumer.Start(ctx); err != nil {
			log.Fatalf("RabbitMQ consumer error: %v", err)
		}
	}()

	// Give consumer time to start
	time.Sleep(1 * time.Second)
```

**After**:
```go
	// Start RabbitMQ consumer
	go func() {
		log.Println("Starting RabbitMQ consumer...")
		if err := rabbitConsumer.Start(ctx); err != nil {
			log.Fatalf("RabbitMQ consumer error: %v", err)
		}
	}()

	// Start Auto Square-Off Scheduler                   ← NEW
	go func() {                                           ← NEW
		log.Println("Starting Auto Square-Off Scheduler...")  ← NEW
		if err := autoSquareOffScheduler.Start(ctx); err {  ← NEW
			log.Printf("Auto Square-Off Scheduler error: %v", err)  ← NEW
		}                                                 ← NEW
	}()                                                   ← NEW

	// Give consumer time to start
	time.Sleep(1 * time.Second)
```

---

### Change 4: Updated Service Status Display (Line 166)

**Before**:
```go
	log.Println("✓ Trade Execution Service Started")
	log.Printf("  - gRPC Server: localhost:%d", cfg.GRPCPort)
	log.Printf("  - RabbitMQ Queue: %s", cfg.QueueName)
	log.Printf("  - Kafka Topic: %s (Group: %s)", cfg.KafkaTopic, cfg.KafkaGroupID)
	log.Printf("  - Workers: %d", cfg.WorkerCount)
	log.Println("========================================")
```

**After**:
```go
	log.Println("✓ Trade Execution Service Started")
	log.Printf("  - gRPC Server: localhost:%d", cfg.GRPCPort)
	log.Printf("  - RabbitMQ Queue: %s", cfg.QueueName)
	log.Printf("  - Kafka Topic: %s (Group: %s)", cfg.KafkaTopic, cfg.KafkaGroupID)
	log.Printf("  - Workers: %d", cfg.WorkerCount)
	log.Printf("  - Auto Square-Off Time: %s", cfg.AutoSquareOffTime)  ← NEW
	log.Println("========================================")
```

---

### Change 5: Added Scheduler Shutdown (Line 175)

**Before**:
```go
	log.Printf("\nReceived signal: %v", sig)
	log.Println("Initiating graceful shutdown...")

	cancel()

	// Give time for graceful shutdown
	time.Sleep(5 * time.Second)
```

**After**:
```go
	log.Printf("\nReceived signal: %v", sig)
	log.Println("Initiating graceful shutdown...")

	// Stop scheduler                                    ← NEW
	autoSquareOffScheduler.Stop()                        ← NEW

	cancel()

	// Give time for graceful shutdown
	time.Sleep(5 * time.Second)
```

---

### Change 6: Updated Config Struct (Lines 194-209)

**Before**:
```go
// Config holds service configuration
type Config struct {
	GRPCPort      int
	RabbitMQURL   string
	QueueName     string
	Exchange      string
	RoutingKey    string
	PrefetchCount int
	WorkerCount   int
	KafkaBrokers  []string
	KafkaGroupID  string
	KafkaTopic    string
	MaxRetries    int
	RetryDelay    time.Duration
	PostgresURL   string
}
```

**After**:
```go
// Config holds service configuration
type Config struct {
	GRPCPort         int
	RabbitMQURL      string
	QueueName        string
	Exchange         string
	RoutingKey       string
	PrefetchCount    int
	WorkerCount      int
	KafkaBrokers     []string
	KafkaGroupID     string
	KafkaTopic       string
	MaxRetries       int
	RetryDelay       time.Duration
	PostgresURL      string
	AutoSquareOffTime string            ← NEW
}
```

---

### Change 7: Updated loadConfig() Function (Line 230)

**Before**:
```go
	return Config{
		GRPCPort:      getEnvInt("SERVICE_PORT", 9004),
		RabbitMQURL:   getEnv("RABBITMQ_URL", "amqp://admin:admin123@localhost:5672/"),
		QueueName:     getEnv("RABBITMQ_QUEUE", "trade.executions"),
		Exchange:      getEnv("RABBITMQ_EXCHANGE", "trade.execution"),
		RoutingKey:    getEnv("RABBITMQ_ROUTING_KEY", "order.new"),
		PrefetchCount: getEnvInt("RABBITMQ_PREFETCH", 10),
		WorkerCount:   getEnvInt("WORKER_COUNT", 10),
		KafkaBrokers:  kafkaBrokers,
		KafkaGroupID:  getEnv("KAFKA_GROUP_ID", "trade-execution-service"),
		KafkaTopic:    getEnv("KAFKA_TOPIC", "trade-signals"),
		MaxRetries:    getEnvInt("MAX_RETRIES", 3),
		RetryDelay:    time.Duration(getEnvInt("RETRY_DELAY_SEC", 1)) * time.Second,
		PostgresURL:   buildPostgresURL(),
	}
```

**After**:
```go
	return Config{
		GRPCPort:          getEnvInt("SERVICE_PORT", 9004),
		RabbitMQURL:       getEnv("RABBITMQ_URL", "amqp://admin:admin123@localhost:5672/"),
		QueueName:         getEnv("RABBITMQ_QUEUE", "trade.executions"),
		Exchange:          getEnv("RABBITMQ_EXCHANGE", "trade.execution"),
		RoutingKey:        getEnv("RABBITMQ_ROUTING_KEY", "order.new"),
		PrefetchCount:     getEnvInt("RABBITMQ_PREFETCH", 10),
		WorkerCount:       getEnvInt("WORKER_COUNT", 10),
		KafkaBrokers:      kafkaBrokers,
		KafkaGroupID:      getEnv("KAFKA_GROUP_ID", "trade-execution-service"),
		KafkaTopic:        getEnv("KAFKA_TOPIC", "trade-signals"),
		MaxRetries:        getEnvInt("MAX_RETRIES", 3),
		RetryDelay:        time.Duration(getEnvInt("RETRY_DELAY_SEC", 1)) * time.Second,
		PostgresURL:       buildPostgresURL(),
		AutoSquareOffTime: getEnv("AUTO_SQUARE_OFF_TIME", "15:05"),  ← NEW
	}
```

---

## Summary of Changes

| Change | Type | Impact |
|--------|------|--------|
| Import scheduler package | Addition | Enables access to scheduler |
| Initialize scheduler | Addition | Creates scheduler instance |
| Start scheduler | Addition | Launches scheduled task |
| Stop scheduler | Addition | Graceful shutdown |
| Config field | Addition | Configuration management |
| Config loading | Addition | Load from environment |
| Status display | Update | Shows active features |

---

## Testing After Deployment

### 1. Verify Service Starts
```bash
cd services/trade-execution
go run cmd/main.go
```

**Expected Log Output**:
```
✓ Order executor initialized
✓ Auto Square-Off scheduler initialized           ← NEW
Starting Auto Square-Off Scheduler...             ← NEW
Auto Square-Off Scheduler (Time: 15:05)           ← NEW
✓ Trade Execution Service Started
  - gRPC Server: localhost:9004
  - RabbitMQ Queue: trade.executions
  - Kafka Topic: trade-signals (Group: trade-execution-service)
  - Workers: 10
  - Auto Square-Off Time: 15:05                   ← NEW
```

### 2. Create Test Position
Create an INTRADAY order before 15:05

### 3. Monitor Scheduler Execution
Watch logs at 15:05 for square-off events:
```
Auto Square-Off Time Reached - Initiating square-off for all open positions
Found X open orders to square off
...
Auto Square-Off: Complete (Success: X, Failed: Y)
```

---

## Backward Compatibility

✅ **Fully Backward Compatible**
- Default square-off time: 15:05
- Existing deployments work without changes
- Optional `.env` variable (uses default if not set)

---

## Performance Impact

✅ **Minimal**
- Scheduler uses 1 goroutine
- Checks every 1 minute
- Only queries on weekdays at scheduled time
- Negligible CPU/Memory overhead

---

## Rollback Instructions

If needed, revert these 7 changes:
1. Remove scheduler import
2. Remove scheduler initialization
3. Remove scheduler goroutine
4. Remove scheduler shutdown
5. Remove AutoSquareOffTime from Config
6. Remove AutoSquareOffTime from loadConfig()
7. Remove auto square-off status line

Or simply use: `git revert <commit-hash>`

