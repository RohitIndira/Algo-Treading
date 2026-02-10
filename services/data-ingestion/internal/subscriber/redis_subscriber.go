package subscriber

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	redispkg "github.com/RohitIndira/Algo-Treading/pkg/database/redis"
	"github.com/RohitIndira/Algo-Treading/pkg/logger"
	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/internal/detector"
	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/internal/publisher"
	"go.uber.org/zap"
)

// RedisSubscriber subscribes to Redis keyspace notifications for real-time
// market data updates and detects 52-week breakouts
//
// Architecture:
// - Subscribes to __keyevent@0__:hset pattern (hash field updates)
// - Filters for market:nse:* and market:bse:* keys
// - Fetches full market snapshot from Redis
// - Runs breakout detection
// - Publishes to Kafka if breakout detected
type RedisSubscriber struct {
	client       *redispkg.Client
	detector     *detector.BreakoutDetector
	publisher    publisher.KafkaPublisher
	logger       *logger.Logger
	workerCount  int
	
	mu      sync.Mutex
	metrics *Metrics
}

// Metrics tracks subscriber performance
type Metrics struct {
	EventsReceived   int64
	EventsFiltered   int64
	BreakoutsDetected int64
	PublishSuccesses int64
	PublishFailures  int64
	Errors           int64
	LastUpdated      time.Time
}

// Config holds subscriber configuration
type Config struct {
	WorkerCount int // Number of concurrent event processors
}

// NewRedisSubscriber creates a new Redis keyspace subscriber
func NewRedisSubscriber(
	client *redispkg.Client,
	detector *detector.BreakoutDetector,
	publisher publisher.KafkaPublisher,
	lgr *logger.Logger,
	cfg Config,
) *RedisSubscriber {
	if cfg.WorkerCount <= 0 {
		cfg.WorkerCount = 10 // Default: 10 workers
	}

	return &RedisSubscriber{
		client:      client,
		detector:    detector,
		publisher:   publisher,
		logger:      lgr,
		workerCount: cfg.WorkerCount,
		metrics: &Metrics{
			LastUpdated: time.Now(),
		},
	}
}

// Start begins subscribing to Redis keyspace notifications
// This is the event-driven replacement for Redis SCAN polling
//
// IMPORTANT: We use timestamp-based dedupe - each breakout timestamp is unique.
// If service restarts, the same timestamp won't be published again.
// Multiple breakouts same day (increasing price) will have different timestamps.
func (s *RedisSubscriber) Start(ctx context.Context) error {
	s.logger.Info("Starting Redis keyspace subscriber (event-driven mode)",
		zap.Int("workers", s.workerCount))

	// Subscribe to real-time keyspace events
	// Timestamp-based dedupe ensures no duplicates on restart
	pubsub := s.client.PSubscribe(ctx, "__keyevent@0__:set")
	defer pubsub.Close()

	s.logger.Info("Subscribed to Redis keyspace notifications",
		zap.String("pattern", "__keyevent@0__:set"))

	// Create worker pool
	eventChan := make(chan string, s.workerCount*2)
	var wg sync.WaitGroup

	// Start worker goroutines
	for i := 0; i < s.workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			s.worker(ctx, workerID, eventChan)
		}(i)
	}

	// Start metrics logger
	go s.logMetrics(ctx)

	// Main event loop
	for {
		select {
		case <-ctx.Done():
			s.logger.Info("Redis subscriber stopping")
			close(eventChan)
			wg.Wait()
			return ctx.Err()
		default:
			msg, err := pubsub.ReceiveMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					close(eventChan)
					wg.Wait()
					return ctx.Err()
				}
				s.logger.Error("Failed to receive message from Redis pubsub", zap.Error(err))
				s.incrementMetric("errors")
				time.Sleep(time.Second)
				continue
			}

			s.incrementMetric("events_received")

			// msg.Payload contains the key that was updated
			key := msg.Payload

			// Filter: only process market:nse:* and market:bse:* keys
			if !s.isMarketKey(key) {
				s.incrementMetric("events_filtered")
				continue
			}

			// Send to worker pool
			select {
			case eventChan <- key:
				// Event sent to worker
			case <-ctx.Done():
				close(eventChan)
				wg.Wait()
				return ctx.Err()
			}
		}
	}
}

// worker processes Redis key update events
func (s *RedisSubscriber) worker(ctx context.Context, workerID int, eventChan <-chan string) {
	s.logger.Debug("Redis subscriber worker started", zap.Int("worker_id", workerID))

	for key := range eventChan {
		if err := s.processKey(ctx, key); err != nil {
			s.logger.Error("Failed to process key",
				zap.Int("worker_id", workerID),
				zap.String("key", key),
				zap.Error(err))
			s.incrementMetric("errors")
		}
	}

	s.logger.Debug("Redis subscriber worker stopped", zap.Int("worker_id", workerID))
}

// processKey fetches market data and checks for breakouts
func (s *RedisSubscriber) processKey(ctx context.Context, key string) error {
	// Fetch the full hash from Redis
	raw, err := s.client.Get(ctx, key).Result()
	if err != nil {
		s.logger.Warn("Failed to fetch key from Redis",
			zap.String("key", key),
			zap.Error(err))
		return err
	}

	// Parse market snapshot
	var snap models.MarketSnapshot
	if err := json.Unmarshal([]byte(raw), &snap); err != nil {
		s.logger.Warn("Failed to unmarshal market snapshot",
			zap.String("key", key),
			zap.Error(err))
		return err
	}

	// Infer exchange from key if missing
	if snap.Exchange == "" {
		if strings.Contains(key, "market:nse:") {
			snap.Exchange = "NSE"
		} else if strings.Contains(key, "market:bse:") {
			snap.Exchange = "BSE"
		}
	}

	// Run breakout detection
	event, err := s.detector.DetectBreakout(ctx, &snap)
	if err != nil {
		return fmt.Errorf("breakout detection failed: %w", err)
	}

	// No breakout detected
	if event == nil {
		return nil
	}

	s.incrementMetric("breakouts_detected")

	// Publish to Kafka
	if err := s.publisher.PublishBreakout(ctx, event); err != nil {
		s.logger.Error("Failed to publish breakout to Kafka",
			zap.String("symbol", event.Symbol),
			zap.String("token", event.Token),
			zap.Error(err))
		s.incrementMetric("publish_failures")
		return err
	}

	s.incrementMetric("publish_successes")

	s.logger.Info("52-week breakout published to Kafka",
		zap.String("symbol", event.Symbol),
		zap.String("token", event.Token),
		zap.String("exchange", event.Exchange),
		zap.Float64("ltp", event.LTP),
		zap.String("event_id", event.EventID))

	return nil
}

// isMarketKey checks if a key matches market data pattern
func (s *RedisSubscriber) isMarketKey(key string) bool {
	return strings.HasPrefix(key, "market:nse:") || strings.HasPrefix(key, "market:bse:")
}

// incrementMetric safely increments a metric counter
func (s *RedisSubscriber) incrementMetric(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch name {
	case "events_received":
		s.metrics.EventsReceived++
	case "events_filtered":
		s.metrics.EventsFiltered++
	case "breakouts_detected":
		s.metrics.BreakoutsDetected++
	case "publish_successes":
		s.metrics.PublishSuccesses++
	case "publish_failures":
		s.metrics.PublishFailures++
	case "errors":
		s.metrics.Errors++
	}
	s.metrics.LastUpdated = time.Now()
}

// logMetrics periodically logs performance metrics
func (s *RedisSubscriber) logMetrics(ctx context.Context) {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.mu.Lock()
			m := *s.metrics // Copy
			s.mu.Unlock()

			s.logger.Info("Redis subscriber metrics",
				zap.Int64("events_received", m.EventsReceived),
				zap.Int64("events_filtered", m.EventsFiltered),
				zap.Int64("breakouts_detected", m.BreakoutsDetected),
				zap.Int64("publish_successes", m.PublishSuccesses),
				zap.Int64("publish_failures", m.PublishFailures),
				zap.Int64("errors", m.Errors),
				zap.Time("last_updated", m.LastUpdated))
		}
	}
}

// GetMetrics returns a copy of current metrics
func (s *RedisSubscriber) GetMetrics() Metrics {
	s.mu.Lock()
	defer s.mu.Unlock()
	return *s.metrics
}
