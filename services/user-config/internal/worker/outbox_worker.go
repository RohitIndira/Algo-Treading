package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/repository"
	"github.com/segmentio/kafka-go"
)

// OutboxWorker handles processing of outbox events
type OutboxWorker struct {
	repo        *repository.StrategyRepository
	kafkaWriter *kafka.Writer
	interval    time.Duration
}

// ConfigEvent represents the event structure to be published
type ConfigEvent struct {
	EventType string           `json:"event_type"`
	Strategy  *models.Strategy `json:"strategy"`
	Timestamp int64            `json:"timestamp"`
}

// NewOutboxWorker creates a new outbox worker
func NewOutboxWorker(repo *repository.StrategyRepository, kafkaWriter *kafka.Writer, interval time.Duration) *OutboxWorker {
	if interval == 0 {
		interval = 500 * time.Millisecond
	}
	return &OutboxWorker{
		repo:        repo,
		kafkaWriter: kafkaWriter,
		interval:    interval,
	}
}

// Start starts the worker loop
func (w *OutboxWorker) Start(ctx context.Context) {
	fmt.Println("[OutboxWorker] Starting outbox worker...")
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("[OutboxWorker] Stopping outbox worker...")
			return
		case <-ticker.C:
			if err := w.processOutbox(ctx); err != nil {
				fmt.Printf("[OutboxWorker] Error processing outbox: %v\n", err)
			}
		}
	}
}

// processOutbox fetches and processes pending events
func (w *OutboxWorker) processOutbox(ctx context.Context) error {
	// Fetch pending events (batch size 50)
	events, err := w.repo.ListPendingOutboxEvents(ctx, 50)
	if err != nil {
		return fmt.Errorf("failed to fetch events: %w", err)
	}

	if len(events) == 0 {
		return nil
	}

	fmt.Printf("[OutboxWorker] Processing %d events\n", len(events))
	processedIDs := make([]int64, 0, len(events))

	for _, event := range events {
		// Parse payload
		var strategy models.Strategy
		if err := json.Unmarshal(event.Payload, &strategy); err != nil {
			fmt.Printf("[OutboxWorker] Failed to unmarshal payload for event %d: %v\n", event.ID, err)
			// Decide whether to mark as processed or DLQ. For now, skip marking to retry (dangerous if persistent error)
			// Better: mark as processed but log error, or move to failed_events table.
			// Currently: skip marking, will retry next loop.
			continue
		}

		// Construct Kafka message
		kafkaEvent := ConfigEvent{
			EventType: event.EventType,
			Strategy:  &strategy,
			Timestamp: time.Now().Unix(),
		}

		eventBytes, err := json.Marshal(kafkaEvent)
		if err != nil {
			fmt.Printf("[OutboxWorker] Failed to marshal kafka event for %d: %v\n", event.ID, err)
			continue
		}

		// Publish to Kafka
		msg := kafka.Message{
			Key:   []byte(event.AggregateID.String()),
			Value: eventBytes,
			Headers: []kafka.Header{
				{Key: "event_type", Value: []byte(event.EventType)},
				{Key: "user_id", Value: []byte(strategy.UserID)},
				{Key: "trading_mode", Value: []byte(string(strategy.TradingMode))},
			},
		}

		// Use a separate context with timeout for Kafka write to avoid blocking too long
		writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err = w.kafkaWriter.WriteMessages(writeCtx, msg)
		cancel()

		if err != nil {
			fmt.Printf("[OutboxWorker] Failed to publish event %d to Kafka: %v\n", event.ID, err)
			// Stop processing batch on Kafka error to maintain order
			break 
		}

		processedIDs = append(processedIDs, event.ID)
	}

	// Mark processed events
	if len(processedIDs) > 0 {
		if err := w.repo.MarkOutboxEventsProcessed(ctx, processedIDs); err != nil {
			return fmt.Errorf("failed to mark events processed: %w", err)
		}
		fmt.Printf("[OutboxWorker] Successfully processed %d events\n", len(processedIDs))
	}

	return nil
}
