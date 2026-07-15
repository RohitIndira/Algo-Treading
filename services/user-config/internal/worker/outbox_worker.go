package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"runtime/debug"
	"strings"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/events"
	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/repository"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

// OutboxWorker handles processing of outbox events
type OutboxWorker struct {
	repo        *repository.StrategyRepository
	kafkaWriter KafkaWriter
	interval    time.Duration
}

// KafkaWriter is a minimal interface wrapper for kafka-go writer.
// This allows unit testing the outbox worker without a real Kafka broker.
type KafkaWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// NewOutboxWorker creates a new outbox worker
func NewOutboxWorker(repo *repository.StrategyRepository, kafkaWriter KafkaWriter, interval time.Duration) *OutboxWorker {
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
	log.Println("[OutboxWorker] Starting outbox worker...")
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("[OutboxWorker] Stopping outbox worker...")
			return
		case <-ticker.C:
			// Recover guards this tick only, so a panic in one batch doesn't
			// cancel every future tick — Kafka publishing must keep running.
			func() {
				defer func() {
					if rec := recover(); rec != nil {
						log.Printf("[OutboxWorker] PANIC recovered: %v\n%s", rec, debug.Stack())
					}
				}()
				if err := w.processOutbox(ctx); err != nil {
					log.Printf("[OutboxWorker] Error processing outbox: %v", err)
				}
			}()
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

	log.Printf("[OutboxWorker] Processing %d events", len(events))
	processedIDs := make([]int64, 0, len(events))

	for _, event := range events {
		published, err := w.processOneOutboxEvent(ctx, event)
		if err != nil {
			// Malformed JSON or Kafka failure: return error so the outer loop retries later.
			// But do mark anything already successfully published.
			if len(processedIDs) > 0 {
				if markErr := w.repo.MarkOutboxEventsProcessed(ctx, processedIDs); markErr != nil {
					return fmt.Errorf("failed to mark events processed after partial success: %w", markErr)
				}
				log.Printf("[OutboxWorker] Successfully processed %d events (partial batch)", len(processedIDs))
			}
			return err
		}
		if published {
			processedIDs = append(processedIDs, event.ID)
		}
	}

	// Mark processed events
	if len(processedIDs) > 0 {
		if err := w.repo.MarkOutboxEventsProcessed(ctx, processedIDs); err != nil {
			return fmt.Errorf("failed to mark events processed: %w", err)
		}
		log.Printf("[OutboxWorker] Successfully processed %d events", len(processedIDs))
	}

	return nil
}

type activationOutboxPayload struct {
	StrategyID string `json:"strategy_id"`
	UserID     string `json:"user_id"`
	Version    uint64 `json:"version"`
	Active     bool   `json:"active"`
}

type deleteOutboxPayload struct {
	StrategyID string `json:"strategy_id"`
	UserID     string `json:"user_id"`
	Version    uint64 `json:"version"`
}

// processOneOutboxEvent translates a single outbox row into the new Kafka contract and publishes it.
// Returns (published=true) when the outbox row should be marked processed.
func (w *OutboxWorker) processOneOutboxEvent(ctx context.Context, event *models.ExecutionOutbox) (bool, error) {
	if len(event.Payload) == 0 {
		log.Printf("[OutboxWorker] WARNING: empty payload for outbox id=%d event_type=%s, marking processed", event.ID, event.EventType)
		return true, nil
	}

	var kafkaEvent *events.ConfigEvent

	switch event.EventType {
	case "STRATEGY_CREATED":
		var s models.Strategy
		if err := json.Unmarshal(event.Payload, &s); err != nil {
			return false, fmt.Errorf("failed to unmarshal STRATEGY_CREATED payload for outbox id=%d: %w payload=%s", event.ID, err, truncate(string(event.Payload), 500))
		}
		if s.UserID == "" || s.StrategyID.String() == "" {
			log.Printf("[OutboxWorker] CRITICAL: empty user_id/strategy_id in STRATEGY_CREATED outbox id=%d, skipping+marking processed", event.ID)
			return true, nil
		}
		kafkaEvent = events.ToFullConfigEvent(events.ConfigCreated, &s)

	case "STRATEGY_UPDATED":
		var s models.Strategy
		if err := json.Unmarshal(event.Payload, &s); err != nil {
			return false, fmt.Errorf("failed to unmarshal STRATEGY_UPDATED payload for outbox id=%d: %w payload=%s", event.ID, err, truncate(string(event.Payload), 500))
		}
		if s.UserID == "" || s.StrategyID.String() == "" {
			log.Printf("[OutboxWorker] CRITICAL: empty user_id/strategy_id in STRATEGY_UPDATED outbox id=%d, skipping+marking processed", event.ID)
			return true, nil
		}
		kafkaEvent = events.ToFullConfigEvent(events.ConfigUpdated, &s)

	case "STRATEGY_DELETED":
		var thin deleteOutboxPayload
		if err := json.Unmarshal(event.Payload, &thin); err != nil {
			return false, fmt.Errorf("failed to unmarshal STRATEGY_DELETED payload for outbox id=%d: %w payload=%s", event.ID, err, truncate(string(event.Payload), 500))
		}
		if thin.UserID == "" || thin.StrategyID == "" {
			log.Printf("[OutboxWorker] CRITICAL: empty user_id/strategy_id in STRATEGY_DELETED outbox id=%d, skipping+marking processed", event.ID)
			return true, nil
		}
		kafkaEvent = events.ToThinConfigEvent(events.ConfigDeleted, thin.UserID, thin.StrategyID, thin.Version)

	case "STRATEGY_ACTIVATED":
		var thin activationOutboxPayload
		if err := json.Unmarshal(event.Payload, &thin); err != nil {
			return false, fmt.Errorf("failed to unmarshal STRATEGY_ACTIVATED payload for outbox id=%d: %w payload=%s", event.ID, err, truncate(string(event.Payload), 500))
		}
		if thin.UserID == "" || thin.StrategyID == "" {
			log.Printf("[OutboxWorker] CRITICAL: empty user_id/strategy_id in STRATEGY_ACTIVATED outbox id=%d, skipping+marking processed", event.ID)
			return true, nil
		}
		// Emit a full CONFIG_ACTIVATED so the rules engine can upsert the strategy even
		// after a restart AND re-run the AMN backfill for AMN strategies. (A thin
		// CONFIG_RESUMED silently no-ops when the rules engine restarted between
		// deactivation and reactivation, because inactive strategies are absent from
		// its in-memory Paused map after bulk-load; and a plain CONFIG_UPDATED must
		// never re-run the backfill.)
		strategyUUID, err := uuid.Parse(thin.StrategyID)
		if err != nil {
			return false, fmt.Errorf("invalid strategy_id in STRATEGY_ACTIVATED outbox id=%d: %w", event.ID, err)
		}
		fullStrategy, err := w.repo.GetByID(ctx, strategyUUID, thin.UserID)
		if err != nil {
			return false, fmt.Errorf("failed to fetch strategy for STRATEGY_ACTIVATED outbox id=%d: %w", event.ID, err)
		}
		// Scope the reactivation backfill to the fresh AMN pick just persisted. Only
		// AMN strategies carry a selection; non-AMN strategies get an empty list and
		// the rules-engine skips the backfill trigger.
		if fullStrategy.ProcessAfterMarketNews {
			isins, isinErr := w.repo.GetLatestActivationISINs(ctx, strategyUUID)
			if isinErr != nil {
				return false, fmt.Errorf("failed to load AMN selection for STRATEGY_ACTIVATED outbox id=%d: %w", event.ID, isinErr)
			}
			fullStrategy.AMNSelectedStocks = isins
		}
		kafkaEvent = events.ToFullConfigEvent(events.ConfigActivated, fullStrategy)

	case "STRATEGY_DEACTIVATED":
		var thin activationOutboxPayload
		if err := json.Unmarshal(event.Payload, &thin); err != nil {
			return false, fmt.Errorf("failed to unmarshal STRATEGY_DEACTIVATED payload for outbox id=%d: %w payload=%s", event.ID, err, truncate(string(event.Payload), 500))
		}
		if thin.UserID == "" || thin.StrategyID == "" {
			log.Printf("[OutboxWorker] CRITICAL: empty user_id/strategy_id in STRATEGY_DEACTIVATED outbox id=%d, skipping+marking processed", event.ID)
			return true, nil
		}
		kafkaEvent = events.ToThinConfigEvent(events.ConfigPaused, thin.UserID, thin.StrategyID, thin.Version)

	default:
		log.Printf("[OutboxWorker] WARNING: unknown outbox event_type=%s id=%d, skipping+marking processed", event.EventType, event.ID)
		return true, nil
	}

	if kafkaEvent.UserID == "" {
		log.Printf("[OutboxWorker] CRITICAL: computed kafka event has empty user_id for outbox id=%d, skipping+marking processed", event.ID)
		return true, nil
	}

	payload, err := json.Marshal(kafkaEvent)
	if err != nil {
		return false, fmt.Errorf("failed to marshal kafka event for outbox id=%d: %w", event.ID, err)
	}

	msg := kafka.Message{
		Key:   []byte(kafkaEvent.UserID),
		Value: payload,
		Headers: []kafka.Header{
			{Key: "event_type", Value: []byte(string(kafkaEvent.Type))},
			{Key: "user_id", Value: []byte(kafkaEvent.UserID)},
		},
	}

	writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := w.kafkaWriter.WriteMessages(writeCtx, msg); err != nil {
		return false, fmt.Errorf("kafka publish failed for outbox id=%d: %w", event.ID, err)
	}

	return true, nil
}

func truncate(s string, max int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) <= max {
		return s
	}
	return s[:max]
}
