package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/repository"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

// StrategyService handles business logic for strategies
type StrategyService struct {
	repo *repository.StrategyRepository
	// Primary Kafka stream for all strategy events (user-configs topic)
	kafkaWriter  *kafka.Writer
	kafkaTopic   string
	kafkaEnabled bool
	// Optional dedicated Kafka stream for JOBBING strategies only
	jobbingWriter       *kafka.Writer
	jobbingTopic        string
	jobbingKafkaEnabled bool
}

// NewStrategyService creates a new strategy service
func NewStrategyService(repo *repository.StrategyRepository, kafkaWriter *kafka.Writer, kafkaTopic string, jobbingWriter *kafka.Writer, jobbingTopic string) *StrategyService {
	return &StrategyService{
		repo:                repo,
		kafkaWriter:         kafkaWriter,
		kafkaTopic:          kafkaTopic,
		kafkaEnabled:        kafkaWriter != nil,
		jobbingWriter:       jobbingWriter,
		jobbingTopic:        jobbingTopic,
		jobbingKafkaEnabled: jobbingWriter != nil && jobbingTopic != "",
	}
}

// ConfigureCash52WeekStrategy creates or updates the managed Cash 52-week High
// strategy for a user based on a small set of high-level parameters. This
// hides most of the low-level fields from the frontend.
func (s *StrategyService) ConfigureCash52WeekStrategy(ctx context.Context, req *models.ConfigureCash52WeekStrategyRequest) (*models.Strategy, error) {
	if req.UserID == "" {
		return nil, fmt.Errorf("user_id is required")
	}

	// Backend defaults if caller doesn't specify overrides
	capitalPerStock := req.CapitalPerStock
	if capitalPerStock <= 0 {
		capitalPerStock = 20000 // ₹20,000 per stock
	}
	maxPositions := req.MaxPositions
	if maxPositions <= 0 {
		maxPositions = 25
	}
	stopLossPct := req.StopLossPct
	if stopLossPct <= 0 {
		stopLossPct = 10
	}
	takeProfitPct := req.TakeProfitPct
	if takeProfitPct <= 0 {
		takeProfitPct = 20
	}

	// Normalise trading mode; default to LIVE when empty/invalid. This will
	// be stored on the strategy row and propagated via Kafka to rules-engine.
	tradingMode := strings.ToUpper(strings.TrimSpace(req.TradingMode))
	if tradingMode != "PAPER" {
		tradingMode = "LIVE"
	}

	// Delegate actual persistence logic to repository helper that knows how to
	// find/create the CASH_52W_HIGH strategy for this user. For now we keep
	// this high-level API here; repository implements the DB details.
	strategy, err := s.repo.ConfigureCash52WeekStrategy(ctx, req.UserID, capitalPerStock, maxPositions, stopLossPct, takeProfitPct, req.RiskProfile, tradingMode, req.Enabled)
	if err != nil {
		return nil, fmt.Errorf("failed to configure cash 52w strategy: %w", err)
	}

	// Publish config change to Kafka as a normal strategy event
	if strategy != nil {
		if err := s.publishToKafka(ctx, "UPDATE", strategy); err != nil {
			fmt.Printf("Warning: failed to publish 52w config to kafka: %v\n", err)
		}
	}

	return strategy, nil
}

// ConfigEvent represents a strategy configuration event for Kafka
type ConfigEvent struct {
	EventType string           `json:"event_type"` // CREATE, UPDATE, DELETE, ACTIVATE, DEACTIVATE
	Strategy  *models.Strategy `json:"strategy"`
	Timestamp int64            `json:"timestamp"`
}

// publishToKafka publishes a strategy event to Kafka
func (s *StrategyService) publishToKafka(ctx context.Context, eventType string, strategy *models.Strategy) error {
	if !s.kafkaEnabled {
		return nil // Kafka is disabled, skip publishing
	}

	event := ConfigEvent{
		EventType: eventType,
		Strategy:  strategy,
		Timestamp: strategy.UpdatedAt.Unix(),
	}

	eventBytes, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	// Log the exact JSON being published for debugging
	fmt.Printf("[USER-CONFIG] Publishing to Kafka: event_type=%s, strategy_id=%s, user_id=%s\n",
		eventType, strategy.StrategyID.String(), strategy.UserID)
	if strategy.TradeConfig != nil {
		fmt.Printf("[USER-CONFIG] TradeConfig: order_type=%s, quantity=%d, exchange=%s, stop_loss=%.2f, take_profit=%.2f\n",
			strategy.TradeConfig.OrderType, strategy.TradeConfig.Quantity, strategy.TradeConfig.Exchange,
			valueOrZero(strategy.TradeConfig.StopLossPct), valueOrZero(strategy.TradeConfig.TakeProfitPct))
	} else {
		fmt.Printf("[USER-CONFIG] WARNING: TradeConfig is nil!\n")
	}
	fmt.Printf("[USER-CONFIG] JSON payload (first 500 chars): %s\n", string(eventBytes[:min(500, len(eventBytes))]))

	msg := kafka.Message{
		Key:   []byte(strategy.StrategyID.String()),
		Value: eventBytes,
		Headers: []kafka.Header{
			{Key: "event_type", Value: []byte(eventType)},
			{Key: "user_id", Value: []byte(strategy.UserID)},
		},
	}

	// Publish to main user-configs topic
	if err := s.kafkaWriter.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("failed to publish to kafka: %w", err)
	}

	// Optionally mirror JOBBING strategies to a dedicated topic so
	// rules-engine can subscribe to a focused stream.
	if s.jobbingKafkaEnabled && isJobbingStrategy(strategy) {
		if err := s.jobbingWriter.WriteMessages(ctx, msg); err != nil {
			fmt.Printf("Warning: failed to publish jobbing config to kafka: %v\n", err)
		}
	}

	return nil
}

// Helper function to safely get float pointer value
func valueOrZero(ptr *float64) float64 {
	if ptr == nil {
		return 0.0
	}
	return *ptr
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// isJobbingStrategy determines whether the given strategy should be treated
// as a JOBBING strategy for the purpose of emitting dedicated jobbing
// configuration events. We use a simple naming convention here.
func isJobbingStrategy(strategy *models.Strategy) bool {
	if strategy == nil {
		return false
	}
	name := strings.ToUpper(strings.TrimSpace(strategy.StrategyName))
	if name == "" {
		return false
	}
	return name == "JOBBING" || strings.HasPrefix(name, "JOBBING_")
}

// CreateStrategy creates a new strategy
func (s *StrategyService) CreateStrategy(ctx context.Context, req *models.CreateStrategyRequest) (*models.Strategy, error) {
	// Validate request
	if err := s.validateCreateRequest(req); err != nil {
		return nil, fmt.Errorf("validation failed: %w", err)
	}

	// Create strategy in database
	strategy, err := s.repo.Create(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to create strategy: %w", err)
	}

	// Publish to Kafka
	if err := s.publishToKafka(ctx, "CREATE", strategy); err != nil {
		// Log error but don't fail the operation
		fmt.Printf("Warning: failed to publish to kafka: %v\n", err)
	}

	return strategy, nil
}

// GetStrategy retrieves a strategy by ID
func (s *StrategyService) GetStrategy(ctx context.Context, strategyID uuid.UUID, userID string) (*models.Strategy, error) {
	return s.repo.GetByID(ctx, strategyID, userID)
}

// ListUserStrategies lists all strategies for a user
func (s *StrategyService) ListUserStrategies(ctx context.Context, userID string, activeOnly bool, limit, offset int) ([]*models.Strategy, int, error) {
	// Set default pagination
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	if offset < 0 {
		offset = 0
	}

	return s.repo.ListByUserID(ctx, userID, activeOnly, limit, offset)
}

// UpdateStrategy updates a strategy
func (s *StrategyService) UpdateStrategy(ctx context.Context, req *models.UpdateStrategyRequest) (*models.Strategy, error) {
	// Validate request
	if err := s.validateUpdateRequest(req); err != nil {
		return nil, fmt.Errorf("validation failed: %w", err)
	}

	// Update strategy in database
	strategy, err := s.repo.Update(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to update strategy: %w", err)
	}

	// Publish to Kafka
	if err := s.publishToKafka(ctx, "UPDATE", strategy); err != nil {
		fmt.Printf("Warning: failed to publish to kafka: %v\n", err)
	}

	return strategy, nil
}

// DeleteStrategy deletes a strategy
func (s *StrategyService) DeleteStrategy(ctx context.Context, strategyID uuid.UUID, userID string) error {
	// Get strategy before deletion for Kafka event
	strategy, err := s.repo.GetByID(ctx, strategyID, userID)
	if err != nil {
		return fmt.Errorf("failed to get strategy: %w", err)
	}

	// Delete from database
	if err := s.repo.Delete(ctx, strategyID, userID); err != nil {
		return fmt.Errorf("failed to delete strategy: %w", err)
	}

	// Publish to Kafka
	if err := s.publishToKafka(ctx, "DELETE", strategy); err != nil {
		fmt.Printf("Warning: failed to publish to kafka: %v\n", err)
	}

	return nil
}

// ActivateStrategy activates a strategy
func (s *StrategyService) ActivateStrategy(ctx context.Context, strategyID uuid.UUID, userID string) (*models.Strategy, error) {
	// Activate in database
	if err := s.repo.Activate(ctx, strategyID, userID); err != nil {
		return nil, fmt.Errorf("failed to activate strategy: %w", err)
	}

	// Get updated strategy
	strategy, err := s.repo.GetByID(ctx, strategyID, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to get strategy: %w", err)
	}

	// Publish to Kafka
	if err := s.publishToKafka(ctx, "ACTIVATE", strategy); err != nil {
		fmt.Printf("Warning: failed to publish to kafka: %v\n", err)
	}

	return strategy, nil
}

// DeactivateStrategy deactivates a strategy
func (s *StrategyService) DeactivateStrategy(ctx context.Context, strategyID uuid.UUID, userID string) (*models.Strategy, error) {
	// Deactivate in database
	if err := s.repo.Deactivate(ctx, strategyID, userID); err != nil {
		return nil, fmt.Errorf("failed to deactivate strategy: %w", err)
	}

	// Get updated strategy
	strategy, err := s.repo.GetByID(ctx, strategyID, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to get strategy: %w", err)
	}

	// Publish to Kafka
	if err := s.publishToKafka(ctx, "DEACTIVATE", strategy); err != nil {
		fmt.Printf("Warning: failed to publish to kafka: %v\n", err)
	}

	return strategy, nil
}

// GetStrategiesByIDs retrieves multiple strategies by their IDs
func (s *StrategyService) GetStrategiesByIDs(ctx context.Context, strategyIDs []uuid.UUID) ([]*models.Strategy, error) {
	return s.repo.GetByIDs(ctx, strategyIDs)
}

// validateCreateRequest validates a create strategy request
func (s *StrategyService) validateCreateRequest(req *models.CreateStrategyRequest) error {
	if req.UserID == "" {
		return fmt.Errorf("user_id is required")
	}
	if req.StrategyName == "" {
		return fmt.Errorf("strategy_name is required")
	}
	if req.Conditions == nil {
		return fmt.Errorf("conditions are required")
	}
	if req.TradeConfig == nil {
		return fmt.Errorf("trade_config is required")
	}
	if req.RiskLimits == nil {
		return fmt.Errorf("risk_limits are required")
	}

	// Validate conditions
	if req.Conditions.ImpactScoreThreshold < 1 || req.Conditions.ImpactScoreThreshold > 10 {
		return fmt.Errorf("impact_score_threshold must be between 1 and 10")
	}

	// Validate trade config
	if req.TradeConfig.Quantity <= 0 {
		return fmt.Errorf("quantity must be greater than 0")
	}
	if req.TradeConfig.OrderType == "" {
		return fmt.Errorf("order_type is required")
	}
	if req.TradeConfig.Exchange == "" {
		return fmt.Errorf("exchange is required")
	}

	return nil
}

// validateUpdateRequest validates an update strategy request
func (s *StrategyService) validateUpdateRequest(req *models.UpdateStrategyRequest) error {
	if req.StrategyID == uuid.Nil {
		return fmt.Errorf("strategy_id is required")
	}
	if req.UserID == "" {
		return fmt.Errorf("user_id is required")
	}
	if req.Version < 1 {
		return fmt.Errorf("version must be greater than 0")
	}

	// Validate optional fields if provided
	if req.Conditions != nil {
		if req.Conditions.ImpactScoreThreshold < 1 || req.Conditions.ImpactScoreThreshold > 10 {
			return fmt.Errorf("impact_score_threshold must be between 1 and 10")
		}
	}

	if req.TradeConfig != nil {
		if req.TradeConfig.Quantity <= 0 {
			return fmt.Errorf("quantity must be greater than 0")
		}
	}

	return nil
}
