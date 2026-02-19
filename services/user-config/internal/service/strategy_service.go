package service

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/repository"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

// StrategyService handles business logic for strategies
type StrategyService struct {
	repo         *repository.StrategyRepository
	kafkaWriter  *kafka.Writer
	kafkaTopic   string
	kafkaEnabled bool
}

// NewStrategyService creates a new strategy service
func NewStrategyService(repo *repository.StrategyRepository, kafkaWriter *kafka.Writer, kafkaTopic string) *StrategyService {
	return &StrategyService{
		repo:         repo,
		kafkaWriter:  kafkaWriter,
		kafkaTopic:   kafkaTopic,
		kafkaEnabled: kafkaWriter != nil,
	}
}

// ConfigEvent represents a strategy configuration event for Kafka
type ConfigEvent struct {
	EventType string           `json:"event_type"` // CREATE, UPDATE, DELETE, ACTIVATE, DEACTIVATE
	Strategy  *models.Strategy `json:"strategy"`
	Timestamp int64            `json:"timestamp"`
}

// publishToKafka publishes a strategy event to Kafka
// Note: This is now redundant for transactional consistency as Repo writes to Outbox,
// but kept here for "fire and forget" if needed or for immediate debug logging.
// The actual reliability comes from the Outbox Poller (to be implemented/running separately).
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
	fmt.Printf("[USER-CONFIG] Publishing to Kafka: event_type=%s, strategy_id=%s, user_id=%s, mode=%s\n",
		eventType, strategy.StrategyID.String(), strategy.UserID, strategy.TradingMode)
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
			{Key: "trading_mode", Value: []byte(string(strategy.TradingMode))},
		},
	}

	err = s.kafkaWriter.WriteMessages(ctx, msg)
	if err != nil {
		return fmt.Errorf("failed to publish to kafka: %w", err)
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

// CreateStrategy creates a new strategy
func (s *StrategyService) CreateStrategy(ctx context.Context, req *models.CreateStrategyRequest) (*models.Strategy, error) {
	// Validate request
	if err := s.validateCreateRequest(req); err != nil {
		return nil, fmt.Errorf("validation failed: %w", err)
	}

	// Create strategy in database (includes Outbox insertion)
	strategy, err := s.repo.Create(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to create strategy: %w", err)
	}

	// NOTE: We don't strictly need to publish here because Repo inserts to Outbox.
	// However, if we want immediate feedback without waiting for Outbox Poller, we can try.
	// But to avoid duplicates, the consumers should be idempotent OR we rely solely on Outbox Poller.
	// For now, keeping it as is for backward compat/debug, assuming consumers handle idempotency.

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

	// Update strategy in database (includes Outbox insertion)
	strategy, err := s.repo.Update(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to update strategy: %w", err)
	}

	return strategy, nil
}

// DeleteStrategy deletes a strategy
func (s *StrategyService) DeleteStrategy(ctx context.Context, strategyID uuid.UUID, userID string) error {
	// Delete from database (includes Outbox insertion)
	if err := s.repo.Delete(ctx, strategyID, userID); err != nil {
		return fmt.Errorf("failed to delete strategy: %w", err)
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

	// Default trading mode
	if req.TradingMode == "" {
		req.TradingMode = models.TradingModePaper
	}
	if req.TradingMode != models.TradingModePaper && req.TradingMode != models.TradingModeLive {
		return fmt.Errorf("invalid trading_mode: %s", req.TradingMode)
	}

	// Validate conditions
	if req.Conditions.ImpactScoreMin < 0 || req.Conditions.ImpactScoreMin > 10 {
		return fmt.Errorf("impact_score_min must be between 0 and 10")
	}
	if req.Conditions.ImpactScoreMax < 0 || req.Conditions.ImpactScoreMax > 10 {
		return fmt.Errorf("impact_score_max must be between 0 and 10")
	}
	if req.Conditions.ImpactScoreMin > req.Conditions.ImpactScoreMax {
		return fmt.Errorf("impact_score_min cannot be greater than impact_score_max")
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
	if req.TradeConfig.OrderSide != "BUY" && req.TradeConfig.OrderSide != "SELL" {
		return fmt.Errorf("order_side must be BUY or SELL")
	}

	// Validate stop loss / take profit
	if req.TradeConfig.StopLossPct != nil && *req.TradeConfig.StopLossPct < 0 {
		return fmt.Errorf("stop_loss_pct must be non-negative")
	}
	if req.TradeConfig.TakeProfitPct != nil && *req.TradeConfig.TakeProfitPct < 0 {
		return fmt.Errorf("take_profit_pct must be non-negative")
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
		if req.Conditions.ImpactScoreMin < 0 || req.Conditions.ImpactScoreMin > 10 {
			return fmt.Errorf("impact_score_min must be between 0 and 10")
		}
		if req.Conditions.ImpactScoreMax < 0 || req.Conditions.ImpactScoreMax > 10 {
			return fmt.Errorf("impact_score_max must be between 0 and 10")
		}
		if req.Conditions.ImpactScoreMin > req.Conditions.ImpactScoreMax {
			return fmt.Errorf("impact_score_min cannot be greater than impact_score_max")
		}
	}

	if req.TradeConfig != nil {
		if req.TradeConfig.Quantity <= 0 {
			return fmt.Errorf("quantity must be greater than 0")
		}
		if req.TradeConfig.StopLossPct != nil && *req.TradeConfig.StopLossPct < 0 {
			return fmt.Errorf("stop_loss_pct must be non-negative")
		}
		if req.TradeConfig.TakeProfitPct != nil && *req.TradeConfig.TakeProfitPct < 0 {
			return fmt.Errorf("take_profit_pct must be non-negative")
		}
	}
	
	if req.TradingMode != nil {
		if *req.TradingMode != models.TradingModePaper && *req.TradingMode != models.TradingModeLive {
			return fmt.Errorf("invalid trading_mode: %s", *req.TradingMode)
		}
	}

	return nil
}
