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

	msg := kafka.Message{
		Key:   []byte(strategy.StrategyID.String()),
		Value: eventBytes,
		Headers: []kafka.Header{
			{Key: "event_type", Value: []byte(eventType)},
			{Key: "user_id", Value: []byte(strategy.UserID)},
		},
	}

	err = s.kafkaWriter.WriteMessages(ctx, msg)
	if err != nil {
		return fmt.Errorf("failed to publish to kafka: %w", err)
	}

	return nil
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
