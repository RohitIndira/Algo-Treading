package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/repository"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

func floatPtr(v float64) *float64 { return &v }

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

// ListUserStrategies lists all strategies for a user.
func (s *StrategyService) ListUserStrategies(ctx context.Context, userID string, activeOnly bool, limit, offset int) ([]*models.Strategy, int, error) {
	// Set default pagination
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	if offset < 0 {
		offset = 0
	}

	strategies, total, err := s.repo.ListByUserID(ctx, userID, activeOnly, limit, offset)
	if err != nil {
		return nil, 0, err
	}

	return strategies, total, nil
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

// ========================================================================
// Jobbing Strategy Configuration Service Methods
// ========================================================================

// ConfigureJobbingStrategy creates or updates jobbing configurations for multiple tokens for a user
func (s *StrategyService) ConfigureJobbingStrategy(ctx context.Context, req *models.ConfigureJobbingStrategyRequest) (*models.ConfigureJobbingStrategyResponse, error) {
	if req.UserID == "" {
		return nil, fmt.Errorf("user_id is required")
	}

	if len(req.Configs) == 0 {
		return nil, fmt.Errorf("at least one token configuration is required")
	}

	var savedConfigs []models.JobbingConfig
	var errors []string

	for i, tokenCfg := range req.Configs {
		// Apply defaults
		tokenCfg.ApplyDefaults()

		// Validate
		if err := tokenCfg.Validate(); err != nil {
			errors = append(errors, fmt.Sprintf("config[%d]: %v", i, err))
			continue
		}

		// Build JobbingConfig model
		cfg := &models.JobbingConfig{
			ID:               uuid.New(),
			UserID:           req.UserID,
			StrategyID:       "JOBBING",
			Token:            tokenCfg.Token,
			Symbol:           tokenCfg.Symbol,
			Exchange:         tokenCfg.Exchange,
			LowerRange:       tokenCfg.LowerRange,
			HigherRange:      tokenCfg.HigherRange,
			InitialBuyOffset: *tokenCfg.InitialBuyOffset,
			DistanceContinue: *tokenCfg.DistanceContinue,
			QuantityPerOrder: *tokenCfg.QuantityPerOrder,
			MaxQuantity:      *tokenCfg.MaxQuantity,
			TradingMode:      *tokenCfg.TradingMode,
			Enabled:          *tokenCfg.Enabled,
		}

		// Save to database
		if err := s.repo.UpsertJobbingConfig(ctx, cfg); err != nil {
			errors = append(errors, fmt.Sprintf("token %s: failed to save: %v", tokenCfg.Token, err))
			continue
		}

		savedConfigs = append(savedConfigs, *cfg)

		// Publish Kafka event for this config
		eventType := "CREATED"
		existingCfg, _ := s.repo.GetJobbingConfig(ctx, req.UserID, tokenCfg.Token)
		if existingCfg != nil {
			eventType = "UPDATED"
		}

		if err := s.publishJobbingConfigEvent(ctx, eventType, *cfg); err != nil {
			// Log error but don't fail the request
			errors = append(errors, fmt.Sprintf("token %s: kafka publish warning: %v", tokenCfg.Token, err))
		}
	}

	if len(savedConfigs) == 0 && len(errors) > 0 {
		return nil, fmt.Errorf("failed to save any configurations: %s", strings.Join(errors, "; "))
	}

	response := &models.ConfigureJobbingStrategyResponse{
		Success:    true,
		Message:    "Jobbing strategy configured successfully",
		UserID:     req.UserID,
		Configs:    savedConfigs,
		TotalCount: len(savedConfigs),
	}

	if len(errors) > 0 {
		response.Message = fmt.Sprintf("Configured %d tokens with warnings: %s", len(savedConfigs), strings.Join(errors, "; "))
	}

	return response, nil
}

// GetJobbingConfigs retrieves all jobbing configurations for a user
func (s *StrategyService) GetJobbingConfigs(ctx context.Context, userID string, enabledOnly bool) ([]models.JobbingConfig, error) {
	if userID == "" {
		return nil, fmt.Errorf("user_id is required")
	}

	return s.repo.ListJobbingConfigs(ctx, userID, enabledOnly)
}

// GetJobbingConfig retrieves a single jobbing configuration for a user and token
func (s *StrategyService) GetJobbingConfig(ctx context.Context, userID, token string) (*models.JobbingConfig, error) {
	if userID == "" || token == "" {
		return nil, fmt.Errorf("user_id and token are required")
	}

	return s.repo.GetJobbingConfig(ctx, userID, token)
}

// UpdateJobbingConfig updates an existing jobbing configuration
func (s *StrategyService) UpdateJobbingConfig(ctx context.Context, req *models.UpdateJobbingConfigRequest) (*models.JobbingConfig, error) {
	if req.UserID == "" || req.Token == "" {
		return nil, fmt.Errorf("user_id and token are required")
	}

	// Fetch existing config
	existing, err := s.repo.GetJobbingConfig(ctx, req.UserID, req.Token)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch existing config: %w", err)
	}
	if existing == nil {
		return nil, fmt.Errorf("jobbing config not found for user %s and token %s", req.UserID, req.Token)
	}

	// Apply updates
	if req.LowerRange != nil {
		existing.LowerRange = *req.LowerRange
	}
	if req.HigherRange != nil {
		existing.HigherRange = *req.HigherRange
	}
	if req.InitialBuyOffset != nil {
		existing.InitialBuyOffset = *req.InitialBuyOffset
	}
	if req.DistanceContinue != nil {
		existing.DistanceContinue = *req.DistanceContinue
	}
	if req.QuantityPerOrder != nil {
		existing.QuantityPerOrder = *req.QuantityPerOrder
	}
	if req.MaxQuantity != nil {
		existing.MaxQuantity = *req.MaxQuantity
	}
	if req.TradingMode != nil {
		existing.TradingMode = *req.TradingMode
	}
	if req.Enabled != nil {
		existing.Enabled = *req.Enabled
	}

	// Validate updated config
	if existing.LowerRange <= 0 {
		return nil, fmt.Errorf("lower_range must be greater than 0")
	}
	if existing.HigherRange <= existing.LowerRange {
		return nil, fmt.Errorf("higher_range must be greater than lower_range")
	}
	if existing.MaxQuantity < existing.QuantityPerOrder {
		return nil, fmt.Errorf("max_quantity must be >= quantity_per_order")
	}

	// Save to database
	if err := s.repo.UpsertJobbingConfig(ctx, existing); err != nil {
		return nil, fmt.Errorf("failed to update jobbing config: %w", err)
	}

	// Publish Kafka event
	if err := s.publishJobbingConfigEvent(ctx, "UPDATED", *existing); err != nil {
		// Log warning but don't fail the request
		fmt.Printf("Warning: failed to publish jobbing config update event: %v\n", err)
	}

	return existing, nil
}

// DeleteJobbingConfig deletes a jobbing configuration for a user and token
func (s *StrategyService) DeleteJobbingConfig(ctx context.Context, userID, token string) error {
	if userID == "" || token == "" {
		return fmt.Errorf("user_id and token are required")
	}

	// Fetch existing config for Kafka event
	existing, err := s.repo.GetJobbingConfig(ctx, userID, token)
	if err != nil {
		return fmt.Errorf("failed to fetch config before delete: %w", err)
	}
	if existing == nil {
		return fmt.Errorf("jobbing config not found for user %s and token %s", userID, token)
	}

	// Delete from database
	if err := s.repo.DeleteJobbingConfig(ctx, userID, token); err != nil {
		return fmt.Errorf("failed to delete jobbing config: %w", err)
	}

	// Publish Kafka event
	if err := s.publishJobbingConfigEvent(ctx, "DELETED", *existing); err != nil {
		// Log warning but don't fail the request
		fmt.Printf("Warning: failed to publish jobbing config delete event: %v\n", err)
	}

	return nil
}

// EnableJobbingConfig enables a jobbing configuration
func (s *StrategyService) EnableJobbingConfig(ctx context.Context, userID, token string) error {
	if err := s.repo.UpdateJobbingConfigStatus(ctx, userID, token, true); err != nil {
		return err
	}

	// Fetch updated config for Kafka event
	cfg, err := s.repo.GetJobbingConfig(ctx, userID, token)
	if err != nil {
		return fmt.Errorf("failed to fetch config after enable: %w", err)
	}

	// Publish Kafka event
	if err := s.publishJobbingConfigEvent(ctx, "ENABLED", *cfg); err != nil {
		fmt.Printf("Warning: failed to publish jobbing config enable event: %v\n", err)
	}

	return nil
}

// DisableJobbingConfig disables a jobbing configuration
func (s *StrategyService) DisableJobbingConfig(ctx context.Context, userID, token string) error {
	if err := s.repo.UpdateJobbingConfigStatus(ctx, userID, token, false); err != nil {
		return err
	}

	// Fetch updated config for Kafka event
	cfg, err := s.repo.GetJobbingConfig(ctx, userID, token)
	if err != nil {
		return fmt.Errorf("failed to fetch config after disable: %w", err)
	}

	// Publish Kafka event
	if err := s.publishJobbingConfigEvent(ctx, "DISABLED", *cfg); err != nil {
		fmt.Printf("Warning: failed to publish jobbing config disable event: %v\n", err)
	}

	return nil
}

// publishJobbingConfigEvent publishes a jobbing configuration event to Kafka
func (s *StrategyService) publishJobbingConfigEvent(ctx context.Context, eventType string, cfg models.JobbingConfig) error {
	if !s.jobbingKafkaEnabled {
		return nil // Kafka disabled, skip
	}

	event := models.JobbingConfigEvent{
		EventType: eventType,
		Timestamp: time.Now(),
		UserID:    cfg.UserID, // Top-level for consumer compatibility
		Token:     cfg.Token,  // Top-level for consumer compatibility
		Config:    cfg,
	}

	eventJSON, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal jobbing config event: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(cfg.UserID + ":" + cfg.Token),
		Value: eventJSON,
		Headers: []kafka.Header{
			{Key: "event_type", Value: []byte(eventType)},
			{Key: "user_id", Value: []byte(cfg.UserID)},
			{Key: "token", Value: []byte(cfg.Token)},
			{Key: "timestamp", Value: []byte(time.Now().Format(time.RFC3339))},
		},
	}

	if err := s.jobbingWriter.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("failed to write jobbing config event to Kafka: %w", err)
	}

	return nil
}
