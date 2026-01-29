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
	// Optional dedicated Kafka stream for managed Cash 52W strategy configs
	cash52wWriter       *kafka.Writer
	cash52wTopic        string
	cash52wKafkaEnabled bool
}

// NewStrategyService creates a new strategy service
func NewStrategyService(repo *repository.StrategyRepository, kafkaWriter *kafka.Writer, kafkaTopic string, jobbingWriter *kafka.Writer, jobbingTopic string, cash52wWriter *kafka.Writer, cash52wTopic string) *StrategyService {
	return &StrategyService{
		repo:                repo,
		kafkaWriter:         kafkaWriter,
		kafkaTopic:          kafkaTopic,
		kafkaEnabled:        kafkaWriter != nil,
		jobbingWriter:       jobbingWriter,
		jobbingTopic:        jobbingTopic,
		jobbingKafkaEnabled: jobbingWriter != nil && jobbingTopic != "",
		cash52wWriter:       cash52wWriter,
		cash52wTopic:        cash52wTopic,
		cash52wKafkaEnabled: cash52wWriter != nil && cash52wTopic != "",
	}
}

// Cash52WeekConfigEvent is a minimal, 52W-specific configuration event
// published to a dedicated Kafka topic so downstream services (such as
// rules-engine) don't have to parse the full generic Strategy payload.
//
// EventType indicates what happened to the managed 52W strategy for
// this user: CREATE, UPDATE, or DELETE.
type Cash52WeekConfigEvent struct {
	EventType       string  `json:"event_type"`
	UserID          string  `json:"user_id"`
	Enabled         bool    `json:"enabled"`
	CapitalPerStock float64 `json:"capital_per_stock"`
	TradingMode     string  `json:"trading_mode"` // "LIVE" or "PAPER"
	Timestamp       int64   `json:"timestamp"`
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

	// Determine prior state from the dedicated 52W config table so we no
	// longer depend on generic strategies/trade_configs rows.
	existingCfg, err := s.repo.GetCash52WConfig(ctx, req.UserID)
	if err != nil {
		return nil, fmt.Errorf("failed to load existing cash 52w config: %w", err)
	}
	hadExisting := existingCfg != nil

	// Persist the minimal 52W configuration into the dedicated
	// cash52w_configs table so that production systems don't rely on
	// generic strategies/trade_configs with dummy values.
	cfg := &models.Cash52WConfig{
		UserID:          req.UserID,
		Enabled:         req.Enabled,
		CapitalPerStock: capitalPerStock,
		TradingMode:     tradingMode,
	}

	// Determine event type and update DB accordingly.
	var eventType string
	if !req.Enabled {
		if hadExisting {
			// Disable existing config.
			if err := s.repo.DeleteCash52WConfig(ctx, req.UserID); err != nil {
				return nil, fmt.Errorf("failed to delete cash52w_config: %w", err)
			}
			eventType = "DELETE"
		} else {
			// No prior config and disabling -> nothing to do.
			return nil, nil
		}
	} else {
		// Enable or update config.
		if err := s.repo.UpsertCash52WConfig(ctx, cfg); err != nil {
			return nil, fmt.Errorf("failed to upsert cash52w_config: %w", err)
		}
		if !hadExisting {
			eventType = "CREATE"
		} else {
			eventType = "UPDATE"
		}
	}

	// Build a synthetic Strategy object purely for response and for
	// publishing the compact 52W config event. We no longer create or
	// update generic strategy/trade_config/risk_limits rows for 52W.
	now := time.Now()
	strategy := &models.Strategy{
		StrategyID:   uuid.New(),
		UserID:       req.UserID,
		StrategyName: "Cash 52W High",
		Description:  "Managed Cash 52-Week High breakout strategy",
		Active:       req.Enabled,
		TradingMode:  tradingMode,
		Version:      1,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	// Expose capital_per_stock via TradeConfig.MaxPositionSize so that
	// gateways that still map response from Strategy can derive the
	// configured capital.
	strategy.TradeConfig = &models.TradeConfig{
		MaxPositionSize: &capitalPerStock,
	}

	// IMPORTANT: For the managed 52W strategy we no longer publish the
	// generic Strategy payload to the main user-configs topic. Instead we
	// only emit the compact Cash52WeekConfigEvent to the dedicated
	// Cash52WConfigTopic so that downstream services don't have to
	// interpret a full generic strategy object for this managed case.
	if eventType != "" {
		if err := s.publishCash52WeekConfig(ctx, strategy, capitalPerStock, tradingMode, req.Enabled, eventType); err != nil {
			fmt.Printf("Warning: failed to publish Cash52W config to dedicated topic: %v\n", err)
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

	// Skip generic publishing for the managed Cash 52W strategy. All
	// configuration changes for this strategy are now emitted via the
	// dedicated Cash52WeekConfigEvent on the user-configs.cash52w topic,
	// so there is no need to send a full generic Strategy payload (with
	// trade_config, risk_limits, etc.) to the main user-configs stream.
	if strategy != nil {
		name := strings.ToUpper(strings.TrimSpace(strategy.StrategyName))
		if name == "CASH 52W HIGH" {
			return nil
		}
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

// publishCash52WeekConfig publishes a compact 52W-only configuration event
// to the dedicated Cash52W Kafka topic, if configured.
func (s *StrategyService) publishCash52WeekConfig(ctx context.Context, strategy *models.Strategy, capitalPerStock float64, tradingMode string, enabled bool, eventType string) error {
	if !s.cash52wKafkaEnabled || s.cash52wWriter == nil || strategy == nil {
		return nil
	}

	event := Cash52WeekConfigEvent{
		EventType:       eventType,
		UserID:          strategy.UserID,
		Enabled:         enabled,
		CapitalPerStock: capitalPerStock,
		TradingMode:     tradingMode,
		Timestamp:       strategy.UpdatedAt.Unix(),
	}

	eventBytes, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal Cash52W config event: %w", err)
	}

	// Log a concise 52W-specific line so we can easily trace all managed
	// 52W configuration changes without looking at the generic strategy
	// events.
	fmt.Printf("[USER-CONFIG][52W] event_type=%s user_id=%s enabled=%v capital=%.2f mode=%s\\n",
		event.EventType, event.UserID, event.Enabled, event.CapitalPerStock, event.TradingMode)

	msg := kafka.Message{
		Key:   []byte(strategy.UserID),
		Value: eventBytes,
	}

	if err := s.cash52wWriter.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("failed to publish Cash52W config event to kafka: %w", err)
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
