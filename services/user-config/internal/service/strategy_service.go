package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/repository"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

// StrategyService handles business logic for strategies
type StrategyService struct {
	repo         *repository.StrategyRepository
	credsRepo    repository.CredentialsRepository
	kafkaWriter  *kafka.Writer
	kafkaTopic   string
	kafkaEnabled bool
}

// NewStrategyService creates a new strategy service
func NewStrategyService(repo *repository.StrategyRepository, credsRepo repository.CredentialsRepository, kafkaWriter *kafka.Writer, kafkaTopic string) *StrategyService {
	return &StrategyService{
		repo:         repo,
		credsRepo:    credsRepo,
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

// UpdateUserCredentialsRequest holds the input for refreshing a user's broker
// auth (called from the SSO login flow + JWT refresh).
type UpdateUserCredentialsRequest struct {
	UserID       string // platform user id (== indiraUserID for Indira)
	IndiraUserID string
	AppID        string
	Source       string // WEB / IOS / AND
	BearerToken  string
}

// CredentialsEvent is the Kafka payload for USER_CREDENTIALS_UPDATED events.
// Consumers (trade-execution) use it to invalidate any cached JWT for this
// user so the protective replayer's 15:35 IST cron and the live order path
// both pick up the freshest token without waiting for the cache TTL.
//
// Field tags mirror the existing strategy events on the same topic
// (events/config_event.go) — `type` not `event_type` — so a single consumer
// can deserialize both shapes and dispatch on Type value.
type CredentialsEvent struct {
	Type      string `json:"type"`               // "USER_CREDENTIALS_UPDATED"
	UserID    string `json:"user_id"`
	AppID     string `json:"app_id,omitempty"`
	Source    string `json:"source,omitempty"`
	Timestamp int64  `json:"timestamp"`          // UnixNano
}

// UpdateUserCredentials encrypts + upserts the user's broker credentials in
// trading_execution.user_credentials, then publishes a USER_CREDENTIALS_UPDATED
// event to Kafka so any service caching the JWT can invalidate.
//
// Idempotent: ON CONFLICT (user_id) DO UPDATE on the underlying table — the
// latest call wins. Bypasses the outbox pattern: cache invalidation is best-
// effort with a 5-min TTL fallback in trade-execution's CredentialsCache, so
// at-least-once delivery isn't a hard requirement here.
func (s *StrategyService) UpdateUserCredentials(ctx context.Context, req UpdateUserCredentialsRequest) error {
	if req.UserID == "" {
		return fmt.Errorf("user_id is required")
	}
	if req.BearerToken == "" {
		return fmt.Errorf("bearer_token is required")
	}
	indiraUserID := req.IndiraUserID
	if indiraUserID == "" {
		indiraUserID = req.UserID
	}
	source := req.Source
	if source == "" {
		source = "WEB"
	}

	if err := s.credsRepo.StoreIndiraCredentials(ctx, req.UserID, indiraUserID, req.AppID, source, req.BearerToken); err != nil {
		return fmt.Errorf("store credentials: %w", err)
	}

	// Best-effort cache-invalidation event. Failure here doesn't roll back the
	// DB upsert — the consumer's TTL fallback covers staleness.
	if !s.kafkaEnabled {
		return nil
	}
	ev := CredentialsEvent{
		Type:      "USER_CREDENTIALS_UPDATED",
		UserID:    req.UserID,
		AppID:     req.AppID,
		Source:    source,
		Timestamp: time.Now().UnixNano(),
	}
	body, err := json.Marshal(ev)
	if err != nil {
		log.Printf("[user-config] WARN: marshal credentials event: %v", err)
		return nil
	}
	msg := kafka.Message{
		Key:   []byte(req.UserID),
		Value: body,
		Headers: []kafka.Header{
			{Key: "event_type", Value: []byte(ev.Type)},
			{Key: "user_id", Value: []byte(req.UserID)},
		},
	}
	if err := s.kafkaWriter.WriteMessages(ctx, msg); err != nil {
		log.Printf("[user-config] WARN: publish credentials event: %v", err)
		return nil
	}
	log.Printf("[user-config] credentials refreshed + event published for user=%s", req.UserID)
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

	// Persist Indira broker credentials so trade-execution can authenticate orders.
	// This is best-effort: a failure here does NOT abort the strategy creation.
	if req.IndiraAuth != nil && req.IndiraAuth.BearerToken != "" {
		if err := s.credsRepo.StoreIndiraCredentials(
			ctx,
			req.UserID,
			req.IndiraAuth.UserID,
			req.IndiraAuth.AppID,
			req.IndiraAuth.Source,
			req.IndiraAuth.BearerToken,
		); err != nil {
			log.Printf("[WARN] user-config: failed to store credentials for user %s: %v", req.UserID, err)
		} else {
			log.Printf("[INFO] user-config: stored Indira credentials for user %s", req.UserID)
		}
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

// ListAllActiveStrategies returns active, non-deleted strategies for startup bulk-load.
func (s *StrategyService) ListAllActiveStrategies(ctx context.Context, limit, offset int) ([]*models.Strategy, error) {
	return s.repo.ListAllActive(ctx, limit, offset)
}

// DeactivateAllActiveStrategies deactivates every active strategy (used by the
// end-of-day scheduler at market close). Returns the count of strategies deactivated.
func (s *StrategyService) DeactivateAllActiveStrategies(ctx context.Context) (int, error) {
	return s.repo.DeactivateAllActive(ctx)
}

// validateCreateRequest validates a create strategy request
func (s *StrategyService) validateCreateRequest(req *models.CreateStrategyRequest) error {
	if req.UserID == "" {
		return fmt.Errorf("user_id is required")
	}
	if req.StrategyName == "" {
		return fmt.Errorf("strategy_name is required")
	}
	// Default strategy type
	if req.StrategyType == "" {
		req.StrategyType = models.StrategyTypeNews
	}

	// Default trading mode
	if req.TradingMode == "" {
		req.TradingMode = models.TradingModePaper
	}
	if req.TradingMode != models.TradingModePaper && req.TradingMode != models.TradingModeLive {
		return fmt.Errorf("invalid trading_mode: %s", req.TradingMode)
	}

	// --- HFT_BIDDING strategy: all runtime params live in HFTConfig,
	// persisted as trade_configs.config_extra. No trade_config required
	// from the caller — checked before the generic trade_config gate. ---
	if req.StrategyType == models.StrategyTypeHFTBidding {
		if len(req.StrategyName) < 3 || len(req.StrategyName) > 100 {
			return fmt.Errorf("strategy_name must be between 3 and 100 characters")
		}
		if req.HFTConfig == nil {
			return fmt.Errorf("hft_config is required for HFT_BIDDING strategy")
		}
		h := req.HFTConfig
		if h.Symbol == "" {
			return fmt.Errorf("hft_config.symbol is required")
		}
		if h.ISIN == "" {
			return fmt.Errorf("hft_config.isin is required")
		}
		if h.Exchange == "" {
			h.Exchange = "NSE"
		}
		switch h.Side {
		case "":
			h.Side = "BOTH"
		case "BUY", "SELL", "BOTH":
			// ok
		default:
			return fmt.Errorf("hft_config.side must be BUY, SELL, or BOTH")
		}
		switch h.ProductType {
		case "":
			h.ProductType = "INTRADAY"
		case "INTRADAY", "DELIVERY", "CASH":
			// ok
		default:
			return fmt.Errorf("hft_config.product_type must be INTRADAY, DELIVERY, or CASH")
		}
		if h.TickSize <= 0 {
			h.TickSize = 0.05
		}
		if h.Side == "BUY" || h.Side == "BOTH" {
			if h.MaxBuyQty <= 0 {
				return fmt.Errorf("hft_config.max_buy_qty must be > 0 for side %s", h.Side)
			}
			if h.SingleBuyQty < 1 || h.SingleBuyQty > h.MaxBuyQty {
				return fmt.Errorf("hft_config.single_buy_qty must be between 1 and max_buy_qty")
			}
		}
		if h.Side == "SELL" || h.Side == "BOTH" {
			if h.MaxSellQty <= 0 {
				return fmt.Errorf("hft_config.max_sell_qty must be > 0 for side %s", h.Side)
			}
			if h.SingleSellQty < 1 || h.SingleSellQty > h.MaxSellQty {
				return fmt.Errorf("hft_config.single_sell_qty must be between 1 and max_sell_qty")
			}
		}
		if h.BuyLimitPrice < 0 || h.SellLimitPrice < 0 {
			return fmt.Errorf("hft_config limit prices must be non-negative")
		}
		// Trigger price gate (required per active side).
		// BUY arms when LTP >= buy_trigger_price; SELL arms when LTP >= sell_trigger_price.
		// Sanity guards:
		//   - BUY: trigger must sit BELOW the limit ceiling, otherwise we arm
		//     at a price already above the halt threshold and never trade.
		//   - SELL: trigger must sit ABOVE the limit floor, otherwise we arm
		//     at a price already below the floor and instantly halt.
		if h.Side == "BUY" || h.Side == "BOTH" {
			if h.BuyTriggerPrice <= 0 {
				return fmt.Errorf("hft_config.buy_trigger_price must be > 0 for side %s", h.Side)
			}
			if h.BuyLimitPrice > 0 && h.BuyTriggerPrice >= h.BuyLimitPrice {
				return fmt.Errorf("hft_config.buy_trigger_price (%.2f) must be < buy_limit_price (%.2f)", h.BuyTriggerPrice, h.BuyLimitPrice)
			}
		}
		if h.Side == "SELL" || h.Side == "BOTH" {
			if h.SellTriggerPrice <= 0 {
				return fmt.Errorf("hft_config.sell_trigger_price must be > 0 for side %s", h.Side)
			}
			if h.SellLimitPrice > 0 && h.SellTriggerPrice <= h.SellLimitPrice {
				return fmt.Errorf("hft_config.sell_trigger_price (%.2f) must be > sell_limit_price (%.2f)", h.SellTriggerPrice, h.SellLimitPrice)
			}
		}
		// Mode mirrors the strategy's trading mode — the hft-engine gates
		// on it to refuse a PAPER strategy on a LIVE engine and vice versa.
		h.Mode = string(req.TradingMode)

		// The hft-engine reads every runtime param from config_extra; the
		// trade_configs typed columns are unused for HFT, but the row must
		// still exist (FK + repo invariant). Fill harmless placeholders.
		if req.TradeConfig == nil {
			req.TradeConfig = &models.TradeConfig{}
		}
		req.TradeConfig.OrderType = "LIMIT"
		req.TradeConfig.ProductType = h.ProductType
		req.TradeConfig.Validity = "DAY"
		req.TradeConfig.Exchange = h.Exchange
		req.TradeConfig.OrderSide = "BUY"
		req.TradeConfig.StopLossType = "FIXED"        // chk_stop_loss_type
		req.TradeConfig.PositionSizingMode = "FIXED_QTY" // trade_configs_position_sizing_mode_check
		if req.TradeConfig.Quantity <= 0 {
			req.TradeConfig.Quantity = 1
		}
		if req.Conditions == nil {
			req.Conditions = &models.StrategyCondition{}
		}
		if req.RiskLimits == nil {
			req.RiskLimits = &models.RiskLimits{
				EnableRiskChecks:    true,
				EnableAutoSquareOff: false,
			}
		}
		return nil
	}

	if req.TradeConfig == nil {
		return fmt.Errorf("trade_config is required")
	}

	// --- 52W_BREAKOUT strategy: production-grade validation ---
	if req.StrategyType == models.StrategyType52WBreakout {

		// Strategy name: 3-100 chars, no SQL injection
		if len(req.StrategyName) < 3 {
			return fmt.Errorf("strategy_name must be at least 3 characters")
		}
		if len(req.StrategyName) > 100 {
			return fmt.Errorf("strategy_name must be less than 100 characters")
		}

		// Total capital: min ₹10,000, max ₹10 crore
		if req.TradeConfig.TotalCapital != nil {
			cap := *req.TradeConfig.TotalCapital
			if cap < 10000 {
				return fmt.Errorf("total_capital must be at least ₹10,000")
			}
			if cap > 100000000 { // 10 crore
				return fmt.Errorf("total_capital cannot exceed ₹10,00,00,000")
			}
		}

		// Max positions: 1-100
		if req.TradeConfig.MaxPositions != nil {
			pos := *req.TradeConfig.MaxPositions
			if pos < 1 || pos > 100 {
				return fmt.Errorf("max_positions must be between 1 and 100")
			}
		}

		// Stop loss: 0.1% - 50%
		if req.TradeConfig.StopLossPct != nil {
			sl := *req.TradeConfig.StopLossPct
			if sl < 0.1 || sl > 50 {
				return fmt.Errorf("stop_loss_pct must be between 0.1%% and 50%%")
			}
		} else {
			return fmt.Errorf("stop_loss_pct is required")
		}

		// Take profit: 0.1% - 100%
		if req.TradeConfig.TakeProfitPct != nil {
			tp := *req.TradeConfig.TakeProfitPct
			if tp < 0.1 || tp > 100 {
				return fmt.Errorf("take_profit_pct must be between 0.1%% and 100%%")
			}
		} else {
			return fmt.Errorf("take_profit_pct is required")
		}

		// Per stock amount must be meaningful (at least ₹100)
		// Set defaults
		if req.TradeConfig.PositionSizingMode == "" {
			req.TradeConfig.PositionSizingMode = "EMA_ALLOCATION"
		}
		if req.TradeConfig.TotalCapital == nil {
			defaultCap := 100000.0
			req.TradeConfig.TotalCapital = &defaultCap
		}
		if req.TradeConfig.MaxPositions == nil {
			defaultPos := int32(25)
			req.TradeConfig.MaxPositions = &defaultPos
		}

		// Auto-calculate per_stock_amount
		perStock := *req.TradeConfig.TotalCapital / float64(*req.TradeConfig.MaxPositions)
		if perStock < 100 {
			return fmt.Errorf("per_stock_amount too low (₹%.0f). Increase total_capital or reduce max_positions", perStock)
		}
		req.TradeConfig.PerStockAmount = &perStock

		// Fixed system defaults (user doesn't control these for 52W)
		req.TradeConfig.OrderType = "MARKET"
		req.TradeConfig.ProductType = "INTRADAY"
		req.TradeConfig.Validity = "DAY"
		req.TradeConfig.Exchange = "NSE"
		req.TradeConfig.OrderSide = "BUY"
		req.TradeConfig.StopLossType = "FIXED"
		if req.TradeConfig.Quantity <= 0 {
			req.TradeConfig.Quantity = 1
		}

		// Empty conditions (52W doesn't use news conditions)
		if req.Conditions == nil {
			req.Conditions = &models.StrategyCondition{}
		}

		// Default risk limits for 52W (no EOD square-off for positional)
		if req.RiskLimits == nil {
			req.RiskLimits = &models.RiskLimits{
				EnableRiskChecks:    true,
				EnableAutoSquareOff: false, // Positional — no auto square-off
			}
		}

		return nil
	}

	// --- MANTHAN strategy: minimal user input, backend fills everything ---
	if req.StrategyType == models.StrategyTypeManthan {
		if len(req.StrategyName) < 3 {
			return fmt.Errorf("strategy_name must be at least 3 characters")
		}
		if len(req.StrategyName) > 100 {
			return fmt.Errorf("strategy_name must be less than 100 characters")
		}

		// Total capital: min ₹5,00,000 (5 lakh), no upper limit
		if req.TradeConfig == nil {
			req.TradeConfig = &models.TradeConfig{}
		}
		if req.TradeConfig.TotalCapital == nil {
			return fmt.Errorf("total_capital is required (minimum ₹5,00,000)")
		}
		cap := *req.TradeConfig.TotalCapital
		if cap < 500000 {
			return fmt.Errorf("total_capital must be at least ₹5,00,000 for Manthan strategy")
		}

		// Max positions: ≤25L → 25 stocks, >25L → 50 stocks
		maxPos := int32(25)
		if cap > 2500000 {
			maxPos = 50
		}
		req.TradeConfig.MaxPositions = &maxPos

		perStock := cap / float64(maxPos)
		req.TradeConfig.PerStockAmount = &perStock

		// Fixed system defaults — user does not control these
		req.TradeConfig.PositionSizingMode = "EMA_ALLOCATION"
		req.TradeConfig.OrderType = "MARKET"
		req.TradeConfig.ProductType = "DELIVERY"
		req.TradeConfig.Validity = "DAY"
		req.TradeConfig.Exchange = "NSE"
		req.TradeConfig.OrderSide = "BUY"
		req.TradeConfig.StopLossType = "TRAILING"
		req.TradeConfig.Quantity = 1

		sl := 20.0  // 20% trailing SL from 52W high
		req.TradeConfig.StopLossPct = &sl

		trail := 2.0 // 2% trail increments
		req.TradeConfig.TrailingSLPct = &trail

		// No conditions (signal comes from manthan.signals pipeline)
		if req.Conditions == nil {
			req.Conditions = &models.StrategyCondition{}
		}

		// Risk limits — positional, no auto square-off
		if req.RiskLimits == nil {
			req.RiskLimits = &models.RiskLimits{
				EnableRiskChecks:    true,
				EnableAutoSquareOff: false,
			}
		}

		return nil
	}

	// --- NEWS strategy: existing validation ---
	if req.Conditions == nil {
		return fmt.Errorf("conditions are required")
	}
	if req.RiskLimits == nil {
		return fmt.Errorf("risk_limits are required")
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
