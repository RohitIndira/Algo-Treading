package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/apperr"
	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/repository"
	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/tradeexec"
	goredis "github.com/go-redis/redis/v8"
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

	// extRedis is the external Indira Redis where the symbol↔isin master
	// data lives (keys: `symbol:{TICKER}` → {"symbol":"...","isin":"..."}).
	// Used to derive ISIN at create-time when the caller supplies only a
	// symbol. May be nil — in that case the service still accepts requests
	// that already carry ISIN and rejects symbol-only requests with a clear
	// error so the caller can fall back to providing ISIN explicitly.
	extRedis *goredis.Client

	// tradeExec is the HTTP client to trade-execution's force-exit
	// endpoints. Used when Deactivate/Delete callers request
	// SQUARE_OFF_AT_MARKET. May be nil — in that case SQUARE_OFF_AT_MARKET
	// requests are rejected with a clear error (KEEP_POSITIONS_OPEN still
	// works). Set via SetTradeExecClient on boot.
	tradeExec *tradeexec.Client
}

// NewStrategyService creates a new strategy service
func NewStrategyService(repo *repository.StrategyRepository, credsRepo repository.CredentialsRepository, kafkaWriter *kafka.Writer, kafkaTopic string, extRedis *goredis.Client) *StrategyService {
	return &StrategyService{
		repo:         repo,
		credsRepo:    credsRepo,
		kafkaWriter:  kafkaWriter,
		kafkaTopic:   kafkaTopic,
		kafkaEnabled: kafkaWriter != nil,
		extRedis:     extRedis,
	}
}

// SetTradeExecClient wires the trade-execution HTTP client used by the
// SQUARE_OFF_AT_MARKET path of Deactivate + Delete. Optional — nil-safe
// (callers requesting square-off without a client wired get a clean error).
func (s *StrategyService) SetTradeExecClient(c *tradeexec.Client) {
	s.tradeExec = c
}

// resolveISINFromSymbol looks up the ISIN for a symbol via the ext-Redis
// `symbol:{TICKER}` master-data key. Returns the ISIN, or an error if
// ext-Redis isn't wired, the symbol is unknown, or the blob is malformed.
// 2-second bound on the Redis call so a slow network doesn't hang validation.
func (s *StrategyService) resolveISINFromSymbol(ctx context.Context, symbol string) (string, error) {
	if s.extRedis == nil {
		return "", fmt.Errorf("symbol-only create requires ext-Redis (EXT_REDIS_ADDR) to be configured on user-config; either set the env or supply hft_config.isin explicitly")
	}
	if symbol == "" {
		return "", fmt.Errorf("symbol is empty")
	}
	rctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	raw, err := s.extRedis.Get(rctx, "symbol:"+symbol).Result()
	if errors.Is(err, goredis.Nil) {
		return "", fmt.Errorf("unknown symbol %q — not present in NSE EQ master data; verify the ticker or supply isin explicitly", symbol)
	}
	if err != nil {
		return "", fmt.Errorf("ext-Redis lookup symbol:%s: %w", symbol, err)
	}
	var blob struct {
		Symbol string `json:"symbol"`
		ISIN   string `json:"isin"`
	}
	if err := json.Unmarshal([]byte(raw), &blob); err != nil {
		return "", fmt.Errorf("parse symbol:%s value: %w", symbol, err)
	}
	if blob.ISIN == "" {
		return "", fmt.Errorf("symbol:%s blob has no isin field", symbol)
	}
	return blob.ISIN, nil
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
// execution_db.user_credentials, then publishes a USER_CREDENTIALS_UPDATED
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

// UserCredentials is the decrypted credential bundle returned by GetUserCredentials.
// Mirrors the repository's IndiraCredentials struct but lives in the service layer
// so callers (gRPC handler) don't import the repository package directly.
type UserCredentials struct {
	IndiraUserID string
	AppID        string
	Source       string
	BearerToken  string // decrypted
}

// GetUserCredentials reads the broker credentials for userID, decrypts the
// bearer token, and returns the bundle. Returns repository.ErrCredentialsNotFound
// when no row exists for the user (gRPC handler maps that to NOT_FOUND).
//
// This is the READ side of the auth-domain ownership boundary: every other
// service must call user-config via gRPC instead of reading the table directly.
// See docs/architecture/data-ownership.md.
func (s *StrategyService) GetUserCredentials(ctx context.Context, userID string) (*UserCredentials, error) {
	if userID == "" {
		return nil, fmt.Errorf("user_id is required")
	}
	creds, err := s.credsRepo.GetIndiraCredentials(ctx, userID)
	if err != nil {
		return nil, err // includes ErrCredentialsNotFound
	}
	return &UserCredentials{
		IndiraUserID: creds.IndiraUserID,
		AppID:        creds.AppID,
		Source:       creds.Source,
		BearerToken:  creds.BearerToken,
	}, nil
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
	// Validate request. validateCreateRequest already wraps failures with
	// apperr.ErrValidation (message begins "validation failed: …"), so return
	// it as-is rather than double-prefixing.
	if err := s.validateCreateRequest(ctx, req); err != nil {
		return nil, err
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
	// Validate request. validateUpdateRequest wraps failures with
	// apperr.ErrValidation; return as-is rather than double-prefixing.
	if err := s.validateUpdateRequest(req); err != nil {
		return nil, err
	}

	// Update strategy in database (includes Outbox insertion). Pass the repo
	// error up unwrapped — it already carries a clean typed sentinel
	// (apperr.ErrNotFound / ErrVersionConflict) or a repo-prefixed DB error.
	strategy, err := s.repo.Update(ctx, req)
	if err != nil {
		return nil, err
	}

	return strategy, nil
}

// ForceExitParams carries the square-off decision + broker auth needed
// to place LIVE exit orders. Zero value == KEEP_POSITIONS_OPEN, which
// is safe for every caller — the atomic square-off flow is opt-in.
type ForceExitParams struct {
	// SquareOff = true triggers the reverse-exit call to trade-execution
	// BEFORE the state transition (deactivate / delete). If the exit call
	// fails, the state transition is aborted so the UI can retry.
	SquareOff bool

	// BearerToken + AppID + Source are only consulted when SquareOff==true
	// AND the strategy's TradingMode == LIVE. Paper square-off doesn't
	// touch the broker so it doesn't need auth.
	BearerToken string
	AppID       string
	Source      string
}

// positionHandlingWireValue folds ForceExitParams into the
// position_handling string that we persist on the outbox payload +
// propagate through Kafka. Consumers (trade-execution) branch on this
// value — see events/config_event.go PositionHandling docstring.
func positionHandlingWireValue(params ForceExitParams) string {
	if params.SquareOff {
		return "SQUARE_OFF_AT_MARKET"
	}
	return "KEEP_POSITIONS_OPEN"
}

// squareOffIfRequested performs the pre-transition force-exit call to
// trade-execution when params.SquareOff is set. Returns the number of
// exit orders placed (0 for KEEP_POSITIONS_OPEN), or a wrapped error.
//
// Both callers (DeactivateStrategy + DeleteStrategy) apply the same
// pre-check, so factor it here to avoid drift.
func (s *StrategyService) squareOffIfRequested(ctx context.Context, strategyID uuid.UUID, userID string, params ForceExitParams) (int, error) {
	if !params.SquareOff {
		return 0, nil
	}
	if s.tradeExec == nil {
		return 0, fmt.Errorf("SQUARE_OFF_AT_MARKET not available: trade-execution client not wired on user-config")
	}

	// Need to know PAPER vs LIVE to pick the right endpoint.
	strategy, err := s.repo.GetByID(ctx, strategyID, userID)
	if err != nil {
		return 0, fmt.Errorf("SQUARE_OFF_AT_MARKET: %w", err)
	}
	switch strategy.TradingMode {
	case models.TradingModeLive:
		return s.tradeExec.ForceExitStrategyLive(
			ctx, strategyID.String(), userID,
			params.BearerToken, params.AppID, params.Source,
		)
	case models.TradingModePaper:
		return s.tradeExec.ForceExitStrategyPaper(ctx, strategyID.String(), userID)
	default:
		return 0, fmt.Errorf("SQUARE_OFF_AT_MARKET: unsupported trading_mode %q", strategy.TradingMode)
	}
}

// DeleteStrategy deletes a strategy.
//
// When params.SquareOff is set, user-config first calls trade-execution
// to place reverse exit orders for every ACTIVE position of this
// strategy. Only if that succeeds is the strategy row deleted. The
// returned positionsExited is the count of exit orders placed
// (0 when KEEP_POSITIONS_OPEN or the strategy had nothing open).
func (s *StrategyService) DeleteStrategy(ctx context.Context, strategyID uuid.UUID, userID string, params ForceExitParams) (positionsExited int, _ error) {
	positionsExited, err := s.squareOffIfRequested(ctx, strategyID, userID, params)
	if err != nil {
		return 0, err
	}

	// Delete from database (includes Outbox insertion). Stamp the
	// position_handling on the outbox so downstream consumers know
	// whether we already placed exit orders.
	if err := s.repo.Delete(ctx, strategyID, userID, positionHandlingWireValue(params)); err != nil {
		return positionsExited, err
	}

	return positionsExited, nil
}

// ActivateStrategy activates a strategy
func (s *StrategyService) ActivateStrategy(ctx context.Context, strategyID uuid.UUID, userID string) (*models.Strategy, error) {
	// Activate in database
	if err := s.repo.Activate(ctx, strategyID, userID); err != nil {
		return nil, err
	}

	// Get updated strategy
	strategy, err := s.repo.GetByID(ctx, strategyID, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to get strategy: %w", err)
	}

	return strategy, nil
}

// DeactivateStrategy pauses a strategy — flips active=false so
// rules-engine stops generating new signals.
//
// When params.SquareOff is set, user-config first calls trade-execution
// to place reverse exit orders for every ACTIVE position of this
// strategy. Only if that succeeds is the deactivate applied. See
// ForceExitParams for the auth requirements.
func (s *StrategyService) DeactivateStrategy(ctx context.Context, strategyID uuid.UUID, userID string, params ForceExitParams) (*models.Strategy, int, error) {
	positionsExited, err := s.squareOffIfRequested(ctx, strategyID, userID, params)
	if err != nil {
		return nil, 0, err
	}

	// Deactivate in database. Stamp position_handling on the outbox
	// so trade-execution's consumer branches correctly.
	if err := s.repo.Deactivate(ctx, strategyID, userID, positionHandlingWireValue(params)); err != nil {
		return nil, positionsExited, err
	}

	// Get updated strategy
	strategy, err := s.repo.GetByID(ctx, strategyID, userID)
	if err != nil {
		return nil, positionsExited, fmt.Errorf("failed to get strategy: %w", err)
	}

	return strategy, positionsExited, nil
}

// GetStrategiesByIDs retrieves multiple strategies by their IDs
func (s *StrategyService) GetStrategiesByIDs(ctx context.Context, strategyIDs []uuid.UUID) ([]*models.Strategy, error) {
	return s.repo.GetByIDs(ctx, strategyIDs)
}

// ListAllActiveStrategies returns active, non-deleted strategies for startup bulk-load.
func (s *StrategyService) ListAllActiveStrategies(ctx context.Context, limit, offset int) ([]*models.Strategy, error) {
	return s.repo.ListAllActive(ctx, limit, offset)
}

// validateCreateRequest validates a create strategy request.
// Takes ctx because HFT_BIDDING may do an ext-Redis lookup to resolve
// symbol → ISIN when the caller supplies only a symbol.
func (s *StrategyService) validateCreateRequest(ctx context.Context, req *models.CreateStrategyRequest) error {
	if req.UserID == "" {
		return fmt.Errorf("%w: user_id is required", apperr.ErrValidation)
	}
	if req.StrategyName == "" {
		return fmt.Errorf("%w: strategy_name is required", apperr.ErrValidation)
	}
	// Default strategy type
	if req.StrategyType == "" {
		req.StrategyType = models.StrategyTypeManthan
	}

	// Default trading mode
	if req.TradingMode == "" {
		req.TradingMode = models.TradingModePaper
	}
	if req.TradingMode != models.TradingModePaper && req.TradingMode != models.TradingModeLive {
		return fmt.Errorf("%w: invalid trading_mode: %s", apperr.ErrValidation, req.TradingMode)
	}

	if req.TradeConfig == nil {
		return fmt.Errorf("%w: trade_config is required", apperr.ErrValidation)
	}

	// --- MANTHAN strategy: minimal user input, backend fills everything ---
	if req.StrategyType == models.StrategyTypeManthan {
		if len(req.StrategyName) < 3 {
			return fmt.Errorf("%w: strategy_name must be at least 3 characters", apperr.ErrValidation)
		}
		if len(req.StrategyName) > 100 {
			return fmt.Errorf("%w: strategy_name must be less than 100 characters", apperr.ErrValidation)
		}

		// Total capital: min ₹5,00,000 (5 lakh), no upper limit
		if req.TradeConfig == nil {
			req.TradeConfig = &models.TradeConfig{}
		}
		if req.TradeConfig.TotalCapital == nil {
			return fmt.Errorf("%w: total_capital is required (minimum ₹5,00,000)", apperr.ErrValidation)
		}
		cap := *req.TradeConfig.TotalCapital
		if cap < 500000 {
			return fmt.Errorf("%w: total_capital must be at least ₹5,00,000 for Manthan strategy", apperr.ErrValidation)
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

		// Risk limits — placeholder row only (risk fields dropped 2026-07-30).
		if req.RiskLimits == nil {
			req.RiskLimits = &models.RiskLimits{}
		}

		return nil
	}

	// Unknown strategy type — MANTHAN is the only supported type after 2026-07-20.
	return fmt.Errorf("%w: unsupported strategy_type: %s (only MANTHAN is supported)", apperr.ErrValidation, req.StrategyType)
}

// validateUpdateRequest validates an update strategy request
func (s *StrategyService) validateUpdateRequest(req *models.UpdateStrategyRequest) error {
	if req.StrategyID == uuid.Nil {
		return fmt.Errorf("%w: strategy_id is required", apperr.ErrValidation)
	}
	if req.UserID == "" {
		return fmt.Errorf("%w: user_id is required", apperr.ErrValidation)
	}
	if req.Version < 1 {
		return fmt.Errorf("%w: version must be greater than 0", apperr.ErrValidation)
	}

	// Conditions has no user-facing fields after 2026-07-20 cleanup — nothing to validate.

	if req.TradeConfig != nil {
		if req.TradeConfig.Quantity <= 0 {
			return fmt.Errorf("%w: quantity must be greater than 0", apperr.ErrValidation)
		}
		if req.TradeConfig.StopLossPct != nil && *req.TradeConfig.StopLossPct < 0 {
			return fmt.Errorf("%w: stop_loss_pct must be non-negative", apperr.ErrValidation)
		}
		if req.TradeConfig.TakeProfitPct != nil && *req.TradeConfig.TakeProfitPct < 0 {
			return fmt.Errorf("%w: take_profit_pct must be non-negative", apperr.ErrValidation)
		}
	}

	if req.TradingMode != nil {
		if *req.TradingMode != models.TradingModePaper && *req.TradingMode != models.TradingModeLive {
			return fmt.Errorf("%w: invalid trading_mode: %s", apperr.ErrValidation, *req.TradingMode)
		}
	}

	return nil
}
