package jobbing

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/publisher"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/risk"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// Config for the Jobbing strategy engine.
type Config struct {
	// List of user IDs to run this strategy for
	UserIDs []string

	// Price range limits (absolute prices)
	LowerRange  float64 // Minimum price threshold
	HigherRange float64 // Maximum price threshold

	// Initial offset from LTP for first BUY order (e.g., 0.01)
	InitialBuyOffset float64

	// Distance between consecutive orders (e.g., 0.01)
	DistanceContinue float64

	// Quantity per order
	QuantityPerOrder int32

	// Maximum total quantity allowed
	MaxQuantity int32

	// List of tokens (stocks) to apply this strategy on
	Tokens []string
	
	// TradingMode controls whether orders are sent to broker (LIVE) or
	// simulated (PAPER). Defaults to LIVE when empty.
	TradingMode string
}

// UserTokenConfig holds per-user, per-token parameters for the Jobbing
// strategy. These can be loaded dynamically from the user-config service
// instead of (or in addition to) global defaults provided via env/config.
type UserTokenConfig struct {
	LowerRange       float64
	HigherRange      float64
	InitialBuyOffset float64
	DistanceContinue float64
	QuantityPerOrder int32
	MaxQuantity      int32
}

// userTokenState tracks the state for a specific user-token combination
type userTokenState struct {
	Token            string
	Symbol           string
	Exchange         string
	TotalQuantityBuy int32    // Total quantity bought so far
	LastOrderPrice   float64  // Last price at which order was placed
	ActiveOrders     []string // List of active order IDs
	LastUpdated      time.Time
}

// userState tracks all token states for a user
type userState struct {
	TokenStates map[string]*userTokenState // key: token
}

// Engine implements the Jobbing strategy for multiple users and tokens.
type Engine struct {
	cfg        Config
	riskClient *risk.Client
	rabbitPub  *publisher.Publisher
	kafkaPub   *publisher.KafkaPublisher
	allocPub   *publisher.KafkaPublisher
	logger     *zap.Logger

	mu sync.Mutex
	// trading day tracking (for resetting intraday state)
	day string
	// userState holds per-user runtime state (filled qty, active orders, etc.).
	userState map[string]*userState // key: userID
	// userConfigs holds per-user, per-token jobbing parameters loaded from
	// user-config service. Keyed as userID -> token -> config.
	userConfigs map[string]map[string]UserTokenConfig
	// tokenUsers is the reverse index: token -> set of userIDs with configs.
	tokenUsers map[string]map[string]struct{}
}

// NewEngine creates a new Jobbing strategy engine.
func NewEngine(cfg Config, riskClient *risk.Client, rabbitPub *publisher.Publisher, kafkaPub *publisher.KafkaPublisher, allocPub *publisher.KafkaPublisher, logger *zap.Logger) *Engine {
	// defaults
	if cfg.LowerRange <= 0 {
		cfg.LowerRange = 10.0
	}
	if cfg.HigherRange <= 0 {
		cfg.HigherRange = 15.0
	}
	if cfg.InitialBuyOffset <= 0 {
		cfg.InitialBuyOffset = 0.01
	}
	if cfg.DistanceContinue <= 0 {
		cfg.DistanceContinue = 0.01
	}
	if cfg.QuantityPerOrder <= 0 {
		cfg.QuantityPerOrder = 1
	}
	if cfg.MaxQuantity <= 0 {
		cfg.MaxQuantity = 10
	}

	// normalize user IDs
	users := make([]string, 0, len(cfg.UserIDs))
	for _, u := range cfg.UserIDs {
		u = strings.TrimSpace(u)
		if u != "" {
			users = append(users, u)
		}
	}
	cfg.UserIDs = users

	// normalize tokens
	tokens := make([]string, 0, len(cfg.Tokens))
	for _, t := range cfg.Tokens {
		t = strings.TrimSpace(t)
		if t != "" {
			tokens = append(tokens, t)
		}
	}
	cfg.Tokens = tokens
	
	// Normalize trading mode
	mode := strings.ToUpper(strings.TrimSpace(cfg.TradingMode))
	if mode != "PAPER" {
		mode = "LIVE"
	}
	cfg.TradingMode = mode

	return &Engine{
		cfg:         cfg,
		riskClient:  riskClient,
		rabbitPub:   rabbitPub,
		kafkaPub:    kafkaPub,
		allocPub:    allocPub,
		logger:      logger,
		day:         todayStr(),
		userState:   make(map[string]*userState),
		userConfigs: make(map[string]map[string]UserTokenConfig),
		tokenUsers:  make(map[string]map[string]struct{}),
	}
}

func todayStr() string { return time.Now().Format("2006-01-02") }

func (e *Engine) resetIfNewDay() {
	e.mu.Lock()
	defer e.mu.Unlock()

	day := todayStr()
	if day != e.day {
		e.logger.Info("New trading day detected, resetting jobbing state",
			zap.String("old_day", e.day),
			zap.String("new_day", day))
		e.day = day
		// Reset only runtime state (positions, totals, etc.). Keep
		// configuration loaded from user-config service.
		e.userState = make(map[string]*userState)
	}
}

func (e *Engine) getUserState(userID string) *userState {
	e.mu.Lock()
	defer e.mu.Unlock()

	st, ok := e.userState[userID]
	if !ok {
		st = &userState{TokenStates: make(map[string]*userTokenState)}
		e.userState[userID] = st
	}
	return st
}

func (e *Engine) getTokenState(userID, token string) *userTokenState {
	st := e.getUserState(userID)

	e.mu.Lock()
	defer e.mu.Unlock()

	tokenSt, ok := st.TokenStates[token]
	if !ok {
		tokenSt = &userTokenState{
			Token:            token,
			TotalQuantityBuy: 0,
			LastOrderPrice:   0,
			ActiveOrders:     make([]string, 0),
			LastUpdated:      time.Now(),
		}
		st.TokenStates[token] = tokenSt
	}
	return tokenSt
}

// SetJobbingConfig registers or updates jobbing parameters for a given user
// and a set of tokens. This is typically called from the rules-engine main
// process after loading strategies from the user-config service.
func (e *Engine) SetJobbingConfig(userID string, tokens []string, cfg UserTokenConfig) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.userConfigs == nil {
		e.userConfigs = make(map[string]map[string]UserTokenConfig)
	}
	if e.tokenUsers == nil {
		e.tokenUsers = make(map[string]map[string]struct{})
	}

	userCfg, ok := e.userConfigs[userID]
	if !ok {
		userCfg = make(map[string]UserTokenConfig)
		e.userConfigs[userID] = userCfg
	}

	for _, t := range tokens {
		t = strings.TrimSpace(t)
		if t == "" {
			continue
		}
		userCfg[t] = cfg

		usersForToken, ok := e.tokenUsers[t]
		if !ok {
			usersForToken = make(map[string]struct{})
			e.tokenUsers[t] = usersForToken
		}
		usersForToken[userID] = struct{}{}
	}
}

// getConfig returns a per-user, per-token config if present.
func (e *Engine) getConfig(userID, token string) (UserTokenConfig, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	userCfg, ok := e.userConfigs[userID]
	if !ok {
		return UserTokenConfig{}, false
	}
	cfg, ok := userCfg[token]
	return cfg, ok
}

// getUsersForToken returns all user IDs that have a jobbing config
// for the given token.
func (e *Engine) getUsersForToken(token string) []string {
	e.mu.Lock()
	defer e.mu.Unlock()

	usersMap, ok := e.tokenUsers[token]
	if !ok || len(usersMap) == 0 {
		return nil
	}
	users := make([]string, 0, len(usersMap))
	for u := range usersMap {
		users = append(users, u)
	}
	return users
}

// HandleJobbing processes a market depth event for the jobbing strategy.
func (e *Engine) HandleJobbing(ctx context.Context, ev *models.JobbingMarketDepthEvent) error {
	if ev == nil {
		return fmt.Errorf("nil JobbingMarketDepthEvent")
	}

	// Basic sanity checks
	if ev.MarketData.LastTradedPrice <= 0 {
		e.logger.Warn("Skipping jobbing event with invalid LTP",
			zap.String("symbol", ev.StockData.Symbol),
			zap.Float64("ltp", ev.MarketData.LastTradedPrice))
		return nil
	}

	// Determine token identifier for config/state lookup
	token := fmt.Sprintf("%d", ev.StockData.StockCode)

	// If we have dynamic configs loaded (from user-config service), use those
	// to decide which users are active for this token. Otherwise, fall back
	// to static cfg.UserIDs / cfg.Tokens from env.
	users := e.getUsersForToken(token)
	if len(users) == 0 {
		// Fallback: use static configuration if no dynamic configs exist.
		if !e.isTokenEnabled(token) {
			// Not a token we're trading
			return nil
		}
		users = e.cfg.UserIDs
	}

	e.resetIfNewDay()

	// Process for all relevant users
	for _, userID := range users {
		if err := e.handleForUser(ctx, userID, ev); err != nil {
			e.logger.Error("Failed to handle jobbing for user",
				zap.Error(err),
				zap.String("user_id", userID),
				zap.String("symbol", ev.StockData.Symbol))
			// continue with other users
		}
	}

	return nil
}

func (e *Engine) isTokenEnabled(token string) bool {
	if len(e.cfg.Tokens) == 0 {
		// If no tokens configured, trade all
		return true
	}
	for _, t := range e.cfg.Tokens {
		if t == token {
			return true
		}
	}
	return false
}

func (e *Engine) handleForUser(ctx context.Context, userID string, ev *models.JobbingMarketDepthEvent) error {
	token := fmt.Sprintf("%d", ev.StockData.StockCode)
	ltp := ev.MarketData.LastTradedPrice

	// Start from global defaults and override with per-user+token config if
	// available. This allows a mix of env-based and dynamic configurations.
	cfg := UserTokenConfig{
		LowerRange:       e.cfg.LowerRange,
		HigherRange:      e.cfg.HigherRange,
		InitialBuyOffset: e.cfg.InitialBuyOffset,
		DistanceContinue: e.cfg.DistanceContinue,
		QuantityPerOrder: e.cfg.QuantityPerOrder,
		MaxQuantity:      e.cfg.MaxQuantity,
	}
	if userCfg, ok := e.getConfig(userID, token); ok {
		if userCfg.LowerRange > 0 {
			cfg.LowerRange = userCfg.LowerRange
		}
		if userCfg.HigherRange > 0 {
			cfg.HigherRange = userCfg.HigherRange
		}
		if userCfg.InitialBuyOffset > 0 {
			cfg.InitialBuyOffset = userCfg.InitialBuyOffset
		}
		if userCfg.DistanceContinue > 0 {
			cfg.DistanceContinue = userCfg.DistanceContinue
		}
		if userCfg.QuantityPerOrder > 0 {
			cfg.QuantityPerOrder = userCfg.QuantityPerOrder
		}
		if userCfg.MaxQuantity > 0 {
			cfg.MaxQuantity = userCfg.MaxQuantity
		}
	}

	// Check if LTP is within configured range
	if ltp < cfg.LowerRange || ltp > cfg.HigherRange {
		e.logger.Debug("LTP outside jobbing range, skipping",
			zap.String("user_id", userID),
			zap.String("symbol", ev.StockData.Symbol),
			zap.Float64("ltp", ltp),
			zap.Float64("lower_range", cfg.LowerRange),
			zap.Float64("higher_range", cfg.HigherRange))
		return nil
	}

	tokenState := e.getTokenState(userID, token)

	// Check if max quantity reached
	if tokenState.TotalQuantityBuy >= cfg.MaxQuantity {
		e.logger.Debug("Max quantity reached for token",
			zap.String("user_id", userID),
			zap.String("symbol", ev.StockData.Symbol),
			zap.Int32("total_qty", tokenState.TotalQuantityBuy),
			zap.Int32("max_qty", cfg.MaxQuantity))
		return nil
	}

	// Calculate order price
	var orderPrice float64
	if tokenState.LastOrderPrice == 0 {
		// First order: LTP - InitialBuyOffset
		orderPrice = ltp - cfg.InitialBuyOffset
	} else {
		// Subsequent orders: LastOrderPrice - DistanceContinue
		orderPrice = tokenState.LastOrderPrice - cfg.DistanceContinue
	}

	// Round to 2 decimal places
	orderPrice = math.Round(orderPrice*100) / 100

	// Ensure order price is within range
	if orderPrice < cfg.LowerRange {
		e.logger.Debug("Calculated order price below lower range",
			zap.String("user_id", userID),
			zap.String("symbol", ev.StockData.Symbol),
			zap.Float64("order_price", orderPrice),
			zap.Float64("lower_range", cfg.LowerRange))
		return nil
	}

	// Check market depth conditions for better execution
	if !e.validateMarketDepth(ev, orderPrice) {
		e.logger.Debug("Market depth conditions not favorable",
			zap.String("user_id", userID),
			zap.String("symbol", ev.StockData.Symbol),
			zap.Float64("spread_pct", ev.MarketData.DepthMetrics.SpreadPct))
		return nil
	}

	// Calculate remaining quantity
	remainingQty := cfg.MaxQuantity - tokenState.TotalQuantityBuy
	qty := cfg.QuantityPerOrder
	if qty > remainingQty {
		qty = remainingQty
	}

	// Build order request
	orderReq := &models.OrderRequest{
		OrderID:      uuid.New().String(),
		UserID:       userID,
		StrategyID:   "JOBBING",
		StrategyName: "Jobbing Strategy",
		EventID:      ev.EventID,
		StockCode:    int64(ev.StockData.StockCode),
		Token:        int64(ev.StockData.StockCode),
		Symbol:       ev.StockData.Symbol,
		Exchange:     strings.ToUpper(ev.StockData.Exchange),
		OrderType:    "LIMIT", // Jobbing uses LIMIT orders
		OrderSide:    "BUY",
		Quantity:     qty,
		Price:        orderPrice,
		// Jobbing strategy typically doesn't use SL/TP per order
		// as it relies on rapid entry/exit based on price movements
		StopLoss:     0,
		TakeProfit:   0,
		Timestamp:    time.Now(),
		MatchScore:   100.0,
		ImpactScore:  0,
		Sentiment:    "",
		NewsCategory: "",
	}

	// Run risk check
	if e.riskClient != nil {
		strategy := &models.Strategy{
			StrategyID:   "JOBBING",
			UserID:       userID,
			StrategyName: "Jobbing Strategy",
			RiskLimits: models.RiskLimits{
				MaxDailyTrades:  100, // Jobbing requires many trades
				MaxLossPerDay:   10000,
				MaxPositionSize: float64(e.cfg.MaxQuantity) * orderPrice,
				MaxPerTradeRisk: float64(qty) * orderPrice * 0.02, // 2% per trade
				PositionSizing:  "FIXED",
			},
		}

		riskResp, err := e.riskClient.CheckPreTradeRisk(ctx, orderReq, strategy)
		if err != nil {
			e.logger.Error("Risk check failed for jobbing order",
				zap.Error(err),
				zap.String("user_id", userID),
				zap.String("symbol", ev.StockData.Symbol))
			return nil
		}
		orderReq.RiskApproved = riskResp.Approved
		orderReq.RiskScore = riskResp.RiskScore
		if !riskResp.Approved {
			e.logger.Warn("Jobbing order rejected by risk",
				zap.String("user_id", userID),
				zap.String("symbol", ev.StockData.Symbol),
				zap.Float64("risk_score", riskResp.RiskScore))
			return nil
		}
	} else {
		orderReq.RiskApproved = true
		orderReq.RiskScore = 0
	}

	// Publish to Kafka trade signals
	if e.kafkaPub != nil {
		if err := e.kafkaPub.PublishTradeSignal(ctx, orderReq); err != nil {
			e.logger.Error("Failed to publish jobbing trade signal to Kafka",
				zap.Error(err),
				zap.String("order_id", orderReq.OrderID))
		}
	}

	// Check trading mode before publishing to RabbitMQ
	if e.cfg.TradingMode == "PAPER" {
		e.logger.Info("PAPER mode: simulating jobbing order (no RabbitMQ publish)",
			zap.String("order_id", orderReq.OrderID),
			zap.String("user_id", userID),
			zap.String("symbol", ev.StockData.Symbol),
			zap.Float64("order_price", orderPrice),
			zap.Int32("quantity", qty))
		// Skip RabbitMQ publish but continue to update state
	} else {
		// LIVE mode: Publish to RabbitMQ
		if err := e.rabbitPub.PublishOrder(ctx, orderReq); err != nil {
			return fmt.Errorf("failed to publish jobbing order: %w", err)
		}
	}

	// Update token state
	e.mu.Lock()
	tokenState.TotalQuantityBuy += qty
	tokenState.LastOrderPrice = orderPrice
	tokenState.ActiveOrders = append(tokenState.ActiveOrders, orderReq.OrderID)
	tokenState.LastUpdated = time.Now()
	tokenState.Symbol = ev.StockData.Symbol
	tokenState.Exchange = orderReq.Exchange
	e.mu.Unlock()

	// Publish allocation snapshot
	e.publishAllocation(ctx, userID)

	e.logger.Info("Jobbing order published",
		zap.String("user_id", userID),
		zap.String("symbol", ev.StockData.Symbol),
		zap.String("exchange", orderReq.Exchange),
		zap.Int32("quantity", qty),
		zap.Float64("order_price", orderPrice),
		zap.Float64("ltp", ltp),
		zap.Int32("total_qty_bought", tokenState.TotalQuantityBuy),
		zap.Int32("max_qty", e.cfg.MaxQuantity))

	return nil
}

// validateMarketDepth checks if market conditions are favorable for order placement
func (e *Engine) validateMarketDepth(ev *models.JobbingMarketDepthEvent, orderPrice float64) bool {
	// Check spread - jobbing works best with tight spreads
	if ev.MarketData.DepthMetrics.SpreadPct > 0.5 { // 0.5% spread threshold
		return false
	}

	// Check if there's sufficient liquidity at bid side
	if ev.MarketData.DepthMetrics.TotalBidQty < 100 {
		return false
	}

	// Check bid-ask ratio - avoid extreme imbalances
	if ev.MarketData.DepthMetrics.BidAskRatio < 0.3 || ev.MarketData.DepthMetrics.BidAskRatio > 3.0 {
		return false
	}

	return true
}

// publishAllocation emits the current allocation snapshot for jobbing strategy
func (e *Engine) publishAllocation(ctx context.Context, userID string) {
	if e.allocPub == nil {
		return
	}

	e.mu.Lock()
	st, ok := e.userState[userID]
	if !ok {
		e.mu.Unlock()
		return
	}

	positions := make([]models.AllocationPosition, 0, len(st.TokenStates))
	for _, tokenSt := range st.TokenStates {
		if tokenSt.TotalQuantityBuy > 0 {
			positions = append(positions, models.AllocationPosition{
				Token:    tokenSt.Token,
				Symbol:   tokenSt.Symbol,
				Exchange: tokenSt.Exchange,
			})
		}
	}
	e.mu.Unlock()

	ev := &models.PortfolioAllocationEvent{
		UserID:          userID,
		StrategyID:      "JOBBING",
		StrategyName:    "Jobbing Strategy",
		Date:            todayStr(),
		Positions:       positions,
		TotalPositions:  len(positions),
		MaxPositions:    len(e.cfg.Tokens),
		CapitalPerStock: float64(e.cfg.MaxQuantity) * e.cfg.LowerRange, // approximate
		Timestamp:       time.Now(),
	}

	if err := e.allocPub.PublishAllocation(ctx, ev); err != nil {
		e.logger.Error("Failed to publish jobbing allocation",
			zap.String("user_id", userID),
			zap.Error(err))
	}
}

// GetStats returns current statistics for monitoring
func (e *Engine) GetStats() map[string]interface{} {
	e.mu.Lock()
	defer e.mu.Unlock()

	stats := make(map[string]interface{})
	stats["day"] = e.day
	stats["total_users"] = len(e.userState)
	stats["config"] = map[string]interface{}{
		"lower_range":        e.cfg.LowerRange,
		"higher_range":       e.cfg.HigherRange,
		"initial_buy_offset": e.cfg.InitialBuyOffset,
		"distance_continue":  e.cfg.DistanceContinue,
		"quantity_per_order": e.cfg.QuantityPerOrder,
		"max_quantity":       e.cfg.MaxQuantity,
	}

	return stats
}
