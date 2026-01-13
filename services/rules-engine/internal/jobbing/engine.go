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

	mu        sync.Mutex
	day       string
	userState map[string]*userState // key: userID
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

	return &Engine{
		cfg:        cfg,
		riskClient: riskClient,
		rabbitPub:  rabbitPub,
		kafkaPub:   kafkaPub,
		allocPub:   allocPub,
		logger:     logger,
		day:        todayStr(),
		userState:  make(map[string]*userState),
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

	// Check if this token is in our configured list
	token := fmt.Sprintf("%d", ev.StockData.StockCode)
	if !e.isTokenEnabled(token) {
		// Not a token we're trading
		return nil
	}

	e.resetIfNewDay()

	// Process for all configured users
	for _, userID := range e.cfg.UserIDs {
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

	// Check if LTP is within configured range
	if ltp < e.cfg.LowerRange || ltp > e.cfg.HigherRange {
		e.logger.Debug("LTP outside jobbing range, skipping",
			zap.String("user_id", userID),
			zap.String("symbol", ev.StockData.Symbol),
			zap.Float64("ltp", ltp),
			zap.Float64("lower_range", e.cfg.LowerRange),
			zap.Float64("higher_range", e.cfg.HigherRange))
		return nil
	}

	tokenState := e.getTokenState(userID, token)

	// Check if max quantity reached
	if tokenState.TotalQuantityBuy >= e.cfg.MaxQuantity {
		e.logger.Debug("Max quantity reached for token",
			zap.String("user_id", userID),
			zap.String("symbol", ev.StockData.Symbol),
			zap.Int32("total_qty", tokenState.TotalQuantityBuy),
			zap.Int32("max_qty", e.cfg.MaxQuantity))
		return nil
	}

	// Calculate order price
	var orderPrice float64
	if tokenState.LastOrderPrice == 0 {
		// First order: LTP - InitialBuyOffset
		orderPrice = ltp - e.cfg.InitialBuyOffset
	} else {
		// Subsequent orders: LastOrderPrice - DistanceContinue
		orderPrice = tokenState.LastOrderPrice - e.cfg.DistanceContinue
	}

	// Round to 2 decimal places
	orderPrice = math.Round(orderPrice*100) / 100

	// Ensure order price is within range
	if orderPrice < e.cfg.LowerRange {
		e.logger.Debug("Calculated order price below lower range",
			zap.String("user_id", userID),
			zap.String("symbol", ev.StockData.Symbol),
			zap.Float64("order_price", orderPrice),
			zap.Float64("lower_range", e.cfg.LowerRange))
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
	remainingQty := e.cfg.MaxQuantity - tokenState.TotalQuantityBuy
	qty := e.cfg.QuantityPerOrder
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

	// Publish to RabbitMQ
	if err := e.rabbitPub.PublishOrder(ctx, orderReq); err != nil {
		return fmt.Errorf("failed to publish jobbing order: %w", err)
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
