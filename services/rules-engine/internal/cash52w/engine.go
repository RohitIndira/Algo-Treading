package cash52w

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

// Config for the Cash 52-week High engine.
type Config struct {
	// List of user IDs to run this strategy for (comma-separated env parsed in main).
	UserIDs []string

	CapitalPerStock float64
	MaxPositions    int
	SLPercent       float64
	TSLPercent      float64
}

// per-user in-memory state (Phase 1: approximate, reset daily).
type userState struct {
	Positions map[string]struct{} // tokens we already opened today
}

// Engine implements the 52-week breakout strategy for multiple users.
type Engine struct {
	cfg        Config
	riskClient *risk.Client
	rabbitPub  *publisher.Publisher
	kafkaPub   *publisher.KafkaPublisher
	logger     *zap.Logger

	mu        sync.Mutex
	day       string
	userState map[string]*userState // key: userID
}

// NewEngine creates a new Cash 52-week engine.
func NewEngine(cfg Config, riskClient *risk.Client, rabbitPub *publisher.Publisher, kafkaPub *publisher.KafkaPublisher, logger *zap.Logger) *Engine {
	// defaults
	if cfg.CapitalPerStock <= 0 {
		cfg.CapitalPerStock = 20000
	}
	if cfg.MaxPositions <= 0 {
		cfg.MaxPositions = 25
	}
	if cfg.SLPercent <= 0 {
		cfg.SLPercent = 10
	}
	if cfg.TSLPercent <= 0 {
		cfg.TSLPercent = 20
	}

	// normalize user IDs (trim spaces)
	users := make([]string, 0, len(cfg.UserIDs))
	for _, u := range cfg.UserIDs {
		u = strings.TrimSpace(u)
		if u != "" {
			users = append(users, u)
		}
	}
	cfg.UserIDs = users

	return &Engine{
		cfg:        cfg,
		riskClient: riskClient,
		rabbitPub:  rabbitPub,
		kafkaPub:   kafkaPub,
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
		e.day = day
		e.userState = make(map[string]*userState)
	}
}

// HandleBreakout processes a single 52-week breakout event for all configured users.
func (e *Engine) HandleBreakout(ctx context.Context, ev *models.Breakout52WEvent) error {
	if ev == nil {
		return fmt.Errorf("nil Breakout52WEvent")
	}

	// basic sanity
	if ev.LTP <= 0 {
		e.logger.Warn("Skipping 52w breakout with invalid LTP",
			zap.String("symbol", ev.Symbol),
			zap.String("token", ev.Token),
			zap.Float64("ltp", ev.LTP))
		return nil
	}

	e.resetIfNewDay()

	for _, userID := range e.cfg.UserIDs {
		if err := e.handleForUser(ctx, userID, ev); err != nil {
			e.logger.Error("Failed to handle 52w breakout for user",
				zap.Error(err),
				zap.String("user_id", userID),
				zap.String("token", ev.Token))
			// continue with other users
		}
	}

	return nil
}

func (e *Engine) getUserState(userID string) *userState {
	e.mu.Lock()
	defer e.mu.Unlock()

	st, ok := e.userState[userID]
	if !ok {
		st = &userState{Positions: make(map[string]struct{})}
		e.userState[userID] = st
	}
	return st
}

func (e *Engine) handleForUser(ctx context.Context, userID string, ev *models.Breakout52WEvent) error {
	st := e.getUserState(userID)

	// enforce max positions per user per day
	if len(st.Positions) >= e.cfg.MaxPositions {
		e.logger.Debug("User already has max 52w positions for today",
			zap.String("user_id", userID),
			zap.Int("max_positions", e.cfg.MaxPositions))
		return nil
	}

	// don't re-enter same token for this user on the same day
	if _, exists := st.Positions[ev.Token]; exists {
		return nil
	}

	// compute quantity based on capital per stock
	qty := int32(math.Floor(e.cfg.CapitalPerStock / ev.LTP))
	if qty <= 0 {
		e.logger.Warn("Computed non-positive quantity for 52w breakout",
			zap.String("user_id", userID),
			zap.String("token", ev.Token),
			zap.Float64("ltp", ev.LTP),
			zap.Float64("capital_per_stock", e.cfg.CapitalPerStock))
		return nil
	}

	// Build a minimal order request compatible with risk + trade-execution.
	orderReq := &models.OrderRequest{
		OrderID:      uuid.New().String(),
		UserID:       userID,
		StrategyID:   "CASH_52W_HIGH", // fixed strategy id for phase 1
		StrategyName: "Cash 52-Week High",
		EventID:      "", // not tied to news event
		StockCode:    0,  // unknown here; can be mapped later if needed
		Symbol:       ev.Symbol,
		Exchange:     strings.ToUpper(ev.Exchange),
		OrderType:    "MARKET",
		Quantity:     qty,
		Price:        ev.LTP,
		// initial SL/TP based on config
		StopLoss:     ev.LTP * (1 - e.cfg.SLPercent/100),
		TakeProfit:   ev.LTP * (1 + e.cfg.TSLPercent/100),
		Timestamp:    time.Now(),
		MatchScore:   100.0,
		ImpactScore:  0,
		Sentiment:    "",
		NewsCategory: "",
	}

	// For now we treat all as BUY; SELL leg / exits will be managed by
	// risk/execution logic and future enhancements.
	orderReq.OrderSide = "BUY"

	// Run risk check if client is available
	if e.riskClient != nil {
		// Construct a synthetic strategy object so we can pass explicit
		// risk limits for this CASH_52W_HIGH strategy into the risk
		// management service. Later, in Phase 2, these limits will come
		// from user-config per user/strategy.
		strategy := &models.Strategy{
			StrategyID:   "CASH_52W_HIGH",
			UserID:       userID,
			StrategyName: "Cash 52-Week High",
			RiskLimits: models.RiskLimits{
				// Allow a reasonable number of trades per day for
				// re-entries/rebalancing of the 25-stock basket.
				MaxDailyTrades: 50,
				// Cap total daily loss for this strategy. This can be
				// tuned later or made user-configurable.
				MaxLossPerDay: 50000,
				// Single-position not to exceed the configured
				// capital per stock (e.g. ₹20,000).
				MaxPositionSize: e.cfg.CapitalPerStock,
				// Per-trade risk roughly equals capitalPerStock * SL%.
				// For 20k and 10%% SL this is ~₹2,000.
				MaxPerTradeRisk: e.cfg.CapitalPerStock * e.cfg.SLPercent / 100.0,
				PositionSizing:  "FIXED",
			},
		}

		riskResp, err := e.riskClient.CheckPreTradeRisk(ctx, orderReq, strategy)
		if err != nil {
			e.logger.Error("Risk check failed for 52w order",
				zap.Error(err),
				zap.String("user_id", userID),
				zap.String("token", ev.Token))
			return nil
		}
		orderReq.RiskApproved = riskResp.Approved
		orderReq.RiskScore = riskResp.RiskScore
		if !riskResp.Approved {
			e.logger.Warn("52w order rejected by risk",
				zap.String("user_id", userID),
				zap.String("token", ev.Token),
				zap.Float64("risk_score", riskResp.RiskScore))
			return nil
		}
	} else {
		orderReq.RiskApproved = true
		orderReq.RiskScore = 0
	}

	// Optionally publish to Kafka "trade-signals" topic for tracking/analytics,
	// mirroring the behaviour of news-based orders in the main handler.
	if e.kafkaPub != nil {
		if err := e.kafkaPub.PublishTradeSignal(ctx, orderReq); err != nil {
			e.logger.Error("Failed to publish 52w trade signal to Kafka",
				zap.Error(err),
				zap.String("order_id", orderReq.OrderID))
		} else {
			e.logger.Debug("52w trade signal published to Kafka",
				zap.String("order_id", orderReq.OrderID))
		}
	}

	// Publish to RabbitMQ
	if err := e.rabbitPub.PublishOrder(ctx, orderReq); err != nil {
		return fmt.Errorf("failed to publish 52w order: %w", err)
	}

	// Track that this user has taken this token today
	st.Positions[ev.Token] = struct{}{}

	e.logger.Info("52w-high order published",
		zap.String("user_id", userID),
		zap.String("token", ev.Token),
		zap.String("symbol", ev.Symbol),
		zap.String("exchange", orderReq.Exchange),
		zap.Int32("quantity", qty),
		zap.Float64("price", ev.LTP))

	return nil
}
