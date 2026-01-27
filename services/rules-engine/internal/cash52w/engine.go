package cash52w

import (
	"context"
	"fmt"
	"math"
	"strconv"
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

	// TradingMode controls how orders are handled for this strategy.
	// Accepted values (uppercased by caller):
	//   - "LIVE":  normal behaviour, send real orders via RabbitMQ.
	//   - "PAPER": paper trading; still uses real breakout prices but
	//              does NOT publish orders to RabbitMQ. Trade-signals
	//              can still be sent to Kafka for analytics.
	TradingMode string
}

// per-user in-memory state (Phase 1: approximate, reset daily). We track
// positions opened today to avoid re-entries.
type userState struct {
	// Positions we already opened today, keyed by token. We keep the
	// symbol/exchange so we can publish a useful allocation snapshot.
	Positions map[string]models.AllocationPosition
}

// Engine implements the 52-week breakout strategy for multiple users.
type Engine struct {
	cfg        Config
	riskClient *risk.Client
	rabbitPub  *publisher.Publisher
	// kafkaPub publishes trade-signals (order requests)
	kafkaPub *publisher.KafkaPublisher
	// allocPub publishes portfolio allocation snapshots
	allocPub *publisher.KafkaPublisher
	logger   *zap.Logger

	mu        sync.Mutex
	day       string
	userState map[string]*userState // key: userID
	// userTradingMode holds per-user overrides for trading mode (LIVE/PAPER)
	// fetched from Elasticsearch via QueryEngine. When a user is present
	// here with mode PAPER, their 52W orders will be simulated even if the
	// global cfg.TradingMode is LIVE.
	userTradingMode map[string]string // key: userID -> "LIVE" / "PAPER"
}

// NewEngine creates a new Cash 52-week engine.
func NewEngine(cfg Config, riskClient *risk.Client, rabbitPub *publisher.Publisher, kafkaPub *publisher.KafkaPublisher, allocPub *publisher.KafkaPublisher, logger *zap.Logger) *Engine {
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

	// Normalise trading mode for internal use
	mode := strings.ToUpper(strings.TrimSpace(cfg.TradingMode))
	if mode != "PAPER" {
		mode = "LIVE"
	}
	cfg.TradingMode = mode

	return &Engine{
		cfg:             cfg,
		riskClient:      riskClient,
		rabbitPub:       rabbitPub,
		kafkaPub:        kafkaPub,
		allocPub:        allocPub,
		logger:          logger,
		day:             todayStr(),
		userState:       make(map[string]*userState),
		userTradingMode: make(map[string]string),
	}
}

func todayStr() string { return time.Now().Format("2006-01-02") }

// parseToken converts the breakout event's token string into an int64 token
// used by OrderRequest. If parsing fails, it returns 0 so that we never
// panic; in practice tokens should always be numeric.
func parseToken(tok string) int64 {
	if tok == "" {
		return 0
	}
	val, err := strconv.ParseInt(tok, 10, 64)
	if err != nil {
		return 0
	}
	return val
}

func (e *Engine) resetIfNewDay() {
	e.mu.Lock()
	defer e.mu.Unlock()

	day := todayStr()
	if day != e.day {
		e.day = day
		e.userState = make(map[string]*userState)
	}
}

// SetUsers replaces the configured user list for the 52W engine with a new
// set discovered dynamically from user-config DB (via Elasticsearch index).
// This allows the engine to run for all users who have an active
// CASH_52W_HIGH strategy instead of relying on static env lists.
func (e *Engine) SetUsers(userIDs []string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	normalized := make([]string, 0, len(userIDs))
	seen := make(map[string]bool)
	for _, u := range userIDs {
		u = strings.TrimSpace(u)
		if u == "" || seen[u] {
			continue
		}
		seen[u] = true
		normalized = append(normalized, u)
	}

	e.cfg.UserIDs = normalized

	e.logger.Info("Updated Cash52W user list from dynamic source",
		zap.Int("user_count", len(e.cfg.UserIDs)),
		zap.Strings("users", e.cfg.UserIDs))
}

// SetUserModes updates per-user trading modes (LIVE/PAPER) based on data
// fetched from Elasticsearch. This allows user-config to control whether
// a given user's 52W strategy runs live or in paper mode.
func (e *Engine) SetUserModes(modes map[string]string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.userTradingMode = make(map[string]string, len(modes))
	for uid, mode := range modes {
		m := strings.ToUpper(strings.TrimSpace(mode))
		if m != "PAPER" { // default to LIVE if anything else
			m = "LIVE"
		}
		e.userTradingMode[uid] = m
	}

	e.logger.Info("Updated Cash52W user trading modes",
		zap.Int("user_count", len(e.userTradingMode)))
}

// effectiveModeForUser returns the trading mode for a specific user,
// falling back to the global engine cfg.TradingMode when no per-user
// override is present.
func (e *Engine) effectiveModeForUser(userID string) string {
	e.mu.Lock()
	defer e.mu.Unlock()

	if m, ok := e.userTradingMode[userID]; ok {
		return m
	}
	return e.cfg.TradingMode
}

// HandleBreakout processes a single 52-week breakout event for all configured users.
func (e *Engine) HandleBreakout(ctx context.Context, ev *models.Breakout52WEvent) error {
	if ev == nil {
		return fmt.Errorf("nil Breakout52WEvent")
	}

	// Only act on breakouts that occurred today. The data-ingestion Redis
	// watcher already tries to enforce this, but since the consumer now
	// starts from the earliest offsets (to support same-day backlog), we add
	// a final safeguard here.
	if ev.Week52HighDate != "" && ev.Week52HighDate != todayStr() {
		return nil
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
		st = &userState{Positions: make(map[string]models.AllocationPosition)}
		e.userState[userID] = st
	}
	return st
}

// publishAllocation emits the current allocation snapshot for a given user to
// the portfolio.allocations topic. It is safe to call this frequently; callers
// should only invoke it after a meaningful change (e.g. new position opened
// or closed).
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

	positions := make([]models.AllocationPosition, 0, len(st.Positions))
	for _, pos := range st.Positions {
		positions = append(positions, pos)
	}
	e.mu.Unlock()

	ev := &models.PortfolioAllocationEvent{
		UserID:          userID,
		StrategyID:      "CASH_52W_HIGH",
		StrategyName:    "Cash 52-Week High",
		Date:            todayStr(),
		Positions:       positions,
		TotalPositions:  len(positions),
		MaxPositions:    e.cfg.MaxPositions,
		CapitalPerStock: e.cfg.CapitalPerStock,
		Timestamp:       time.Now(),
	}

	if err := e.allocPub.PublishAllocation(ctx, ev); err != nil {
		e.logger.Error("Failed to publish portfolio allocation",
			zap.String("user_id", userID),
			zap.Error(err))
	} else {
		e.logger.Debug("Published portfolio allocation",
			zap.String("user_id", userID),
			zap.Int("total_positions", ev.TotalPositions))
	}
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

	// Compute quantity from capital per stock so that we invest roughly
	// ₹CapitalPerStock per breakout: qty ≈ CapitalPerStock / LTP.
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
		// For 52W engine we derive StockCode directly from the numeric
		// trading token provided in the breakout event. This token is
		// sourced from stocks.db via the data-ingestion service, so using
		// it here ensures trade-execution sees a real stock_code instead
		// of 0.
		StockCode: parseToken(ev.Token),
		Token:     parseToken(ev.Token),
		Symbol:    ev.Symbol,
		Exchange:  strings.ToUpper(ev.Exchange),
		OrderType: "MARKET",
		Quantity:  qty,
		Price:     ev.LTP,
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

	mode := e.effectiveModeForUser(userID)

	// In PAPER mode we stop here: we have computed the order using real
	// breakout prices and sent a trade-signal to Kafka (if configured),
	// but we deliberately do NOT send the order to RabbitMQ /
	// trade-execution.
	if mode == "PAPER" {
		e.logger.Info("52w-high paper trade simulated (no real order sent)",
			zap.String("user_id", userID),
			zap.String("token", ev.Token),
			zap.String("symbol", ev.Symbol),
			zap.String("exchange", orderReq.Exchange),
			zap.Int32("quantity", qty),
			zap.Float64("price", ev.LTP))
	} else {
		// LIVE mode: publish order to RabbitMQ for real execution
		if err := e.rabbitPub.PublishOrder(ctx, orderReq); err != nil {
			return fmt.Errorf("failed to publish 52w order: %w", err)
		}
	}

	// Track that this user has taken this token today
	st.Positions[ev.Token] = models.AllocationPosition{
		Token:      ev.Token,
		Symbol:     ev.Symbol,
		Exchange:   orderReq.Exchange,
		Quantity:   qty,
		EntryPrice: ev.LTP,
	}

	// Publish updated allocation snapshot
	e.publishAllocation(ctx, userID)

	modeLabel := mode
	e.logger.Info("52w-high order processed",
		zap.String("mode", modeLabel),
		zap.String("user_id", userID),
		zap.String("token", ev.Token),
		zap.String("symbol", ev.Symbol),
		zap.String("exchange", orderReq.Exchange),
		zap.Int32("quantity", qty),
		zap.Float64("price", ev.LTP))

	return nil
}
