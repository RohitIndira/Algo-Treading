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
	store      *ConfigStore
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
	// userTradingMode holds per-user trading modes (LIVE/PAPER) fetched
	// from Elasticsearch via QueryEngine. When a user is present here
	// with mode PAPER, their 52W orders will be simulated (no real
	// orders sent to trade-execution). When absent, we default to LIVE.
	userTradingMode map[string]string // key: userID -> "LIVE" / "PAPER"
}

// NewEngine creates a new Cash 52-week engine.
func NewEngine(cfg Config, store *ConfigStore, riskClient *risk.Client, rabbitPub *publisher.Publisher, kafkaPub *publisher.KafkaPublisher, allocPub *publisher.KafkaPublisher, logger *zap.Logger) *Engine {
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
		cfg:             cfg,
		store:           store,
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

// effectiveModeForUser returns the trading mode for a specific user.
// When there is no explicit per-user override from user-config
// (via Elasticsearch), we default to LIVE.
func (e *Engine) effectiveModeForUser(userID string) string {
	e.mu.Lock()
	defer e.mu.Unlock()

	if m, ok := e.userTradingMode[userID]; ok {
		return m
	}
	return "LIVE"
}

// HandleBreakout processes a single 52-week breakout event for all configured users.
func (e *Engine) HandleBreakout(ctx context.Context, ev *models.Breakout52WEvent) error {
	if ev == nil {
		return fmt.Errorf("nil Breakout52WEvent")
	}

	// Safety guard: the breakout topic may retain older messages; we only
	// want to trade the current trading day's 52W highs.
	//
	// data-ingestion already tries to publish only today's breakouts, but
	// we re-check here so a new consumer group starting from old offsets
	// doesn't generate historical orders.
	if strings.TrimSpace(ev.Week52HighDate) != "" && strings.TrimSpace(ev.Week52HighDate) != todayStr() {
		e.logger.Debug("Skipping 52W breakout not from today",
			zap.String("symbol", ev.Symbol),
			zap.String("token", ev.Token),
			zap.String("week_52_high_date", ev.Week52HighDate),
			zap.String("today", todayStr()))
		return nil
	}

	// LTP MUST be present in breakout event from Redis 52W watcher.
	// Since we're sourcing breakouts from Redis (which has real-time market data),
	// there's no fallback - we need the LTP to calculate position size.
	if ev.LTP <= 0 {
		e.logger.Error("CRITICAL: 52w breakout event missing LTP from Redis",
			zap.String("symbol", ev.Symbol),
			zap.String("token", ev.Token),
			zap.Float64("ltp", ev.LTP),
			zap.String("exchange", ev.Exchange))
		return fmt.Errorf("breakout event has invalid LTP: %f", ev.LTP)
	}

	// Log the breakout event for debugging
	e.logger.Info("Processing 52W breakout from Redis",
		zap.String("symbol", ev.Symbol),
		zap.String("token", ev.Token),
		zap.String("exchange", ev.Exchange),
		zap.Float64("ltp", ev.LTP),
		zap.String("52w_high_date", ev.Week52HighDate))

	e.resetIfNewDay()

	// Get the list of users from the ConfigStore directly to avoid race
	// conditions where the periodic refresh hasn't synced new users yet.
	// This ensures immediate processing of breakout events for users who
	// just enabled the strategy.
	var userIDs []string
	if e.store != nil {
		userIDs, _ = e.store.Snapshot()
	} else {
		userIDs = e.cfg.UserIDs
	}

	if len(userIDs) == 0 {
		e.logger.Warn("No users configured for 52W strategy - skipping breakout",
			zap.String("symbol", ev.Symbol),
			zap.String("token", ev.Token))
		return nil
	}

	e.logger.Info("Processing breakout for users",
		zap.Int("user_count", len(userIDs)),
		zap.Strings("user_ids", userIDs),
		zap.String("symbol", ev.Symbol),
		zap.String("token", ev.Token))

	successCount := 0
	for _, userID := range userIDs {
		e.logger.Debug("Calling handleForUser",
			zap.String("user_id", userID),
			zap.String("token", ev.Token),
			zap.String("symbol", ev.Symbol))
		
		if err := e.handleForUser(ctx, userID, ev); err != nil {
			e.logger.Error("Failed to handle 52w breakout for user",
				zap.Error(err),
				zap.String("user_id", userID),
				zap.String("token", ev.Token),
				zap.String("symbol", ev.Symbol))
			// continue with other users
		} else {
			successCount++
		}
	}

	e.logger.Info("Breakout processing complete",
		zap.String("symbol", ev.Symbol),
		zap.String("token", ev.Token),
		zap.Int("success_count", successCount),
		zap.Int("total_users", len(userIDs)))

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

	// For now, publish allocation snapshots only for PAPER users so that
	// we can focus on paper-trading analytics without mixing in live
	// execution portfolios.
	mode := e.effectiveModeForUser(userID)
	if mode != "PAPER" {
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
		e.logger.Debug("Published portfolio allocation (PAPER)",
			zap.String("user_id", userID),
			zap.Int("total_positions", ev.TotalPositions))
	}
}

func (e *Engine) handleForUser(ctx context.Context, userID string, ev *models.Breakout52WEvent) error {
	// Check per-user 52W configuration from the in-memory store. If the
	// user has not enabled the managed 52W strategy, or if the breakout
	// event occurred before the user enabled it, we skip.
	var capitalPerStock float64
	mode := "LIVE"
	if e.store != nil {
		cfg, ok := e.store.Get(userID)
		if !ok {
			e.logger.Debug("User not found in config store",
				zap.String("user_id", userID),
				zap.String("token", ev.Token))
			return nil
		}
		if !cfg.Enabled {
			e.logger.Debug("User has disabled 52W strategy",
				zap.String("user_id", userID),
				zap.String("token", ev.Token))
			return nil
		}
		capitalPerStock = cfg.CapitalPerStock

		// IMPORTANT: derive trading mode from the config store directly so
		// that a newly enabled user is immediately treated as PAPER/LIVE
		// (no waiting for the 15s refresh loop that calls SetUserModes).
		m := strings.ToUpper(strings.TrimSpace(cfg.TradingMode))
		if m == "PAPER" {
			mode = "PAPER"
		} else {
			mode = "LIVE"
		}
		// Keep the engine cache in sync so helper methods like
		// effectiveModeForUser()/publishAllocation() reflect the latest.
		e.mu.Lock()
		e.userTradingMode[userID] = mode
		e.mu.Unlock()
	}

	// Fallback to engine-level default if config is missing or invalid.
	if capitalPerStock <= 0 {
		capitalPerStock = e.cfg.CapitalPerStock
	}

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
		e.logger.Info("Skipping duplicate 52w breakout - position already opened today",
			zap.String("user_id", userID),
			zap.String("token", ev.Token),
			zap.String("symbol", ev.Symbol),
			zap.Int("current_positions", len(st.Positions)))
		return nil
	}

	// Compute quantity from capital per stock so that we invest roughly
	// ₹CapitalPerStock per breakout: qty ≈ CapitalPerStock / LTP.
	qty := int32(math.Floor(capitalPerStock / ev.LTP))
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

	// Attach trading mode (LIVE/PAPER) to the order request so that
	// downstream services (paper-execution) and analytics can distinguish
	// simulated vs real trades in the trade-signals stream.
	//
	// NOTE: `mode` is derived above from config store for immediate
	// correctness after a user config event.
	orderReq.TradingMode = mode

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
