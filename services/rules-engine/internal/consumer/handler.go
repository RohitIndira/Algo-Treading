package consumer

import (
	"context"
	"fmt"
	"math"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/cache"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/engine"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/holiday"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/publisher"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/repository"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/risk"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/utils"
	"go.uber.org/zap"
)

// ── Pre-trade compliance caps (firm-wide SEBI algo safety ceilings) ──────────
// Hard limits applied to EVERY order before it is published, regardless of the
// per-strategy config. Overridable via env for ops tuning without a redeploy.
var (
	// maxOrderQuantity is the absolute share-count ceiling per order.
	maxOrderQuantity = envInt32("MAX_ORDER_QUANTITY", 50000)
	// maxOrderValueINR is the absolute order-value (quantity × price) ceiling,
	// in rupees. Default ₹2,00,00,000 (2 crore).
	maxOrderValueINR = envFloat("MAX_ORDER_VALUE", 20000000)

	// velocityPct is the market-price-protection threshold: if a stock's
	// peak-to-trough move within velocityWindow ≥ this %, the order is rejected.
	velocityPct = envFloat("VELOCITY_PCT", 1.0)
	// velocityWindowMs is the look-back window (milliseconds) for the move above.
	velocityWindowMs = envInt32("VELOCITY_WINDOW_MS", 1000)

	// maxExposureLimitINR is the per-user total submitted-order value ceiling.
	// 0 = disabled. Set MAX_EXPOSURE_LIMIT env var (e.g. 10000000 = ₹1 crore).
	maxExposureLimitINR = envFloat("MAX_EXPOSURE_LIMIT", 0)

	// bannedTokens is the set of NSE token codes that must never be traded.
	// Populated once at startup from BANNED_TOKENS env var (comma-separated).
	bannedTokens = loadBannedTokens()
)

func envInt32(key string, def int32) int32 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 32); err == nil && n > 0 {
			return int32(n)
		}
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			return f
		}
	}
	return def
}

func loadBannedTokens() map[int64]struct{} {
	m := make(map[int64]struct{})
	for _, s := range strings.Split(os.Getenv("BANNED_TOKENS"), ",") {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		if t, err := strconv.ParseInt(s, 10, 64); err == nil {
			m[t] = struct{}{}
		}
	}
	return m
}

// auditOrderReceived emits one structured audit record for every order request
// the engine builds — the complete order detail required by the compliance
// trail: timestamp, user, security, quantity, price, order type, product type.
func (h *Handler) auditOrderReceived(o *models.OrderRequest, ltp float64) {
	h.logger.Info("ORDER_AUDIT",
		zap.String("event", "ORDER_AUDIT"),
		zap.String("order_id", o.OrderID),
		zap.Time("ts", time.Now()),
		zap.String("user_id", o.UserID),
		zap.String("strategy_id", o.StrategyID),
		zap.String("symbol", o.Symbol),
		zap.Int64("stock_code", o.StockCode),
		zap.String("exchange", o.Exchange),
		zap.String("side", o.OrderSide),
		zap.Int32("quantity", o.Quantity),
		zap.Float64("price", o.Price),
		zap.String("order_type", o.OrderType),
		zap.String("product_type", o.ProductType),
		zap.String("trading_mode", o.TradingMode),
		zap.Float64("ltp", ltp),
	)
}

// rejectForCompliance logs a structured ORDER_REJECTED audit record. The caller
// must `return nil` immediately after so the order is dropped (not published).
func (h *Handler) rejectForCompliance(o *models.OrderRequest, ltp float64, reason string, limit, actual float64) {
	h.logger.Warn("ORDER_REJECTED",
		zap.String("event", "ORDER_REJECTED"),
		zap.String("reason", reason),
		zap.Float64("limit_value", limit),
		zap.Float64("actual_value", actual),
		zap.String("order_id", o.OrderID),
		zap.Time("ts", time.Now()),
		zap.String("user_id", o.UserID),
		zap.String("strategy_id", o.StrategyID),
		zap.String("symbol", o.Symbol),
		zap.Int64("stock_code", o.StockCode),
		zap.String("exchange", o.Exchange),
		zap.String("side", o.OrderSide),
		zap.Int32("quantity", o.Quantity),
		zap.Float64("price", o.Price),
		zap.String("order_type", o.OrderType),
		zap.String("product_type", o.ProductType),
		zap.Float64("ltp", ltp),
	)
}

// rejectForDPR logs ORDER_REJECTED for DPR band breaches and includes both the
// lower and upper DPR bounds so the audit trail shows the full band at the time
// of rejection, not just the breached limit.
func (h *Handler) rejectForDPR(o *models.OrderRequest, ltp float64, reason string, limit, actual, dprLower, dprUpper float64) {
	h.logger.Warn("ORDER_REJECTED",
		zap.String("event", "ORDER_REJECTED"),
		zap.String("reason", reason),
		zap.Float64("limit_value", limit),
		zap.Float64("actual_value", actual),
		zap.Float64("dpr_lower", dprLower),
		zap.Float64("dpr_upper", dprUpper),
		zap.String("order_id", o.OrderID),
		zap.Time("ts", time.Now()),
		zap.String("user_id", o.UserID),
		zap.String("strategy_id", o.StrategyID),
		zap.String("symbol", o.Symbol),
		zap.Int64("stock_code", o.StockCode),
		zap.String("exchange", o.Exchange),
		zap.String("side", o.OrderSide),
		zap.Int32("quantity", o.Quantity),
		zap.Float64("price", o.Price),
		zap.String("order_type", o.OrderType),
		zap.String("product_type", o.ProductType),
		zap.Float64("ltp", ltp),
	)
}

// Handler handles market events
type Handler struct {
	engine         *engine.Engine
	kafkaPubl      *publisher.KafkaPublisher
	signalRepo     *repository.TradeSignalRepository
	riskClient     *risk.Client
	redisCache     *cache.RedisCache
	tickCache      *TickSizeCache
	stats          *models.MatchingStats
	logger         *zap.Logger
	marketHours    *utils.MarketHours
	enforceHours   bool
	holidayChecker *holiday.Checker
	// tickHistory powers the market-price-protection (velocity) check. nil-safe:
	// when unset the velocity check is skipped (fail-open).
	tickHistory *TickHistory
	// strategyLocks serializes the per-strategy "count → cap check → insert"
	// window so concurrent worker goroutines cannot both pass a near-limit
	// check and over-issue trade signals. Keyed by strategy_id.
	strategyLocks sync.Map
}

// SetTickHistory wires the tick-store reader used by the market-price-protection
// (velocity) check. Pass nil to leave the check disabled. Call before Start().
func (h *Handler) SetTickHistory(th *TickHistory) {
	h.tickHistory = th
}

// lockStrategy returns a function that releases a per-strategy mutex. Callers
// must defer the returned func. The mutex covers the daily-cap check and the
// signal save so that two events for the same strategy cannot race past the
// limit. Process-local only — assumes a single rules-engine instance owns each
// strategy's Kafka partition, which the current deployment guarantees.
func (h *Handler) lockStrategy(strategyID string) func() {
	v, _ := h.strategyLocks.LoadOrStore(strategyID, &sync.Mutex{})
	mu := v.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

// NewHandler creates a new event handler
func NewHandler(
	eng *engine.Engine,
	kafkaPubl *publisher.KafkaPublisher,
	signalRepo *repository.TradeSignalRepository,
	riskClient *risk.Client,
	redisCache *cache.RedisCache,
	stats *models.MatchingStats,
	logger *zap.Logger,
	marketHours *utils.MarketHours,
	enforceHours bool,
	holidayChecker *holiday.Checker,
) *Handler {
	return &Handler{
		engine:         eng,
		kafkaPubl:      kafkaPubl,
		signalRepo:     signalRepo,
		riskClient:     riskClient,
		redisCache:     redisCache,
		tickCache:      NewTickSizeCache(),
		stats:          stats,
		logger:         logger,
		marketHours:    marketHours,
		enforceHours:   enforceHours,
		holidayChecker: holidayChecker,
	}
}

// HandleEvent processes a market event.
//
// Parallel processing: strategy evaluation and market-data prefetch run
// concurrently so the Redis round-trip is hidden behind the CPU work of
// evaluating strategies. By the time matches are ready, the market data
// (LTP, prev_close, tick_size) is already available.
func (h *Handler) HandleEvent(ctx context.Context, event *models.MarketEvent) error {
	h.stats.IncrementEventsProcessed()

	// Check if today is a trading holiday (BSE/NSE) — skip all processing
	if h.holidayChecker != nil && h.holidayChecker.IsTodayHoliday() {
		h.logger.Info("Skipping event processing - today is a trading holiday",
			zap.String("event_id", event.EventID),
			zap.Time("event_timestamp", event.Timestamp))
		return nil
	}

	// Check if market is open before generating trade signals (only if enforcement is enabled)
	if h.enforceHours && !h.marketHours.IsMarketOpen() {
		status := h.marketHours.GetMarketStatus()
		h.logger.Info("Skipping trade signal generation - market is closed",
			zap.String("event_id", event.EventID),
			zap.String("market_status", status),
			zap.Time("event_timestamp", event.Timestamp))
		return nil
	}

	h.logger.Info("Handling event",
		zap.String("event_id", event.EventID),
		zap.Int64("stock_code", event.StockData.StockCode),
		zap.String("symbol", event.StockData.Symbol),
		zap.String("exchange", event.StockData.Exchange),
		zap.String("category", event.NewsData.Category),
		zap.String("sentiment", event.Analysis.GetSentimentValue()),
		zap.Int32("impact_score", event.Analysis.ImpactScore),
		zap.Float64("event_pct_change", event.MarketData.PctChange),
		zap.Int64("volume", event.MarketData.PriceMap.Volume),
	)

	// ── Parallel: strategy evaluation + market data prefetch ────────────
	// Both are independent and IO/CPU-bound respectively. Running them
	// concurrently hides the Redis latency behind the evaluation work.
	type evalResult struct {
		matches []*models.RuleMatch
		err     error
	}
	type mdResult struct {
		data *MarketDataResult
		err  error
	}

	var (
		evalRes evalResult
		mdRes   mdResult
		wg      sync.WaitGroup
	)

	// Goroutine 1: evaluate strategies (CPU-bound, uses worker pool internally)
	wg.Add(1)
	go func() {
		defer wg.Done()
		// recover() only catches panics in THIS goroutine — EvaluateEvent's own
		// code (dedup, snapshot, wg.Wait aggregation) runs here directly, outside
		// the worker pool's own recovery. Without this, a panic here kills the
		// whole rules-engine over a single event instead of just failing it.
		defer func() {
			if rec := recover(); rec != nil {
				h.logger.Error("PANIC RECOVERED evaluating event",
					zap.String("event_id", event.EventID),
					zap.Any("panic", rec),
					zap.String("stacktrace", string(debug.Stack())))
				evalRes.err = fmt.Errorf("panic recovered during evaluation: %v", rec)
			}
		}()
		evalRes.matches, evalRes.err = h.engine.EvaluateEvent(ctx, event)
	}()

	// Goroutine 2: prefetch market data from Redis (IO-bound, single GET)
	// Check tick-size cache first; if hit, we can skip the Redis call when
	// the event already carries a valid LTP.
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() {
			if rec := recover(); rec != nil {
				h.logger.Error("PANIC RECOVERED prefetching market data",
					zap.String("event_id", event.EventID),
					zap.Any("panic", rec),
					zap.String("stacktrace", string(debug.Stack())))
				mdRes.err = fmt.Errorf("panic recovered during market data prefetch: %v", rec)
			}
		}()
		// Always fetch full market data: we need LTP (if event lacks it),
		// prev_close (for below_min case), and tick_size.
		// If tick_size is already cached, we still need LTP/prev_close.
		md, err := getMarketDataFromRedis(ctx, h.redisCache, event.StockData, h.logger)
		if err != nil {
			mdRes.err = err
			return
		}
		mdRes.data = md
		// Update tick-size cache for future use.
		h.tickCache.Set(event.StockData.StockCode, md.TickSize)
	}()

	wg.Wait()

	// Handle evaluation errors.
	if evalRes.err != nil {
		h.stats.IncrementEvaluationErrors()
		return fmt.Errorf("failed to match event: %w", evalRes.err)
	}

	matches := evalRes.matches
	if len(matches) == 0 {
		h.logger.Info("No strategies matched event (exact-match semantics)",
			zap.String("event_id", event.EventID),
			zap.Int64("stock_code", event.StockData.StockCode),
			zap.String("symbol", event.StockData.Symbol))
		return nil
	}

	// Market data is required for order construction. If the prefetch failed
	// and the event itself doesn't carry a usable LTP, we cannot proceed.
	if mdRes.err != nil && event.MarketData.LastTradedPrice <= 0 {
		h.logger.Error("Market data prefetch failed and event has no LTP",
			zap.String("event_id", event.EventID),
			zap.String("symbol", event.StockData.Symbol),
			zap.Error(mdRes.err))
		return fmt.Errorf("no market data available for stock %s: %w", event.StockData.Symbol, mdRes.err)
	}

	h.logger.Info("Event matched strategies (exact-match semantics)",
		zap.String("event_id", event.EventID),
		zap.Int("match_count", len(matches)))
	for _, m := range matches {
		h.logger.Info("Matched strategy",
			zap.String("event_id", event.EventID),
			zap.String("user_id", m.UserID),
			zap.String("strategy_id", m.StrategyID),
			zap.String("strategy_name", m.StrategyName),
			zap.Strings("matched_conditions", m.MatchedConditions),
			zap.Float64("score_observability", m.MatchScore),
		)
	}

	// Record statistics
	h.stats.IncrementMatchesFound()
	for _, match := range matches {
		h.stats.RecordStrategyMatch(match.StrategyID, match.StrategyName)
		h.stats.RecordStockMatch(event.StockData.StockCode, event.StockData.Symbol)
	}

	// Process each match with preloaded market data.
	for _, match := range matches {
		if err := h.processMatch(ctx, match, event, mdRes.data); err != nil {
			h.logger.Error("Failed to process match",
				zap.Error(err),
				zap.String("strategy_id", match.StrategyID),
				zap.String("user_id", match.UserID))
			// Continue processing other matches
			continue
		}
	}

	return nil
}

// processMatch builds and publishes an order for a single strategy match.
//
// The preloaded MarketDataResult (md) contains LTP, prev_close, and tick_size
// fetched in a single Redis GET during HandleEvent's parallel prefetch phase.
// This eliminates redundant Redis calls and ensures all price rounding uses
// the actual tick size from the exchange feed, not a hardcoded default.
//
// Percentage accuracy: SL and TP are always computed from the final buying
// price (the limit price the order will actually execute at), ensuring the
// user's configured percentage is applied exactly (subject to tick rounding).
// isWithinTradeWindow returns true when the current IST time falls within
// the strategy's configured trade window [start, end] (both inclusive).
// Returns true when either bound is empty (i.e. no restriction is configured).
//
// windowStart / windowEnd must be in "HH:MM" 24-hour format.
// IST = UTC+5:30.
func isWithinTradeWindow(windowStart, windowEnd string) bool {
	if windowStart == "" || windowEnd == "" {
		return true // no window configured
	}

	ist := time.FixedZone("IST", 5*60*60+30*60)
	now := time.Now().In(ist)

	// Parse HH:MM for start and end.
	parseHHMM := func(s string) (int, int, bool) {
		var h, m int
		_, err := fmt.Sscanf(s, "%d:%d", &h, &m)
		return h, m, err == nil
	}

	sh, sm, ok1 := parseHHMM(windowStart)
	eh, em, ok2 := parseHHMM(windowEnd)
	if !ok1 || !ok2 {
		return true // malformed window — fail open (do not block trading)
	}

	// Express as minutes since midnight for simple comparison.
	nowMins := now.Hour()*60 + now.Minute()
	startMins := sh*60 + sm
	endMins := eh*60 + em

	return nowMins >= startMins && nowMins <= endMins
}

func (h *Handler) processMatch(ctx context.Context, match *models.RuleMatch, event *models.MarketEvent, md *MarketDataResult) error {
	strategy := match.Strategy
	if strategy == nil {
		h.logger.Error("Strategy is nil in match, cannot generate order",
			zap.String("strategy_id", match.StrategyID),
			zap.String("user_id", match.UserID))
		return fmt.Errorf("strategy is nil in match")
	}

	// Serialize this strategy's processing so the daily-cap check and the
	// signal insert cannot race with another goroutine for the same strategy.
	// Held until the function returns (covers cap check, order build, and save).
	defer h.lockStrategy(strategy.StrategyID)()

	// ── Trade window check ──────────────────────────────────────────────────
	// If the strategy has a trade window configured, only place orders when
	// the current IST time is within that window.
	if !isWithinTradeWindow(strategy.TradeConfig.TradeWindowStart, strategy.TradeConfig.TradeWindowEnd) {
		h.logger.Info("Skipping order — current time is outside strategy trade window",
			zap.String("strategy_id", strategy.StrategyID),
			zap.String("user_id", strategy.UserID),
			zap.String("trade_window_start", strategy.TradeConfig.TradeWindowStart),
			zap.String("trade_window_end", strategy.TradeConfig.TradeWindowEnd),
			zap.String("symbol", event.StockData.Symbol))
		return nil
	}

	// ── Daily per-stock lock ─────────────────────────────────────────────────
	// Once any signal is created for this (strategy, stock) on today's IST date
	// (and after the strategy's last activation), block further signals for the
	// rest of that day. Resets next day or when the user reactivates the strategy.
	alreadyTraded, err := h.signalRepo.HasSignalToday(ctx, strategy.StrategyID, event.StockData.StockCode, strategy.UpdatedAt)
	if err != nil {
		h.logger.Error("Failed to check daily signal lock, skipping order to be safe",
			zap.String("strategy_id", strategy.StrategyID),
			zap.Int64("stock_code", event.StockData.StockCode),
			zap.Error(err))
		return nil
	}
	if alreadyTraded {
		h.logger.Info("Skipping — stock already entered for this strategy today",
			zap.String("strategy_id", strategy.StrategyID),
			zap.String("symbol", event.StockData.Symbol),
			zap.Int64("stock_code", event.StockData.StockCode))
		return nil
	}

	// ── Per-strategy daily trade cap ──────────────────────────────────────────
	if limit := strategy.RiskLimits.MaxTradesPerStrategy; limit > 0 && h.signalRepo != nil {
		count, err := h.signalRepo.GetStrategyTradesToday(ctx, strategy.StrategyID, strategy.UpdatedAt)
		if err != nil {
			h.logger.Error("Failed to fetch strategy trade count, skipping to be safe",
				zap.String("strategy_id", strategy.StrategyID),
				zap.Error(err))
			return nil
		}
		if int32(count) >= limit {
			h.logger.Info("Skipping — strategy has reached its daily trade limit",
				zap.String("strategy_id", strategy.StrategyID),
				zap.String("user_id", strategy.UserID),
				zap.Int("trades_today", count),
				zap.Int32("max_trades_per_strategy", limit))
			return nil
		}
	}

	// When amount-based sizing is active, quantity is derived from the budget at
	// execution time — skip the stored-qty check in that case.
	if strategy.RiskLimits.MaxAmountPerStock == 0 && strategy.TradeConfig.Quantity <= 0 {
		h.logger.Error("Strategy has invalid quantity in trade_config",
			zap.String("strategy_id", strategy.StrategyID),
			zap.String("user_id", strategy.UserID),
			zap.Int32("quantity", strategy.TradeConfig.Quantity))
		return fmt.Errorf("strategy %s has invalid quantity: %d", strategy.StrategyID, strategy.TradeConfig.Quantity)
	}

	// ── Resolve tick size ───────────────────────────────────────────────
	// Source of truth: Redis market data JSON (tick_size field).
	// Cached in TickSizeCache after the first fetch for low-latency reuse.
	var tickSize float64
	if md != nil {
		tickSize = md.TickSize
	}
	if tickSize <= 0 {
		// Try the in-memory cache (populated by a previous event for this stock).
		if cached, ok := h.tickCache.Get(event.StockData.StockCode); ok && cached > 0 {
			tickSize = cached
		}
	}
	if tickSize <= 0 {
		h.logger.Error("tick_size not available from Redis or cache, cannot place order",
			zap.String("strategy_id", strategy.StrategyID),
			zap.String("symbol", event.StockData.Symbol),
			zap.Int64("stock_code", event.StockData.StockCode))
		return fmt.Errorf("tick_size not available for stock %s", event.StockData.Symbol)
	}

	// ── Resolve LTP from Redis (source of truth for current price) ──────
	if md == nil {
		h.logger.Error("No Redis market data available, cannot place order",
			zap.String("event_id", event.EventID),
			zap.String("symbol", event.StockData.Symbol))
		return fmt.Errorf("no market data available for stock %s", event.StockData.Symbol)
	}
	ltp := md.LTP
	if ltp <= 0 {
		h.logger.Error("No LTP available from Redis",
			zap.String("event_id", event.EventID),
			zap.String("symbol", event.StockData.Symbol))
		return fmt.Errorf("no price available for stock %s", event.StockData.Symbol)
	}

	// ── Re-validate pct_change against live Redis data ───────────────────
	// The evaluator used the event's (potentially stale) pct_change.
	// Re-check here with real-time Redis percent_change before placing.
	if maxPct := strategy.Conditions.MaxPctChange; maxPct > 0 {
		if absCurrent := math.Abs(md.PercentChange); absCurrent >= maxPct {
			h.logger.Info("Skipping order — live pct_change exceeds max",
				zap.String("strategy_id", strategy.StrategyID),
				zap.String("symbol", event.StockData.Symbol),
				zap.Float64("live_pct_change", md.PercentChange),
				zap.Float64("max_pct_change", maxPct))
			return nil
		}
	}

	// ── Resolve prev_close ──────────────────────────────────────────────
	prevClose := event.MarketData.PriceMap.PrevClose
	if prevClose <= 0 && md != nil {
		prevClose = md.PrevClose
	}

	// Log resolved market data.
	h.logger.Info("Using strategy configuration from in-memory config store",
		zap.String("strategy_id", strategy.StrategyID),
		zap.String("user_id", strategy.UserID),
		zap.Int32("quantity", strategy.TradeConfig.Quantity),
		zap.String("order_type", strategy.TradeConfig.OrderType),
		zap.String("order_side", "BUY"),
		zap.String("exchange", strategy.TradeConfig.Exchange),
		zap.Float64("stop_loss_pct", strategy.TradeConfig.StopLossPct),
		zap.Float64("take_profit_pct", strategy.TradeConfig.TakeProfitPct))

	h.logger.Info("Resolved market data for order",
		zap.String("symbol", event.StockData.Symbol),
		zap.Int64("stock_code", event.StockData.StockCode),
		zap.Float64("ltp", ltp),
		zap.Float64("prev_close", prevClose),
		zap.Float64("tick_size", tickSize),
		zap.String("ltp_source", ltpSource(event, md)))

	// ── Create order request (template — SL/TP filled below) ────────────
	orderReq := models.NewOrderRequest(match, event, strategy)
	orderReq.Price = ltp // will be overridden below for Case 1/2

	// sizingPrice = the price the order is expected to actually FILL at.
	// For Case 2 (STOP_LOSS + BRACKET) orderReq.Price holds the trigger price,
	// not the limit price, so we track the limit separately for budget sizing.
	// Defaults to orderReq.Price; switch cases below override when they diverge.
	sizingPrice := orderReq.Price

	// ── Price-change case routing ───────────────────────────────────────
	// entryPrice = the limit price the order will actually execute at.
	// slBasePrice = the price from which SL/TP percentages are computed.
	// In most cases entryPrice == slBasePrice, except Case 2 where the
	// order.Price is set to targetMonitorPrice (for the PriceMonitor) but
	// SL/TP are based on the limit price the monitor will use.
	orderReq.PctChangeStatus = match.PctChangeStatus
	orderReq.CurrentPctChange = md.PercentChange

	slPct := strategy.TradeConfig.StopLossPct
	tpPct := strategy.TradeConfig.TakeProfitPct
	isTrailingSL := strategy.TradeConfig.StopLossType == "TRAILING"
	// For MULTI_LEVEL modes, absolute SL/TP prices are computed by the multi-level
	// manager from level price_pct values. Setting them here from slPct/tpPct=0 would
	// produce SL=TP=entry price, causing the paper monitor to exit immediately.
	isMultiLevelSL := strategy.TradeConfig.StopLossType == "MULTI_LEVEL"
	isMultiLevelTP := strategy.TradeConfig.TakeProfitType == "MULTI_LEVEL"

	switch match.PctChangeStatus {
	case "within_range":
		// ── Case 1: immediate execution ──────────────────────────────
		// Limit price = LTP + 0.5% buffer to cross the spread.
		limitPrice := roundToTickSize(ltp*1.005, tickSize)

		orderReq.OrderType = "LIMIT"
		orderReq.Price = limitPrice
		sizingPrice = limitPrice

		if isTrailingSL {
			// Trailing SL → route to custom OCO in trade-execution.
			// Use INTRADAY (not BRACKET) because OCO places SL+TP legs separately.
			orderReq.ProductType = "INTRADAY"
		} else if isMultiLevelSL || isMultiLevelTP {
			// Any ML leg → INTRADAY so the broker does NOT place its own SL/TP legs.
			// ML manager places N separate exit orders; a broker bracket leg would
			// conflict (double-exit) or be rejected with TP=0.
			orderReq.ProductType = "INTRADAY"
		} else {
			orderReq.ProductType = "BRACKET"
		}

		// SL/TP from the trigger price (LTP).
		// Leave as 0 for MULTI_LEVEL modes — ML manager computes its own triggers.
		// Leave as 0 when pct is 0 — user disabled that leg (enable_stop_loss /
		// enable_take_profit = false on the frontend). Computing ltp*(1±0/100)=ltp
		// would set SL/TP equal to the entry price and cause an immediate exit.
		if !isMultiLevelSL && slPct > 0 {
			orderReq.StopLoss = roundToTickSize(ltp*(1-slPct/100), tickSize)
		}
		if !isMultiLevelTP && tpPct > 0 {
			orderReq.TakeProfit = roundToTickSize(ltp*(1+tpPct/100), tickSize)
		}

		h.logger.Info("Case 1: pct_change within range — immediate order",
			zap.String("strategy_id", strategy.StrategyID),
			zap.String("order_type", orderReq.OrderType),
			zap.Float64("current_pct_change", md.PercentChange),
			zap.Float64("ltp", ltp),
			zap.Float64("limit_price", limitPrice),
			zap.Float64("tick_size", tickSize),
			zap.Float64("stop_loss_pct", slPct),
			zap.Float64("take_profit_pct", tpPct),
			zap.Float64("stop_loss", orderReq.StopLoss),
			zap.Float64("take_profit", orderReq.TakeProfit))

	case "below_min":
		// ── Case 2: pending STOP_LOSS + BRACKET order ───────────────
		minPct := strategy.Conditions.MinPctChange

		// Reference price for target calculation.
		referencePrice := prevClose
		if referencePrice <= 0 {
			h.logger.Warn("prev_close unavailable, falling back to LTP as reference",
				zap.String("symbol", event.StockData.Symbol),
				zap.Float64("ltp", ltp))
			referencePrice = ltp
		}

		// targetMonitorPrice = price at exactly minPct% above prev_close.
		targetMonitorPrice := referencePrice * (1 + minPct/100)

		// limitPrice = target + 0.5% buffer, tick-rounded.
		limitPrice := roundToTickSize(targetMonitorPrice*1.005, tickSize)
		if limitPrice <= 0 {
			h.logger.Error("Computed limit price is invalid, skipping order",
				zap.String("strategy_id", strategy.StrategyID),
				zap.Float64("ltp", ltp),
				zap.Float64("prev_close", prevClose),
				zap.Float64("min_pct_change", minPct))
			return fmt.Errorf("invalid limit price computed for stock %s", event.StockData.Symbol)
		}

		orderReq.OrderType = "STOP_LOSS"
		orderReq.Price = targetMonitorPrice // PriceMonitor watches this level
		// Size against the limit price (target + 0.5% buffer) — that is what the
		// order will actually fill at, so the budget cap is honored on fill, not
		// underestimated against the lower trigger price.
		sizingPrice = limitPrice
		// Always BRACKET for below_min — price monitor detects this combination
		// (STOP_LOSS + BRACKET) to route to watch queue. SL type (FIXED/TRAILING)
		// is carried by stop_loss_type and handled after the price monitor fires.
		orderReq.ProductType = "BRACKET"

		// SL/TP from the trigger price (targetMonitorPrice).
		// Leave as 0 for MULTI_LEVEL modes — ML manager computes its own triggers.
		// Leave as 0 when pct is 0 — user disabled that leg.
		if !isMultiLevelSL && slPct > 0 {
			orderReq.StopLoss = roundToTickSize(targetMonitorPrice*(1-slPct/100), tickSize)
		}
		if !isMultiLevelTP && tpPct > 0 {
			orderReq.TakeProfit = roundToTickSize(targetMonitorPrice*(1+tpPct/100), tickSize)
		}

		// Max monitor price (upper bound).
		maxPct := strategy.Conditions.MaxPctChange
		if maxPct > 0 {
			orderReq.MaxMonitorPrice = referencePrice * (1 + maxPct/100)
		}

		h.logger.Info("Case 2: pct_change below min — order sent to price monitor",
			zap.String("strategy_id", strategy.StrategyID),
			zap.String("order_type", orderReq.OrderType),
			zap.Float64("current_pct_change", md.PercentChange),
			zap.Float64("min_pct_change", minPct),
			zap.Float64("max_pct_change", maxPct),
			zap.Float64("ltp", ltp),
			zap.Float64("prev_close", prevClose),
			zap.Float64("reference_price", referencePrice),
			zap.Float64("target_monitor_price", targetMonitorPrice),
			zap.Float64("max_monitor_price", orderReq.MaxMonitorPrice),
			zap.Float64("limit_price_with_buffer", limitPrice),
			zap.Float64("tick_size", tickSize),
			zap.Float64("stop_loss_pct", slPct),
			zap.Float64("take_profit_pct", tpPct),
			zap.Float64("stop_loss", orderReq.StopLoss),
			zap.Float64("take_profit", orderReq.TakeProfit))

	default:
		// ── No pct_change filter: SL/TP from LTP directly ───────────
		// Leave as 0 when pct is 0 — user disabled that leg.
		if !isMultiLevelSL && slPct > 0 {
			orderReq.StopLoss = roundToTickSize(ltp*(1-slPct/100), tickSize)
		}
		if !isMultiLevelTP && tpPct > 0 {
			orderReq.TakeProfit = roundToTickSize(ltp*(1+tpPct/100), tickSize)
		}
	}

	// ── Amount-based position sizing ─────────────────────────────────────────
	// When max_amount_per_stock > 0 the user chose amount-based sizing: we derive
	// the share count from 98% of their budget at the resolved fill price
	// (sizingPrice — the limit price the order will actually execute at). The
	// 2% headroom covers spread, brokerage, and minor slippage so the order
	// never exceeds the stated limit. math.Floor makes truncation explicit and
	// guards against any future signed-input regression. This qty is forwarded
	// to trade-execution for SL / TP legs unchanged.
	if maxAmt := strategy.RiskLimits.MaxAmountPerStock; maxAmt > 0 {
		if sizingPrice <= 0 {
			h.logger.Error("Skipping — invalid sizing price for amount-based budget",
				zap.String("strategy_id", strategy.StrategyID),
				zap.String("symbol", event.StockData.Symbol),
				zap.Float64("sizing_price", sizingPrice))
			return nil
		}
		effectiveBudget := maxAmt * 0.98
		computedQty := int32(math.Floor(effectiveBudget / sizingPrice))
		if computedQty < 1 {
			h.logger.Info("Skipping — cannot afford even 1 share within max_amount_per_stock budget",
				zap.String("strategy_id", strategy.StrategyID),
				zap.String("user_id", strategy.UserID),
				zap.String("symbol", event.StockData.Symbol),
				zap.Float64("max_amount_per_stock", maxAmt),
				zap.Float64("effective_budget_98pct", effectiveBudget),
				zap.Float64("sizing_price", sizingPrice))
			return nil
		}
		orderReq.Quantity = computedQty
		h.logger.Info("Amount-based sizing: derived quantity from budget",
			zap.String("strategy_id", strategy.StrategyID),
			zap.Float64("max_amount_per_stock", maxAmt),
			zap.Float64("effective_budget_98pct", effectiveBudget),
			zap.Float64("sizing_price", sizingPrice),
			zap.Int32("computed_qty", computedQty))
	}

	// Warn when both SL and TP are disabled (pct = 0) on a non-multi-level order.
	// The position will remain open until manual exit or auto square-off.
	if !isMultiLevelSL && !isMultiLevelTP && orderReq.StopLoss == 0 && orderReq.TakeProfit == 0 {
		h.logger.Warn("Order has no SL or TP — position will have no automatic exit protection",
			zap.String("strategy_id", strategy.StrategyID),
			zap.String("user_id", strategy.UserID),
			zap.String("symbol", event.StockData.Symbol))
	}

	// ── Validate order ──────────────────────────────────────────────────
	if err := orderReq.Validate(); err != nil {
		return fmt.Errorf("invalid order request: %w", err)
	}

	// ── Pre-trade compliance audit + hard-cap checks ─────────────────────
	// Capture the complete order detail for the audit trail, then enforce the
	// firm-wide safety ceilings and the exchange DPR band. Any breach is logged
	// as ORDER_REJECTED and the order is dropped (not published) — mirroring the
	// other skip paths above.
	h.auditOrderReceived(orderReq, ltp)

	// (1) Ban list — reject tokens that are prohibited from trading.
	if _, banned := bannedTokens[orderReq.StockCode]; banned {
		h.rejectForCompliance(orderReq, ltp, "BANNED_TOKEN",
			float64(orderReq.StockCode), float64(orderReq.StockCode))
		return nil
	}

	// (2) Quantity ceiling — never place an order above the share-count cap.
	if orderReq.Quantity > maxOrderQuantity {
		h.rejectForCompliance(orderReq, ltp, "QTY_LIMIT_EXCEEDED",
			float64(maxOrderQuantity), float64(orderReq.Quantity))
		return nil
	}

	// (3) Order-value ceiling — quantity × price must not exceed the cap.
	// Use the limit price when set; fall back to LTP for MARKET orders (price 0).
	// effPrice and orderValue are declared here (not inside the if-block) so they
	// remain accessible for the exposure check (4) and post-publish update below.
	effPrice := orderReq.Price
	if effPrice <= 0 {
		effPrice = ltp
	}
	orderValue := float64(orderReq.Quantity) * effPrice
	if orderValue > maxOrderValueINR {
		h.rejectForCompliance(orderReq, ltp, "ORDER_VALUE_LIMIT_EXCEEDED",
			maxOrderValueINR, orderValue)
		return nil
	}

	// (4) Exposure wallet — cumulative submitted-order value per user must not
	// exceed the configured ceiling. Exposure is tracked in Redis key
	// "exposure:{user_id}" and incremented after a successful order publish.
	if maxExposureLimitINR > 0 {
		exposureKey := fmt.Sprintf("exposure:%s", orderReq.UserID)
		currentExposure, _ := h.redisCache.GetFloat(ctx, exposureKey) // 0 if key absent
		if currentExposure+orderValue > maxExposureLimitINR {
			h.rejectForCompliance(orderReq, ltp, "EXPOSURE_LIMIT_EXCEEDED",
				maxExposureLimitINR, currentExposure+orderValue)
			return nil
		}
	}

	// (5) DPR band — never place a priced (LIMIT/SL) order outside the exchange
	// daily price range. Both bounds are logged for full audit context.
	if orderReq.Price > 0 {
		if md.DPRLower > 0 && orderReq.Price < md.DPRLower {
			h.rejectForDPR(orderReq, ltp, "DPR_LOWER_BREACH",
				md.DPRLower, orderReq.Price, md.DPRLower, md.DPRUpper)
			return nil
		}
		if md.DPRUpper > 0 && orderReq.Price > md.DPRUpper {
			h.rejectForDPR(orderReq, ltp, "DPR_UPPER_BREACH",
				md.DPRUpper, orderReq.Price, md.DPRLower, md.DPRUpper)
			return nil
		}
	}

	// (6) Market price protection — reject if the stock moved ≥ velocityPct
	// within the last velocityWindow (a fast spike we should not chase).
	// Best-effort & fail-open: when tick history is unavailable the check is
	// skipped so missing data never blocks an order.
	if h.tickHistory != nil {
		window := time.Duration(velocityWindowMs) * time.Millisecond
		if movePct, ok := h.tickHistory.MaxMovePct(ctx, md.Exchange, md.Token, window); ok && movePct >= velocityPct {
			h.rejectForCompliance(orderReq, ltp, "VELOCITY_BREACH", velocityPct, movePct)
			return nil
		}
	}

	// ── Risk management check ───────────────────────────────────────────
	// TODO: TEMPORARILY BYPASSED FOR TESTING - REMOVE THIS BEFORE PRODUCTION
	if false && h.riskClient != nil {
		riskResp, err := h.riskClient.CheckPreTradeRisk(ctx, orderReq, strategy)
		if err != nil {
			h.logger.Error("Risk check failed",
				zap.Error(err),
				zap.String("order_id", orderReq.OrderID))
			orderReq.RiskApproved = false
			orderReq.RiskScore = 100.0
		} else {
			orderReq.RiskApproved = riskResp.Approved
			orderReq.RiskScore = riskResp.RiskScore

			if !riskResp.Approved {
				h.logger.Warn("Order rejected by risk management",
					zap.String("order_id", orderReq.OrderID),
					zap.String("user_id", orderReq.UserID),
					zap.Float64("risk_score", riskResp.RiskScore),
					zap.Int("violations", len(riskResp.Violations)))
				for _, violation := range riskResp.Violations {
					h.logger.Debug("Risk violation",
						zap.String("order_id", orderReq.OrderID),
						zap.String("type", violation.Type.String()),
						zap.String("message", violation.Message))
				}
				return nil
			}
		}
	} else {
		h.logger.Warn("RISK CHECK BYPASSED FOR TESTING - Auto-approving order",
			zap.String("order_id", orderReq.OrderID))
		orderReq.RiskApproved = true
		orderReq.RiskScore = 0.0
	}

	// ── Publish order (parallel: DB + Kafka, then RabbitMQ) ─────────────
	// DB save and Kafka publish are independent, so run them concurrently.
	var pubWg sync.WaitGroup

	if h.signalRepo != nil {
		pubWg.Add(1)
		go func() {
			defer pubWg.Done()
			if err := h.signalRepo.SaveTradeSignal(ctx, orderReq); err != nil {
				h.logger.Error("Failed to save trade signal to database",
					zap.Error(err),
					zap.String("order_id", orderReq.OrderID))
			} else {
				h.logger.Debug("Trade signal saved to PostgreSQL",
					zap.String("order_id", orderReq.OrderID),
					zap.String("status", "PENDING"))
			}
		}()
	}

	if h.kafkaPubl != nil {
		pubWg.Add(1)
		go func() {
			defer pubWg.Done()
			if err := h.kafkaPubl.PublishTradeSignal(ctx, orderReq); err != nil {
				h.logger.Error("Failed to publish to Kafka trade-signals",
					zap.Error(err),
					zap.String("order_id", orderReq.OrderID))
			} else {
				h.logger.Debug("Trade signal published to Kafka",
					zap.String("order_id", orderReq.OrderID),
					zap.String("topic", "trade-signals"))
			}
		}()
	}

	pubWg.Wait()

	// Record exposure consumed by this order so the next order for this user
	// sees the updated running total. Best-effort: a Redis error here is logged
	// but does NOT roll back the already-published order.
	if maxExposureLimitINR > 0 {
		exposureKey := fmt.Sprintf("exposure:%s", orderReq.UserID)
		if _, err := h.redisCache.IncrByFloat(ctx, exposureKey, orderValue); err != nil {
			h.logger.Warn("Failed to update exposure tracker in Redis",
				zap.String("user_id", orderReq.UserID),
				zap.Float64("order_value", orderValue),
				zap.Error(err))
		}
	}

	h.stats.IncrementOrdersGenerated()

	h.logger.Info("Order published and tracked",
		zap.String("order_id", orderReq.OrderID),
		zap.String("user_id", orderReq.UserID),
		zap.String("strategy_id", orderReq.StrategyID),
		zap.String("symbol", orderReq.Symbol),
		zap.Int64("stock_code", orderReq.StockCode),
		zap.String("order_type", orderReq.OrderType),
		zap.String("product_type", orderReq.ProductType),
		zap.Float64("price", orderReq.Price),
		zap.Float64("stop_loss", orderReq.StopLoss),
		zap.Float64("take_profit", orderReq.TakeProfit),
		zap.Float64("stop_loss_pct", slPct),
		zap.Float64("take_profit_pct", tpPct),
		zap.Float64("tick_size", tickSize),
		zap.Float64("match_score", orderReq.MatchScore))

	return nil
}

// ltpSource returns a human-readable label for where the LTP came from.
func ltpSource(_ *models.MarketEvent, md *MarketDataResult) string {
	if md != nil && md.LTP > 0 {
		return "redis"
	}
	return "none"
}
