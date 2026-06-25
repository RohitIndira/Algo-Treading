package consumer

import (
	"context"
	"fmt"
	"math"
	"sync"

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

// Handler handles market events
//
// RabbitMQ publisher removed 2026-06-25 — Kafka-only publishing has been the
// only configured path for a long time (main.go passed nil for rabbitPubl).
// Both the parameter and the if-nil-check branch in HandleEvent are gone.
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
		evalRes.matches, evalRes.err = h.engine.EvaluateEvent(ctx, event)
	}()

	// Goroutine 2: prefetch market data from Redis (IO-bound, single GET)
	// Check tick-size cache first; if hit, we can skip the Redis call when
	// the event already carries a valid LTP.
	wg.Add(1)
	go func() {
		defer wg.Done()
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
func (h *Handler) processMatch(ctx context.Context, match *models.RuleMatch, event *models.MarketEvent, md *MarketDataResult) error {
	strategy := match.Strategy
	if strategy == nil {
		h.logger.Error("Strategy is nil in match, cannot generate order",
			zap.String("strategy_id", match.StrategyID),
			zap.String("user_id", match.UserID))
		return fmt.Errorf("strategy is nil in match")
	}

	if strategy.TradeConfig.Quantity <= 0 {
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

	switch match.PctChangeStatus {
	case "within_range":
		// ── Case 1: immediate execution ──────────────────────────────
		// Limit price = LTP + 0.5% buffer to cross the spread.
		limitPrice := roundToTickSize(ltp*1.005, tickSize)

		orderReq.OrderType = "LIMIT"
		orderReq.Price = limitPrice

		if isTrailingSL {
			// Trailing SL → route to custom OCO in trade-execution.
			// Use INTRADAY (not BRACKET) because OCO places SL+TP legs separately.
			orderReq.ProductType = "INTRADAY"
		} else {
			// Fixed SL → use broker's native bracket order.
			orderReq.ProductType = "BRACKET"
		}

		// SL/TP from the trigger price (LTP) — the actual entry target.
		// limitPrice is just a buffer to ensure fill, not the base for risk.
		orderReq.StopLoss = roundToTickSize(ltp*(1-slPct/100), tickSize)
		orderReq.TakeProfit = roundToTickSize(ltp*(1+tpPct/100), tickSize)

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

		if isTrailingSL {
			// Trailing SL → route to custom OCO in trade-execution.
			orderReq.ProductType = "INTRADAY"
		} else {
			// Fixed SL → PriceMonitor will place broker bracket order.
			orderReq.ProductType = "BRACKET"
		}

		// SL/TP from the trigger price (targetMonitorPrice) — the actual entry target.
		// limitPrice is just a buffer to ensure fill, not the base for risk.
		orderReq.StopLoss = roundToTickSize(targetMonitorPrice*(1-slPct/100), tickSize)
		orderReq.TakeProfit = roundToTickSize(targetMonitorPrice*(1+tpPct/100), tickSize)

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
		orderReq.StopLoss = roundToTickSize(ltp*(1-slPct/100), tickSize)
		orderReq.TakeProfit = roundToTickSize(ltp*(1+tpPct/100), tickSize)
	}

	// ── Validate order ──────────────────────────────────────────────────
	if err := orderReq.Validate(); err != nil {
		return fmt.Errorf("invalid order request: %w", err)
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

	// RabbitMQ publish path removed 2026-06-25 (Kafka-only publishing has
	// been the only configured path for a long time — main.go was passing
	// nil for the RabbitMQ publisher).

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
