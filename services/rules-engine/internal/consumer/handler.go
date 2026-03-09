package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/cache"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/engine"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/publisher"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/repository"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/risk"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/utils"
	"go.uber.org/zap"
)

// Handler handles market events
type Handler struct {
	engine       *engine.Engine
	rabbitPubl   *publisher.Publisher
	kafkaPubl    *publisher.KafkaPublisher
	signalRepo   *repository.TradeSignalRepository
	riskClient   *risk.Client
	redisCache   *cache.RedisCache
	stats        *models.MatchingStats
	logger       *zap.Logger
	marketHours  *utils.MarketHours
	enforceHours bool
}

// NewHandler creates a new event handler
func NewHandler(
	eng *engine.Engine,
	rabbitPubl *publisher.Publisher,
	kafkaPubl *publisher.KafkaPublisher,
	signalRepo *repository.TradeSignalRepository,
	riskClient *risk.Client,
	redisCache *cache.RedisCache,
	stats *models.MatchingStats,
	logger *zap.Logger,
	marketHours *utils.MarketHours,
	enforceHours bool,
) *Handler {
	return &Handler{
		engine:       eng,
		rabbitPubl:   rabbitPubl,
		kafkaPubl:    kafkaPubl,
		signalRepo:   signalRepo,
		riskClient:   riskClient,
		redisCache:   redisCache,
		stats:        stats,
		logger:       logger,
		marketHours:  marketHours,
		enforceHours: enforceHours,
	}
}

// HandleEvent processes a market event
func (h *Handler) HandleEvent(ctx context.Context, event *models.MarketEvent) error {
	h.stats.IncrementEventsProcessed()

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
		zap.Float64("pct_change", event.MarketData.PctChange),
		zap.Int64("volume", event.MarketData.PriceMap.Volume),
	)

	// Evaluate event against in-memory snapshot
	matches, err := h.engine.EvaluateEvent(ctx, event)
	if err != nil {
		h.stats.IncrementEvaluationErrors()
		return fmt.Errorf("failed to match event: %w", err)
	}

	if len(matches) == 0 {
		h.logger.Info("No strategies matched event (exact-match semantics)",
			zap.String("event_id", event.EventID),
			zap.Int64("stock_code", event.StockData.StockCode),
			zap.String("symbol", event.StockData.Symbol))
		return nil
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

	// Process each match
	for _, match := range matches {
		if err := h.processMatch(ctx, match, event); err != nil {
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

// getLTPFromRedis retrieves Last Traded Price from Redis
// Redis key pattern: market:{exchange}:{token}
// Example: market:nse:10227 or market:bse:532259
func (h *Handler) getLTPFromRedis(ctx context.Context, stockData models.StockData) (float64, error) {
	// Normalize exchange to lowercase for Redis key
	exchangeLower := strings.ToLower(stockData.Exchange)

	// Remove _EQ suffix if present (NSE_EQ -> nse, BSE_EQ -> bse)
	exchangeLower = strings.TrimSuffix(exchangeLower, "_eq")

	// Always try both NSE and BSE to maximize chances of finding the stock
	// Priority order: specified exchange first, then the other one
	exchanges := []string{"nse", "bse"}
	if exchangeLower == "bse" {
		exchanges = []string{"bse", "nse"}
	}

	var lastErr error
	for _, exch := range exchanges {
		// Use the correct exchange-specific code
		var token int64
		if exch == "nse" {
			token = stockData.NSECode
		} else {
			token = stockData.BSECode
		}

		// Skip if this exchange doesn't have a valid code
		if token <= 0 {
			h.logger.Debug("No valid code for exchange, skipping",
				zap.String("exchange", exch),
				zap.Int64("stock_code", stockData.StockCode))
			continue
		}

		// Construct Redis key: market:{exchange}:{token}
		key := fmt.Sprintf("market:%s:%d", exch, token)

		// Get value from Redis
		jsonData, err := h.redisCache.Get(ctx, key)
		if err != nil {
			// If it's a cache miss, try the next exchange
			if err == models.ErrCacheMiss {
				h.logger.Debug("LTP not found in Redis for exchange, trying next",
					zap.String("key", key),
					zap.String("exchange", exch))
				lastErr = err
				continue
			}
			lastErr = fmt.Errorf("redis get error for key %s: %w", key, err)
			continue
		}

		// Parse JSON to extract LTP
		var marketData struct {
			LTP float64 `json:"ltp"`
		}

		if err := json.Unmarshal([]byte(jsonData), &marketData); err != nil {
			lastErr = fmt.Errorf("failed to parse market data JSON for key %s: %w", key, err)
			continue
		}

		if marketData.LTP <= 0 {
			lastErr = fmt.Errorf("invalid LTP value %.2f for token %d on %s", marketData.LTP, token, exch)
			continue
		}

		h.logger.Debug("Successfully retrieved LTP from Redis",
			zap.String("key", key),
			zap.Int64("token", token),
			zap.String("exchange", exch),
			zap.Int64("nse_code", stockData.NSECode),
			zap.Int64("bse_code", stockData.BSECode),
			zap.Float64("ltp", marketData.LTP))

		return marketData.LTP, nil
	}

	// If we get here, stock not found on either exchange
	h.logger.Warn("Stock not found in Redis on any exchange",
		zap.Int64("stock_code", stockData.StockCode),
		zap.Int64("nse_code", stockData.NSECode),
		zap.Int64("bse_code", stockData.BSECode),
		zap.String("symbol", stockData.Symbol),
		zap.Strings("tried_exchanges", exchanges))

	if lastErr != nil {
		return 0, fmt.Errorf("LTP not found in Redis for stock %s (NSE:%d, BSE:%d) (tried exchanges: %v): %w", stockData.Symbol, stockData.NSECode, stockData.BSECode, exchanges, lastErr)
	}
	return 0, fmt.Errorf("LTP not found in Redis for stock %s (NSE:%d, BSE:%d) (tried exchanges: %v)", stockData.Symbol, stockData.NSECode, stockData.BSECode, exchanges)
}

// getOpenFromRedis retrieves today's open price from Redis.
// Uses the same key pattern as getLTPFromRedis but reads the "open" field.
func (h *Handler) getOpenFromRedis(ctx context.Context, stockData models.StockData) (float64, error) {
	exchangeLower := strings.ToLower(stockData.Exchange)
	exchangeLower = strings.TrimSuffix(exchangeLower, "_eq")

	exchanges := []string{"nse", "bse"}
	if exchangeLower == "bse" {
		exchanges = []string{"bse", "nse"}
	}

	var lastErr error
	for _, exch := range exchanges {
		var token int64
		if exch == "nse" {
			token = stockData.NSECode
		} else {
			token = stockData.BSECode
		}
		if token <= 0 {
			continue
		}

		key := fmt.Sprintf("market:%s:%d", exch, token)
		jsonData, err := h.redisCache.Get(ctx, key)
		if err != nil {
			if err == models.ErrCacheMiss {
				lastErr = err
				continue
			}
			lastErr = fmt.Errorf("redis get error for key %s: %w", key, err)
			continue
		}

		var marketData struct {
			Open float64 `json:"open"`
		}
		if err := json.Unmarshal([]byte(jsonData), &marketData); err != nil {
			lastErr = fmt.Errorf("failed to parse market data JSON for key %s: %w", key, err)
			continue
		}
		if marketData.Open <= 0 {
			lastErr = fmt.Errorf("invalid open price %.2f for token %d on %s", marketData.Open, token, exch)
			continue
		}

		h.logger.Debug("Successfully retrieved open price from Redis",
			zap.String("key", key),
			zap.Int64("token", token),
			zap.String("exchange", exch),
			zap.Float64("open", marketData.Open))

		return marketData.Open, nil
	}

	if lastErr != nil {
		return 0, fmt.Errorf("open price not found in Redis for stock %s (NSE:%d, BSE:%d): %w", stockData.Symbol, stockData.NSECode, stockData.BSECode, lastErr)
	}
	return 0, fmt.Errorf("open price not found in Redis for stock %s (NSE:%d, BSE:%d)", stockData.Symbol, stockData.NSECode, stockData.BSECode)
}

// processMatch processes a single match
func (h *Handler) processMatch(ctx context.Context, match *models.RuleMatch, event *models.MarketEvent) error {
	// Use the full strategy from the match (already includes trade_config from in-memory config store)
	strategy := match.Strategy
	if strategy == nil {
		h.logger.Error("Strategy is nil in match, cannot generate order",
			zap.String("strategy_id", match.StrategyID),
			zap.String("user_id", match.UserID))
		return fmt.Errorf("strategy is nil in match")
	}

	// Validate strategy has complete trade configuration
	if strategy.TradeConfig.Quantity <= 0 {
		h.logger.Error("Strategy has invalid quantity in trade_config",
			zap.String("strategy_id", strategy.StrategyID),
			zap.String("user_id", strategy.UserID),
			zap.Int32("quantity", strategy.TradeConfig.Quantity))
		return fmt.Errorf("strategy %s has invalid quantity: %d", strategy.StrategyID, strategy.TradeConfig.Quantity)
	}

	// Log strategy configuration being used
	h.logger.Info("Using strategy configuration from in-memory config store",
		zap.String("strategy_id", strategy.StrategyID),
		zap.String("user_id", strategy.UserID),
		zap.Int32("quantity", strategy.TradeConfig.Quantity),
		zap.String("order_type", strategy.TradeConfig.OrderType),
		zap.String("order_side", "BUY"),
		zap.String("exchange", strategy.TradeConfig.Exchange),
		zap.Float64("stop_loss_pct", strategy.TradeConfig.StopLossPct),
		zap.Float64("take_profit_pct", strategy.TradeConfig.TakeProfitPct))

	// Create order request
	orderReq := models.NewOrderRequest(match, event, strategy)

	// Ensure we have a valid LTP — needed for both MARKET and LIMIT price computation.
	if orderReq.Price <= 0 {
		if event.MarketData.LastTradedPrice > 0 {
			orderReq.Price = event.MarketData.LastTradedPrice
			h.logger.Debug("Using LTP from event data",
				zap.Int64("token", orderReq.Token),
				zap.Int64("stock_code", event.StockData.StockCode),
				zap.String("exchange", event.StockData.Exchange),
				zap.Float64("price", orderReq.Price))
		} else {
			price, err := h.getLTPFromRedis(ctx, event.StockData)
			if err != nil {
				h.logger.Error("Failed to get LTP from Redis, skipping order",
					zap.String("event_id", event.EventID),
					zap.Int64("stock_code", event.StockData.StockCode),
					zap.Int64("nse_code", event.StockData.NSECode),
					zap.Int64("bse_code", event.StockData.BSECode),
					zap.String("symbol", event.StockData.Symbol),
					zap.String("exchange", event.StockData.Exchange),
					zap.Error(err))
				return fmt.Errorf("no price available for stock %s", event.StockData.Symbol)
			}
			orderReq.Price = price
			h.logger.Info("Using LTP from Redis",
				zap.Int64("stock_code", event.StockData.StockCode),
				zap.Int64("nse_code", event.StockData.NSECode),
				zap.Int64("bse_code", event.StockData.BSECode),
				zap.String("symbol", event.StockData.Symbol),
				zap.String("exchange", event.StockData.Exchange),
				zap.Float64("price", price))
		}

		// Recalculate stop loss and take profit using the resolved LTP.
		orderReq.StopLoss = orderReq.Price * (1 - strategy.TradeConfig.StopLossPct/100)
		orderReq.TakeProfit = orderReq.Price * (1 + strategy.TradeConfig.TakeProfitPct/100)
	}

	// ── Price-change case routing ────────────────────────────────────────────
	// Case 2: price change is below the strategy minimum.
	// Instead of skipping, place a LIMIT (bracket) order at the price level where
	// the stock would reach exactly the minimum % change threshold from today's open.
	//
	//   limitPrice = todayOpen × (1 + minPctChange / 100)
	//
	// Case 1 (within_range / ""): use the strategy's configured order type as-is.
	// Case 3 (above_max): the evaluator already marks this as a failed condition,
	//   so the match is dropped before reaching this function.
	orderReq.PctChangeStatus = match.PctChangeStatus
	if match.PctChangeStatus == "below_min" {
		minPct := strategy.Conditions.MinPctChange
		ltp := orderReq.Price

		// Prefer today's open from the event; if missing (news events carry no OHLCV),
		// fetch it from Redis where the live market feed stores the full OHLCV.
		todayOpen := event.MarketData.PriceMap.Open
		if todayOpen <= 0 {
			if redisOpen, err := h.getOpenFromRedis(ctx, event.StockData); err == nil {
				todayOpen = redisOpen
			} else {
				h.logger.Warn("Could not fetch open price from Redis, falling back to LTP",
					zap.String("symbol", event.StockData.Symbol),
					zap.Float64("ltp", ltp),
					zap.Error(err))
			}
		}

		// Fall back to LTP only if open is still unavailable.
		referencePrice := todayOpen
		if referencePrice <= 0 {
			referencePrice = ltp
		}

		// limitPrice = the price at which the stock will have moved exactly minPct% from today's open,
		// rounded to the nearest NSE tick (0.05) using integer paise arithmetic.
		// We use int64 paise to avoid float64 drift: e.g. 5911.3 → 591130 paise → 5911.30 exactly.
		rawLimit := referencePrice * (1 + minPct/100)
		paise := int64(math.Round(rawLimit * 100))
		// Round paise to nearest 5 (= 0.05 tick)
		paise = ((paise + 2) / 5) * 5
		limitPrice := float64(paise) / 100.0
		if limitPrice <= 0 {
			h.logger.Error("Computed limit price is invalid, skipping order",
				zap.String("strategy_id", strategy.StrategyID),
				zap.Float64("ltp", ltp),
				zap.Float64("today_open", todayOpen),
				zap.Float64("min_pct_change", minPct))
			return fmt.Errorf("invalid limit price computed for stock %s", event.StockData.Symbol)
		}

		// Preserve BRACKET order type if the user configured it; otherwise use LIMIT.
		// BRACKET already carries stop-loss and take-profit, so the price floor logic
		// still applies — we just don't downgrade the order type.
		if orderReq.OrderType != "BRACKET" {
			orderReq.OrderType = "LIMIT"
		}
		orderReq.Price = limitPrice
		orderReq.StopLoss = limitPrice * (1 - strategy.TradeConfig.StopLossPct/100)
		orderReq.TakeProfit = limitPrice * (1 + strategy.TradeConfig.TakeProfitPct/100)

		h.logger.Info("Case 2: pct_change below min — order placed at target price",
			zap.String("strategy_id", strategy.StrategyID),
			zap.String("order_type", orderReq.OrderType),
			zap.Float64("current_pct_change", event.MarketData.PctChange),
			zap.Float64("min_pct_change", minPct),
			zap.Float64("ltp", ltp),
			zap.Float64("today_open", todayOpen),
			zap.Float64("limit_price", limitPrice))
	}

	// Validate order request
	if err := orderReq.Validate(); err != nil {
		return fmt.Errorf("invalid order request: %w", err)
	}

	// 1. Check risk management BEFORE publishing
	// TODO: TEMPORARILY BYPASSED FOR TESTING - REMOVE THIS BEFORE PRODUCTION
	if false && h.riskClient != nil {
		riskResp, err := h.riskClient.CheckPreTradeRisk(ctx, orderReq, strategy)
		if err != nil {
			h.logger.Error("Risk check failed",
				zap.Error(err),
				zap.String("order_id", orderReq.OrderID))
			// Set as not approved if risk check fails
			orderReq.RiskApproved = false
			orderReq.RiskScore = 100.0 // High risk score for failures
		} else {
			// Update order with risk check results
			orderReq.RiskApproved = riskResp.Approved
			orderReq.RiskScore = riskResp.RiskScore

			if !riskResp.Approved {
				h.logger.Warn("Order rejected by risk management",
					zap.String("order_id", orderReq.OrderID),
					zap.String("user_id", orderReq.UserID),
					zap.Float64("risk_score", riskResp.RiskScore),
					zap.Int("violations", len(riskResp.Violations)))

				// Log violations
				for _, violation := range riskResp.Violations {
					h.logger.Debug("Risk violation",
						zap.String("order_id", orderReq.OrderID),
						zap.String("type", violation.Type.String()),
						zap.String("message", violation.Message))
				}
				// Don't publish rejected orders
				return nil
			}
		}
	} else {
		// TESTING MODE: Bypassing risk checks - Auto-approving all orders
		h.logger.Warn("RISK CHECK BYPASSED FOR TESTING - Auto-approving order",
			zap.String("order_id", orderReq.OrderID))
		orderReq.RiskApproved = true
		orderReq.RiskScore = 0.0
	}

	// 2. Save order to PostgreSQL (for tracking)
	if h.signalRepo != nil {
		if err := h.signalRepo.SaveTradeSignal(ctx, orderReq); err != nil {
			h.logger.Error("Failed to save trade signal to database",
				zap.Error(err),
				zap.String("order_id", orderReq.OrderID))
			// Continue anyway - don't fail the order
		} else {
			h.logger.Debug("Trade signal saved to PostgreSQL",
				zap.String("order_id", orderReq.OrderID),
				zap.String("status", "PENDING"))
		}
	}

	// 3. Publish to Kafka "trade-signals" topic
	if h.kafkaPubl != nil {
		if err := h.kafkaPubl.PublishTradeSignal(ctx, orderReq); err != nil {
			h.logger.Error("Failed to publish to Kafka trade-signals",
				zap.Error(err),
				zap.String("order_id", orderReq.OrderID))
			// Continue anyway - don't fail the order
		} else {
			h.logger.Debug("Trade signal published to Kafka",
				zap.String("order_id", orderReq.OrderID),
				zap.String("topic", "trade-signals"))
		}
	}

	// 4. Publish order to RabbitMQ
	if h.rabbitPubl != nil {
		if err := h.rabbitPubl.PublishOrder(ctx, orderReq); err != nil {
			h.stats.IncrementRabbitMQErrors()
			return fmt.Errorf("failed to publish order: %w", err)
		}
	}

	h.stats.IncrementOrdersGenerated()

	h.logger.Info("Order published and tracked",
		zap.String("order_id", orderReq.OrderID),
		zap.String("user_id", orderReq.UserID),
		zap.String("strategy_id", orderReq.StrategyID),
		zap.Int64("stock_code", orderReq.StockCode),
		zap.Float64("match_score", orderReq.MatchScore),
		zap.Float64("price", orderReq.Price))

	return nil
}
