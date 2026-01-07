package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/cache"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/matcher"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/publisher"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/repository"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/risk"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/utils"
	"go.uber.org/zap"
)

// Handler handles market events
type Handler struct {
	matcher       *matcher.Matcher
	rabbitPubl    *publisher.Publisher
	kafkaPubl     *publisher.KafkaPublisher
	signalRepo    *repository.TradeSignalRepository
	riskClient    *risk.Client
	redisCache    *cache.RedisCache
	strategyCache *cache.StrategyCache
	stats         *models.MatchingStats
	logger        *zap.Logger
	marketHours   *utils.MarketHours
	enforceHours  bool
}

// NewHandler creates a new event handler
func NewHandler(
	matcher *matcher.Matcher,
	rabbitPubl *publisher.Publisher,
	kafkaPubl *publisher.KafkaPublisher,
	signalRepo *repository.TradeSignalRepository,
	riskClient *risk.Client,
	redisCache *cache.RedisCache,
	strategyCache *cache.StrategyCache,
	stats *models.MatchingStats,
	logger *zap.Logger,
	marketHours *utils.MarketHours,
	enforceHours bool,
) *Handler {
	return &Handler{
		matcher:       matcher,
		rabbitPubl:    rabbitPubl,
		kafkaPubl:     kafkaPubl,
		signalRepo:    signalRepo,
		riskClient:    riskClient,
		redisCache:    redisCache,
		strategyCache: strategyCache,
		stats:         stats,
		logger:        logger,
		marketHours:   marketHours,
		enforceHours:  enforceHours,
	}
}

// HandleEvent processes a market event
func (h *Handler) HandleEvent(ctx context.Context, event *models.MarketEvent) error {
	// Check if market is open before generating trade signals (only if enforcement is enabled)
	if h.enforceHours && !h.marketHours.IsMarketOpen() {
		status := h.marketHours.GetMarketStatus()
		h.logger.Debug("Skipping trade signal generation - market is closed",
			zap.String("event_id", event.EventID),
			zap.String("market_status", status),
			zap.Time("event_timestamp", event.Timestamp))
		return nil
	}

	h.logger.Debug("Handling event",
		zap.String("event_id", event.EventID),
		zap.Int64("stock_code", event.StockData.StockCode),
		zap.String("symbol", event.StockData.Symbol),
		zap.Int32("impact_score", event.Analysis.ImpactScore))

	// Match event against strategies
	matches, err := h.matcher.MatchEvent(ctx, event)
	if err != nil {
		h.stats.IncrementEvaluationErrors()
		return fmt.Errorf("failed to match event: %w", err)
	}

	if len(matches) == 0 {
		h.logger.Debug("No matches found for event",
			zap.String("event_id", event.EventID))
		return nil
	}

	h.logger.Info("Event matched strategies",
		zap.String("event_id", event.EventID),
		zap.Int("match_count", len(matches)))

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

// processMatch processes a single match
func (h *Handler) processMatch(ctx context.Context, match *models.RuleMatch, event *models.MarketEvent) error {
	// Use the full strategy from the match (already includes trade_config from Kafka user-configs topic)
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

	// Log strategy configuration being used from Kafka
	h.logger.Info("Using strategy configuration from Kafka user-configs",
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

	// For MARKET orders, ensure we have a valid price from the event
	if orderReq.Price <= 0 {
		// Priority 1: Use price from event market data
		if event.MarketData.LastTradedPrice > 0 {
			orderReq.Price = event.MarketData.LastTradedPrice
			h.logger.Debug("Using LTP from event data",
				zap.Int64("token", orderReq.Token),
				zap.Int64("stock_code", event.StockData.StockCode),
				zap.String("exchange", event.StockData.Exchange),
				zap.Float64("price", orderReq.Price))
		} else {
			// Priority 2: Query Redis for LTP using exchange-specific codes
			// Redis key format: market:{exchange}:{token}
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

		// Recalculate stop loss and take profit with the correct price
		orderReq.StopLoss = orderReq.Price * (1 - strategy.TradeConfig.StopLossPct/100)
		orderReq.TakeProfit = orderReq.Price * (1 + strategy.TradeConfig.TakeProfitPct/100)
	}

	// Validate order request
	if err := orderReq.Validate(); err != nil {
		return fmt.Errorf("invalid order request: %w", err)
	}

	// 1. Check risk management BEFORE publishing
	if h.riskClient != nil {
		// Call central RiskManagementService to validate the order before it goes
		// to RabbitMQ / Trade Execution.
		riskResp, err := h.riskClient.CheckPreTradeRisk(ctx, orderReq, strategy)
		if err != nil {
			h.logger.Error("Risk check failed",
				zap.Error(err),
				zap.String("order_id", orderReq.OrderID))
			// Mark as not approved and DO NOT publish this order.
			orderReq.RiskApproved = false
			orderReq.RiskScore = 100.0 // High risk score for failures
			return nil
		}

		// Update order with risk check results
		orderReq.RiskApproved = riskResp.Approved
		orderReq.RiskScore = riskResp.RiskScore

		if !riskResp.Approved {
			h.logger.Warn("Order rejected by risk management",
				zap.String("order_id", orderReq.OrderID),
				zap.String("user_id", orderReq.UserID),
				zap.Float64("risk_score", riskResp.RiskScore),
				zap.Int("violations", len(riskResp.Violations)))

			// Log violations for debugging / audit
			for _, violation := range riskResp.Violations {
				h.logger.Debug("Risk violation",
					zap.String("order_id", orderReq.OrderID),
					zap.String("type", violation.Type.String()),
					zap.String("message", violation.Message))
			}
			// Don't publish rejected orders
			return nil
		}
	} else {
		// No risk client configured. For safety in production, you may prefer to
		// block orders here instead of auto-approving.
		h.logger.Warn("Risk client not configured - auto-approving order by default",
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
	if err := h.rabbitPubl.PublishOrder(ctx, orderReq); err != nil {
		h.stats.IncrementRabbitMQErrors()
		return fmt.Errorf("failed to publish order: %w", err)
	}

	h.stats.IncrementOrdersGenerated()

	h.logger.Info("Order published and tracked",
		zap.String("order_id", orderReq.OrderID),
		zap.String("user_id", orderReq.UserID),
		zap.String("strategy_id", orderReq.StrategyID),
		zap.Int64("stock_code", orderReq.StockCode),
		zap.Float64("match_score", orderReq.MatchScore),
		zap.Float64("price", orderReq.Price))

	// Publish match event to Redis for real-time WebSocket updates
	if err := h.publishMatchEvent(ctx, orderReq, event, match); err != nil {
		h.logger.Error("Failed to publish match event to Redis",
			zap.Error(err),
			zap.String("user_id", orderReq.UserID))
		// Don't fail the order, just log the error
	}

	return nil
}

// publishMatchEvent publishes match event to Redis Pub/Sub for real-time updates
func (h *Handler) publishMatchEvent(ctx context.Context, orderReq *models.OrderRequest, event *models.MarketEvent, match *models.RuleMatch) error {
	// Create match event payload
	matchEvent := map[string]interface{}{
		"order_id":      orderReq.OrderID,
		"user_id":       orderReq.UserID,
		"strategy_id":   orderReq.StrategyID,
		"strategy_name": match.StrategyName,
		"event_id":      event.EventID,
		"stock_code":    orderReq.StockCode,
		"token":         orderReq.Token,
		"symbol":        orderReq.Symbol,
		"exchange":      orderReq.Exchange,
		"match_score":   orderReq.MatchScore,
		"impact_score":  orderReq.ImpactScore,
		"sentiment":     orderReq.Sentiment,
		"news_category": orderReq.NewsCategory,
		"news_title":    event.NewsData.ShortSummary,
		"news_content":  event.NewsData.ShortSummary,
		"news_link":     event.NewsData.NewsLink,
		"order_price":   orderReq.Price,
		"stop_loss":     orderReq.StopLoss,
		"take_profit":   orderReq.TakeProfit,
		"order_status":  "PENDING",
		"risk_approved": orderReq.RiskApproved,
		"timestamp":     time.Now().Unix(),
		"time_ago":      "just now",
	}

	// Convert to JSON
	jsonData, err := json.Marshal(matchEvent)
	if err != nil {
		return fmt.Errorf("failed to marshal match event: %w", err)
	}

	// Publish to Redis channel: user:{user_id}:matches
	channel := fmt.Sprintf("user:%s:matches", orderReq.UserID)
	err = h.redisCache.Publish(ctx, channel, string(jsonData))
	if err != nil {
		return fmt.Errorf("failed to publish to Redis channel %s: %w", channel, err)
	}

	h.logger.Debug("Published match event to Redis",
		zap.String("channel", channel),
		zap.String("order_id", orderReq.OrderID))

	return nil
}
