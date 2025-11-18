package consumer

import (
	"context"
	"fmt"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/matcher"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/publisher"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/repository"
	"go.uber.org/zap"
)

// Handler handles market events
type Handler struct {
	matcher    *matcher.Matcher
	rabbitPubl *publisher.Publisher
	kafkaPubl  *publisher.KafkaPublisher
	signalRepo *repository.TradeSignalRepository
	stats      *models.MatchingStats
	logger     *zap.Logger
}

// NewHandler creates a new event handler
func NewHandler(
	matcher *matcher.Matcher,
	rabbitPubl *publisher.Publisher,
	kafkaPubl *publisher.KafkaPublisher,
	signalRepo *repository.TradeSignalRepository,
	stats *models.MatchingStats,
	logger *zap.Logger,
) *Handler {
	return &Handler{
		matcher:    matcher,
		rabbitPubl: rabbitPubl,
		kafkaPubl:  kafkaPubl,
		signalRepo: signalRepo,
		stats:      stats,
		logger:     logger,
	}
}

// HandleEvent processes a market event
func (h *Handler) HandleEvent(ctx context.Context, event *models.MarketEvent) error {
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

// processMatch processes a single match
func (h *Handler) processMatch(ctx context.Context, match *models.RuleMatch, event *models.MarketEvent) error {
	// Note: In production, you would fetch the full strategy here
	// For now, we'll create a basic strategy from available data
	strategy := &models.Strategy{
		StrategyID:   match.StrategyID,
		UserID:       match.UserID,
		StrategyName: match.StrategyName,
		TradeConfig: models.TradeConfig{
			OrderType:     "MARKET",
			Quantity:      1,
			StopLossPct:   2.0, // Default 2% stop loss
			TakeProfitPct: 5.0, // Default 5% take profit
		},
	}

	// Create order request
	orderReq := models.NewOrderRequest(match, event, strategy)

	// For MARKET orders, ensure we have a valid price from the event
	if orderReq.Price <= 0 {
		// Use price from market data
		if event.MarketData.LastTradedPrice > 0 {
			orderReq.Price = event.MarketData.LastTradedPrice
		} else {
			// Fallback: use a nominal price of 100 for testing
			orderReq.Price = 100.0
			h.logger.Warn("No price available in event, using fallback",
				zap.String("event_id", event.EventID),
				zap.Int64("stock_code", event.StockData.StockCode))
		}

		// Recalculate stop loss and take profit with the correct price
		orderReq.StopLoss = orderReq.Price * (1 - strategy.TradeConfig.StopLossPct/100)
		orderReq.TakeProfit = orderReq.Price * (1 + strategy.TradeConfig.TakeProfitPct/100)
	}

	// Validate order request
	if err := orderReq.Validate(); err != nil {
		return fmt.Errorf("invalid order request: %w", err)
	}

	// 1. Save order to PostgreSQL first (for tracking)
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

	// 2. Publish to Kafka "trade-signals" topic
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

	// 3. Publish order to RabbitMQ
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

	return nil
}
