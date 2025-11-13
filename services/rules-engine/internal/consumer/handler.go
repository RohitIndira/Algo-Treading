package consumer

import (
	"context"
	"fmt"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/matcher"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/publisher"
	"go.uber.org/zap"
)

// Handler handles market events
type Handler struct {
	matcher   *matcher.Matcher
	publisher *publisher.Publisher
	stats     *models.MatchingStats
	logger    *zap.Logger
}

// NewHandler creates a new event handler
func NewHandler(
	matcher *matcher.Matcher,
	publisher *publisher.Publisher,
	stats *models.MatchingStats,
	logger *zap.Logger,
) *Handler {
	return &Handler{
		matcher:   matcher,
		publisher: publisher,
		stats:     stats,
		logger:    logger,
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
			OrderType: "MARKET",
			Quantity:  1,
		},
	}

	// Create order request
	orderReq := models.NewOrderRequest(match, event, strategy)

	// Validate order request
	if err := orderReq.Validate(); err != nil {
		return fmt.Errorf("invalid order request: %w", err)
	}

	// Publish order to RabbitMQ
	if err := h.publisher.PublishOrder(ctx, orderReq); err != nil {
		h.stats.IncrementRabbitMQErrors()
		return fmt.Errorf("failed to publish order: %w", err)
	}

	h.stats.IncrementOrdersGenerated()

	h.logger.Info("Order published",
		zap.String("order_id", orderReq.OrderID),
		zap.String("user_id", orderReq.UserID),
		zap.String("strategy_id", orderReq.StrategyID),
		zap.Int64("stock_code", orderReq.StockCode),
		zap.Float64("match_score", orderReq.MatchScore))

	return nil
}
