package userconfig

import (
	"context"
	"fmt"
	"time"

	"github.com/RohitIndira/Algo-Treading/api/proto/common"
	pb "github.com/RohitIndira/Algo-Treading/api/proto/user_config"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client is a gRPC client for user-config service
type Client struct {
	conn   *grpc.ClientConn
	client pb.UserConfigServiceClient
	logger *zap.Logger
}

// Config holds user-config client configuration
type Config struct {
	Address          string
	Timeout          time.Duration
	MaxRetries       int
	RetryBackoff     time.Duration
	KeepAlive        time.Duration
	KeepAliveTimeout time.Duration
}

// NewClient creates a new user-config client
func NewClient(cfg Config, logger *zap.Logger) (*Client, error) {
	// Set up connection options
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	// Create context with timeout for connection
	connCtx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()

	// Connect to user-config service
	conn, err := grpc.DialContext(connCtx, cfg.Address, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to user-config service: %w", err)
	}

	client := pb.NewUserConfigServiceClient(conn)

	logger.Info("User-config client initialized",
		zap.String("address", cfg.Address))

	return &Client{
		conn:   conn,
		client: client,
		logger: logger,
	}, nil
}

// GetStrategy fetches a strategy by ID from user-config service
func (c *Client) GetStrategy(ctx context.Context, strategyID, userID string) (*models.Strategy, error) {
	req := &pb.GetStrategyRequest{
		StrategyId: strategyID,
		UserId:     userID,
	}

	resp, err := c.client.GetStrategy(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to get strategy: %w", err)
	}

	if resp.Strategy == nil {
		return nil, fmt.Errorf("strategy not found")
	}

	// Convert proto strategy to internal model
	strategy, err := c.convertProtoToStrategy(resp.Strategy)
	if err != nil {
		return nil, fmt.Errorf("failed to convert strategy: %w", err)
	}

	return strategy, nil
}

// ListActiveStrategies fetches all active strategies for a user from user-config service
func (c *Client) ListActiveStrategies(ctx context.Context, userID string) ([]*models.Strategy, error) {
	pagination := &common.PaginationRequest{
		Page:     1,
		PageSize: 1000,
	}

	req := &pb.ListUserStrategiesRequest{
		UserId:     userID,
		ActiveOnly: true,
		Pagination: pagination,
	}

	resp, err := c.client.ListUserStrategies(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to list strategies: %w", err)
	}

	strategies := make([]*models.Strategy, 0, len(resp.Strategies))
	for _, protoStrategy := range resp.Strategies {
		strategy, err := c.convertProtoToStrategy(protoStrategy)
		if err != nil {
			c.logger.Warn("Failed to convert strategy, skipping",
				zap.String("strategy_id", protoStrategy.StrategyId),
				zap.Error(err))
			continue
		}
		strategies = append(strategies, strategy)
	}

	return strategies, nil
}

// convertProtoToStrategy converts proto Strategy to internal model
func (c *Client) convertProtoToStrategy(protoStrategy *pb.Strategy) (*models.Strategy, error) {
	if protoStrategy == nil {
		return nil, fmt.Errorf("proto strategy is nil")
	}

	strategy := &models.Strategy{
		StrategyID:   protoStrategy.StrategyId,
		UserID:       protoStrategy.UserId,
		StrategyName: protoStrategy.StrategyName,
		Active:       protoStrategy.Active,
	}

	// Parse conditions
	if protoStrategy.Conditions != nil {
		conditions := models.Conditions{
			Stocks:             protoStrategy.Conditions.StockCodes,
			VolumeThreshold:    protoStrategy.Conditions.VolumeThreshold,
			PctChangeThreshold: protoStrategy.Conditions.PctChangeThreshold,
			MinBidQuantity:     protoStrategy.Conditions.MinBidQuantity,
			MinAskQuantity:     protoStrategy.Conditions.MinAskQuantity,
			MaxSpreadPct:       protoStrategy.Conditions.MaxSpreadPct,
		}

		// Parse price range
		if protoStrategy.Conditions.PriceRange != nil {
			conditions.PriceRange = models.PriceRange{
				MinPrice: protoStrategy.Conditions.PriceRange.MinPrice,
				MaxPrice: protoStrategy.Conditions.PriceRange.MaxPrice,
			}
		}

		strategy.Conditions = conditions
	}

	// Parse trade config
	if protoStrategy.TradeConfig != nil {
		tradeConfig := models.TradeConfig{
			Quantity:        protoStrategy.TradeConfig.Quantity,
			OrderType:       normalizeOrderType(protoStrategy.TradeConfig.OrderType.String()),
			OrderSide:       normalizeOrderSide(protoStrategy.TradeConfig.OrderSide.String()),
			Exchange:        normalizeExchange(protoStrategy.TradeConfig.Exchange.String()),
			StopLossPct:     protoStrategy.TradeConfig.StopLossPct,
			TakeProfitPct:   protoStrategy.TradeConfig.TakeProfitPct,
			MaxPositionSize: protoStrategy.TradeConfig.MaxPositionSize,
			StopLossType:    normalizeStopLossType(protoStrategy.TradeConfig.StopLossType.String()),
			TrailingSLPct:   protoStrategy.TradeConfig.TrailingSlPct,
			ProductType:     protoStrategy.TradeConfig.ProductType,
		}

		strategy.TradeConfig = tradeConfig
	}

	// Parse risk limits
	if protoStrategy.RiskLimits != nil {
		riskLimits := models.RiskLimits{
			MaxDailyTrades:  protoStrategy.RiskLimits.MaxDailyTrades,
			MaxLossPerDay:   protoStrategy.RiskLimits.MaxLossPerDay,
			MaxPerTradeRisk: protoStrategy.RiskLimits.MaxPerTradeRisk,
			PositionSizing:  normalizePositionSizing(protoStrategy.RiskLimits.PositionSizing.String()),
		}

		strategy.RiskLimits = riskLimits
	}

	return strategy, nil
}

// convertSentiments converts proto sentiment enums to strings
func convertSentiments(sentiments []common.Sentiment) []string {
	result := make([]string, 0, len(sentiments))
	for _, s := range sentiments {
		switch s {
		case common.Sentiment_SENTIMENT_POSITIVE:
			result = append(result, "POSITIVE")
		case common.Sentiment_SENTIMENT_NEGATIVE:
			result = append(result, "NEGATIVE")
		case common.Sentiment_SENTIMENT_NEUTRAL:
			result = append(result, "NEUTRAL")
		}
	}
	return result
}

// normalizePositionSizing converts proto enum to internal format
func normalizePositionSizing(positionSizing string) string {
	switch positionSizing {
	case "POSITION_SIZING_FIXED":
		return "FIXED"
	case "POSITION_SIZING_PERCENTAGE":
		return "PERCENTAGE"
	default:
		return "FIXED"
	}
}

// normalizeOrderType converts proto enum to internal format
func normalizeOrderType(orderType string) string {
	switch orderType {
	case "ORDER_TYPE_MARKET":
		return "MARKET"
	case "ORDER_TYPE_LIMIT":
		return "LIMIT"
	default:
		return "MARKET"
	}
}

// normalizeExchange converts proto enum to internal format
func normalizeExchange(exchange string) string {
	switch exchange {
	case "EXCHANGE_NSE":
		return "NSE"
	case "EXCHANGE_BSE":
		return "BSE"
	default:
		return "NSE"
	}
}

// normalizeOrderSide converts proto enum to internal format
func normalizeOrderSide(orderSide string) string {
	switch orderSide {
	case "ORDER_SIDE_BUY":
		return "BUY"
	case "ORDER_SIDE_SELL":
		return "SELL"
	default:
		return "BUY"
	}
}

// normalizeStopLossType converts proto enum to internal format
func normalizeStopLossType(stopLossType string) string {
	switch stopLossType {
	case "STOP_LOSS_TYPE_FIXED":
		return "FIXED"
	case "STOP_LOSS_TYPE_TRAILING":
		return "TRAILING"
	default:
		return "FIXED"
	}
}

// Close closes the client connection
func (c *Client) Close() error {
	c.logger.Info("Closing user-config client connection")
	return c.conn.Close()
}
