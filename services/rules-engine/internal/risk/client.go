package risk

import (
	"context"
	"fmt"
	"strings"
	"time"

	common "github.com/RohitIndira/Algo-Treading/api/proto/common"
	pb "github.com/RohitIndira/Algo-Treading/api/proto/risk_management"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client is a gRPC client for risk management service
type Client struct {
	conn   *grpc.ClientConn
	client pb.RiskManagementServiceClient
	logger *zap.Logger
}

// Config holds risk management client configuration
type Config struct {
	Address          string
	Timeout          time.Duration
	MaxRetries       int
	RetryBackoff     time.Duration
	KeepAlive        time.Duration
	KeepAliveTimeout time.Duration
}

// NewClient creates a new risk management client
func NewClient(cfg Config, logger *zap.Logger) (*Client, error) {
	// Set up connection options
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	// Create context with timeout for connection
	connCtx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()

	// Connect to risk management service
	conn, err := grpc.DialContext(connCtx, cfg.Address, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to risk management service: %w", err)
	}

	client := pb.NewRiskManagementServiceClient(conn)

	logger.Info("Risk management client initialized",
		zap.String("address", cfg.Address))

	return &Client{
		conn:   conn,
		client: client,
		logger: logger,
	}, nil
}

// Helper functions to convert string to proto enums
func stringToExchange(exchange string) common.Exchange {
	switch strings.ToUpper(exchange) {
	case "NSE":
		return common.Exchange_EXCHANGE_NSE
	case "BSE":
		return common.Exchange_EXCHANGE_BSE
	default:
		return common.Exchange_EXCHANGE_UNSPECIFIED
	}
}

func stringToOrderType(orderType string) common.OrderType {
	switch strings.ToUpper(orderType) {
	case "MARKET", "MKT":
		return common.OrderType_ORDER_TYPE_MARKET
	case "LIMIT", "RL":
		return common.OrderType_ORDER_TYPE_LIMIT
	case "STOP_LOSS", "SL":
		return common.OrderType_ORDER_TYPE_STOP_LOSS
	case "STOP_LOSS_MARKET", "SL-M":
		return common.OrderType_ORDER_TYPE_STOP_LOSS_MARKET
	default:
		return common.OrderType_ORDER_TYPE_UNSPECIFIED
	}
}

func stringToOrderSide(orderSide string) common.OrderSide {
	switch strings.ToUpper(orderSide) {
	case "BUY":
		return common.OrderSide_ORDER_SIDE_BUY
	case "SELL":
		return common.OrderSide_ORDER_SIDE_SELL
	default:
		return common.OrderSide_ORDER_SIDE_UNSPECIFIED
	}
}

// CheckPreTradeRisk validates an order request against risk limits
func (c *Client) CheckPreTradeRisk(ctx context.Context, orderReq *models.OrderRequest, strategy *models.Strategy) (*pb.PreTradeRiskResponse, error) {
	// Convert order request to risk check request
	req := &pb.PreTradeRiskRequest{
		UserId:     orderReq.UserID,
		StrategyId: orderReq.StrategyID,
		StockCode:  orderReq.StockCode,
		Exchange:   stringToExchange(orderReq.Exchange),
		OrderType:  stringToOrderType(orderReq.OrderType),
		OrderSide:  stringToOrderSide(orderReq.OrderSide),
		Quantity:   orderReq.Quantity,
		Price:      orderReq.Price,
		StopLoss:   orderReq.StopLoss,
		TakeProfit: orderReq.TakeProfit,
	}

	// Set risk limits from strategy or use defaults
	req.MaxDailyTrades = 20
	req.MaxLossPerDay = 10000.0
	req.MaxPositionSize = 100000.0
	req.MaxPerTradeRisk = 2000.0

	if strategy != nil {
		if strategy.RiskLimits.MaxDailyTrades > 0 {
			req.MaxDailyTrades = strategy.RiskLimits.MaxDailyTrades
		}
		if strategy.RiskLimits.MaxLossPerDay > 0 {
			req.MaxLossPerDay = strategy.RiskLimits.MaxLossPerDay
		}
		if strategy.RiskLimits.MaxPositionSize > 0 {
			req.MaxPositionSize = strategy.RiskLimits.MaxPositionSize
		}
		if strategy.RiskLimits.MaxPerTradeRisk > 0 {
			req.MaxPerTradeRisk = strategy.RiskLimits.MaxPerTradeRisk
		}
	}

	// Call risk management service
	resp, err := c.client.CheckPreTradeRisk(ctx, req)
	if err != nil {
		c.logger.Error("Failed to check pre-trade risk",
			zap.String("order_id", orderReq.OrderID),
			zap.String("user_id", orderReq.UserID),
			zap.Error(err))
		return nil, fmt.Errorf("risk check failed: %w", err)
	}

	// Log risk check result
	if !resp.Approved {
		c.logger.Warn("Order rejected by risk management",
			zap.String("order_id", orderReq.OrderID),
			zap.String("user_id", orderReq.UserID),
			zap.Float64("risk_score", resp.RiskScore),
			zap.Int("violations", len(resp.Violations)))
	} else {
		c.logger.Debug("Order approved by risk management",
			zap.String("order_id", orderReq.OrderID),
			zap.String("user_id", orderReq.UserID),
			zap.Float64("risk_score", resp.RiskScore))
	}

	return resp, nil
}

// Close closes the gRPC connection
func (c *Client) Close() error {
	if c.conn != nil {
		c.logger.Info("Closing risk management client connection")
		return c.conn.Close()
	}
	return nil
}
