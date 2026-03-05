package startup

import (
	"context"
	"fmt"
	"time"

	commonpb "github.com/RohitIndira/Algo-Treading/api/proto/common"
	proto "github.com/RohitIndira/Algo-Treading/api/proto/user_config"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// UserConfigClient connects to User Config gRPC with retry and provides
// convenience methods for Rule Engine bootstrap.
type UserConfigClient struct {
	conn   *grpc.ClientConn
	client proto.UserConfigServiceClient
}

// NewUserConfigClient dials User Config gRPC with exponential backoff retry.
// Retries: 1s,2s,4s,8s,16s,30s(max) up to 5 minutes total.
func NewUserConfigClient(ctx context.Context, address string) (*UserConfigClient, error) {
	if address == "" {
		return nil, fmt.Errorf("user-config address is empty")
	}

	deadline := time.Now().Add(5 * time.Minute)
	backoff := 1 * time.Second

	for {
		// Respect caller ctx
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("timed out after 5m connecting to user-config at %s", address)
		}

		connCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		conn, err := grpc.DialContext(connCtx, address,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		)
		cancel()
		if err == nil {
			return &UserConfigClient{conn: conn, client: proto.NewUserConfigServiceClient(conn)}, nil
		}

		// Sleep with backoff
		sleep := backoff
		if sleep > 30*time.Second {
			sleep = 30 * time.Second
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(sleep):
		}

		if backoff < 30*time.Second {
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
		}
	}
}

// GetAllActiveStrategies fetches ALL active strategies via pagination.
// Page size is fixed at 500.
func (c *UserConfigClient) GetAllActiveStrategies(ctx context.Context) ([]*models.StrategyConfig, error) {
	var all []*models.StrategyConfig
	pageToken := ""

	for {
		resp, err := c.client.GetAllActiveStrategies(ctx, &proto.GetAllActiveStrategiesRequest{
			PageSize:  500,
			PageToken: pageToken,
		})
		if err != nil {
			return nil, fmt.Errorf("GetAllActiveStrategies page failed: %w", err)
		}
		if !resp.Success {
			return nil, fmt.Errorf("GetAllActiveStrategies unsuccessful: %v", resp.Error)
		}

		for _, s := range resp.Strategies {
			m, err := protoToModel(s)
			if err != nil {
				// skip invalid, but don't fail whole load
				continue
			}
			all = append(all, m)
		}

		if resp.NextPageToken == "" {
			break
		}
		pageToken = resp.NextPageToken
	}

	return all, nil
}

func protoToModel(ps *proto.Strategy) (*models.StrategyConfig, error) {
	if ps == nil {
		return nil, fmt.Errorf("proto strategy is nil")
	}
	if ps.StrategyId == "" || ps.UserId == "" {
		return nil, fmt.Errorf("missing ids")
	}

	m := &models.StrategyConfig{
		StrategyID:   ps.StrategyId,
		UserID:       ps.UserId,
		StrategyName: ps.StrategyName,
		Active:       ps.Active,
		TradingMode:  normalizeTradingMode(ps.TradingMode.String()),
		Version:      uint64(ps.Version),
	}
	if ps.CreatedAt != nil {
		m.CreatedAt = time.Unix(ps.CreatedAt.Seconds, int64(ps.CreatedAt.Nanos))
	}
	if ps.UpdatedAt != nil {
		m.UpdatedAt = time.Unix(ps.UpdatedAt.Seconds, int64(ps.UpdatedAt.Nanos))
	}

	if ps.Conditions != nil {
		m.Conditions.MatchAllNews = ps.Conditions.MatchAllNews
		m.Conditions.ImpactScoreMin = ps.Conditions.ImpactScoreMin
		m.Conditions.ImpactScoreMax = ps.Conditions.ImpactScoreMax
		m.Conditions.Categories = ps.Conditions.Categories
		m.Conditions.Stocks = ps.Conditions.StockCodes
		m.Conditions.VolumeThreshold = ps.Conditions.VolumeThreshold
		if ps.Conditions.PctChangeRange != nil {
			m.Conditions.MinPctChange = ps.Conditions.PctChangeRange.MinPctChange
			m.Conditions.MaxPctChange = ps.Conditions.PctChangeRange.MaxPctChange
		}
		m.Conditions.Sentiments = sentimentsToStrings(ps.Conditions.Sentiments)
		m.Conditions.Exchanges = exchangesToStrings(ps.Conditions.Exchanges)
		if ps.Conditions.MarketCapRange != nil {
			m.Conditions.MarketCapRange = models.MarketCapRange{MinMcap: ps.Conditions.MarketCapRange.MinMcap, MaxMcap: ps.Conditions.MarketCapRange.MaxMcap}
		}
	}

	if ps.TradeConfig != nil {
		m.TradeConfig.OrderType = normalizeOrderType(ps.TradeConfig.OrderType.String())
		m.TradeConfig.ProductType = ps.TradeConfig.ProductType.String()
		m.TradeConfig.Validity = ps.TradeConfig.Validity
		m.TradeConfig.Quantity = ps.TradeConfig.Quantity
		m.TradeConfig.Exchange = normalizeExchange(ps.TradeConfig.Exchange.String())
		m.TradeConfig.OrderSide = normalizeOrderSide(ps.TradeConfig.OrderSide.String())
		m.TradeConfig.LimitPrice = ps.TradeConfig.LimitPrice
		m.TradeConfig.StopLossPct = ps.TradeConfig.StopLossPct
		m.TradeConfig.TakeProfitPct = ps.TradeConfig.TakeProfitPct
		m.TradeConfig.StopLossType = normalizeStopLossType(ps.TradeConfig.StopLossType.String())
		m.TradeConfig.TrailingSLPct = ps.TradeConfig.TrailingSlPct
	}

	if ps.RiskLimits != nil {
		m.RiskLimits.MaxDailyTrades = ps.RiskLimits.MaxDailyTrades
		m.RiskLimits.MaxLossPerDay = ps.RiskLimits.MaxLossPerDay
		m.RiskLimits.MaxPerTradeRisk = ps.RiskLimits.MaxPerTradeRisk
		m.RiskLimits.PositionSizing = ps.RiskLimits.PositionSizing.String()
		m.RiskLimits.EnableAutoSquareOff = ps.RiskLimits.EnableAutoSquareOff
		m.RiskLimits.AutoSquareOffTime = ps.RiskLimits.AutoSquareOffTime
	}

	return m, nil
}

func sentimentsToStrings(in []commonpb.Sentiment) []string {
	out := make([]string, 0, len(in))
	for _, s := range in {
		switch s {
		case commonpb.Sentiment_SENTIMENT_POSITIVE:
			out = append(out, "positive")
		case commonpb.Sentiment_SENTIMENT_NEGATIVE:
			out = append(out, "negative")
		case commonpb.Sentiment_SENTIMENT_NEUTRAL:
			out = append(out, "neutral")
		default:
			out = append(out, "neutral")
		}
	}
	return out
}

func exchangesToStrings(in []commonpb.Exchange) []string {
	out := make([]string, 0, len(in))
	for _, ex := range in {
		switch ex {
		case commonpb.Exchange_EXCHANGE_NSE:
			out = append(out, "NSE")
		case commonpb.Exchange_EXCHANGE_BSE:
			out = append(out, "BSE")
		default:
			out = append(out, ex.String())
		}
	}
	return out
}

func normalizeOrderType(v string) string {
	switch v {
	case "ORDER_TYPE_MARKET", "MARKET":
		return "MARKET"
	case "ORDER_TYPE_LIMIT", "LIMIT":
		return "LIMIT"
	default:
		return "MARKET"
	}
}

func normalizeExchange(v string) string {
	switch v {
	case "EXCHANGE_NSE", "NSE":
		return "NSE"
	case "EXCHANGE_BSE", "BSE":
		return "BSE"
	default:
		return v
	}
}

func normalizeOrderSide(v string) string {
	switch v {
	case "ORDER_SIDE_BUY", "BUY":
		return "BUY"
	case "ORDER_SIDE_SELL", "SELL":
		return "SELL"
	default:
		return "BUY"
	}
}

func normalizeStopLossType(v string) string {
	switch v {
	case "STOP_LOSS_TYPE_FIXED", "FIXED":
		return "FIXED"
	case "STOP_LOSS_TYPE_TRAILING", "TRAILING":
		return "TRAILING"
	default:
		return "FIXED"
	}
}

func normalizeTradingMode(v string) string {
	switch v {
	case "TRADING_MODE_LIVE", "LIVE":
		return "LIVE"
	case "TRADING_MODE_PAPER", "PAPER":
		return "PAPER"
	default:
		return "PAPER" // Safe default
	}
}

func (c *UserConfigClient) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}
