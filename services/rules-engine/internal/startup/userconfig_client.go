package startup

import (
	"context"
	"fmt"
	"time"

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
		StrategyType: normalizeStrategyType(ps.StrategyType),
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

	// ps.Conditions mapping removed 2026-06-25 — Strategy no longer has a
	// Conditions struct (news-event path gone). Incoming proto field is
	// silently ignored; user-config will stop sending it once their schema
	// catches up.

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
		m.TradeConfig.TotalCapital = ps.TradeConfig.TotalCapital
		m.TradeConfig.MaxPositions = ps.TradeConfig.MaxPositions
		m.TradeConfig.PerStockAmount = ps.TradeConfig.PerStockAmount
	}

	// ps.RiskLimits mapping removed 2026-06-25 — Strategy.RiskLimits gone
	// with the news path. Manthan enforces its own portfolio caps via
	// the rebalancer + allocator.

	return m, nil
}

// sentimentsToStrings/exchangesToStrings removed 2026-06-25 — they only
// served the Conditions mapper that's gone with the news-event path.

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

func normalizeStrategyType(t proto.StrategyType) string {
	switch t {
	case proto.StrategyType_MANTHAN:
		return "MANTHAN"
	case proto.StrategyType_WEEK52_BREAKOUT:
		return "52W_BREAKOUT"
	case proto.StrategyType_NEWS:
		return "NEWS"
	default:
		return "NEWS"
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

// IsStrategyAlive returns true when user-config still has this strategy
// (not soft-deleted, not hard-deleted, not unauthorized for the user).
//
// This is the gRPC replacement for the legacy
//
//	SELECT deleted_at FROM strategies WHERE strategy_id = $1
//
// the orphan scanner used to run directly against user-config's table.
// Single-owner principle: only user-config touches the strategies table;
// everyone else asks user-config — see docs/architecture/data-ownership.md.
//
// userID + strategyID are both required because GetStrategy enforces
// user-scoped authorization. The orphan scanner has both at every call
// site (they live on the same manthan_positions row).
//
// Return contract — designed so an ambiguous answer NEVER causes the
// scanner to EXIT a real position:
//
//   - (true,  nil)        strategy is alive at user-config — keep position
//   - (false, nil)        explicit NOT_FOUND from user-config — orphan,
//     safe to EXIT the position
//   - (false, non-nil)    network / transport / unknown failure — caller
//     MUST skip (never EXIT on uncertainty)
func (c *UserConfigClient) IsStrategyAlive(ctx context.Context, userID, strategyID string) (bool, error) {
	if strategyID == "" {
		return false, fmt.Errorf("strategyID is required")
	}
	if userID == "" {
		return false, fmt.Errorf("userID is required")
	}

	resp, err := c.client.GetStrategy(ctx, &proto.GetStrategyRequest{
		StrategyId: strategyID,
		UserId:     userID,
	})
	if err != nil {
		// Transport-level failure — uncertain, propagate.
		return false, fmt.Errorf("GetStrategy RPC: %w", err)
	}
	if resp.Success {
		return true, nil
	}
	// Structured failure. Only NOT_FOUND maps cleanly to "gone"; any other
	// code (INVALID_STRATEGY_ID, INTERNAL, ...) is uncertain.
	if resp.Error != nil && resp.Error.Code == "NOT_FOUND" {
		return false, nil
	}
	if resp.Error != nil {
		return false, fmt.Errorf("GetStrategy unsuccessful: %s %s", resp.Error.Code, resp.Error.Message)
	}
	return false, fmt.Errorf("GetStrategy unsuccessful with no error detail")
}
