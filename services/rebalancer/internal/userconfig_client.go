// rebalancer's gRPC client to user-config.
//
// Background: until Phase 0.2 (this file), rebalancer read the strategies
// table directly across services — the classic cross-bounded-context
// anti-pattern called out in
//   docs/architecture/communication-patterns.md
// and
//   docs/architecture/data-ownership.md.
// user-config is the single owner of the strategies table; everyone else
// asks via gRPC. This file is rebalancer's "asker".
//
// Design choices specific to rebalancer (vs the rules-engine client):
//   - rebalancer is a SHORT-LIVED CLI, not a long-running service. The
//     5-minute startup-retry pattern used by rules-engine is overkill
//     here. We dial with a single 5-second timeout; if user-config isn't
//     reachable when someone runs the CLI, fail fast and surface the
//     error.
//   - We expose only the one RPC the CLI actually needs:
//     FetchActiveMANTHANStrategies. Other RPCs can be added when other
//     reads migrate (Phase 0.6 will need GetUserCredentials).
package internal

import (
	"context"
	"fmt"
	"time"

	pb "github.com/RohitIndira/Algo-Treading/api/proto/user_config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// UserConfigClient is rebalancer's thin wrapper around the user-config
// gRPC stub. Hold the underlying ClientConn so the caller can Close() it
// on shutdown.
type UserConfigClient struct {
	conn   *grpc.ClientConn
	client pb.UserConfigServiceClient
}

// NewUserConfigClient dials user-config gRPC at addr with a 5-second
// timeout. CLI semantics: fail fast if user-config is unreachable, don't
// retry for minutes. Returned client must be Close()d.
//
// TODO(mTLS): replace insecure.NewCredentials() with mTLS once
// service-mesh certs land. Inside the VPC this is acceptable for now;
// the bearer token never traverses this RPC (different RPC, Phase 0.6).
func NewUserConfigClient(ctx context.Context, addr string) (*UserConfigClient, error) {
	if addr == "" {
		return nil, fmt.Errorf("user-config gRPC address is required")
	}

	dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(dialCtx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("dial user-config %s: %w", addr, err)
	}

	return &UserConfigClient{
		conn:   conn,
		client: pb.NewUserConfigServiceClient(conn),
	}, nil
}

// Close shuts down the underlying gRPC connection.
func (c *UserConfigClient) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// FetchActiveMANTHANStrategies fetches every active MANTHAN strategy
// via user-config gRPC, mapping the proto into rebalancer's local
// StrategyConfig shape.
//
// Replaces the legacy SQL:
//
//	SELECT s.strategy_id, s.user_id, s.strategy_name, s.trading_mode,
//	       COALESCE(t.total_capital, 0),       ... etc.
//	FROM strategies s LEFT JOIN trade_configs t ON ...
//	WHERE s.strategy_type = 'MANTHAN'
//	  AND s.deleted_at IS NULL AND s.active = true
//	ORDER BY s.user_id, s.created_at
//
// Pagination is handled inside (500-row pages, same as rules-engine).
// Server-side already enforces deleted_at IS NULL and active = true; we
// filter strategy_type = MANTHAN client-side because no RPC filter exists
// yet (would be a small server-side improvement later).
//
// trading_mode is normalized to a canonical "LIVE" / "PAPER" string so
// downstream allocator code can compare strings without proto enums
// leaking in.
//
// Defaults like MaxPositions ≤25L → 25 / >25L → 50 stay in snapshot.go
// (the calling site) — separation of concerns: client fetches, caller
// computes derived fields.
func (c *UserConfigClient) FetchActiveMANTHANStrategies(ctx context.Context) ([]StrategyConfig, error) {
	out := make([]StrategyConfig, 0)
	pageToken := ""

	for {
		resp, err := c.client.GetAllActiveStrategies(ctx, &pb.GetAllActiveStrategiesRequest{
			PageSize:  500,
			PageToken: pageToken,
		})
		if err != nil {
			return nil, fmt.Errorf("GetAllActiveStrategies page: %w", err)
		}
		if !resp.Success {
			if resp.Error != nil {
				return nil, fmt.Errorf("GetAllActiveStrategies unsuccessful: %s %s",
					resp.Error.Code, resp.Error.Message)
			}
			return nil, fmt.Errorf("GetAllActiveStrategies unsuccessful with no error detail")
		}

		for _, s := range resp.Strategies {
			if s == nil {
				continue
			}
			if s.StrategyType != pb.StrategyType_MANTHAN {
				continue
			}
			c := StrategyConfig{
				StrategyID:   s.StrategyId,
				UserID:       s.UserId,
				StrategyName: s.StrategyName,
				TradingMode:  tradingModeToString(s.TradingMode),
			}
			if s.TradeConfig != nil {
				c.TotalCapital = s.TradeConfig.TotalCapital
				c.MaxPositions = int(s.TradeConfig.MaxPositions)
				c.StopLossPct = s.TradeConfig.StopLossPct
				c.TrailingSLPct = s.TradeConfig.TrailingSlPct
			}
			out = append(out, c)
		}

		if resp.NextPageToken == "" {
			break
		}
		pageToken = resp.NextPageToken
	}

	return out, nil
}

// tradingModeToString turns the proto enum into the canonical string the
// rest of rebalancer expects. Matches rules-engine's normalize helper —
// both services agree on "LIVE" / "PAPER" as the on-the-wire spelling.
func tradingModeToString(m pb.TradingMode) string {
	switch m {
	case pb.TradingMode_LIVE:
		return "LIVE"
	case pb.TradingMode_PAPER:
		return "PAPER"
	default:
		return "PAPER" // safe default
	}
}
