package grpc_clients

import (
	"context"
	"time"

	common "github.com/RohitIndira/Algo-Treading/api/proto/common"
	pb "github.com/RohitIndira/Algo-Treading/api/proto/user_config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type UserConfigClient struct {
	client  pb.UserConfigServiceClient
	conn    *grpc.ClientConn
	timeout time.Duration
}

func NewUserConfigClient(addr string, timeout time.Duration) (*UserConfigClient, error) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	return &UserConfigClient{
		client:  pb.NewUserConfigServiceClient(conn),
		conn:    conn,
		timeout: timeout,
	}, nil
}

func (c *UserConfigClient) Close() error {
	return c.conn.Close()
}

func (c *UserConfigClient) CreateStrategy(ctx context.Context, req *pb.CreateStrategyRequest) (*pb.CreateStrategyResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.CreateStrategy(ctx, req)
}

func (c *UserConfigClient) UpdateStrategy(ctx context.Context, req *pb.UpdateStrategyRequest) (*pb.UpdateStrategyResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.UpdateStrategy(ctx, req)
}

func (c *UserConfigClient) DeleteStrategy(ctx context.Context, req *pb.DeleteStrategyRequest) (*pb.DeleteStrategyResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.DeleteStrategy(ctx, req)
}

func (c *UserConfigClient) GetStrategy(ctx context.Context, req *pb.GetStrategyRequest) (*pb.GetStrategyResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.GetStrategy(ctx, req)
}

func (c *UserConfigClient) ListUserStrategies(ctx context.Context, req *pb.ListUserStrategiesRequest) (*pb.ListUserStrategiesResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.ListUserStrategies(ctx, req)
}

func (c *UserConfigClient) ActivateStrategy(ctx context.Context, req *pb.ActivateStrategyRequest) (*pb.ActivateStrategyResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.ActivateStrategy(ctx, req)
}

func (c *UserConfigClient) DeactivateStrategy(ctx context.Context, req *pb.DeactivateStrategyRequest) (*pb.DeactivateStrategyResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.DeactivateStrategy(ctx, req)
}

func (c *UserConfigClient) HealthCheck(ctx context.Context, req *common.HealthCheckRequest) (*common.HealthCheckResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.HealthCheck(ctx, req)
}

// ConfigureCash52WeekStrategy calls the high-level 52W configuration RPC.
// This is used by the API gateway HTTP endpoint to let the frontend enable/
// disable the managed Cash 52-week High strategy for a user without
// dealing with low-level trade_config/risk_limits fields.
func (c *UserConfigClient) ConfigureCash52WeekStrategy(ctx context.Context, req *pb.ConfigureCash52WeekStrategyRequest) (*pb.ConfigureCash52WeekStrategyResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.ConfigureCash52WeekStrategy(ctx, req)
}

// ============================================================================
// PHASE 1: Enhanced Cash52W Configuration Client Methods
// ============================================================================

func (c *UserConfigClient) ConfigureCash52WStrategyEnhanced(ctx context.Context, req *pb.ConfigureCash52WStrategyEnhancedRequest) (*pb.ConfigureCash52WStrategyEnhancedResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.ConfigureCash52WStrategyEnhanced(ctx, req)
}

func (c *UserConfigClient) GetCash52WConfig(ctx context.Context, req *pb.GetCash52WConfigRequest) (*pb.GetCash52WConfigResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.GetCash52WConfig(ctx, req)
}

func (c *UserConfigClient) ForceExitAll(ctx context.Context, req *pb.ForceExitAllRequest) (*pb.ForceExitAllResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.ForceExitAll(ctx, req)
}

func (c *UserConfigClient) ForceExitStocks(ctx context.Context, req *pb.ForceExitStocksRequest) (*pb.ForceExitStocksResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.ForceExitStocks(ctx, req)
}

func (c *UserConfigClient) UpdateManualControls(ctx context.Context, req *pb.UpdateManualControlsRequest) (*pb.UpdateManualControlsResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.UpdateManualControls(ctx, req)
}

func (c *UserConfigClient) DisableCash52W(ctx context.Context, req *pb.DisableCash52WRequest) (*pb.DisableCash52WResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.DisableCash52W(ctx, req)
}

func (c *UserConfigClient) GetAllEnabledConfigs(ctx context.Context, req *pb.GetAllEnabledConfigsRequest) (*pb.GetAllEnabledConfigsResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.GetAllEnabledConfigs(ctx, req)
}
