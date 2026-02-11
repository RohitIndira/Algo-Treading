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

// ========================================================================
// Jobbing Strategy Configuration Client Methods
// ========================================================================

func (c *UserConfigClient) ConfigureJobbingStrategy(ctx context.Context, req *pb.ConfigureJobbingStrategyRequest) (*pb.ConfigureJobbingStrategyResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.ConfigureJobbingStrategy(ctx, req)
}

func (c *UserConfigClient) GetJobbingConfigs(ctx context.Context, req *pb.GetJobbingConfigsRequest) (*pb.GetJobbingConfigsResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.GetJobbingConfigs(ctx, req)
}

func (c *UserConfigClient) GetJobbingConfig(ctx context.Context, req *pb.GetJobbingConfigRequest) (*pb.GetJobbingConfigResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.GetJobbingConfig(ctx, req)
}

func (c *UserConfigClient) UpdateJobbingConfig(ctx context.Context, req *pb.UpdateJobbingConfigRequest) (*pb.UpdateJobbingConfigResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.UpdateJobbingConfig(ctx, req)
}

func (c *UserConfigClient) DeleteJobbingConfig(ctx context.Context, req *pb.DeleteJobbingConfigRequest) (*pb.DeleteJobbingConfigResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.DeleteJobbingConfig(ctx, req)
}

func (c *UserConfigClient) EnableJobbingConfig(ctx context.Context, req *pb.EnableJobbingConfigRequest) (*pb.EnableJobbingConfigResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.EnableJobbingConfig(ctx, req)
}

func (c *UserConfigClient) DisableJobbingConfig(ctx context.Context, req *pb.DisableJobbingConfigRequest) (*pb.DisableJobbingConfigResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.DisableJobbingConfig(ctx, req)
}
