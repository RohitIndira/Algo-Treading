package grpc_clients

import (
	"context"
	"time"

	common "github.com/RohitIndira/Algo-Treading/api/proto/common"
	pb "github.com/RohitIndira/Algo-Treading/api/proto/user_config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// UserConfigClient Struct that holds the gRPC client and connection details for the User Config Service.
type UserConfigClient struct {
	client  pb.UserConfigServiceClient
	conn    *grpc.ClientConn
	timeout time.Duration
}

// NewUserConfigClient creates a new UserConfigClient by establishing a gRPC connection to the User Config Service at the specified address with the given timeout for calls.
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

// Close closes the gRPC connection to the User Config Service.
func (c *UserConfigClient) Close() error {
	return c.conn.Close()
}

// CreateStrategy calls the CreateStrategy method on the User Config Service via gRPC.
func (c *UserConfigClient) CreateStrategy(ctx context.Context, req *pb.CreateStrategyRequest) (*pb.CreateStrategyResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.CreateStrategy(ctx, req)
}

// UpdateStrategy updates an existing trading strategy for a user.
func (c *UserConfigClient) UpdateStrategy(ctx context.Context, req *pb.UpdateStrategyRequest) (*pb.UpdateStrategyResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.UpdateStrategy(ctx, req)
}

// DeleteStrategy deletes a trading strategy for a user.
func (c *UserConfigClient) DeleteStrategy(ctx context.Context, req *pb.DeleteStrategyRequest) (*pb.DeleteStrategyResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.DeleteStrategy(ctx, req)
}

// GetStrategy retrieves a specific trading strategy for a user.
func (c *UserConfigClient) GetStrategy(ctx context.Context, req *pb.GetStrategyRequest) (*pb.GetStrategyResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.GetStrategy(ctx, req)
}

// ListUserStrategies lists all trading strategies for a user.
func (c *UserConfigClient) ListUserStrategies(ctx context.Context, req *pb.ListUserStrategiesRequest) (*pb.ListUserStrategiesResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.ListUserStrategies(ctx, req)
}

// ActivateStrategy activates a trading strategy for a user.
func (c *UserConfigClient) ActivateStrategy(ctx context.Context, req *pb.ActivateStrategyRequest) (*pb.ActivateStrategyResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.ActivateStrategy(ctx, req)
}

// DeactivateStrategy deactivates a trading strategy for a user.
func (c *UserConfigClient) DeactivateStrategy(ctx context.Context, req *pb.DeactivateStrategyRequest) (*pb.DeactivateStrategyResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.DeactivateStrategy(ctx, req)
}

// HealthCheck performs a health check on the User Config Service.
func (c *UserConfigClient) HealthCheck(ctx context.Context, req *common.HealthCheckRequest) (*common.HealthCheckResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.HealthCheck(ctx, req)
}
