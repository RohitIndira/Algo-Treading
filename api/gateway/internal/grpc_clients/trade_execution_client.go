package grpc_clients

import (
	"context"
	"time"

	common "github.com/RohitIndira/Algo-Treading/api/proto/common"
	pb "github.com/RohitIndira/Algo-Treading/api/proto/trade_execution"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TradeExecutionClient is a thin wrapper around the gRPC TradeExecutionService
// client, adding per-call timeouts similar to UserConfigClient.
type TradeExecutionClient struct {
	client  pb.TradeExecutionServiceClient
	conn    *grpc.ClientConn
	timeout time.Duration
}

func NewTradeExecutionClient(addr string, timeout time.Duration) (*TradeExecutionClient, error) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	return &TradeExecutionClient{
		client:  pb.NewTradeExecutionServiceClient(conn),
		conn:    conn,
		timeout: timeout,
	}, nil
}

func (c *TradeExecutionClient) Close() error {
	return c.conn.Close()
}

// GetUserOrders proxies the GetUserOrders RPC with a timeout.
func (c *TradeExecutionClient) GetUserOrders(ctx context.Context, req *pb.GetUserOrdersRequest) (*pb.GetUserOrdersResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.GetUserOrders(ctx, req)
}

// HealthCheck allows the gateway to monitor the trade-execution service.
func (c *TradeExecutionClient) HealthCheck(ctx context.Context, req *common.HealthCheckRequest) (*common.HealthCheckResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.HealthCheck(ctx, req)
}
