package grpc_clients

import (
	"context"
	"time"

	hftpb "github.com/RohitIndira/Algo-Treading/api/proto/hft_engine"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// HFTClient is the api-gateway's gRPC client to the hft-engine. It drives
// the strategy lifecycle (Entry/Exit) and reads the live snapshot
// (GetState) for HFT_BIDDING strategies.
type HFTClient struct {
	client  hftpb.HFTServiceClient
	conn    *grpc.ClientConn
	timeout time.Duration
}

// NewHFTClient dials the hft-engine gRPC server. addr is e.g.
// "localhost:29090". The dial is lazy — it does not block on connect.
func NewHFTClient(addr string, timeout time.Duration) (*HFTClient, error) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &HFTClient{
		client:  hftpb.NewHFTServiceClient(conn),
		conn:    conn,
		timeout: timeout,
	}, nil
}

func (c *HFTClient) Close() error {
	return c.conn.Close()
}

// Entry starts a (previously created) HFT strategy running in the engine.
func (c *HFTClient) Entry(ctx context.Context, req *hftpb.EntryRequest) (*hftpb.EntryResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.Entry(ctx, req)
}

// Exit halts a running HFT strategy and cancels its resting orders.
func (c *HFTClient) Exit(ctx context.Context, req *hftpb.ExitRequest) (*hftpb.ExitResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.Exit(ctx, req)
}

// GetState returns the current snapshot of a running HFT strategy.
func (c *HFTClient) GetState(ctx context.Context, req *hftpb.GetStateRequest) (*hftpb.GetStateResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.GetState(ctx, req)
}

// Ping is the hft-engine health probe.
func (c *HFTClient) Ping(ctx context.Context, req *hftpb.PingRequest) (*hftpb.PingResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.Ping(ctx, req)
}
