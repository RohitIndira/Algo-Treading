package grpc_clients

import (
	"context"
	"time"

	pfpb "github.com/RohitIndira/Algo-Treading/api/proto/portfolio"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// PortfolioClient is the api-gateway's gRPC client to portfolio svc.
// portfolio svc is the read side of the positions CQRS split — see
// docs/portfolio_service_design.md.
//
// One conn per gateway process, shared across handlers (matches the
// pattern of UserConfigClient in this package).
type PortfolioClient struct {
	client  pfpb.PortfolioServiceClient
	conn    *grpc.ClientConn
	timeout time.Duration
}

// NewPortfolioClient dials portfolio svc. addr is typically
// "localhost:9005" locally or the K8s service DNS in staging. Lazy dial —
// does not block on connect, mirroring UserConfig behaviour.
func NewPortfolioClient(addr string, timeout time.Duration) (*PortfolioClient, error) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &PortfolioClient{
		client:  pfpb.NewPortfolioServiceClient(conn),
		conn:    conn,
		timeout: timeout,
	}, nil
}

func (c *PortfolioClient) Close() error {
	return c.conn.Close()
}

// GetPortfolioSummary — per-user rollup: invested, realized (lifetime +
// today), counts, MANTHAN/USER_MANUAL split. Unrealized P&L is NOT
// returned — the handler computes it after LTP fetch.
func (c *PortfolioClient) GetPortfolioSummary(ctx context.Context, userID string) (*pfpb.GetPortfolioSummaryResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.GetPortfolioSummary(ctx, &pfpb.GetPortfolioSummaryRequest{UserId: userID})
}

// GetActivePositions — all ACTIVE lots for a user, entry_time ASC.
// Handler joins each row with (a) exchange_token → (b) LTP → (c) unrealized P&L.
func (c *PortfolioClient) GetActivePositions(ctx context.Context, userID string) (*pfpb.GetActivePositionsResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.GetActivePositions(ctx, &pfpb.GetActivePositionsRequest{UserId: userID})
}

// GetClosedPositions — paginated EXITED lots, exit_time DESC.
// pageSize is clamped server-side to [1, 200]; page to >=1.
func (c *PortfolioClient) GetClosedPositions(ctx context.Context, userID string, page, pageSize int32) (*pfpb.GetClosedPositionsResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.GetClosedPositions(ctx, &pfpb.GetClosedPositionsRequest{
		UserId:   userID,
		Page:     page,
		PageSize: pageSize,
	})
}
