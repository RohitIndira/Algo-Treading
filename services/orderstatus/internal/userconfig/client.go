// Package userconfig is orderstatus svc's thin gRPC client for user-config.
//
// Same pattern as services/rebalancer/internal/userconfig_client.go — the
// canonical way for cross-service DB reads in this codebase. user-config
// owns the strategies + user_credentials tables; everyone else asks via
// gRPC. This avoids:
//
//   - orderstatus needing a connection to execution_db DB
//   - orderstatus holding the token encryption key
//   - orderstatus running any decryption code
//
// All three concerns live behind GetUserCredentials on user-config's side,
// which also writes a SEBI-grade audit log on every read.
package userconfig

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/RohitIndira/Algo-Treading/api/proto/user_config"
	indira "github.com/RohitIndira/Algo-Treading/pkg/indira"
)

// Client is orderstatus svc's user-config gRPC client.
type Client struct {
	conn *grpc.ClientConn
	rpc  pb.UserConfigServiceClient
}

// NewClient dials user-config gRPC at addr with a 10s connect timeout.
// Fails fast — orderstatus svc can't do useful work without user-config.
//
// TODO(mTLS): swap insecure.NewCredentials() for mTLS when service-mesh
// certs land. Same TODO the rebalancer's client has.
func NewClient(ctx context.Context, addr string) (*Client, error) {
	if addr == "" {
		return nil, fmt.Errorf("user-config gRPC address is required")
	}

	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(dialCtx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("dial user-config %s: %w", addr, err)
	}

	return &Client{
		conn: conn,
		rpc:  pb.NewUserConfigServiceClient(conn),
	}, nil
}

// Close shuts down the gRPC connection.
func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// ListActiveUserIDs returns the deduplicated set of user_ids that have at
// least one active strategy. One user can have multiple strategies (e.g.
// MANTHAN + HFT) — orderstatus svc only needs ONE WSS subscription per
// user, so we dedup here.
//
// Server pagination handled inside (500-row pages, same as rebalancer).
func (c *Client) ListActiveUserIDs(ctx context.Context) ([]string, error) {
	seen := make(map[string]bool)
	pageToken := ""

	for {
		resp, err := c.rpc.GetAllActiveStrategies(ctx, &pb.GetAllActiveStrategiesRequest{
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
			if s == nil || s.UserId == "" {
				continue
			}
			seen[s.UserId] = true
		}

		if resp.NextPageToken == "" {
			break
		}
		pageToken = resp.NextPageToken
	}

	out := make([]string, 0, len(seen))
	for userID := range seen {
		out = append(out, userID)
	}
	return out, nil
}

// FetchUserCredentials fetches broker credentials for userID via user-config
// gRPC. Server-side decrypts the bearer token — orderstatus svc never sees
// the encryption key.
//
// Returns:
//
//	(auth, nil) — user found, ready-to-use AuthContext
//	(nil,  nil) — user has no broker creds on file (NOT_FOUND) — caller
//	              logs + skips this user's subscription
//	(nil, err)  — network / unknown failure — caller decides retry vs abort
func (c *Client) FetchUserCredentials(ctx context.Context, userID string) (*indira.AuthContext, error) {
	if userID == "" {
		return nil, fmt.Errorf("userID is required")
	}

	resp, err := c.rpc.GetUserCredentials(ctx, &pb.GetUserCredentialsRequest{UserId: userID})
	if err != nil {
		return nil, fmt.Errorf("GetUserCredentials RPC: %w", err)
	}
	if !resp.Success {
		if resp.Error != nil && resp.Error.Code == "NOT_FOUND" {
			return nil, nil
		}
		if resp.Error != nil {
			return nil, fmt.Errorf("GetUserCredentials unsuccessful: %s %s",
				resp.Error.Code, resp.Error.Message)
		}
		return nil, fmt.Errorf("GetUserCredentials unsuccessful with no error detail")
	}
	if resp.IndiraAuth == nil {
		return nil, fmt.Errorf("GetUserCredentials returned success but nil IndiraAuth")
	}

	return &indira.AuthContext{
		UserId:      userID,
		BearerToken: resp.IndiraAuth.BearerToken,
		AppId:       resp.IndiraAuth.AppId,
		Source:      resp.IndiraAuth.Source,
	}, nil
}
