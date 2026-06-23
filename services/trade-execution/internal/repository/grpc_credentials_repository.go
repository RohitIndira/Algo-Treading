// Package repository — gRPC-backed CredentialsRepository.
//
// This file is the "asker" half of the user_credentials ownership refactor.
// Trade-execution used to query the user_credentials TABLE directly (see
// credentials_repository.go); after this refactor it asks user-config via
// gRPC for the decrypted JWT, while writes (StoreIndiraCredentials) continue
// to flow through the local DB repo for backward compatibility until the
// table itself moves to stockk_auth.
//
// See docs/architecture/data-ownership.md  (single-writer rule) and
//     docs/architecture/communication-patterns.md  (Pattern B: gRPC).
package repository

import (
	"context"
	"fmt"
	"log"
	"time"

	pb "github.com/RohitIndira/Algo-Treading/api/proto/user_config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// userConfigCredsClient is the minimal interface we need from user-config's
// gRPC client. Kept as an interface so unit tests can swap in a fake.
type userConfigCredsClient interface {
	GetUserCredentials(ctx context.Context, in *pb.GetUserCredentialsRequest, opts ...grpc.CallOption) (*pb.GetUserCredentialsResponse, error)
}

// grpcCredentialsRepository implements CredentialsRepository by delegating:
//   - GetIndiraCredentials  → user-config gRPC (primary), local DB (fallback)
//   - StoreIndiraCredentials → local DB (writes are not yet migrated)
//
// The DB fallback (dbFallback) is what makes this safe to deploy. If user-config
// is briefly unreachable mid-cron (deploy, network blip), we still satisfy the
// read from the DB row that was last persisted there. After we move the table
// to stockk_auth and trust gRPC fully, the fallback comes out.
type grpcCredentialsRepository struct {
	conn       *grpc.ClientConn
	client     userConfigCredsClient
	dbFallback CredentialsRepository
	timeout    time.Duration
}

// NewGRPCCredentialsRepository dials the given user-config gRPC address and
// returns a CredentialsRepository that reads via gRPC and falls back to the
// supplied dbFallback on RPC error. The caller is responsible for closing the
// returned repo (it embeds the gRPC ClientConn).
//
// addr     — user-config gRPC host:port (e.g. "user-config:9003")
// timeout  — per-RPC deadline; 2-3s is a reasonable production value
// fallback — the local DB-backed CredentialsRepository (used for writes +
//            read-fallback). Must not be nil; pass NoopCredentialsRepository
//            in tests if no DB is available.
func NewGRPCCredentialsRepository(addr string, timeout time.Duration, fallback CredentialsRepository) (*GRPCCredentialsRepository, error) {
	if addr == "" {
		return nil, fmt.Errorf("user-config gRPC address is required")
	}
	if fallback == nil {
		return nil, fmt.Errorf("DB fallback CredentialsRepository is required")
	}
	if timeout <= 0 {
		timeout = 3 * time.Second
	}

	// TODO(mTLS): replace insecure.NewCredentials() with mTLS once we have
	// service-mesh certs. Plain text is acceptable inside the VPC for now;
	// the bearer token is still encrypted at rest in user-config's DB.
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial user-config %s: %w", addr, err)
	}

	return &GRPCCredentialsRepository{
		inner: &grpcCredentialsRepository{
			conn:       conn,
			client:     pb.NewUserConfigServiceClient(conn),
			dbFallback: fallback,
			timeout:    timeout,
		},
	}, nil
}

// GRPCCredentialsRepository is the public handle. It exposes Close() so
// cmd/main.go can clean up the underlying gRPC ClientConn on shutdown.
type GRPCCredentialsRepository struct {
	inner *grpcCredentialsRepository
}

// Close shuts down the gRPC connection. Safe to call on a nil receiver.
func (g *GRPCCredentialsRepository) Close() error {
	if g == nil || g.inner == nil || g.inner.conn == nil {
		return nil
	}
	return g.inner.conn.Close()
}

// StoreIndiraCredentials delegates to the DB fallback. Writes are NOT yet
// migrated to gRPC — user-config still owns the upsert path via its own
// StrategyService.UpdateUserCredentials, but trade-execution's local
// strategy_events consumer also writes here for cache-warmup purposes. This
// dual-write goes away in Phase 2 of the data-ownership migration.
func (g *GRPCCredentialsRepository) StoreIndiraCredentials(ctx context.Context, userID, indiraUserID, appID, source, bearerToken string) error {
	return g.inner.dbFallback.StoreIndiraCredentials(ctx, userID, indiraUserID, appID, source, bearerToken)
}

// GetIndiraCredentials calls user-config's GetUserCredentials RPC. On any
// gRPC error (network, deadline, NOT_FOUND from server) it falls back to
// the DB read so a transient user-config outage doesn't kill the trading
// crons. Fallback hits are logged so we can alert on them.
//
// Signature mirrors the legacy DB repo (5 return values) so all existing
// call sites work unchanged — the adapter pattern in action.
func (g *GRPCCredentialsRepository) GetIndiraCredentials(ctx context.Context, userID string) (string, string, string, string, error) {
	if userID == "" {
		return "", "", "", "", fmt.Errorf("userID is required")
	}

	rpcCtx, cancel := context.WithTimeout(ctx, g.inner.timeout)
	defer cancel()

	resp, err := g.inner.client.GetUserCredentials(rpcCtx, &pb.GetUserCredentialsRequest{UserId: userID})
	if err == nil && resp != nil && resp.Success && resp.IndiraAuth != nil {
		auth := resp.IndiraAuth
		return auth.UserId, auth.AppId, auth.Source, auth.BearerToken, nil
	}

	// Explicit NOT_FOUND from the server — don't fall back, propagate the
	// canonical sentinel error so callers branch on it (e.g. Layer-4 retry).
	if err == nil && resp != nil && resp.Error != nil && resp.Error.Code == "NOT_FOUND" {
		return "", "", "", "", ErrCredentialsNotFound
	}

	// Anything else (network, deadline, INTERNAL) → fall back to local DB.
	// This is the safety net that lets us deploy gRPC-first without dropping
	// crons during a user-config restart.
	if err != nil {
		log.Printf("[trade-execution] grpc creds fetch failed user=%s err=%v — falling back to DB", userID, err)
	} else if resp != nil && resp.Error != nil {
		log.Printf("[trade-execution] grpc creds fetch unsuccessful user=%s code=%s — falling back to DB", userID, resp.Error.Code)
	}
	return g.inner.dbFallback.GetIndiraCredentials(ctx, userID)
}

// Compile-time check that GRPCCredentialsRepository satisfies the interface.
// If anyone changes CredentialsRepository, this line breaks the build at
// compile time instead of runtime — exactly what we want.
var _ CredentialsRepository = (*GRPCCredentialsRepository)(nil)
