// End-to-end integration test: gRPC credentials adapter → REAL Codifi broker.
//
// This test proves the FULL credential flow against the live broker:
//
//   1. trade-execution code calls GRPCCredentialsRepository.GetIndiraCredentials
//   2. Adapter dials user-config over gRPC
//   3. user-config reads the encrypted JWT from Postgres, decrypts via AES-GCM
//   4. JWT comes back over gRPC into trade-execution
//   5. trade-execution uses the JWT to call REAL Codifi (livemiddleware.indiratrade.com)
//   6. Broker accepts the request and returns real data (fund limit + holdings)
//
// If this passes, the gRPC refactor is end-to-end production-ready for the
// real broker. No part of the chain is mocked.
//
// Requires (all defaults to localhost):
//   - user-config gRPC running on :50051
//   - S4450 row seeded in trading_execution.user_credentials with a VALID JWT
//   - Outbound HTTPS to livemiddleware.indiratrade.com working
//
// Run with:
//   go test ./services/trade-execution/internal/repository/ -run TestBroker -v
//
// To skip (e.g. in CI without broker access):
//   go test -short ...
//
package repository

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/RohitIndira/Algo-Treading/pkg/indira"
)

// brokerTestUser is the user_id whose credentials we fetch + use for the
// broker call. Override with TEST_USER_ID env if needed.
func brokerTestUser() string {
	if u := os.Getenv("TEST_USER_ID"); u != "" {
		return u
	}
	return "S4450"
}

// TestBrokerReadFlow_FundLimit proves the gRPC-fetched JWT successfully
// authenticates against the REAL Codifi broker for a read-only call.
// Calls GET /payments/api/v1/get-fund-limit and asserts a real response.
func TestBrokerReadFlow_FundLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping broker integration test")
	}

	// ── Step 1: fetch creds via the gRPC adapter (the real refactor under test) ──
	fallback := &fakeFallback{}
	repo, err := NewGRPCCredentialsRepository(getAddr(), 5*time.Second, fallback)
	if err != nil {
		t.Fatalf("dial user-config: %v", err)
	}
	defer repo.Close()

	indiraUserID, appID, source, bearerToken, err := repo.GetIndiraCredentials(context.Background(), brokerTestUser())
	if err != nil {
		t.Fatalf("GetIndiraCredentials via gRPC: %v", err)
	}
	if bearerToken == "" {
		t.Fatal("empty bearer token from gRPC adapter")
	}
	if fallback.getCalled {
		t.Fatal("DB fallback was hit — gRPC path failed silently")
	}
	t.Logf("✓ gRPC fetch OK — indiraUserID=%s appID=%s source=%s token-len=%d",
		indiraUserID, appID, source, len(bearerToken))

	// ── Step 2: build AuthContext for the Indira client ──
	auth := &indira.AuthContext{
		UserId:      indiraUserID,
		AppId:       appID,
		Source:      source,
		BearerToken: bearerToken,
	}

	// ── Step 3: hit the REAL broker (read-only) ──
	client := indira.NewDefaultClient()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	fund, err := client.GetFundLimit(ctx, auth)
	if err != nil {
		t.Fatalf("REAL broker GetFundLimit failed: %v", err)
	}
	if fund == nil {
		t.Fatal("nil fund response from broker")
	}

	// We can't assert exact balances (they change), but we can confirm the
	// response decoded as a real FundLimit struct — no broker rejection.
	t.Logf("✓ REAL BROKER call succeeded — fund limit response received")
	t.Logf("    %+v", fund)
}

// TestBrokerReadFlow_Holdings is the second read-only broker assertion.
// Holdings is what we use during EOD Phase A; success here means the cron
// will work too.
func TestBrokerReadFlow_Holdings(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping broker integration test")
	}

	fallback := &fakeFallback{}
	repo, err := NewGRPCCredentialsRepository(getAddr(), 5*time.Second, fallback)
	if err != nil {
		t.Fatalf("dial user-config: %v", err)
	}
	defer repo.Close()

	indiraUserID, appID, source, bearerToken, err := repo.GetIndiraCredentials(context.Background(), brokerTestUser())
	if err != nil {
		t.Fatalf("GetIndiraCredentials via gRPC: %v", err)
	}

	auth := &indira.AuthContext{
		UserId:      indiraUserID,
		AppId:       appID,
		Source:      source,
		BearerToken: bearerToken,
	}

	client := indira.NewDefaultClient()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	holdings, err := client.GetHoldings(ctx, auth)
	if err != nil {
		t.Fatalf("REAL broker GetHoldings failed: %v", err)
	}

	t.Logf("✓ REAL BROKER GetHoldings OK — %d holdings returned", len(holdings))
	for i, h := range holdings {
		if i >= 10 {
			t.Logf("    (+ %d more)", len(holdings)-10)
			break
		}
		t.Logf("    [%d] %+v", i, h)
	}
}
