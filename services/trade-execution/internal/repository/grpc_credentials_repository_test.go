// Integration test for GRPCCredentialsRepository.
//
// Requires a running user-config gRPC server at the address pointed to by
// USER_CONFIG_GRPC_TEST_ADDR (default: localhost:50051) and a seeded row for
// the user TEST_USER_ID (default: S4450). Seed via:
//
//   grpcurl -plaintext -d '{"user_id":"S4450","indira_auth":{...}}' \
//     localhost:50051 user_config.UserConfigService/UpdateUserCredentials
//
// Skipped automatically with `go test -short` so unit-test runs in CI don't
// require the gRPC server. Run it manually with:
//
//   go test ./internal/repository/ -run TestGRPC -v
//
package repository

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

// fakeFallback implements CredentialsRepository for the test. Tracks whether
// the fallback was triggered so we can assert the gRPC path was used.
type fakeFallback struct {
	getCalled   bool
	storeCalled bool
}

func (f *fakeFallback) StoreIndiraCredentials(_ context.Context, _, _, _, _, _ string) error {
	f.storeCalled = true
	return nil
}

func (f *fakeFallback) GetIndiraCredentials(_ context.Context, _ string) (string, string, string, string, error) {
	f.getCalled = true
	return "", "", "", "", ErrCredentialsNotFound
}

func getAddr() string {
	if a := os.Getenv("USER_CONFIG_GRPC_TEST_ADDR"); a != "" {
		return a
	}
	return "localhost:50051"
}

func getTestUser() string {
	if u := os.Getenv("TEST_USER_ID"); u != "" {
		return u
	}
	return "S4450"
}

func TestGRPCCredentialsRepository_HappyPath(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test — needs user-config gRPC running")
	}

	fallback := &fakeFallback{}
	repo, err := NewGRPCCredentialsRepository(getAddr(), 3*time.Second, fallback)
	if err != nil {
		t.Fatalf("dial user-config: %v (is it running on %s?)", err, getAddr())
	}
	defer repo.Close()

	userID := getTestUser()
	indiraUserID, appID, source, token, err := repo.GetIndiraCredentials(context.Background(), userID)
	if err != nil {
		t.Fatalf("GetIndiraCredentials(%s) error: %v", userID, err)
	}

	if indiraUserID == "" || appID == "" || source == "" || token == "" {
		t.Errorf("got empty fields: indiraUserID=%q appID=%q source=%q token-len=%d",
			indiraUserID, appID, source, len(token))
	}
	if fallback.getCalled {
		t.Errorf("fallback was triggered — gRPC path failed silently")
	}
	t.Logf("✓ gRPC fetch OK: indiraUserID=%s appID=%s source=%s token-len=%d",
		indiraUserID, appID, source, len(token))
}

func TestGRPCCredentialsRepository_NotFound(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test — needs user-config gRPC running")
	}

	fallback := &fakeFallback{}
	repo, err := NewGRPCCredentialsRepository(getAddr(), 3*time.Second, fallback)
	if err != nil {
		t.Fatalf("dial user-config: %v", err)
	}
	defer repo.Close()

	_, _, _, _, err = repo.GetIndiraCredentials(context.Background(), "DEFINITELY_DOES_NOT_EXIST_XYZ")
	if !errors.Is(err, ErrCredentialsNotFound) {
		t.Errorf("expected ErrCredentialsNotFound, got: %v", err)
	}
	if fallback.getCalled {
		t.Errorf("NOT_FOUND from server should NOT trigger fallback")
	}
	t.Logf("✓ NOT_FOUND propagated correctly without falling back")
}

func TestGRPCCredentialsRepository_FallbackOnDialError(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	// Point at a closed port so the gRPC call fails and triggers fallback.
	fallback := &fakeFallback{}
	repo, err := NewGRPCCredentialsRepository("localhost:1", 500*time.Millisecond, fallback)
	if err != nil {
		t.Fatalf("ctor should not fail before first RPC: %v", err)
	}
	defer repo.Close()

	_, _, _, _, err = repo.GetIndiraCredentials(context.Background(), "anyone")
	// fakeFallback returns ErrCredentialsNotFound, so that's what we expect.
	if !errors.Is(err, ErrCredentialsNotFound) {
		t.Errorf("expected ErrCredentialsNotFound from fallback, got: %v", err)
	}
	if !fallback.getCalled {
		t.Errorf("fallback should have been triggered on gRPC error")
	}
	t.Logf("✓ Fallback to DB triggered correctly when gRPC unreachable")
}

func TestGRPCCredentialsRepository_EmptyUserID(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	fallback := &fakeFallback{}
	repo, err := NewGRPCCredentialsRepository(getAddr(), 3*time.Second, fallback)
	if err != nil {
		t.Fatalf("dial user-config: %v", err)
	}
	defer repo.Close()

	_, _, _, _, err = repo.GetIndiraCredentials(context.Background(), "")
	if err == nil {
		t.Errorf("expected error for empty userID, got nil")
	}
	t.Logf("✓ Empty userID rejected: %v", err)
}
