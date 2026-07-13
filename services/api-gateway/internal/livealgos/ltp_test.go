package livealgos

// Hermetic tests for the LTP health flag + FetchByTokens no-silent-fail
// contract. Doesn't dial a real Redis — the *redis.Client field is
// exercised through the healthy-flag short-circuits.

import (
	"context"
	"testing"
)

func TestLTP_StartsUnhealthy(t *testing.T) {
	s := &LTPStore{} // no rdb, no explicit MarkHealthy
	if s.IsHealthy() {
		t.Error("fresh LTPStore must start UNHEALTHY (fail-safe default)")
	}
}

func TestLTP_MarkHealthy_TogglesFlag(t *testing.T) {
	s := &LTPStore{}
	s.MarkHealthy(true)
	if !s.IsHealthy() {
		t.Error("MarkHealthy(true) did not flip flag")
	}
	s.MarkHealthy(false)
	if s.IsHealthy() {
		t.Error("MarkHealthy(false) did not clear flag")
	}
}

func TestLTP_NilStore_IsHealthy_ReturnsFalse(t *testing.T) {
	var s *LTPStore
	if s.IsHealthy() {
		t.Error("nil *LTPStore must report UNHEALTHY (not panic)")
	}
}

func TestLTP_FetchByTokens_EmptyInput_HealthyEvenIfProbeDown(t *testing.T) {
	// Empty token list = "nothing to fetch" = vacuously healthy so the
	// UI can render its "no active positions" state normally.
	s := &LTPStore{} // unhealthy
	got, st := s.FetchByTokens(context.Background(), nil)
	if st != StatusHealthy {
		t.Errorf("empty tokens: got status %q, want HEALTHY", st)
	}
	if got == nil || len(got) != 0 {
		t.Errorf("empty tokens: want empty map, got %+v", got)
	}
}

func TestLTP_FetchByTokens_ShortCircuitsWhenUnhealthy(t *testing.T) {
	// Store has rdb set but flag is false — must return Unavailable
	// WITHOUT hitting Redis (we set rdb to nil to prove no dial happens;
	// a real MGet on nil client would panic).
	s := &LTPStore{}
	got, st := s.FetchByTokens(context.Background(), []string{"1234"})
	if st != StatusUnavailable {
		t.Errorf("unhealthy store: got %q, want UNAVAILABLE", st)
	}
	if got != nil {
		t.Errorf("unhealthy store: got map %+v, want nil", got)
	}
}

func TestLTP_FetchByTokens_NilRdb_AlwaysUnavailable(t *testing.T) {
	// Defensive: even if someone constructs an LTPStore with no rdb,
	// FetchByTokens must not panic — return Unavailable cleanly.
	s := &LTPStore{}
	s.MarkHealthy(true) // pretend probe reported OK
	got, st := s.FetchByTokens(context.Background(), []string{"1234"})
	if st != StatusUnavailable {
		t.Errorf("nil rdb: got %q, want UNAVAILABLE", st)
	}
	if got != nil {
		t.Errorf("nil rdb: got map %+v, want nil", got)
	}
}
