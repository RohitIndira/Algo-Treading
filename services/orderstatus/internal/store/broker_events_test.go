package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	_ "github.com/lib/pq"
	"go.uber.org/zap"
)

// testDB opens order_status_db from the same env vars main.go uses and skips
// the test when it's unreachable (CI without a DB). These are integration
// checks for the grace-period durability invariant, not pure unit tests.
func testDB(t *testing.T) *sql.DB {
	t.Helper()
	env := func(k, d string) string {
		if v := os.Getenv(k); v != "" {
			return v
		}
		return d
	}
	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		env("POSTGRES_HOST", "localhost"),
		env("POSTGRES_PORT", "5432"),
		env("POSTGRES_USER", "postgres"),
		env("POSTGRES_PASSWORD", "postgres"),
		env("ORDER_STATUS_DB", "order_status_db"),
		env("POSTGRES_SSL_MODE", "disable"),
	)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Skipf("no test DB (open): %v", err)
	}
	if err := db.PingContext(context.Background()); err != nil {
		t.Skipf("no test DB (ping): %v", err)
	}
	// Registered first → runs LAST (LIFO), so row-cleanup registered by the
	// test still has a live connection.
	t.Cleanup(func() { _ = db.Close() })
	return db
}

const testGracePrefix = "TEST-GRACE-"

func cleanupTestRows(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec(`DELETE FROM broker_events WHERE broker_order_id LIKE $1`, testGracePrefix+"%"); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
}

func newTestEvent(brokerOrderID string, seq int64) *Event {
	return &Event{
		BrokerOrderID: brokerOrderID,
		EventSeq:      seq,
		Source:        SourceRESTTradebook,
		EventType:     EventFilled,
		Status:        "EXECUTED",
		UserID:        "TEST-USER",
		RawPayload:    []byte("{}"),
	}
}

func containsOrder(rows []*PersistedEvent, brokerOrderID string) bool {
	for _, r := range rows {
		if r.BrokerOrderID == brokerOrderID {
			return true
		}
	}
	return false
}

// TestFetchUnpublished_StampedRowNeverReturned: a row published inline and
// stamped is never returned by the worker, grace period regardless.
func TestFetchUnpublished_StampedRowNeverReturned(t *testing.T) {
	db := testDB(t)
	cleanupTestRows(t, db)
	t.Cleanup(func() { cleanupTestRows(t, db) })

	w := NewWriter(db, zap.NewNop())
	ctx := context.Background()

	id, inserted, err := w.Insert(ctx, newTestEvent(testGracePrefix+"STAMPED", 1))
	if err != nil || !inserted {
		t.Fatalf("insert: id=%d inserted=%v err=%v", id, inserted, err)
	}
	if err := w.MarkPublished(ctx, id); err != nil {
		t.Fatalf("mark published: %v", err)
	}

	// Even with grace=0 (all ages eligible), a stamped row is excluded.
	rows, err := w.FetchUnpublished(ctx, 100, 0)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if containsOrder(rows, testGracePrefix+"STAMPED") {
		t.Fatalf("stamped row must NOT appear in FetchUnpublished")
	}
}

// TestFetchUnpublished_GracePeriod: an unstamped row is invisible to the worker
// while younger than the grace period, and becomes visible once aged past it.
func TestFetchUnpublished_GracePeriod(t *testing.T) {
	db := testDB(t)
	cleanupTestRows(t, db)
	t.Cleanup(func() { cleanupTestRows(t, db) })

	w := NewWriter(db, zap.NewNop())
	ctx := context.Background()

	orderID := testGracePrefix + "FRESH"
	id, inserted, err := w.Insert(ctx, newTestEvent(orderID, 1))
	if err != nil || !inserted {
		t.Fatalf("insert: id=%d inserted=%v err=%v", id, inserted, err)
	}

	// Fresh row (observed_at ~ now): must NOT be returned under a 5s grace —
	// the inline path still owns it, so the worker must not double-publish.
	rows, err := w.FetchUnpublished(ctx, 100, 5)
	if err != nil {
		t.Fatalf("fetch (fresh): %v", err)
	}
	if containsOrder(rows, orderID) {
		t.Fatalf("fresh unstamped row must NOT be returned within the grace period")
	}

	// Age it past the grace window (simulate 10s elapsed) without sleeping.
	if _, err := db.ExecContext(ctx,
		`UPDATE broker_events SET observed_at = NOW() - INTERVAL '10 seconds' WHERE id = $1`, id); err != nil {
		t.Fatalf("backdate observed_at: %v", err)
	}

	rows, err = w.FetchUnpublished(ctx, 100, 5)
	if err != nil {
		t.Fatalf("fetch (aged): %v", err)
	}
	if !containsOrder(rows, orderID) {
		t.Fatalf("aged unstamped row MUST be returned by the worker")
	}
}
