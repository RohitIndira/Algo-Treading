package store

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"
)

// TestInsert_BroadcastSourceDedupesWithWSS: the dealer broadcast and the
// per-user socket observing the same broker event collide on
// (broker_order_id, event_seq) whichever arrives first, and the row keeps the
// source that won. Needs migration 004 (source CHECK includes WSS_BROADCAST).
func TestInsert_BroadcastSourceDedupesWithWSS(t *testing.T) {
	db := testDB(t)
	cleanupTestRows(t, db)
	t.Cleanup(func() { cleanupTestRows(t, db) })

	w := NewWriter(db, zap.NewNop())
	ctx := context.Background()

	// Broadcast first.
	ev := newTestEvent(testGracePrefix+"BCAST", 41)
	ev.Source = SourceWSSBroadcast
	id, inserted, err := w.Insert(ctx, ev)
	if err != nil || !inserted || id == 0 {
		t.Fatalf("broadcast insert: id=%d inserted=%v err=%v (migration 004 applied?)", id, inserted, err)
	}
	// Per-user socket sees the same event → dedup.
	dup := newTestEvent(testGracePrefix+"BCAST", 41)
	dup.Source = SourceWSS
	if _, inserted, err := w.Insert(ctx, dup); err != nil || inserted {
		t.Fatalf("per-user duplicate: inserted=%v err=%v, want dedup", inserted, err)
	}
	row, err := w.LookupObserved(ctx, testGracePrefix+"BCAST", 41)
	if err != nil || row == nil {
		t.Fatalf("lookup: row=%v err=%v", row, err)
	}
	if row.Source != SourceWSSBroadcast || time.Since(row.ObservedAt) > time.Minute {
		t.Fatalf("stored row = %+v, want source WSS_BROADCAST observed just now", row)
	}

	// The other order: per-user first, broadcast deduped; lookup shows WSS.
	first := newTestEvent(testGracePrefix+"USER", 42)
	first.Source = SourceWSS
	if _, inserted, err := w.Insert(ctx, first); err != nil || !inserted {
		t.Fatalf("per-user insert: inserted=%v err=%v", inserted, err)
	}
	second := newTestEvent(testGracePrefix+"USER", 42)
	second.Source = SourceWSSBroadcast
	if _, inserted, err := w.Insert(ctx, second); err != nil || inserted {
		t.Fatalf("broadcast duplicate: inserted=%v err=%v, want dedup", inserted, err)
	}
	if row, _ := w.LookupObserved(ctx, testGracePrefix+"USER", 42); row == nil || row.Source != SourceWSS {
		t.Fatalf("lookup after per-user-first = %+v", row)
	}
	if row, err := w.LookupObserved(ctx, testGracePrefix+"NONE", 1); err != nil || row != nil {
		t.Fatalf("lookup of missing row = %+v, %v; want nil, nil", row, err)
	}
}

// TestLookupEquivalent: same content under a different event_seq is found
// within the window; a different status / price is not.
func TestLookupEquivalent(t *testing.T) {
	db := testDB(t)
	cleanupTestRows(t, db)
	t.Cleanup(func() { cleanupTestRows(t, db) })
	w := NewWriter(db, zap.NewNop())
	ctx := context.Background()

	ev := newTestEvent(testGracePrefix+"EQ", 7001)
	ev.Source = SourceWSS
	ev.FilledQty = 10
	ev.TradedPrice = 101.5
	ev.RawPayload = []byte(`{"LastModifiedTimeStamp":"26-Oct-2026 10:01:02","OrderTimeStamp":"26-Oct-2026 10:00:00"}`)
	if _, inserted, err := w.Insert(ctx, ev); err != nil || !inserted {
		t.Fatalf("insert: %v %v", inserted, err)
	}
	key := EquivalentKey{BrokerOrderID: testGracePrefix + "EQ", Status: "executed", FilledQty: 10, TradedPrice: 101.5,
		LastModifiedTimeStamp: "26-Oct-2026 10:01:02", OrderTimeStamp: "26-Oct-2026 10:00:00"}
	row, err := w.LookupEquivalent(ctx, key, 120*time.Second)
	if err != nil || row == nil || row.EventSeq != 7001 || row.Source != SourceWSS {
		t.Fatalf("equivalent lookup = %+v, %v", row, err)
	}
	other := key
	other.TradedPrice = 101.6
	if row, _ := w.LookupEquivalent(ctx, other, 120*time.Second); row != nil {
		t.Fatalf("different price must not match: %+v", row)
	}
	other = key
	other.Status = "PENDING"
	if row, _ := w.LookupEquivalent(ctx, other, 120*time.Second); row != nil {
		t.Fatalf("different status must not match: %+v", row)
	}
	if row, _ := w.LookupEquivalent(ctx, key, 0); row != nil {
		t.Fatalf("zero window must not match: %+v", row)
	}
}
