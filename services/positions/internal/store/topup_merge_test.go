package store

// DB test for MergeTopupWithEvent — the 2026-08-11 PICCADIL positions_db
// undercount (held 7 while the broker held 24). Runs against a local
// positions_db (localhost:5432); skips if unreachable.

import (
	"context"
	"database/sql"
	"os"
	"testing"

	_ "github.com/lib/pq"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

func openPosTestDB(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("POSITIONS_TEST_DSN")
	if dsn == "" {
		dsn = "host=localhost port=5432 user=postgres password=postgres dbname=positions_db sslmode=disable"
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Skipf("positions_db not reachable: %v", err)
	}
	if err := db.PingContext(context.Background()); err != nil {
		t.Skipf("positions_db ping failed: %v", err)
	}
	return New(db, zap.NewNop())
}

func TestMergeTopupWithEvent_VWAPAndIdempotent(t *testing.T) {
	s := openPosTestDB(t)
	ctx := context.Background()
	sig := uuid.New()
	posID := uuid.New()
	parentBID := "TESTBID-PARENT-" + posID.String()[:8]
	topupBID := "TESTBID-TOPUP-" + posID.String()[:8]
	t.Cleanup(func() {
		s.db.ExecContext(ctx, `DELETE FROM position_events WHERE position_id=$1`, posID)
		s.db.ExecContext(ctx, `DELETE FROM positions WHERE position_id=$1`, posID)
	})

	// Parent partial lot: 7 @ 802.20.
	parent := &Position{
		PositionID: posID, Origin: OriginManthan, UserID: "S4450",
		StrategyID: uuid.New().String(),
		Symbol:     "PICTESTMERGE", Exchange: "NSE", Status: StatusActive,
		EntryPrice: 802.20, Quantity: 7, InvestedAmount: 802.20 * 7,
		EntryBrokerOrderID: parentBID, SignalID: sig.String(),
	}
	if err := s.InsertEntryWithEvent(ctx, parent, &PositionEvent{
		EventType: EventTypeEntryFilled, BrokerOrderID: parentBID, SignalID: sig.String(),
		DeltaQty: 7, FillPrice: 802.20, RawSourceEvent: []byte("{}"), SourceEventID: "evt-parent-" + posID.String()[:8],
	}); err != nil {
		t.Fatalf("seed parent: %v", err)
	}

	// Merge the 17 @ 805.25 top-up.
	ev := &PositionEvent{
		EventType: EventTypeTopupMerged, BrokerOrderID: topupBID, SignalID: sig.String(),
		DeltaQty: 17, FillPrice: 805.25, RawSourceEvent: []byte("{}"), SourceEventID: "evt-topup-A-" + posID.String()[:8],
	}
	applied, err := s.MergeTopupWithEvent(ctx, posID, 17, 805.25, topupBID, ev)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if !applied {
		t.Fatal("first merge should apply")
	}

	got, err := s.FindManthanLotBySignalID(ctx, sig.String())
	if err != nil || got == nil {
		t.Fatalf("read back: %v", err)
	}
	if got.Quantity != 24 {
		t.Errorf("qty = %d, want 24 (7+17)", got.Quantity)
	}
	// VWAP = (7*802.20 + 17*805.25)/24 = 804.359...
	if got.EntryPrice < 804.35 || got.EntryPrice > 804.37 {
		t.Errorf("entry_price = %.4f, want ~804.36 VWAP", got.EntryPrice)
	}

	// Idempotency: the SAME top-up broker order arriving again (different
	// source_event_id, e.g. WSS then REST) must NOT double-count.
	ev2 := *ev
	ev2.SourceEventID = "evt-topup-B-" + posID.String()[:8] // different event id, same broker order
	applied2, err := s.MergeTopupWithEvent(ctx, posID, 17, 805.25, topupBID, &ev2)
	if err != nil {
		t.Fatalf("replay merge: %v", err)
	}
	if applied2 {
		t.Error("replay of same top-up broker order must be a no-op")
	}
	got2, _ := s.FindManthanLotBySignalID(ctx, sig.String())
	if got2.Quantity != 24 {
		t.Errorf("qty after replay = %d, want 24 (no double-count)", got2.Quantity)
	}
}
