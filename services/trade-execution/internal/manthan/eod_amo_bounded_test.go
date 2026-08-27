package manthan

// Regressions for the 2026-08-26 evening AMO churn: ghost positions (exits
// that never wrote a SELL row) were re-armed with an identical doomed AMO
// every 5 minutes all evening — placed, Rejected by the broker, synced to
// CANCELLED by the reconciler, re-placed by the next retry tick. Two bounds
// now hold:
//   1. amoWindowOpen — no AMO placement at all between 08:45 and 16:00 IST
//      (the broker rejects them deterministically pre-window);
//   2. CountAMOAttemptsToday — after maxAMOAttempts (5) rows for one
//      (entry, trade_date) inside the arming window, the armer gives up
//      loudly instead of placing attempt N+1.

import (
	"context"
	"testing"
	"time"
)

func TestAMOWindowOpen(t *testing.T) {
	cases := []struct {
		name string
		now  time.Time
		want bool
	}{
		{"09:25 market hours → closed", time.Date(2026, 8, 26, 9, 25, 0, 0, istTZ), false},
		{"15:30 broker cancel sweep → closed", time.Date(2026, 8, 26, 15, 30, 0, 0, istTZ), false},
		{"15:59 just before window → closed", time.Date(2026, 8, 26, 15, 59, 0, 0, istTZ), false},
		{"16:00 window opens → open", time.Date(2026, 8, 26, 16, 0, 0, 0, istTZ), true},
		{"16:35 main EOD pass → open", time.Date(2026, 8, 26, 16, 35, 0, 0, istTZ), true},
		{"00:03 overnight arm-retry → open", time.Date(2026, 8, 27, 0, 3, 0, 0, istTZ), true},
		{"08:44 pre-open tail → open", time.Date(2026, 8, 27, 8, 44, 0, 0, istTZ), true},
		{"08:45 pre-open cutoff → closed", time.Date(2026, 8, 27, 8, 45, 0, 0, istTZ), false},
	}
	for _, c := range cases {
		if got := amoWindowOpen(c.now); got != c.want {
			t.Errorf("%s: got %v want %v", c.name, got, c.want)
		}
	}
}

func TestCountAMOAttemptsToday_CapsGhostPositionChurn(t *testing.T) {
	db := openExecTestDB(t)
	defer db.Close()
	repo := &Repository{db: db}
	ctx := context.Background()

	// Entry order for a "ghost" position (its exit never wrote a SELL row).
	entryID := seedOrder(t, db, "bounded-entry-1", "GHOSTSYM", "BUY", "ENTRY", "FILLED", 100, "NKBOUND01")
	tradeDate := time.Now().In(istTZ).AddDate(0, 0, 1)

	// Simulate the churn: N AMO attempts tonight, each later CANCELLED by
	// the reconciler after the broker rejected it. created_at is pinned
	// explicitly INSIDE the arming window (windowStart+1h) — seeding with
	// NOW() made this test time-of-day dependent: run before 15:29 IST, the
	// rows fell outside armedWindowStart(tradeDate) and counted as zero.
	inWindow := armedWindowStart(tradeDate).Add(1 * time.Hour)
	for i := 0; i < 5; i++ {
		var id int64
		err := db.QueryRow(`
			INSERT INTO manthan_orders
				(signal_id, strategy_id, user_id, symbol, order_type, order_side,
				 qty, filled_qty, status, parent_order_id, trade_date, trigger_price, created_at)
			VALUES ($1,$2,'S4450','GHOSTSYM','SL_SELL_AMO','SELL',100,0,'CANCELLED',$3,$4,468.80,$5)
			RETURNING id`,
			// distinct signal ids — the table has UNIQUE(signal_id)
			"bounded-amo-"+string(rune('a'+i)), nkStrategy, entryID, tradeDate, inWindow).Scan(&id)
		if err != nil {
			t.Fatalf("seed AMO attempt %d: %v", i, err)
		}
	}

	n, err := repo.CountAMOAttemptsToday(ctx, entryID, tradeDate)
	if err != nil {
		t.Fatalf("CountAMOAttemptsToday: %v", err)
	}
	if n != 5 {
		t.Fatalf("attempts = %d, want 5 — the give-up cap would misfire", n)
	}

	// A different entry order is unaffected by GHOSTSYM's burned attempts.
	otherEntry := seedOrder(t, db, "bounded-entry-2", "HEALTHY", "BUY", "ENTRY", "FILLED", 10, "NKBOUND02")
	n, err = repo.CountAMOAttemptsToday(ctx, otherEntry, tradeDate)
	if err != nil {
		t.Fatalf("CountAMOAttemptsToday(other): %v", err)
	}
	if n != 0 {
		t.Fatalf("other entry attempts = %d, want 0", n)
	}
}
