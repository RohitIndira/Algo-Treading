package manthan

import (
	"context"
	"testing"
)

// Regression for the 2026-08-11 PICCADIL over-commit. A partial LIMIT fill (7)
// gets its own 7-qty SL; the MARKET top-up tranche (17) is naked. The monitor
// must protect only the UNCOVERED gap (17), never the full net (24) — else it
// places 7+24=31 SL qty on a 24-share holding, the broker rejects it, and the
// 15s loop re-places forever.
func TestNaked_CoverageAware_NoOvercommit(t *testing.T) {
	db := openExecTestDB(t)
	defer nkClean(t, db)
	r := NewRepository(db)
	ctx := context.Background()

	// LIMIT partial fill 7 + its own 7-qty SL (already protected).
	seedOrder(t, db, "pic-limit", "PICTEST", "BUY", "LIMIT_BUY", "FILLED", 7, "BID-L")
	seedOrder(t, db, "sl-pic-limit", "PICTEST", "SELL", "SL_SELL", "SL_PLACED", 7, "BID-SL7")
	// MARKET top-up 17 — naked (no SL of its own). Net = 24, covered = 7.
	seedOrder(t, db, "mkt-pic", "PICTEST", "BUY", "MARKET_BUY", "FILLED", 17, "BID-M")

	naked, err := r.ListNakedOpenBuyPositions(ctx)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	var pic *PositionNeedingProtection
	for i := range naked {
		if naked[i].Symbol == "PICTEST" {
			pic = &naked[i]
		}
	}
	if pic == nil {
		t.Fatal("PICTEST should be naked (17 uncovered shares)")
	}
	if pic.NetQty != 17 {
		t.Fatalf("protect qty = %d, want 17 (net 24 − covered 7); the bug returned 24", pic.NetQty)
	}

	// Now cover the top-up too → fully protected → no longer naked.
	seedOrder(t, db, "sl-mkt-pic", "PICTEST", "SELL", "SL_SELL", "SL_PLACED", 17, "BID-SL17")
	naked2, err := r.ListNakedOpenBuyPositions(ctx)
	if err != nil {
		t.Fatalf("query2: %v", err)
	}
	for _, p := range naked2 {
		if p.Symbol == "PICTEST" {
			t.Errorf("PICTEST fully covered (7+17=24=net) but still flagged naked, protect_qty=%d", p.NetQty)
		}
	}
}
