package manthan

// FIX 4 — top-up SL lineage.
//
// A MARKET top-up adds a second BUY row (parent_order_id → the original entry).
// The protection query (ListPositionsNeedingProtection) must:
//   1. ANCHOR on the ORIGINAL entry (root, parent_order_id IS NULL), not the
//      later top-up — so Phase A trails from the real reference price/trigger.
//   2. FIND the SL across the WHOLE lineage, keyed by (strategy,symbol) — so an
//      SL placed on the original entry is not missed just because the anchor
//      or the SL sits on a different order id.
//
// The pre-FIX bug: "created_at DESC" picked the top-up as the anchor and keyed
// the SL off it, so the original's SL was missed and Phase A fell back to the
// wrong trigger (UNIPARTS got an 8% band instead of 20%).
//
// Runs against local execution_db; skips if unreachable.

import (
	"context"
	"database/sql"
	"testing"
)

// seedTopupEntry inserts a BUY entry with an explicit parent_order_id and price.
// parentID <= 0 → NULL (root entry).
func seedTopupEntry(t *testing.T, db *sql.DB, signalID, symbol string, parentID int64, price float64, brokerID string) int64 {
	t.Helper()
	var parent interface{}
	if parentID > 0 {
		parent = parentID
	}
	var id int64
	err := db.QueryRow(`
		INSERT INTO manthan_orders
			(signal_id, strategy_id, user_id, symbol, order_type, order_side,
			 qty, filled_qty, avg_fill_price, status, broker_order_id,
			 parent_order_id, filled_at)
		VALUES ($1,$2,'S4450',$3,'LIMIT_BUY','BUY',10,10,$4,'FILLED',$5,$6, now())
		RETURNING id`,
		signalID, nkStrategy, symbol, price, brokerID, parent).Scan(&id)
	if err != nil {
		t.Fatalf("seed topup entry %s: %v", symbol, err)
	}
	return id
}

// seedTopupSL inserts an SL_SELL row carrying an explicit trigger/limit price.
func seedTopupSL(t *testing.T, db *sql.DB, signalID, symbol, status string, trigger, limit float64, brokerID string) {
	t.Helper()
	_, err := db.Exec(`
		INSERT INTO manthan_orders
			(signal_id, strategy_id, user_id, symbol, order_type, order_side,
			 qty, filled_qty, avg_fill_price, status, broker_order_id,
			 trigger_price, limit_price)
		VALUES ($1,$2,'S4450',$3,'SL_SELL','SELL',10,0,0,$4,$5,$6,$7)`,
		signalID, nkStrategy, symbol, status, brokerID, trigger, limit)
	if err != nil {
		t.Fatalf("seed topup SL %s: %v", symbol, err)
	}
}

func TestTopup_ProtectionAnchorsOnOriginalEntryAndFindsSL(t *testing.T) {
	db := openExecTestDB(t)
	defer nkClean(t, db)
	r := NewRepository(db)
	ctx := context.Background()

	const sym = "UNIPARTS"
	// Original entry @ 500 (the strategy's real reference price).
	origID := seedTopupEntry(t, db, "sig-orig", sym, 0, 500.00, "TU-ORIG")
	// MARKET top-up @ 540, child of the original — inserted LATER.
	_ = seedTopupEntry(t, db, "sig-topup", sym, origID, 540.00, "TU-TOP")
	// SL placed on the ORIGINAL entry: trigger 20% below 500 = 400.
	seedTopupSL(t, db, "sl-sig-orig", sym, "SL_PLACED", 400.00, 399.00, "TU-SL")

	got, err := r.ListPositionsNeedingProtection(ctx)
	if err != nil {
		t.Fatalf("query: %v", err)
	}

	var found *PositionNeedingProtection
	for i := range got {
		if got[i].StrategyID == nkStrategy && got[i].Symbol == sym {
			found = &got[i]
			break
		}
	}
	if found == nil {
		t.Fatalf("%s not returned by protection query at all", sym)
	}

	// 1. Anchor is the ORIGINAL entry, not the top-up.
	if found.EntrySignalID != "sig-orig" {
		t.Errorf("anchor should be original entry sig-orig, got %q (top-up leaked in)", found.EntrySignalID)
	}
	if found.EntryOrderID != origID {
		t.Errorf("entry_order_id should be original %d, got %d", origID, found.EntryOrderID)
	}
	if found.EntryFillPrice != 500.00 {
		t.Errorf("entry_fill_price should be original 500.00, got %v (picked top-up price)", found.EntryFillPrice)
	}

	// 2. The SL was found across the lineage (keyed by strategy,symbol).
	if found.LatestTrigger != 400.00 {
		t.Errorf("latest SL trigger should be 400.00 (found via lineage), got %v (SL missed → naked/fallback)", found.LatestTrigger)
	}

	// 3. Net qty covers BOTH the original and the top-up (10 + 10 = 20).
	if found.NetQty != 20 {
		t.Errorf("net qty should be 20 (original + top-up), got %d", found.NetQty)
	}
}
