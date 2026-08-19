package manthan

// AMO conversion sync (2026-08-19): Indira re-ids a queued AMO when it
// releases it at 08:50 IST. Six S4450 stops were REJECTED at conversion
// ("Order entered has invalid data."); the DB still said SL_PLACED under the
// old ids, so the 09:14 cron skipped them as protected. The reconciler must
// match converted AMOs by content and sync the outcome.

import (
	"testing"
	"time"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
)

func bookRow(id, base, action, otype string, qty int, trig float64, status, when string) indiraClient.OrderBook {
	return indiraClient.OrderBook{OrdId: id, OrdAction: action, OrdType: otype, Qty: qty, TriggerPrice: trig,
		Status: status, OrdDate: when, Symbol: indiraClient.OrderBookSymbol{BaseSym: base, DispSym: base + "-EQ"}}
}

func TestMatchConvertedAMO(t *testing.T) {
	ist := time.FixedZone("IST", 5*3600+1800)
	today := time.Now().In(ist).Format("2006-01-02") + " 08:50:00"
	yday := time.Now().In(ist).Add(-24*time.Hour).Format("2006-01-02") + " 16:35:00"
	db := &ManthanOrder{Symbol: "MPSLTD", OrderType: OrderTypeSLSellAMO, Status: StatusSLPlaced,
		Qty: 7, TriggerPrice: 2327.30, BrokerOrderID: "NYMZX002BAB8"}
	book := []indiraClient.OrderBook{
		bookRow("NYMZX00272C8", "MODISONLTD", "BUY", "Limit", 51, 0, "Executed", today), // other symbol / BUY
		bookRow("NYMZX00271C8", "MPSLTD", "SELL", "SL", 7, 2327.3, "Rejected", today),   // the converted AMO
		bookRow("NYMZX0OLD001", "MPSLTD", "SELL", "SL", 7, 2327.3, "Cancelled", yday),   // yesterday's — not today
		bookRow("NYMZX0OTHER1", "MPSLTD", "SELL", "SL", 7, 2300.0, "Pending", today),    // different trigger
	}
	got := matchConvertedAMO(db, book, map[string]bool{})
	if got == nil || got.OrdId != "NYMZX00271C8" {
		t.Fatalf("expected the converted (rejected) order, got %+v", got)
	}
	// Claimed ids are never matched.
	if got := matchConvertedAMO(db, book, map[string]bool{"NYMZX00271C8": true}); got != nil {
		t.Fatalf("claimed id must not match, got %+v", got)
	}
	// Ambiguity (two candidates) → nil, never a guess.
	book2 := append(book, bookRow("NYMZX00271C9", "MPSLTD", "SELL", "SL", 7, 2327.3, "Pending", today))
	if got := matchConvertedAMO(db, book2, map[string]bool{}); got != nil {
		t.Fatalf("ambiguous candidates must return nil, got %+v", got)
	}
	// Alternate date format from the broker.
	book3 := []indiraClient.OrderBook{bookRow("X1", "MPSLTD", "SELL", "Stop-loss", 7, 2327.3, "Pending",
		time.Now().In(ist).Format("02-Jan-2006")+" 08:50:00")}
	if got := matchConvertedAMO(db, book3, map[string]bool{}); got == nil {
		t.Fatal("dd-MMM-yyyy order date must parse")
	}
}
