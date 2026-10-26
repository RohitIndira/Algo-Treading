package manthan

// PLEDGED park tag (2026-10-26) — mirrors the GHOST_SUSPECT exclusions:
//
//	TestPledgedPark_ExcludedFromProtectionList — a position whose stop row is
//	    parked SL_DEFERRED_BAND/PLEDGED (or a PLEDGED marker row) is NOT listed
//	    by ListPositionsNeedingProtection (09:14 planner, EOD Phase A) and NOT
//	    listed as naked by ListNakedOpenBuyPositions; GHOST_SUSPECT behaves the
//	    same; a plain DEFERRED_BAND park stays eligible for the planner.
//	TestPledgedPark_TagAndRefusal — ParkedStopTag resolves the tag (newest
//	    wins), HasGhostSuspectStop stays tag-specific, and the EmergencySell /
//	    trail refusal sentence is the pledged one (POISON for the inbox).
//
// Runs against local execution_db; skips if unreachable.

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestPledgedPark_ExcludedFromProtectionList(t *testing.T) {
	db := openExecTestDB(t)
	defer nkClean(t, db)
	r := NewRepository(db)
	ctx := context.Background()

	// P: FILLED BUY + stop row parked PLEDGED → must NOT need protection.
	seedOrder(t, db, "sig-P", "SYMP", "BUY", "LIMIT_BUY", "FILLED", 10, "NBP")
	pID := seedOrder(t, db, "sl-sig-P", "SYMP", "SELL", "SL_SELL", "SL_PLACED", 0, "")
	if err := r.ParkStopRow(ctx, pID, 0, ParkTagPledged, "PLEDGED at broker: test"); err != nil {
		t.Fatalf("park: %v", err)
	}
	// M: FILLED BUY + a PLEDGED marker row (no real stop ever existed) → not needed.
	seedOrder(t, db, "sig-M", "SYMM", "BUY", "LIMIT_BUY", "FILLED", 10, "NBM")
	seedOrder(t, db, "pledged-S4450-mark-SYMM", "SYMM", "SELL", "SL_SELL", "SL_DEFERRED_BAND", 10, "") // marker covers the whole lot
	if _, err := db.Exec(`UPDATE manthan_orders SET broker_status='PLEDGED' WHERE signal_id='pledged-S4450-mark-SYMM'`); err != nil {
		t.Fatalf("tag marker: %v", err)
	}
	// G: GHOST_SUSPECT park → not needed (unchanged behaviour).
	seedOrder(t, db, "sig-G", "SYMG", "BUY", "LIMIT_BUY", "FILLED", 10, "NBG")
	gID := seedOrder(t, db, "sl-sig-G", "SYMG", "SELL", "SL_SELL", "REJECTED", 0, "")
	if err := r.ParkStopRow(ctx, gID, 0, ParkTagGhostSuspect, "GHOST_SUSPECT: test"); err != nil {
		t.Fatalf("park ghost: %v", err)
	}
	// D: plain band deferral → still a protection candidate for the planner.
	seedOrder(t, db, "sig-D", "SYMD", "BUY", "LIMIT_BUY", "FILLED", 10, "NBD")
	seedOrder(t, db, "sl-sig-D", "SYMD", "SELL", "SL_SELL", "SL_DEFERRED_BAND", 0, "")
	// N: naked → needs protection.
	seedOrder(t, db, "sig-N", "SYMN", "BUY", "LIMIT_BUY", "FILLED", 10, "NBN")

	need, err := r.ListPositionsNeedingProtection(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	got := map[string]bool{}
	for _, p := range need {
		if p.StrategyID == nkStrategy {
			got[p.Symbol] = true
		}
	}
	for _, excluded := range []string{"SYMP", "SYMM", "SYMG"} {
		if got[excluded] {
			t.Errorf("%s is parked hands-off and must not be listed for protection", excluded)
		}
	}
	for _, wanted := range []string{"SYMD", "SYMN"} {
		if !got[wanted] {
			t.Errorf("%s must still be listed for protection", wanted)
		}
	}

	// The naked scan reads every park (SL_DEFERRED_BAND) as cover — a pledged
	// lot must not trigger a fresh stop placement.
	naked, err := r.ListNakedOpenBuyPositions(ctx)
	if err != nil {
		t.Fatalf("naked: %v", err)
	}
	for _, n := range naked {
		if n.StrategyID == nkStrategy && (n.Symbol == "SYMP" || n.Symbol == "SYMM" || n.Symbol == "SYMG") {
			t.Errorf("%s read as naked despite its park", n.Symbol)
		}
	}
}

func TestPledgedPark_TagAndRefusal(t *testing.T) {
	db := openExecTestDB(t)
	defer nkClean(t, db)
	r := NewRepository(db)
	ctx := context.Background()

	seedOrder(t, db, "sig-T", "SYMT", "BUY", "LIMIT_BUY", "FILLED", 10, "NBT")
	if tag, err := r.ParkedStopTag(ctx, nkStrategy, "SYMT"); err != nil || tag != "" {
		t.Fatalf("no park yet: tag=%q err=%v", tag, err)
	}
	id := seedOrder(t, db, "sl-sig-T", "SYMT", "SELL", "SL_SELL", "SL_PLACED", 0, "")
	if err := r.ParkStopRow(ctx, id, 0, ParkTagPledged, "PLEDGED at broker: test"); err != nil {
		t.Fatalf("park: %v", err)
	}
	tag, err := r.ParkedStopTag(ctx, nkStrategy, "SYMT")
	if err != nil || tag != ParkTagPledged {
		t.Fatalf("tag=%q err=%v", tag, err)
	}
	if ghost, _ := r.HasGhostSuspectStop(ctx, nkStrategy, "SYMT"); ghost {
		t.Fatal("a PLEDGED park is not a ghost suspect")
	}
	// An ordinary band deferral carries no hands-off tag.
	if _, err := db.Exec(`UPDATE manthan_orders SET broker_status='DEFERRED_BAND' WHERE id=$1`, id); err != nil {
		t.Fatalf("retag: %v", err)
	}
	if tag, _ := r.ParkedStopTag(ctx, nkStrategy, "SYMT"); tag != "" {
		t.Fatalf("DEFERRED_BAND must not read as hands-off: %q", tag)
	}

	// The refusal the trail and EmergencySell emit, as the inbox sees it.
	msg := parkedStopRefusal(ParkTagPledged, "SYMT")
	if !strings.Contains(msg, "pledged at the broker") || !strings.Contains(msg, "unpledge") {
		t.Fatalf("pledged refusal text: %q", msg)
	}
	perr := poisonErr("pre-check: %s", msg)
	var pe poisonError
	if !errors.As(perr, &pe) || !strings.Contains(pe.Error(), "pledged at the broker") {
		t.Fatalf("pledged refusal must be POISON for the inbox: %v", perr)
	}
	if parkedStopRefusal("DEFERRED_BAND", "SYMT") != "" {
		t.Fatal("unknown tags carry no refusal")
	}
	if !strings.Contains(parkedStopRefusal(ParkTagGhostSuspect, "SYMT"), "ghost suspect") {
		t.Fatal("ghost refusal text lost")
	}
}
