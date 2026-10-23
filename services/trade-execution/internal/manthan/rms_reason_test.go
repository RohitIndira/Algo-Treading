package manthan

import (
	"errors"
	"testing"
)

// Texts copied verbatim from the broker's order book on 2026-10-23.
const (
	rmsFilatex = "Limit exceeded Set= 239, N.Used= 239 ,Prev= 239, T.Free Qty= 0,today free qty=0  across all securities of all exchange combined (expiry/settlement)  of  s4450"
	rmsIOLCP   = "Limit exceeded Set= 0, N.Used= 121 ,Prev= 0, T.Free Qty= 0,today free qty=0  across all securities of all exchange combined (expiry/settlement)  of  s4450"
	rmsLower   = "broker rms: quantity locked by another sell order (limit exceeded set= 18, n.used= 18 ,prev= 18, t.free qty= 0,today free qty=0 ...)"
)

func TestParseRMSLimit(t *testing.T) {
	cases := []struct {
		in             string
		ok             bool
		set, used, prv int
	}{
		{rmsFilatex, true, 239, 239, 239},
		{rmsIOLCP, true, 0, 121, 0},
		{rmsLower, true, 18, 18, 18},
		{"Market order not allowed for Algo order", false, 0, 0, 0},
		{"Order entered has invalid data.", false, 0, 0, 0},
		{"", false, 0, 0, 0},
	}
	for _, c := range cases {
		l, ok := parseRMSLimit(c.in)
		if ok != c.ok {
			t.Fatalf("%q: ok=%v want %v", c.in, ok, c.ok)
		}
		if ok && (l.Set != c.set || l.Used != c.used || l.Prev != c.prv) {
			t.Fatalf("%q: got %+v want set=%d used=%d prev=%d", c.in, l, c.set, c.used, c.prv)
		}
	}
}

func TestRMSSaysNoHolding(t *testing.T) {
	if !rmsSaysNoHolding(rmsIOLCP) {
		t.Fatal("Set= 0 must read as 'broker holds nothing'")
	}
	if rmsSaysNoHolding(rmsFilatex) {
		t.Fatal("Set= 239 is a held position whose qty is committed — not a ghost")
	}
	if rmsSaysNoHolding("Order entered has invalid data.") {
		t.Fatal("a non-RMS text must never be read as a ghost")
	}
	if !isRMSQtyRejection(rmsFilatex) || !isRMSQtyRejection("Free Qty=0") || isRMSQtyRejection("invalid data") {
		t.Fatal("isRMSQtyRejection classification wrong")
	}
}

func TestLooksExecutedCancelError(t *testing.T) {
	if !looksExecutedCancelError(errors.New("broker rejected order [EG003]: FULLY_EXECUTED order not allowed to CANCEL. GatewayOrderNo[102300020]")) {
		t.Fatal("FULLY_EXECUTED cancel refusal must be recognised")
	}
	if looksExecutedCancelError(errors.New("order not found")) || looksExecutedCancelError(nil) {
		t.Fatal("other errors must not read as executed")
	}
}

func TestClassifyLostStop(t *testing.T) {
	cases := []struct {
		name          string
		net           int
		hasEntry      bool
		netKnown      bool
		reason        string
		replacedToday int
		want          lostStopAction
	}{
		{"live position, stop cancelled by a person → re-protect", 239, true, true, "", 0, lostStopReprotect},
		{"live position, rejected for a committed qty (stop filled / exit resting) → re-protect path decides", 239, true, true, rmsFilatex, 0, lostStopReprotect},
		{"ledger says fully sold → orphan row, no action", 0, true, true, "", 0, lostStopOrphan},
		{"ledger negative (over-sold) → orphan", -7, true, true, "", 0, lostStopOrphan},
		{"no entry row at all → orphan", 0, false, true, "", 0, lostStopOrphan},
		{"ledger holds 121 but broker says Set=0 → ghost", 121, true, true, rmsIOLCP, 0, lostStopGhost},
		{"ghost wins over budget", 121, true, true, rmsIOLCP, 9, lostStopGhost},
		{"orphan wins over ghost text", 0, true, true, rmsIOLCP, 0, lostStopOrphan},
		{"budget exhausted → park once", 121, true, true, "", reprotectBudgetPerDay, lostStopBudget},
		{"below budget → re-protect", 121, true, true, "", reprotectBudgetPerDay - 1, lostStopReprotect},
		{"ledger query failed → never blocks protection", 0, false, false, "", 0, lostStopReprotect},
		{"ledger failed but broker says Set=0 → still ghost", 0, false, false, rmsIOLCP, 0, lostStopGhost},
	}
	for _, c := range cases {
		if got := classifyLostStop(c.net, c.hasEntry, c.netKnown, c.reason, c.replacedToday); got != c.want {
			t.Errorf("%s: got %v want %v", c.name, got, c.want)
		}
	}
}
