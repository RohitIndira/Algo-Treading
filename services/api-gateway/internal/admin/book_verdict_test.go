package admin

// Classifier table tests — the comparison rules in book_verdict.go, pure.
// Every production trap the reconciler has met is a row here: the BE-series
// name form, the standing-SL freeQty=0 hide, partial pledges, the
// settlement window, today's tradebook (theirs vs ours), and no broker truth.

import (
	"strings"
	"testing"
	"time"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
)

var classifyNow = time.Date(2026, 10, 26, 11, 0, 0, 0, time.FixedZone("IST", 5*3600+30*60))

func okSnap(holdings []indiraClient.Holding, book []indiraClient.OrderBook, trades []indiraClient.TradeBook, ours map[string]bool) *BrokerSnapshot {
	return BuildSnapshot("OK", "", holdings, book, trades, ours, classifyNow)
}

func hold(disp, base string, qty, free, pledge, used, btst int) indiraClient.Holding {
	return indiraClient.Holding{
		Symbol:    []indiraClient.HoldingSymbol{{Exc: "NSE", DispSym: disp, BaseSym: base}},
		Qty:       qty,
		FreeQty:   free,
		PledgeQty: pledge,
		UsedQty:   used,
		BTST:      btst,
	}
}

func openSell(id, disp string, qty int) indiraClient.OrderBook {
	return indiraClient.OrderBook{OrdId: id, OrdAction: "SELL", Status: "Pending", Cancellable: true,
		Symbol: indiraClient.OrderBookSymbol{Exc: "NSE", DispSym: disp, BaseSym: disp}, Qty: qty, RemainQty: qty}
}

func sellTrade(id, disp string, qty int, price float64) indiraClient.TradeBook {
	return indiraClient.TradeBook{OrdId: id, OrdAction: "SELL", TradedQty: qty, TradedPrice: price,
		Symbol: map[string]any{"dispSym": disp, "baseSym": disp}}
}

func lot(sym string, qty int, ageHours float64) BookLotInput {
	return BookLotInput{UserID: "FIV99", StrategyID: "aaaaaaaa-1111-2222-3333-000000000001", Symbol: sym, Qty: qty,
		EntryTime: classifyNow.Add(-time.Duration(ageHours * float64(time.Hour))), PositionIDs: []string{"p1"}}
}

func TestClassify_Table(t *testing.T) {
	old := 24 * 7.0 // a week old — past settlement
	fresh := 20.0   // bought yesterday — inside settlement

	cases := []struct {
		name       string
		lot        BookLotInput
		snap       *BrokerSnapshot
		wantState  string
		wantReason string  // substring
		wantPrice  float64 // SOLD_TODAY
	}{
		{
			name:      "held in full and free",
			lot:       lot("KEI", 10, old),
			snap:      okSnap([]indiraClient.Holding{hold("KEI", "KEI", 10, 10, 0, 0, 0)}, nil, nil, nil),
			wantState: BrokerStateHeld, wantReason: "Held: broker holds 10 of 10",
		},
		{
			name:      "BE-series name form resolves to the book symbol",
			lot:       lot("SPECIALITY", 50, old),
			snap:      okSnap([]indiraClient.Holding{hold("SPECIALITY-BE", "SPECIALITY", 50, 50, 0, 0, 0)}, nil, nil, nil),
			wantState: BrokerStateHeld,
		},
		{
			name: "locked by our own standing SL (hidden freeQty=0 row) stays HELD",
			lot:  lot("ALIVUS", 30, old),
			// v2 holdings hides the row entirely; the open SELL order proves it.
			snap:      okSnap(nil, []indiraClient.OrderBook{openSell("SL-1", "ALIVUS", 30)}, nil, map[string]bool{"SL-1": true}),
			wantState: BrokerStateHeld, wantReason: "standing SELL",
		},
		{
			name:      "visible row with freeQty 0 under our SL stays HELD",
			lot:       lot("ALIVUS", 30, old),
			snap:      okSnap([]indiraClient.Holding{hold("ALIVUS", "ALIVUS", 30, 0, 0, 0, 0)}, []indiraClient.OrderBook{openSell("SL-1", "ALIVUS", 30)}, nil, nil),
			wantState: BrokerStateHeld,
		},
		{
			name:       "fully pledged",
			lot:        lot("SYRMA", 2850, old),
			snap:       okSnap([]indiraClient.Holding{hold("SYRMA", "SYRMA", 2850, 0, 2850, 0, 0)}, nil, nil, nil),
			wantState:  BrokerStatePledged,
			wantReason: "Pledged: 2850 of 2850 pledged, 0 free — unpledge to let the strategy manage it",
		},
		{
			name:       "pledged via usedQty (margin) with no pledgeQty field",
			lot:        lot("WOCKPHARMA", 1039, old),
			snap:       okSnap([]indiraClient.Holding{hold("WOCKPHARMA", "WOCKPHARMA", 1039, 0, 0, 1039, 0)}, nil, nil, nil),
			wantState:  BrokerStatePledged,
			wantReason: "1039 of 1039 pledged",
		},
		{
			name:       "partial pledge leaves fewer free than the lot → PLEDGED",
			lot:        lot("SYRMA", 100, old),
			snap:       okSnap([]indiraClient.Holding{hold("SYRMA", "SYRMA", 100, 40, 60, 0, 0)}, nil, nil, nil),
			wantState:  BrokerStatePledged,
			wantReason: "60 of 100 pledged, 40 free",
		},
		{
			name:      "partial pledge but still enough free for the lot → HELD",
			lot:       lot("SYRMA", 40, old),
			snap:      okSnap([]indiraClient.Holding{hold("SYRMA", "SYRMA", 100, 40, 60, 0, 0)}, nil, nil, nil),
			wantState: BrokerStateHeld,
		},
		{
			name:       "bought today, not yet free, nothing pledged → HELD (T+1)",
			lot:        lot("NEWBUY", 10, 2),
			snap:       okSnap([]indiraClient.Holding{hold("NEWBUY", "NEWBUY", 10, 0, 0, 0, 10)}, nil, nil, nil),
			wantState:  BrokerStateHeld,
			wantReason: "bought today",
		},
		{
			name:       "broker holds nothing, old lot, no sells → NOT_HELD",
			lot:        lot("AEROENTER", 25, old),
			snap:       okSnap([]indiraClient.Holding{hold("OTHER", "OTHER", 5, 5, 0, 0, 0)}, nil, nil, nil),
			wantState:  BrokerStateNotHeld,
			wantReason: "Broker holds 0 of AEROENTER",
		},
		{
			name:       "broker holds nothing but inside settlement window → UNVERIFIED",
			lot:        lot("FRESH", 25, fresh),
			snap:       okSnap(nil, nil, nil, nil),
			wantState:  BrokerStateUnverified,
			wantReason: "settlement window",
		},
		{
			name: "sold today by hand (not our order id) → SOLD_TODAY at VWAP",
			lot:  lot("SHREEPUSHK", 20, old),
			snap: okSnap(nil, nil, []indiraClient.TradeBook{
				sellTrade("MAN-1", "SHREEPUSHK", 10, 100), sellTrade("MAN-2", "SHREEPUSHK", 10, 110),
			}, map[string]bool{"OURS-9": true}),
			wantState: BrokerStateSoldToday, wantPrice: 105, wantReason: "Sold today outside the system",
		},
		{
			name:      "sold today by hand, BE-series trade name, fresh lot → still SOLD_TODAY",
			lot:       lot("MODISONLTD", 51, fresh),
			snap:      okSnap(nil, nil, []indiraClient.TradeBook{sellTrade("MAN-1", "MODISONLTD-BE", 51, 90)}, nil),
			wantState: BrokerStateSoldToday, wantPrice: 90,
		},
		{
			name:       "sold today by OUR order → UNVERIFIED (book will record the fill)",
			lot:        lot("KEI", 10, old),
			snap:       okSnap(nil, nil, []indiraClient.TradeBook{sellTrade("OURS-1", "KEI", 10, 100)}, map[string]bool{"OURS-1": true}),
			wantState:  BrokerStateUnverified,
			wantReason: "system's own order OURS-1",
		},
		{
			name:       "partial manual sale today leaves qty → QTY_MISMATCH with tradebook note",
			lot:        lot("IDEA", 100, old),
			snap:       okSnap([]indiraClient.Holding{hold("IDEA", "IDEA", 60, 60, 0, 0, 0)}, nil, []indiraClient.TradeBook{sellTrade("MAN-1", "IDEA", 40, 7.5)}, nil),
			wantState:  BrokerStateQtyMismatch,
			wantReason: "book holds 100, broker holds 60",
		},
		{
			name:      "broker holds more than the book (manual top-up) → HELD",
			lot:       lot("KEI", 10, old),
			snap:      okSnap([]indiraClient.Holding{hold("KEI", "KEI", 25, 25, 0, 0, 0)}, nil, nil, nil),
			wantState: BrokerStateHeld,
		},
		{
			name:       "no broker truth → UNVERIFIED naming the leg",
			lot:        lot("KEI", 10, old),
			snap:       BuildSnapshot("AUTH_EXPIRED", "AU004", nil, nil, nil, nil, classifyNow),
			wantState:  BrokerStateUnverified,
			wantReason: "broker session AUTH_EXPIRED",
		},
		{
			name:       "nil snapshot → UNVERIFIED",
			lot:        lot("KEI", 10, old),
			snap:       nil,
			wantState:  BrokerStateUnverified,
			wantReason: "no broker truth",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := Classify(tc.lot, tc.snap, classifyNow)
			if v.State != tc.wantState {
				t.Fatalf("state=%s want %s (reason: %s)", v.State, tc.wantState, v.Reason)
			}
			if tc.wantReason != "" && !strings.Contains(v.Reason, tc.wantReason) {
				t.Fatalf("reason %q lacks %q", v.Reason, tc.wantReason)
			}
			if tc.wantPrice != 0 && v.ExitPrice != tc.wantPrice {
				t.Fatalf("exit price %v want %v", v.ExitPrice, tc.wantPrice)
			}
			if v.Symbol != strings.ToUpper(tc.lot.Symbol) || v.UserID != tc.lot.UserID || v.BookQty != tc.lot.Qty {
				t.Fatalf("identity not carried: %+v", v)
			}
		})
	}
}

// A lot that was PLEDGED and is now free again reads HELD with the
// "Unpledged – protection resumed" lead and is Actionable (the reverse path).
func TestClassify_UnpledgedTransition(t *testing.T) {
	l := lot("SYRMA", 100, 24*7)
	l.PrevState = BrokerStatePledged
	v := Classify(l, okSnap([]indiraClient.Holding{hold("SYRMA", "SYRMA", 100, 100, 0, 0, 0)}, nil, nil, nil), classifyNow)
	if v.State != BrokerStateHeld || !strings.HasPrefix(v.Reason, ReasonUnpledged) {
		t.Fatalf("got %s %q", v.State, v.Reason)
	}
	if !v.Actionable() || !v.Changed() {
		t.Fatalf("unpledge must be actionable and a change: %+v", v)
	}
	// A plain HELD→HELD is neither.
	l.PrevState = BrokerStateHeld
	v = Classify(l, okSnap([]indiraClient.Holding{hold("SYRMA", "SYRMA", 100, 100, 0, 0, 0)}, nil, nil, nil), classifyNow)
	if v.Actionable() || v.Changed() {
		t.Fatalf("HELD→HELD must be a no-op: %+v", v)
	}
}

// Evidence carries the facts the reason rests on — the admin console and the
// client app render both.
func TestClassify_EvidenceFields(t *testing.T) {
	snap := okSnap(
		[]indiraClient.Holding{hold("SYRMA-BE", "SYRMA", 100, 20, 80, 0, 0)},
		[]indiraClient.OrderBook{openSell("SL-7", "SYRMA", 5)},
		[]indiraClient.TradeBook{sellTrade("MAN-1", "SYRMA", 3, 500), sellTrade("OURS-2", "SYRMA", 2, 501)},
		map[string]bool{"OURS-2": true})
	v := Classify(lot("SYRMA", 100, 24*7), snap, classifyNow)
	e := v.Evidence
	if e.BrokerQty != 105 || e.FreeQty != 25 || e.PledgedQty != 80 || e.OpenSellQty != 5 || e.OpenSellOrderID != "SL-7" {
		t.Fatalf("qty evidence wrong: %+v", e)
	}
	if e.TradebookSellQty != 3 || e.TradebookSellPrice != 500 || e.TradebookOurSell != 2 || e.TradebookOurOrder != "OURS-2" {
		t.Fatalf("tradebook evidence wrong: %+v", e)
	}
	if e.HoldingSeenAs != "SYRMA-BE" || e.BrokerLeg != "OK" || e.WithinSettlement {
		t.Fatalf("misc evidence wrong: %+v", e)
	}
	if v.State != BrokerStatePledged {
		t.Fatalf("state %s", v.State)
	}
}

// Snapshot keying: every name form of a holding, an order and a trade
// resolves; the summary and the worst-first sort behave.
func TestBuildSnapshot_NamesAndSummary(t *testing.T) {
	snap := okSnap([]indiraClient.Holding{hold("ABC-BZ", "ABC", 7, 7, 0, 0, 0)},
		[]indiraClient.OrderBook{openSell("O1", "XYZ-BE", 3)},
		[]indiraClient.TradeBook{sellTrade("T1", "PQR-SM", 1, 10)}, nil)
	for _, k := range []string{"ABC", "ABC-BZ"} {
		if snap.Total[k] != 7 {
			t.Fatalf("holding not keyed under %s: %v", k, snap.Total)
		}
	}
	for _, k := range []string{"XYZ", "XYZ-BE"} {
		if snap.OpenSellQty[k] != 3 {
			t.Fatalf("open sell not keyed under %s: %v", k, snap.OpenSellQty)
		}
	}
	for _, k := range []string{"PQR", "PQR-SM"} {
		if len(snap.Sells[k]) != 1 {
			t.Fatalf("trade not keyed under %s: %v", k, snap.Sells)
		}
	}
	vs := []Verdict{{Symbol: "B", State: BrokerStateHeld}, {Symbol: "A", State: BrokerStateHeld},
		{Symbol: "C", State: BrokerStatePledged}, {Symbol: "D", State: BrokerStateNotHeld}}
	sortVerdicts(vs)
	if vs[0].Symbol != "D" || vs[1].Symbol != "C" || vs[2].Symbol != "A" || vs[3].Symbol != "B" {
		t.Fatalf("sort order: %v", vs)
	}
	sum := SummarizeVerdicts(vs)
	if sum[BrokerStateHeld] != 2 || sum[BrokerStatePledged] != 1 || sum[BrokerStateNotHeld] != 1 {
		t.Fatalf("summary: %v", sum)
	}
}
