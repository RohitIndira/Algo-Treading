package admin

// Book-vs-broker verdicts (2026-10-26, operator rule).
//
// "Manthan has its own trade book and the client has their own trade book at
// the broker. Whenever anything happens the system must match and compare the
// two, and when it finds pledged / sold / mismatch it must put a clear reason
// everywhere." This file is the COMPARISON — a pure function from one lot and
// one broker snapshot to one verdict with a plain-language reason and the
// evidence it rests on. Fetching lives in reconcile.go, acting on a verdict in
// book_actions.go / ghost_reaper.go, so the rules here are table-testable
// without a broker or a database.
//
// States
//
//	HELD          broker holds ≥ lot qty and ≥ lot qty is free (a quantity
//	              locked under OUR OWN standing SELL order counts as free — the
//	              shares are there, merely spoken for by the strategy's stop)
//	PLEDGED       broker holds ≥ lot qty but fewer than lot qty are free and the
//	              row carries a pledge/used-for-margin quantity → kept in the
//	              book, no stops, no exits, slot released (see Pledge)
//	SOLD_TODAY    broker holds none and today's trade book shows a SELL of ≥ lot
//	              qty NOT placed by us → booked as MANUAL_EXIT at the trade price
//	NOT_HELD      broker holds none, no sell of ours or theirs today, and the lot
//	              is past the settlement window → closed NOT_HELD_AT_BROKER
//	QTY_MISMATCH  0 < broker qty < lot qty → verdict + reason only
//	UNVERIFIED    no broker truth (session expired / fetch failed), or an absent
//	              holding still inside the settlement window (a T+1 lot can be a
//	              hidden freeQty=0 row), or a sell today by the SYSTEM'S OWN
//	              order whose fill the book has not recorded yet → no action
//
// Nothing here ever places, modifies or cancels a broker order.

import (
	"fmt"
	"sort"
	"strings"
	"time"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
)

// Broker-state values persisted on positions_db.positions.broker_state.
const (
	BrokerStateHeld        = "HELD"
	BrokerStatePledged     = "PLEDGED"
	BrokerStateNotHeld     = "NOT_HELD"
	BrokerStateSoldToday   = "SOLD_TODAY"
	BrokerStateQtyMismatch = "QTY_MISMATCH"
	BrokerStateUnverified  = "UNVERIFIED"
)

// Exit reasons the reconciler writes (positions_db CHECK, migration 007).
const (
	ExitReasonNotHeldAtBroker   = "NOT_HELD_AT_BROKER"
	ExitReasonManualExit        = "MANUAL_EXIT"
	ExitReasonAdminGhostCleanup = "ADMIN_GHOST_CLEANUP"
)

// Verdict sources.
const (
	VerdictSourceLogin  = "login"
	VerdictSourceSweep  = "sweep"
	VerdictSourceManual = "manual"
)

// Plain-language texts shown in the admin console and the client app. Kept as
// constants so the two surfaces can never drift apart.
const (
	ReasonPledgedShort   = "Pledged – unpledge to let the strategy manage it"
	ReasonNotHeldClosed  = "Closed: broker no longer holds this position (sold or transferred outside the system)"
	ReasonUnpledged      = "Unpledged – protection resumed"
	ReasonSoldTodayShort = "Closed: sold today outside the system (booked as manual exit at the trade price)"
)

// BookLotInput is one Manthan lot to judge: an ACTIVE positions_db lot
// (aggregated per strategy+symbol) or an execution-ledger-only lot.
type BookLotInput struct {
	UserID      string
	StrategyID  string
	Symbol      string
	Qty         int
	EntryTime   time.Time // newest filled BUY for ledger-only lots
	PositionIDs []string  // positions_db rows; empty for ledger-only
	LedgerOnly  bool
	PrevState   string // last persisted broker_state ("" = never judged)
}

// TradeSell is one SELL execution from today's trade book.
type TradeSell struct {
	OrderID string
	Qty     int
	Price   float64
	Ours    bool // the broker order id belongs to a manthan_orders row of ours
}

// BrokerSnapshot is everything the classifier needs from ONE holdings + ONE
// orderbook + ONE tradebook read, keyed by every name form a symbol can carry
// (dispSym, baseSym, series-stripped dispSym — "SPECIALITY-BE" is SPECIALITY).
type BrokerSnapshot struct {
	LegStatus string // OK | AUTH_EXPIRED | NO_CREDENTIAL | ERROR
	LegDetail string

	Total       map[string]int    // total qty held under the name (holdings rows only)
	Free        map[string]int    // freely sellable qty
	Pledged     map[string]int    // pledged + used-for-margin qty
	BTST        map[string]int    // bought today, not yet settled
	OpenSellQty map[string]int    // remaining qty in open cancellable SELL orders (ours and theirs)
	OpenSellIDs map[string]string // first open SELL order id per name
	Sells       map[string][]TradeSell
	HoldingName map[string]string // name form → broker display symbol actually seen

	FetchedAt time.Time
}

// OK reports whether the snapshot carries broker truth.
func (s *BrokerSnapshot) OK() bool { return s != nil && s.LegStatus == "OK" }

// BrokerEvidence is the broker side of a verdict, persisted as JSON.
type BrokerEvidence struct {
	BrokerQty          int      `json:"broker_qty"`                     // holdings total + open SELL commitments
	FreeQty            int      `json:"free_qty"`                       // free + qty under our/any open SELL
	PledgedQty         int      `json:"pledged_qty"`                    // pledged + used for margin
	BTSTQty            int      `json:"btst_qty,omitempty"`             // unsettled T+1
	OpenSellQty        int      `json:"open_sell_qty,omitempty"`        // committed in open SELL orders
	OpenSellOrderID    string   `json:"open_sell_order_id,omitempty"`   // one standing SELL at the broker
	TradebookSellQty   int      `json:"tradebook_sell_qty,omitempty"`   // today's SELLs NOT placed by us
	TradebookSellPrice float64  `json:"tradebook_sell_price,omitempty"` // their VWAP
	TradebookOurSell   int      `json:"tradebook_our_sell_qty,omitempty"`
	TradebookOurOrder  string   `json:"tradebook_our_order_id,omitempty"`
	HoldingSeenAs      string   `json:"holding_seen_as,omitempty"` // broker's own symbol form
	NamesChecked       []string `json:"names_checked,omitempty"`
	WithinSettlement   bool     `json:"within_settlement_window"`
	BrokerLeg          string   `json:"broker_leg"`
}

// Verdict is the latest comparison result for one lot.
type Verdict struct {
	UserID      string         `json:"user_id"`
	StrategyID  string         `json:"strategy_id,omitempty"`
	Symbol      string         `json:"symbol"`
	PositionIDs []string       `json:"position_ids,omitempty"`
	LedgerOnly  bool           `json:"ledger_only,omitempty"`
	BookQty     int            `json:"book_qty"`
	State       string         `json:"state"`
	PrevState   string         `json:"prev_state,omitempty"`
	Reason      string         `json:"reason"`
	Evidence    BrokerEvidence `json:"evidence"`
	ExitPrice   float64        `json:"exit_price,omitempty"` // SOLD_TODAY: the trade price
	CheckedAt   time.Time      `json:"checked_at"`
	Source      string         `json:"source"`
}

// Changed reports whether this verdict differs from the last persisted one.
func (v Verdict) Changed() bool { return v.PrevState != v.State }

// Actionable reports whether the reconciler has an automated action for the
// state (NOT_HELD / SOLD_TODAY → close; PLEDGED → park; HELD after PLEDGED →
// unpark). Everything else is verdict + reason only.
func (v Verdict) Actionable() bool {
	switch v.State {
	case BrokerStateNotHeld, BrokerStateSoldToday, BrokerStatePledged:
		return true
	case BrokerStateHeld:
		return v.PrevState == BrokerStatePledged
	}
	return false
}

// symbolNames lists the name forms under which a book symbol is looked up.
func symbolNames(symbol string) []string {
	sym := strings.ToUpper(strings.TrimSpace(symbol))
	names := []string{sym}
	if i := strings.LastIndex(sym, "-"); i > 0 {
		names = append(names, sym[:i])
	}
	return names
}

// lookup sums a per-name map over every name form of the symbol WITHOUT
// double counting: the snapshot already keys each holding under all of its
// own names, so the first name that resolves is the holding.
func lookup(m map[string]int, symbol string) int {
	if m == nil {
		return 0
	}
	for _, n := range symbolNames(symbol) {
		if v, ok := m[n]; ok {
			return v
		}
	}
	return 0
}

func lookupStr(m map[string]string, symbol string) string {
	if m == nil {
		return ""
	}
	for _, n := range symbolNames(symbol) {
		if v, ok := m[n]; ok && v != "" {
			return v
		}
	}
	return ""
}

func lookupSells(m map[string][]TradeSell, symbol string) []TradeSell {
	if m == nil {
		return nil
	}
	for _, n := range symbolNames(symbol) {
		if v, ok := m[n]; ok && len(v) > 0 {
			return v
		}
	}
	return nil
}

// Classify judges one lot against one broker snapshot. Pure: no IO, no clock
// other than `now`.
func Classify(lot BookLotInput, snap *BrokerSnapshot, now time.Time) Verdict {
	sym := strings.ToUpper(strings.TrimSpace(lot.Symbol))
	v := Verdict{
		UserID: lot.UserID, StrategyID: lot.StrategyID, Symbol: sym,
		PositionIDs: lot.PositionIDs, LedgerOnly: lot.LedgerOnly,
		BookQty: lot.Qty, PrevState: lot.PrevState, CheckedAt: now,
	}
	v.Evidence.NamesChecked = symbolNames(sym)
	v.Evidence.WithinSettlement = !lot.EntryTime.IsZero() && now.Sub(lot.EntryTime) < settlementWindow

	if !snap.OK() {
		leg := "UNKNOWN"
		if snap != nil && snap.LegStatus != "" {
			leg = snap.LegStatus
		}
		v.Evidence.BrokerLeg = leg
		v.State = BrokerStateUnverified
		v.Reason = fmt.Sprintf("Unverified: no broker truth for %s (broker session %s) — nothing changed; the check reruns on the next login or sweep", sym, leg)
		return v
	}
	v.Evidence.BrokerLeg = "OK"

	held := lookup(snap.Total, sym)
	free := lookup(snap.Free, sym)
	pledged := lookup(snap.Pledged, sym)
	btst := lookup(snap.BTST, sym)
	openSell := lookup(snap.OpenSellQty, sym)
	// Qty committed to an open SELL order has freeQty=0 and the v2 holdings
	// row may be HIDDEN entirely (the intraday standing-SL trap) — the shares
	// exist and are sellable; count them as held AND free.
	total := held + openSell
	freeTotal := free + openSell

	v.Evidence.BrokerQty, v.Evidence.FreeQty, v.Evidence.PledgedQty = total, freeTotal, pledged
	v.Evidence.BTSTQty, v.Evidence.OpenSellQty = btst, openSell
	v.Evidence.OpenSellOrderID = lookupStr(snap.OpenSellIDs, sym)
	v.Evidence.HoldingSeenAs = lookupStr(snap.HoldingName, sym)

	// Today's trade book: split sells into ours (our ledger's broker order
	// ids) and theirs (placed by hand / another app).
	var theirQty, ourQty int
	var theirValue float64
	var ourOrder string
	for _, t := range lookupSells(snap.Sells, sym) {
		if t.Qty <= 0 {
			continue
		}
		if t.Ours {
			ourQty += t.Qty
			if ourOrder == "" {
				ourOrder = t.OrderID
			}
			continue
		}
		theirQty += t.Qty
		theirValue += float64(t.Qty) * t.Price
	}
	v.Evidence.TradebookSellQty, v.Evidence.TradebookOurSell, v.Evidence.TradebookOurOrder = theirQty, ourQty, ourOrder
	if theirQty > 0 {
		v.Evidence.TradebookSellPrice = round2f(theirValue / float64(theirQty))
	}

	switch {
	case total >= lot.Qty && freeTotal >= lot.Qty:
		v.State = BrokerStateHeld
		v.Reason = fmt.Sprintf("Held: broker holds %d of %d (%d free)", total, lot.Qty, freeTotal)
		if openSell > 0 {
			v.Reason += fmt.Sprintf(", %d under a standing SELL order", openSell)
		}
		if lot.PrevState == BrokerStatePledged {
			v.Reason = ReasonUnpledged + " — " + v.Reason
		}

	case total >= lot.Qty && pledged > 0:
		v.State = BrokerStatePledged
		v.Reason = fmt.Sprintf("Pledged: %d of %d pledged, %d free — unpledge to let the strategy manage it", pledged, total, freeTotal)

	case total >= lot.Qty:
		// Held in full but not yet free and NOT pledged: unsettled T+1 (BTST)
		// or a lock we cannot name. The shares are there — protection stays.
		v.State = BrokerStateHeld
		v.Reason = fmt.Sprintf("Held: broker holds %d of %d but only %d free (unsettled T+1 or locked outside the system) — protection continues", total, lot.Qty, freeTotal)
		if btst > 0 {
			v.Reason = fmt.Sprintf("Held: broker holds %d of %d, %d bought today (settles T+1) — protection continues", total, lot.Qty, btst)
		}
		if lot.PrevState == BrokerStatePledged {
			v.Reason = ReasonUnpledged + " — " + v.Reason
		}

	case total == 0 && theirQty >= lot.Qty:
		v.State = BrokerStateSoldToday
		v.ExitPrice = v.Evidence.TradebookSellPrice
		v.Reason = fmt.Sprintf("Sold today outside the system: trade book shows %d sold at ₹%.2f (not by the strategy) — booked as a manual exit at that price", theirQty, v.ExitPrice)

	case total == 0 && ourQty > 0:
		// The strategy's own exit sold it today; the fill reaches the book via
		// order.events. Never close it twice from here.
		v.State = BrokerStateUnverified
		v.Reason = fmt.Sprintf("Unverified: sold today by the system's own order %s (%d) — waiting for the book to record the fill", ourOrder, ourQty)

	case total == 0 && v.Evidence.WithinSettlement:
		v.State = BrokerStateUnverified
		v.Reason = fmt.Sprintf("Unverified: broker shows no holding of %s but the lot was bought within the settlement window (T+1 can hide a freeQty=0 row) — rechecked after settlement", sym)

	case total == 0:
		v.State = BrokerStateNotHeld
		v.Reason = fmt.Sprintf("Broker holds 0 of %s; today's trade book shows no sell — sold or transferred outside the system while no broker session was available. %s", sym, ReasonNotHeldClosed)

	default: // 0 < total < lot.Qty
		v.State = BrokerStateQtyMismatch
		v.Reason = fmt.Sprintf("Quantity mismatch: book holds %d, broker holds %d (%d free) — partial manual sale, corporate action or a split lot; left for review", lot.Qty, total, freeTotal)
		if theirQty > 0 {
			v.Reason += fmt.Sprintf("; trade book shows %d sold today by hand at ₹%.2f", theirQty, v.Evidence.TradebookSellPrice)
		}
	}
	return v
}

// ── Snapshot construction from the raw broker reads ─────────────────────

// BuildSnapshot keys one holdings / orderbook / tradebook read by every name
// form. ourOrderIDs are the broker order ids of the user's manthan_orders rows
// so a tradebook SELL can be attributed to the strategy or to the client.
func BuildSnapshot(legStatus, legDetail string, holdings []indiraClient.Holding, book []indiraClient.OrderBook,
	trades []indiraClient.TradeBook, ourOrderIDs map[string]bool, now time.Time) *BrokerSnapshot {
	s := &BrokerSnapshot{
		LegStatus: legStatus, LegDetail: legDetail, FetchedAt: now,
		Total: map[string]int{}, Free: map[string]int{}, Pledged: map[string]int{}, BTST: map[string]int{},
		OpenSellQty: map[string]int{}, OpenSellIDs: map[string]string{},
		Sells: map[string][]TradeSell{}, HoldingName: map[string]string{},
	}
	if legStatus != "OK" {
		return s
	}
	for _, h := range holdings {
		total := h.Qty
		if total == 0 {
			total = h.HoldingQty + h.UsedQty + h.BTST + h.PledgeQty
		}
		if total == 0 {
			total = h.FreeQty
		}
		if total <= 0 {
			continue
		}
		pledged := h.PledgeQty + h.UsedQty
		free := h.FreeQty
		if free == 0 && pledged == 0 && h.BTST == 0 && h.HoldingQty > 0 {
			free = h.HoldingQty // some shapes report only holdingQty
		}
		if free > total {
			free = total
		}
		names := holdingNames(h.Symbol)
		seen := ""
		for _, l := range h.Symbol {
			if l.Exc == "NSE" && l.DispSym != "" {
				seen = l.DispSym
				break
			}
		}
		if seen == "" && len(h.Symbol) > 0 {
			seen = h.Symbol[0].DispSym
		}
		for _, k := range names {
			s.Total[k] += total
			s.Free[k] += free
			s.Pledged[k] += pledged
			s.BTST[k] += h.BTST
			if s.HoldingName[k] == "" {
				s.HoldingName[k] = seen
			}
		}
	}
	for _, ob := range book {
		if !strings.EqualFold(ob.OrdAction, "SELL") || !ob.Cancellable {
			continue
		}
		qty := ob.RemainQty
		if qty == 0 {
			qty = ob.Qty - ob.TradedQty
		}
		if qty <= 0 {
			continue
		}
		for _, k := range orderNames(ob.Symbol.DispSym, ob.Symbol.BaseSym) {
			s.OpenSellQty[k] += qty
			if s.OpenSellIDs[k] == "" {
				s.OpenSellIDs[k] = ob.OrdId
			}
		}
	}
	for _, tr := range trades {
		if !strings.EqualFold(tr.OrdAction, "SELL") || tr.TradedQty <= 0 {
			continue
		}
		disp, base := tradeSymbolNames(tr)
		sell := TradeSell{OrderID: tr.OrdId, Qty: tr.TradedQty, Price: tr.TradedPrice, Ours: ourOrderIDs[tr.OrdId]}
		for _, k := range orderNames(disp, base) {
			s.Sells[k] = append(s.Sells[k], sell)
		}
	}
	return s
}

// orderNames mirrors holdingNames for the order/trade books' flat symbol
// objects: dispSym, baseSym and the series-stripped dispSym, upper-cased.
func orderNames(disp, base string) []string {
	var out []string
	seen := map[string]bool{}
	add := func(v string) {
		v = strings.ToUpper(strings.TrimSpace(v))
		if v != "" && !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	add(disp)
	add(base)
	if i := strings.LastIndex(strings.ToUpper(disp), "-"); i > 0 {
		add(disp[:i])
	}
	return out
}

// tradeSymbolNames digs dispSym/baseSym out of the tradebook's loosely typed
// symbol object.
func tradeSymbolNames(tr indiraClient.TradeBook) (disp, base string) {
	m, ok := tr.Symbol.(map[string]any)
	if !ok {
		return "", ""
	}
	if s, ok := m["dispSym"].(string); ok {
		disp = s
	}
	if s, ok := m["baseSym"].(string); ok {
		base = s
	}
	return disp, base
}

// SummarizeVerdicts counts verdicts per state, for the reconcile response and
// the fleet row.
func SummarizeVerdicts(vs []Verdict) map[string]int {
	out := map[string]int{}
	for _, v := range vs {
		out[v.State]++
	}
	return out
}

// sortVerdicts orders worst-first for the operator: NOT_HELD, SOLD_TODAY,
// PLEDGED, QTY_MISMATCH, UNVERIFIED, HELD; then by symbol.
func sortVerdicts(vs []Verdict) {
	rank := map[string]int{
		BrokerStateNotHeld: 0, BrokerStateSoldToday: 1, BrokerStatePledged: 2,
		BrokerStateQtyMismatch: 3, BrokerStateUnverified: 4, BrokerStateHeld: 5,
	}
	sort.SliceStable(vs, func(i, j int) bool {
		if rank[vs[i].State] != rank[vs[j].State] {
			return rank[vs[i].State] < rank[vs[j].State]
		}
		return vs[i].Symbol < vs[j].Symbol
	})
}
