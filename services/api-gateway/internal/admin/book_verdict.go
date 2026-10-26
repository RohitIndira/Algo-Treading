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
//	HELD          broker holds ≥ lot qty (holdings under any name form or the
//	              lot's ISIN, qty under open SELL orders, or an RMS rejection
//	              of ours today saying Set= N>0) and ≥ lot qty is free or
//	              about to be (free + open-SELL qty + BTST) — or the lot is
//	              held but locked without a pledge (reason says so).
//	PLEDGED       broker holds ≥ lot qty, the quantity not free/soon-free is
//	              covered by pledgeQty (usedQty alone is NOT pledge evidence)
//	              → kept in the book, no stops, no exits, slot released —
//	              after the same verdict on two reads ≥ 15 min apart, same day
//	SOLD_TODAY    broker holds none and today's SELLs NOT placed by us (trade
//	              book, or EXECUTED orders in the order book) total ≥ lot qty
//	              → booked MANUAL_EXIT at the trade price, after two reads
//	NOT_HELD      broker holds none, NO sell order of the symbol in today's
//	              book (ours or theirs) other than an RMS "Set= 0" rejection
//	              of ours, the lot is ≥ 2 trading days old, and the read was
//	              complete → closed NOT_HELD_AT_BROKER after two reads
//	QTY_MISMATCH  0 < broker qty < lot qty; several strategies of the user
//	              hold the symbol and the broker total < their sum; or a
//	              partial manual sale today with the holding hidden → review
//	UNVERIFIED    no (or partial) broker truth; an absent holding inside the
//	              settlement window; any SELL order of the symbol in today's
//	              order book (ours — quantity reserved today — or theirs
//	              below the lot qty); a sell today by the SYSTEM'S OWN order;
//	              trade-book / attribution outage
//
// Nothing here ever places, modifies or cancels a broker order.

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
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

// Hysteresis: a NOT_HELD / SOLD_TODAY closure or a PLEDGED park needs the
// SAME verdict on two consecutive reads at least this far apart, both read on
// the same IST trading day.
const verdictConfirmGap = 15 * time.Minute

// settlementTradingDays: a lot younger than this many trading days may be a
// hidden freeQty=0 row (T+1 settlement); an absent holding is UNVERIFIED.
const settlementTradingDays = 2

// Plain-language texts shown in the admin console and the client app. Kept as
// constants so the two surfaces can never drift apart. The "Closed: …"
// sentences are written only at closure (GhostHeal), never on an open row.
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
	ISIN        string // from manthan_orders (strategy+symbol); "" when unknown
	Qty         int
	EntryTime   time.Time // newest filled BUY for ledger-only lots
	PositionIDs []string  // positions_db rows; empty for ledger-only
	LedgerOnly  bool
	// UserSymbolQty is Σ qty of EVERY lot of this user on this symbol across
	// strategies (≥ Qty); Strategies counts them. The broker holds one pile.
	UserSymbolQty int
	Strategies    int
	// Last persisted verdict and when the broker was last read for it (zero =
	// never) — the hysteresis inputs.
	PrevState     string
	PrevCheckedAt time.Time
}

// TradeSell is one SELL execution from today's trade book, or an EXECUTED
// SELL order seen in the order book when the trade book did not list it.
type TradeSell struct {
	OrderID       string
	Qty           int
	Price         float64 // 0 when only the order book knew of it and it carried no price
	Ours          bool    // placed by the strategy (order id or shape match)
	FromOrderBook bool
}

// sellShape is one of OUR SELL rows today as (symbol names, qty): an EXECUTED
// SELL in the order book with the same shape is ours even when its broker id
// is not ours (the AMO conversion issues a new id).
type sellShape struct {
	names map[string]bool
	qty   int
}

// snapshotInputs bundles one user's broker reads and ledger facts.
type snapshotInputs struct {
	holdings []indiraClient.Holding
	book     []indiraClient.OrderBook
	trades   []indiraClient.TradeBook
	ourIDs   map[string]bool // broker/exchange order ids of our SELL rows (all time)
	ourShape []sellShape     // our SELL rows touched today, by symbol+qty
	tradesOK bool
	oursOK   bool
}

// BrokerSnapshot is everything the classifier needs from ONE holdings + ONE
// orderbook + ONE tradebook read, keyed by every name form a symbol can carry
// (dispSym, baseSym, series-stripped dispSym — "SPECIALITY-BE" is SPECIALITY)
// and by "ISIN:<isin>".
type BrokerSnapshot struct {
	LegStatus string // OK | AUTH_EXPIRED | NO_CREDENTIAL | ERROR | UNVERIFIED
	LegDetail string

	Total        map[string]int    // total qty held under the name (holdings rows only)
	Free         map[string]int    // freely sellable qty
	Pledged      map[string]int    // pledgeQty — the only pledge evidence
	Used         map[string]int    // usedQty (margin) — informational
	BTST         map[string]int    // bought today, not yet settled
	OpenSellQty  map[string]int    // remaining qty in open cancellable SELL orders (ours and theirs)
	OpenSellIDs  map[string]string // first open SELL order id per name
	OurSellToday map[string]string // a SELL of OURS in today's book that reserved quantity (any status but RMS Set= 0)
	AnySellToday map[string]string // any SELL order of the symbol in today's book (ours or theirs), non-REJECTED
	RMSHeld      map[string]int    // max Set= N>0 from RMS quantity rejections of ours today — the broker HOLDS ≥ N
	Sells        map[string][]TradeSell
	HoldingName  map[string]string // name form → broker display symbol actually seen

	// TradesOK / OursOK: the trade book and our-order-id attribution were
	// read. Without them an absent holding cannot be told apart from a
	// manual sale or a lost read — the leg is degraded for NOT_HELD/SOLD_TODAY.
	TradesOK bool
	OursOK   bool

	FetchedAt time.Time
}

// OK reports whether the snapshot carries broker truth.
func (s *BrokerSnapshot) OK() bool { return s != nil && s.LegStatus == "OK" }

// Degraded reports a partial read: holdings/orderbook fine, attribution not.
func (s *BrokerSnapshot) Degraded() bool { return s != nil && (!s.TradesOK || !s.OursOK) }

// BrokerEvidence is the broker side of a verdict, persisted as JSON.
type BrokerEvidence struct {
	BrokerQty          int      `json:"broker_qty"`                     // holdings total + open SELL commitments (+ RMS Set evidence)
	FreeQty            int      `json:"free_qty"`                       // free + qty under our/any open SELL
	PledgedQty         int      `json:"pledged_qty"`                    // pledgeQty
	UsedQty            int      `json:"used_qty,omitempty"`             // usedQty (margin) — not pledge evidence
	BTSTQty            int      `json:"btst_qty,omitempty"`             // unsettled T+1
	OpenSellQty        int      `json:"open_sell_qty,omitempty"`        // committed in open SELL orders
	OpenSellOrderID    string   `json:"open_sell_order_id,omitempty"`   // one standing SELL at the broker
	OurSellOrderToday  string   `json:"our_sell_order_today,omitempty"` // our SELL in today's order book (post-close window)
	AnySellOrderToday  string   `json:"any_sell_order_today,omitempty"` // any SELL of the symbol in today's book
	RMSHeldQty         int      `json:"rms_held_qty,omitempty"`         // Set= N from an RMS rejection of ours today
	TradebookSellQty   int      `json:"tradebook_sell_qty,omitempty"`   // today's SELLs NOT placed by us
	TradebookSellPrice float64  `json:"tradebook_sell_price,omitempty"` // their VWAP (0 = unknown)
	TradebookFromBook  bool     `json:"tradebook_from_orderbook,omitempty"`
	TradebookOurSell   int      `json:"tradebook_our_sell_qty,omitempty"`
	TradebookOurOrder  string   `json:"tradebook_our_order_id,omitempty"`
	UserSymbolQty      int      `json:"user_symbol_qty,omitempty"` // Σ across the user's strategies
	Strategies         int      `json:"strategies,omitempty"`
	ISIN               string   `json:"isin,omitempty"`
	HeldByISIN         bool     `json:"held_by_isin,omitempty"`    // found under the ISIN, not the name
	HoldingSeenAs      string   `json:"holding_seen_as,omitempty"` // broker's own symbol form
	NamesChecked       []string `json:"names_checked,omitempty"`
	TradingDaysHeld    int      `json:"trading_days_held"`
	WithinSettlement   bool     `json:"within_settlement_window"`
	Degraded           bool     `json:"degraded_read,omitempty"` // trade book / attribution unavailable
	BrokerLeg          string   `json:"broker_leg"`
}

// Verdict is the latest comparison result for one lot.
type Verdict struct {
	UserID        string         `json:"user_id"`
	StrategyID    string         `json:"strategy_id,omitempty"`
	Symbol        string         `json:"symbol"`
	PositionIDs   []string       `json:"position_ids,omitempty"`
	LedgerOnly    bool           `json:"ledger_only,omitempty"`
	BookQty       int            `json:"book_qty"`
	State         string         `json:"state"`
	PrevState     string         `json:"prev_state,omitempty"`
	PrevCheckedAt time.Time      `json:"prev_checked_at,omitempty"`
	Reason        string         `json:"reason"`
	Evidence      BrokerEvidence `json:"evidence"`
	ExitPrice     float64        `json:"exit_price,omitempty"` // SOLD_TODAY: the trade price (0 = unknown)
	CheckedAt     time.Time      `json:"checked_at"`
	Source        string         `json:"source"`
}

// Changed reports whether this verdict differs from the last persisted one.
func (v Verdict) Changed() bool { return v.PrevState != v.State }

// Confirmed reports the hysteresis condition from the PERSISTED history: the
// previous verdict was the same state, read ≥ verdictConfirmGap ago and on
// the SAME IST calendar day (yesterday's verdict never confirms today's).
// (The reaper adds a process-start bound and an in-memory fallback — see
// ghost_reaper.go.)
func (v Verdict) Confirmed() bool {
	return v.PrevState == v.State && !v.PrevCheckedAt.IsZero() &&
		v.CheckedAt.Sub(v.PrevCheckedAt) >= verdictConfirmGap &&
		sameISTDay(v.PrevCheckedAt, v.CheckedAt)
}

func sameISTDay(a, b time.Time) bool {
	ist := istLoc()
	ya, ma, da := a.In(ist).Date()
	yb, mb, db := b.In(ist).Date()
	return ya == yb && ma == mb && da == db
}

// Actionable reports whether the reconciler has an automated action for the
// state (NOT_HELD / SOLD_TODAY → close; PLEDGED → park; HELD → unpark if the
// lot is still parked somewhere). Everything else is verdict + reason only.
func (v Verdict) Actionable() bool {
	switch v.State {
	case BrokerStateNotHeld, BrokerStateSoldToday, BrokerStatePledged, BrokerStateHeld:
		return true
	}
	return false
}

// ── RMS quantity rejection (ported from trade-execution rms_reason.go) ───

// rmsLimitRe matches "Limit exceeded Set= 239, N.Used= 239 ,Prev= 239 …":
// Set = quantity the broker HOLDS for the scrip (0 → it holds nothing),
// Prev = quantity already committed by other sell orders.
var rmsLimitRe = regexp.MustCompile(`(?i)set\s*=\s*(\d+)\s*,\s*n\.?\s*used\s*=\s*(\d+)\s*,?\s*prev\s*=\s*(\d+)`)

// parseRMSSet extracts Set from an RMS quantity rejection; ok=false when the
// text is not that message.
func parseRMSSet(reason string) (int, bool) {
	m := rmsLimitRe.FindStringSubmatch(reason)
	if m == nil {
		return 0, false
	}
	set, err := strconv.Atoi(m[1])
	if err != nil {
		return 0, false
	}
	return set, true
}

// ── time / name helpers ─────────────────────────────────────────────────

// tradingDaysSince counts trading days strictly after entry's calendar day up
// to and including now's day (IST dates).
func tradingDaysSince(entry, now time.Time) int {
	ist := istLoc()
	e := entry.In(ist)
	n := now.In(ist)
	d := time.Date(e.Year(), e.Month(), e.Day(), 0, 0, 0, 0, ist).AddDate(0, 0, 1)
	end := time.Date(n.Year(), n.Month(), n.Day(), 0, 0, 0, 0, ist)
	count := 0
	for !d.After(end) && count < 60 {
		if indiraClient.IsTradingDay(d) {
			count++
		}
		d = d.AddDate(0, 0, 1)
	}
	return count
}

// pastSettlement: ≥ settlementTradingDays trading days have passed since the
// entry — a hidden unsettled (T+1) row can no longer explain an absence.
func pastSettlement(entry, now time.Time) bool {
	if entry.IsZero() {
		return false // unknown entry time: never assume settled
	}
	return tradingDaysSince(entry, now) >= settlementTradingDays
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

func isinKey(isin string) string {
	isin = strings.ToUpper(strings.TrimSpace(isin))
	if isin == "" {
		return ""
	}
	return "ISIN:" + isin
}

// lookup resolves a per-name map over every name form of the symbol WITHOUT
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
		BookQty: lot.Qty, PrevState: lot.PrevState, PrevCheckedAt: lot.PrevCheckedAt, CheckedAt: now,
	}
	v.Evidence.NamesChecked = symbolNames(sym)
	v.Evidence.ISIN = strings.ToUpper(strings.TrimSpace(lot.ISIN))
	if !lot.EntryTime.IsZero() {
		v.Evidence.TradingDaysHeld = tradingDaysSince(lot.EntryTime, now)
	}
	v.Evidence.WithinSettlement = !pastSettlement(lot.EntryTime, now)
	v.Evidence.UserSymbolQty, v.Evidence.Strategies = lot.UserSymbolQty, lot.Strategies

	if !snap.OK() {
		leg := "UNKNOWN"
		if snap != nil && snap.LegStatus != "" {
			leg = snap.LegStatus
		}
		v.Evidence.BrokerLeg = leg
		v.State = BrokerStateUnverified
		v.Reason = fmt.Sprintf("Unverified: no broker truth for %s (broker read %s) — nothing changed; the check reruns on the next login or sweep", sym, leg)
		if snap != nil && snap.LegDetail != "" {
			v.Reason += " — " + snap.LegDetail
		}
		return v
	}
	v.Evidence.BrokerLeg = "OK"
	v.Evidence.Degraded = snap.Degraded()

	// Holdings by name; when the name finds nothing, by ISIN (a listing the
	// broker shows under a name form we do not know).
	held := lookup(snap.Total, sym)
	free := lookup(snap.Free, sym)
	pledged := lookup(snap.Pledged, sym)
	used := lookup(snap.Used, sym)
	btst := lookup(snap.BTST, sym)
	if held == 0 {
		if k := isinKey(lot.ISIN); k != "" && snap.Total[k] > 0 {
			held, free, pledged, used, btst = snap.Total[k], snap.Free[k], snap.Pledged[k], snap.Used[k], snap.BTST[k]
			v.Evidence.HeldByISIN = true
			v.Evidence.HoldingSeenAs = snap.HoldingName[k]
		}
	}
	openSell := lookup(snap.OpenSellQty, sym)
	rmsHeld := lookup(snap.RMSHeld, sym)
	// Qty committed to an open SELL order has freeQty=0 and the v2 holdings
	// row may be HIDDEN entirely (the intraday standing-SL trap) — the shares
	// exist and are sellable; count them as held AND free.
	total := held + openSell
	freeTotal := free + openSell
	// An RMS rejection of ours today saying "Set= N" is the broker's own
	// statement that it holds N — positive evidence even with the row hidden.
	if rmsHeld > total {
		total = rmsHeld
	}

	v.Evidence.BrokerQty, v.Evidence.FreeQty, v.Evidence.PledgedQty, v.Evidence.UsedQty = total, freeTotal, pledged, used
	v.Evidence.BTSTQty, v.Evidence.OpenSellQty, v.Evidence.RMSHeldQty = btst, openSell, rmsHeld
	v.Evidence.OpenSellOrderID = lookupStr(snap.OpenSellIDs, sym)
	v.Evidence.OurSellOrderToday = lookupStr(snap.OurSellToday, sym)
	v.Evidence.AnySellOrderToday = lookupStr(snap.AnySellToday, sym)
	if v.Evidence.HoldingSeenAs == "" {
		v.Evidence.HoldingSeenAs = lookupStr(snap.HoldingName, sym)
	}

	// Today's sells: ours (our ledger's broker order ids / shapes) vs theirs
	// (placed by hand / another app).
	var theirQty, ourQty int
	var theirValue float64
	var pricedQty int
	var ourOrder string
	fromBook := false
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
		if t.Price > 0 {
			theirValue += float64(t.Qty) * t.Price
			pricedQty += t.Qty
		}
		if t.FromOrderBook {
			fromBook = true
		}
	}
	v.Evidence.TradebookSellQty, v.Evidence.TradebookOurSell, v.Evidence.TradebookOurOrder = theirQty, ourQty, ourOrder
	v.Evidence.TradebookFromBook = fromBook
	if pricedQty > 0 {
		v.Evidence.TradebookSellPrice = round2f(theirValue / float64(pricedQty))
	}

	needQty := lot.Qty
	sumQty := lot.UserSymbolQty
	if sumQty < lot.Qty {
		sumQty = lot.Qty
	}

	switch {
	case total == 0 && !snap.OursOK:
		// Without our own order ids a sell cannot be told from the strategy's
		// own exit — neither SOLD_TODAY nor NOT_HELD may be concluded.
		v.State = BrokerStateUnverified
		v.Reason = fmt.Sprintf("Unverified: broker shows no holding of %s but our order-id attribution could not be read — a manual sale cannot be told from the system's own exit; rechecked next run", sym)

	case total == 0 && theirQty >= needQty:
		// Attribution is known; the sells may come from the trade book OR
		// from EXECUTED orders in the order book (a trade-book outage is not
		// blinding).
		v.State = BrokerStateSoldToday
		v.ExitPrice = v.Evidence.TradebookSellPrice
		if v.ExitPrice > 0 {
			v.Reason = fmt.Sprintf("Sold today outside the system: %d sold at ₹%.2f (not by the strategy) — to be booked as a manual exit at that price once confirmed on a second read", theirQty, v.ExitPrice)
		} else {
			v.Reason = fmt.Sprintf("Sold today outside the system: order book shows %d sold (not by the strategy), price not reported — to be booked as a manual exit, price unknown, once confirmed on a second read", theirQty)
		}

	case total == 0 && theirQty > 0:
		// A PARTIAL manual sale with the rest hidden: never a whole-lot
		// closure; a human looks.
		v.State = BrokerStateQtyMismatch
		v.Reason = fmt.Sprintf("Quantity mismatch: today's books show %d of %d sold by hand and the broker shows no remaining holding of %s — partial manual sale with the rest unsettled or hidden; left for review", theirQty, needQty, sym)

	case total == 0 && snap.Degraded():
		v.State = BrokerStateUnverified
		v.Reason = fmt.Sprintf("Unverified: broker shows no holding of %s but today's trade book could not be read — a manual sale cannot be told from a lost read; rechecked next run", sym)

	case total == 0 && ourQty > 0:
		// The strategy's own exit sold it today; the fill reaches the book via
		// order.events. Never close it twice from here.
		v.State = BrokerStateUnverified
		v.Reason = fmt.Sprintf("Unverified: sold today by the system's own order %s (%d) — waiting for the book to record the fill", ourOrder, ourQty)

	case total == 0 && v.Evidence.OurSellOrderToday != "":
		// Post-close window: a SELL of ours stood in today's order book (a
		// swept/cancelled/expired stop, an executed exit, an RMS "Set= N,
		// Prev= N" lock rejection) → the quantity was reserved or locked today
		// and the holdings row can read empty. Not a ghost.
		v.State = BrokerStateUnverified
		v.Reason = fmt.Sprintf("Unverified: broker shows no holding of %s but our own SELL order %s stood in today's order book (quantity reserved or locked today) — rechecked on the next session", sym, v.Evidence.OurSellOrderToday)

	case total == 0 && v.Evidence.AnySellOrderToday != "":
		// A SELL order of the symbol by someone else today (not filled for
		// the whole lot): the holding is in flux — review, never a closure.
		v.State = BrokerStateUnverified
		v.Reason = fmt.Sprintf("Unverified: broker shows no holding of %s but a SELL order %s of the symbol stands in today's order book — the holding is in flux; rechecked next run", sym, v.Evidence.AnySellOrderToday)

	case total == 0 && v.Evidence.WithinSettlement:
		v.State = BrokerStateUnverified
		v.Reason = fmt.Sprintf("Unverified: broker shows no holding of %s but the lot is only %d trading day(s) old (T+1 settlement can hide a freeQty=0 row) — rechecked after settlement", sym, v.Evidence.TradingDaysHeld)

	case total == 0:
		v.State = BrokerStateNotHeld
		v.Reason = fmt.Sprintf("Broker holds 0 of %s (by name and ISIN); today's trade and order books show no sell of it — sold or transferred outside the system while no broker session was available. Pending automated closure once confirmed on a second read (≥15 min apart, same day)", sym)

	case lot.Strategies > 1 && total < sumQty:
		v.State = BrokerStateQtyMismatch
		v.Reason = fmt.Sprintf("Quantity mismatch: %d strategies of this client hold %d of %s between them, the broker holds %d (%d free) — the strategies cannot all be fully held; left for review", lot.Strategies, sumQty, sym, total, freeTotal)

	case total >= needQty && freeTotal+btst >= needQty:
		v.State = BrokerStateHeld
		v.Reason = fmt.Sprintf("Held: broker holds %d of %d (%d free)", total, needQty, freeTotal)
		if btst > 0 {
			v.Reason += fmt.Sprintf(", %d bought today (settles T+1)", btst)
		}
		if openSell > 0 {
			v.Reason += fmt.Sprintf(", %d under a standing SELL order", openSell)
		}
		if v.Evidence.HeldByISIN {
			v.Reason += fmt.Sprintf(" — found under the broker's listing %s by ISIN", v.Evidence.HoldingSeenAs)
		}
		if lot.PrevState == BrokerStatePledged {
			v.Reason = ReasonUnpledged + " — " + v.Reason
		}

	case total >= needQty && pledged > 0 && pledged >= needQty-(freeTotal+btst):
		// The quantity that is not free/soon-free is explained by the pledge.
		v.State = BrokerStatePledged
		v.Reason = fmt.Sprintf("Pledged: %d of %d pledged, %d free — unpledge to let the strategy manage it", pledged, total, freeTotal)
		if pledged < total {
			v.Reason += fmt.Sprintf(" (%d of %d pledged; the whole lot is parked — the strategy manages whole lots)", pledged, total)
		}

	case total >= needQty:
		// Held in full but not free, and the pledge (if any) does not explain
		// the lock: usedQty / an RMS lock / a lock we cannot name. The shares
		// are there — protection stays.
		v.State = BrokerStateHeld
		v.Reason = fmt.Sprintf("Held: broker holds %d of %d but only %d free (%d pledged, %d used for margin / locked outside the system) — protection continues", total, needQty, freeTotal, pledged, used)
		if rmsHeld > 0 && held == 0 {
			v.Reason = fmt.Sprintf("Held: the broker's RMS reported Set= %d for %s today (quantity locked, holdings row hidden) — protection continues", rmsHeld, sym)
		}
		if lot.PrevState == BrokerStatePledged {
			v.Reason = ReasonUnpledged + " — " + v.Reason
		}

	default: // 0 < total < needQty
		v.State = BrokerStateQtyMismatch
		v.Reason = fmt.Sprintf("Quantity mismatch: book holds %d, broker holds %d (%d free) — partial manual sale, corporate action or a split lot; left for review", needQty, total, freeTotal)
		if theirQty > 0 {
			v.Reason += fmt.Sprintf("; today's books show %d sold by hand", theirQty)
			if v.Evidence.TradebookSellPrice > 0 {
				v.Reason += fmt.Sprintf(" at ₹%.2f", v.Evidence.TradebookSellPrice)
			}
		}
	}
	return v
}

// ── Snapshot construction from the raw broker reads ─────────────────────

// BuildSnapshot keys one holdings / orderbook / tradebook read by every name
// form and by ISIN. in.ourIDs are the broker/exchange order ids of the user's
// manthan_orders SELL rows and in.ourShape their (symbol, qty) shapes today,
// so a SELL can be attributed to the strategy or to the client.
func BuildSnapshot(legStatus, legDetail string, in snapshotInputs, now time.Time) *BrokerSnapshot {
	s := &BrokerSnapshot{
		LegStatus: legStatus, LegDetail: legDetail, FetchedAt: now, TradesOK: in.tradesOK, OursOK: in.oursOK,
		Total: map[string]int{}, Free: map[string]int{}, Pledged: map[string]int{}, Used: map[string]int{}, BTST: map[string]int{},
		OpenSellQty: map[string]int{}, OpenSellIDs: map[string]string{}, OurSellToday: map[string]string{},
		AnySellToday: map[string]string{}, RMSHeld: map[string]int{},
		Sells: map[string][]TradeSell{}, HoldingName: map[string]string{},
	}
	if legStatus != "OK" {
		return s
	}
	for _, h := range in.holdings {
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
		free := h.FreeQty
		if free == 0 && h.PledgeQty == 0 && h.UsedQty == 0 && h.BTST == 0 && h.HoldingQty > 0 {
			free = h.HoldingQty // some shapes report only holdingQty
		}
		if free > total {
			free = total
		}
		keys := holdingNames(h.Symbol)
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
		isins := map[string]bool{}
		for _, l := range h.Symbol {
			if k := isinKey(l.ISIN); k != "" && !isins[k] {
				isins[k] = true
				keys = append(keys, k)
			}
		}
		for _, k := range keys {
			s.Total[k] += total
			s.Free[k] += free
			s.Pledged[k] += h.PledgeQty
			s.Used[k] += h.UsedQty
			s.BTST[k] += h.BTST
			if s.HoldingName[k] == "" {
				s.HoldingName[k] = seen
			}
		}
	}
	ours := func(ob indiraClient.OrderBook, names []string) bool {
		if in.ourIDs != nil {
			if in.ourIDs[ob.OrdId] {
				return true
			}
			if ex := rawString(ob.ExchOrdId); ex != "" && in.ourIDs[ex] {
				return true
			}
		}
		for _, sh := range in.ourShape {
			if sh.qty != ob.Qty {
				continue
			}
			for _, n := range names {
				if sh.names[n] {
					return true
				}
			}
		}
		return false
	}
	tradeIDs := map[string]bool{}
	for _, tr := range in.trades {
		if !strings.EqualFold(tr.OrdAction, "SELL") || tr.TradedQty <= 0 {
			continue
		}
		tradeIDs[tr.OrdId] = true
		names := tradeSymbolForms(tr.Symbol)
		isOurs := in.ourIDs[tr.OrdId]
		if !isOurs {
			for _, sh := range in.ourShape {
				if sh.qty != tr.Qty && sh.qty != tr.TradedQty {
					continue
				}
				for _, n := range names {
					if sh.names[n] {
						isOurs = true
					}
				}
			}
		}
		sell := TradeSell{OrderID: tr.OrdId, Qty: tr.TradedQty, Price: tr.TradedPrice, Ours: isOurs}
		for _, k := range names {
			s.Sells[k] = append(s.Sells[k], sell)
		}
	}
	for _, ob := range in.book {
		if !strings.EqualFold(ob.OrdAction, "SELL") {
			continue
		}
		st := strings.ToUpper(strings.TrimSpace(ob.Status))
		names := orderNames(ob.Symbol.DispSym, ob.Symbol.BaseSym)
		mine := ours(ob, names)
		// Open (cancellable) SELL: its remaining qty is held.
		if ob.Cancellable {
			qty := ob.RemainQty
			if qty == 0 {
				qty = ob.Qty - ob.TradedQty
			}
			if qty > 0 {
				for _, k := range names {
					s.OpenSellQty[k] += qty
					if s.OpenSellIDs[k] == "" {
						s.OpenSellIDs[k] = ob.OrdId
					}
				}
			}
		}
		// A REJECTED order of ours: an RMS "Set= 0" says the broker holds
		// nothing (no evidence of a reservation); any other rejection (RMS
		// "Set= N, Prev= N" lock, a price band, a session error) still means
		// the quantity was there to be locked — and Set= N>0 is positive
		// evidence the broker holds ≥ N.
		rmsZero := false
		if st == "REJECTED" {
			if set, ok := parseRMSSet(ob.RejReason); ok {
				if set == 0 {
					rmsZero = true
				} else if mine {
					for _, k := range names {
						if set > s.RMSHeld[k] {
							s.RMSHeld[k] = set
						}
					}
				}
			}
		}
		if mine && !rmsZero {
			for _, k := range names {
				if s.OurSellToday[k] == "" {
					s.OurSellToday[k] = ob.OrdId
				}
			}
		}
		if st != "REJECTED" {
			for _, k := range names {
				if s.AnySellToday[k] == "" {
					s.AnySellToday[k] = ob.OrdId
				}
			}
		}
		// EXECUTED SELL orders the trade book did not list: attribute them
		// too, so a trade-book outage is not blinding.
		if (st == "EXECUTED" || st == "COMPLETE" || st == "TRADED") && ob.TradedQty > 0 && !tradeIDs[ob.OrdId] {
			sell := TradeSell{OrderID: ob.OrdId, Qty: ob.TradedQty, Price: ob.Price, Ours: mine, FromOrderBook: true}
			for _, k := range names {
				s.Sells[k] = append(s.Sells[k], sell)
			}
		}
	}
	return s
}

// rawString unquotes a JSON raw value that may be a number or a string.
func rawString(raw []byte) string {
	v := strings.TrimSpace(string(raw))
	v = strings.Trim(v, `"`)
	if v == "null" {
		return ""
	}
	return v
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

// tradeSymbolForms lists every name form of the trade book's loosely typed
// symbol: an object {dispSym, baseSym, symbol}, an array of such objects, or
// a plain string ("KEI", "KEI-EQ", "STK_KEI_EQ_NSE_123").
func tradeSymbolForms(sym any) []string {
	var disp, base, wire string
	switch m := sym.(type) {
	case map[string]any:
		disp, _ = m["dispSym"].(string)
		base, _ = m["baseSym"].(string)
		wire, _ = m["symbol"].(string)
		if disp == "" {
			disp, _ = m["tradingSymbol"].(string)
		}
	case []any:
		for _, e := range m {
			if forms := tradeSymbolForms(e); len(forms) > 0 {
				return forms
			}
		}
		return nil
	case string:
		disp = m
		wire = m
	default:
		return nil
	}
	out := orderNames(disp, base)
	seen := map[string]bool{}
	for _, o := range out {
		seen[o] = true
	}
	// Wire form STK_<SYM>_<SERIES>_<EXC>_<TOKEN> → <SYM>.
	if parts := strings.Split(strings.ToUpper(strings.TrimSpace(wire)), "_"); len(parts) >= 3 && parts[0] == "STK" {
		if !seen[parts[1]] {
			out = append(out, parts[1])
		}
	}
	return out
}

// tradeSymbolNames keeps the (disp, base) pair for callers that want it.
func tradeSymbolNames(tr indiraClient.TradeBook) (disp, base string) {
	forms := tradeSymbolForms(tr.Symbol)
	if len(forms) == 0 {
		return "", ""
	}
	if len(forms) == 1 {
		return forms[0], forms[0]
	}
	return forms[0], forms[1]
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

// ── Live order matching (item 15) ───────────────────────────────────────

// liveBook is the set of orders LIVE in a just-fetched order book, matched by
// broker id, by exchange order id, and by (symbol, qty, SELL) — an AMO gets a
// NEW broker id at the 08:50 conversion, so an id miss alone proves nothing.
type liveBook struct {
	ids   map[string]bool
	exch  map[string]bool
	sells []liveSell
}

type liveSell struct {
	names map[string]bool
	qty   int
}

func isTerminalOrderStatus(status string) bool {
	st := strings.ToUpper(strings.TrimSpace(status))
	return st == "EXECUTED" || st == "CANCELLED" || st == "CANCELED" || st == "REJECTED" || st == "EXPIRED" ||
		st == "COMPLETE" || st == "TRADED"
}

func liveBookFrom(book []indiraClient.OrderBook) *liveBook {
	lb := &liveBook{ids: map[string]bool{}, exch: map[string]bool{}}
	for _, ob := range book {
		if isTerminalOrderStatus(ob.Status) {
			continue
		}
		if ob.OrdId != "" {
			lb.ids[ob.OrdId] = true
		}
		if ex := rawString(ob.ExchOrdId); ex != "" {
			lb.exch[ex] = true
		}
		if strings.EqualFold(ob.OrdAction, "SELL") {
			names := map[string]bool{}
			for _, n := range orderNames(ob.Symbol.DispSym, ob.Symbol.BaseSym) {
				names[n] = true
			}
			lb.sells = append(lb.sells, liveSell{names: names, qty: ob.Qty})
		}
	}
	return lb
}

// has reports whether a ledger SELL row (broker id, symbol, qty) still stands
// live at the broker under any of the three matches.
func (lb *liveBook) has(brokerID, symbol string, qty int) bool {
	if lb == nil {
		return false
	}
	if brokerID != "" && (lb.ids[brokerID] || lb.exch[brokerID]) {
		return true
	}
	for _, s := range lb.sells {
		if s.qty != qty || qty <= 0 {
			continue
		}
		for _, n := range symbolNames(symbol) {
			if s.names[n] {
				return true
			}
		}
	}
	return false
}
