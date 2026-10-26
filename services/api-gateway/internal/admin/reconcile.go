package admin

// M5.2 — three-way reconciliation: position book vs order ledger vs broker
// holdings, per user, with NAMED mismatch classes. Observe-only by design:
// remediation is M7's audited interventions (the drift feed's
// never-auto-fix rule holds here too).
//
// M5.3 — broker mirror: raw holdings / order book / tradebook for any
// user, straight from the broker — the "what does Indira actually say"
// tab, with the freeQty=0-rows-hidden caveat rendered as a warning.
//
// The positions reconciler's Detect() (services/positions/internal) is the
// spiritual parent, but Go forbids importing another module's internal
// package — ThreeWay below reimplements its rules extended to three legs,
// as a pure function with its own tests.
//
// Broker-truth caveat that shapes GHOST verdicts: /portfolio/v2/holdings
// HIDES rows whose freeQty=0 (pledged, unsettled T+1, TPIN pending). An
// absent row therefore does NOT prove the account holds nothing — a
// position entered within the settlement window gets a softer verdict.

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
)

// brokerReader is the slice of the indira client M5 needs — an interface
// so tests stub the broker without HTTP.
type brokerReader interface {
	GetHoldings(ctx context.Context, auth *indiraClient.AuthContext) ([]indiraClient.Holding, error)
	GetOrderBook(ctx context.Context, auth *indiraClient.AuthContext) ([]indiraClient.OrderBook, error)
	GetOrderBookRaw(ctx context.Context, auth *indiraClient.AuthContext) ([]byte, error)
	GetTradeBook(ctx context.Context, auth *indiraClient.AuthContext, orderIds ...string) ([]indiraClient.TradeBook, error)
}

// ReconStore performs the broker-facing M5 reads.
type ReconStore struct {
	creds  credentialsFetcher
	broker brokerReader
	fleet  *FleetStore
}

func NewReconStore(creds credentialsFetcher, broker brokerReader, fleet *FleetStore) *ReconStore {
	return &ReconStore{creds: creds, broker: broker, fleet: fleet}
}

// HoldingsWarning is rendered on every mirror/reconcile response so the
// operator never mistakes an absent row for an absent holding.
const HoldingsWarning = "broker totals = v2 holdings + qty committed in open SELL orders (v2 hides freeQty=0 rows — intraday, every armed symbol is hidden under its standing SL)"

// settlementWindow: a position younger than this may legitimately be a
// hidden freeQty=0 row (T+1 settlement + a buffer day).
const settlementWindow = 72 * time.Hour

// ── Pure comparison ─────────────────────────────────────────────────────

// BookLot is the aggregated ACTIVE book for one symbol.
type BookLot struct {
	Symbol    string
	Qty       int
	Rows      int // ACTIVE row count; >1 = duplicate defect
	EntryTime time.Time
}

// Mismatch is one named reconciliation finding.
type Mismatch struct {
	Class     string `json:"class"` // GHOST | UNLEDGERED_EXIT | DUPLICATE_BOOK_ROWS | UNKNOWN_HOLDING | QTY_MISMATCH
	Symbol    string `json:"symbol"`
	BookQty   int    `json:"book_qty,omitempty"`
	BrokerQty int    `json:"broker_qty,omitempty"`
	BookRows  int    `json:"book_rows,omitempty"`
	Detail    string `json:"detail"`
	Caveat    string `json:"caveat,omitempty"`
}

// ThreeWay is the pure comparison across the three legs.
//
//	active           — ACTIVE book aggregated per symbol
//	exitedOpenSL     — symbols the book says EXITED but the SL ledger still
//	                   holds an open protection row for
//	brokerTotals     — broker holdings, TOTAL qty per symbol (nil = broker
//	                   leg unavailable: only ledger-side classes fire)
//	brokerLegOK      — whether brokerTotals is trustworthy
//	now              — for the settlement-window ghost caveat
func ThreeWay(active []BookLot, exitedOpenSL []string, brokerTotals map[string]int, brokerLegOK bool, now time.Time) []Mismatch {
	var out []Mismatch

	seen := map[string]bool{}
	for _, lot := range active {
		sym := strings.ToUpper(lot.Symbol)
		seen[sym] = true

		if lot.Rows > 1 {
			out = append(out, Mismatch{
				Class: "DUPLICATE_BOOK_ROWS", Symbol: sym, BookQty: lot.Qty, BookRows: lot.Rows,
				Detail: fmt.Sprintf("%d ACTIVE book rows for one holding (BALUFORGE-class defect) — totals double-count until deduped", lot.Rows),
			})
		}
		if !brokerLegOK {
			continue
		}
		brokerQty := brokerTotals[sym]
		switch {
		case brokerQty == 0:
			m := Mismatch{
				Class: "GHOST", Symbol: sym, BookQty: lot.Qty, BrokerQty: 0,
				Detail: "book ACTIVE but broker shows no holding (IOLCP class) — exits against it will reject or sell someone else's shares",
			}
			if !lot.EntryTime.IsZero() && now.Sub(lot.EntryTime) < settlementWindow {
				m.Caveat = "entered within the settlement window — likely a hidden freeQty=0 row (unsettled T+1), verify in the broker mirror before treating as ghost"
			}
			out = append(out, m)
		case brokerQty != lot.Qty:
			out = append(out, Mismatch{
				Class: "QTY_MISMATCH", Symbol: sym, BookQty: lot.Qty, BrokerQty: brokerQty,
				Detail: fmt.Sprintf("book %d vs broker %d — partial fill, manual trade, or corp action", lot.Qty, brokerQty),
			})
		}
	}

	if brokerLegOK {
		var unknown []string
		for sym, qty := range brokerTotals {
			if qty > 0 && !seen[strings.ToUpper(sym)] {
				unknown = append(unknown, strings.ToUpper(sym))
			}
		}
		sort.Strings(unknown)
		for _, sym := range unknown {
			out = append(out, Mismatch{
				Class: "UNKNOWN_HOLDING", Symbol: sym, BrokerQty: brokerTotals[sym],
				Detail: "broker holds it, the book does not — manual buy or an entry the system lost",
			})
		}
	}

	for _, sym := range exitedOpenSL {
		out = append(out, Mismatch{
			Class: "UNLEDGERED_EXIT", Symbol: strings.ToUpper(sym),
			Detail: "book says EXITED but an SL ledger row is still open — a stale stop that can fire against nothing (or against a re-entry)",
		})
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Class != out[j].Class {
			return out[i].Class < out[j].Class
		}
		return out[i].Symbol < out[j].Symbol
	})
	return out
}

// holdingsTotals reduces broker holdings to TOTAL quantity per NSE display
// symbol. Total = Qty when the broker filled it, else the sum of the
// disposition buckets — freeQty alone undercounts pledged/unsettled shares.
func holdingsTotals(holdings []indiraClient.Holding) map[string]int {
	out := make(map[string]int, len(holdings))
	for _, h := range holdings {
		var key string
		for _, s := range h.Symbol {
			if s.Exc == "NSE" && s.DispSym != "" {
				key = s.DispSym
				break
			}
		}
		if key == "" && len(h.Symbol) > 0 {
			key = h.Symbol[0].DispSym
		}
		if key == "" {
			continue
		}
		total := h.Qty
		if total == 0 {
			total = h.HoldingQty + h.UsedQty + h.BTST + h.PledgeQty
		}
		if total == 0 {
			total = h.FreeQty
		}
		out[strings.ToUpper(key)] = total
	}
	return out
}

// ── Broker-facing assembly ──────────────────────────────────────────────

// authFor resolves the user's stored credential to a live auth context.
// legStatus: OK | AUTH_EXPIRED | NO_CREDENTIAL | ERROR (mirrors the probe's
// verdicts so the UI speaks one language).
func (rs *ReconStore) authFor(ctx context.Context, userID string) (*indiraClient.AuthContext, string, string) {
	auth, verdict, detail := fetchAuthFor(ctx, rs.creds, userID)
	if verdict != "" {
		return nil, verdict, detail
	}
	return auth, "OK", ""
}

// brokerTotals fetches what the broker ACTUALLY holds for one user:
// v2 holdings PLUS qty committed in open SELL orders. The intraday trap
// (found by UAT 2026-09-01, market open): every armed symbol has its
// entire qty locked under its standing SL → freeQty=0 → the v2 holdings
// row VANISHES — reconcile read the whole armed book as ghosts. An open
// SELL order is proof of holding; its remaining qty counts as held.
func (rs *ReconStore) brokerTotals(ctx context.Context, userID string) (map[string]int, string) {
	auth, legStatus, _ := rs.authFor(ctx, userID)
	if legStatus != "OK" {
		return nil, legStatus
	}
	totals, err := rs.brokerTotalsWithAuth(ctx, auth)
	if err != nil {
		if isAuthExpired(err) {
			return nil, "AUTH_EXPIRED"
		}
		return nil, "ERROR"
	}
	return totals, "OK"
}

func (rs *ReconStore) brokerTotalsWithAuth(ctx context.Context, auth *indiraClient.AuthContext) (map[string]int, error) {
	holdings, err := rs.broker.GetHoldings(ctx, auth)
	if err != nil {
		return nil, err
	}
	totals := holdingsTotals(holdings)
	book, berr := rs.broker.GetOrderBook(ctx, auth)
	if berr != nil {
		return nil, fmt.Errorf("orderbook leg: %w", berr)
	}
	addOpenSellCommitments(totals, book)
	return totals, nil
}

// holdingNames lists every name one holding leg can be known by: baseSym,
// dispSym and dispSym without its series suffix ("SPECIALITY-BE" → SPECIALITY),
// upper-cased. Mirrors trade-execution's holdingKeys.
func holdingNames(legs []indiraClient.HoldingSymbol) []string {
	pick := func(l indiraClient.HoldingSymbol) []string {
		var ks []string
		seen := map[string]bool{}
		add := func(v string) {
			v = strings.ToUpper(strings.TrimSpace(v))
			if v != "" && !seen[v] {
				seen[v] = true
				ks = append(ks, v)
			}
		}
		add(l.BaseSym)
		add(l.DispSym)
		if i := strings.LastIndex(l.DispSym, "-"); i > 0 {
			add(l.DispSym[:i])
		}
		return ks
	}
	for _, l := range legs {
		if l.Exc == "NSE" && (l.DispSym != "" || l.BaseSym != "") {
			return pick(l)
		}
	}
	if len(legs) > 0 {
		return pick(legs[0])
	}
	return nil
}

// holdingsTotalsAllKeys is holdingsTotals keyed by EVERY name a holding can
// carry, so a lookup by our series-free symbol finds a BE/BZ/SM-series lot.
// For the ghost gate only — Reconcile's UNKNOWN_HOLDING pass keeps the
// one-key map (alias keys would read as extra unknown holdings).
func holdingsTotalsAllKeys(holdings []indiraClient.Holding) map[string]int {
	out := make(map[string]int, len(holdings)*3)
	for _, h := range holdings {
		total := h.Qty
		if total == 0 {
			total = h.HoldingQty + h.UsedQty + h.BTST + h.PledgeQty
		}
		if total <= 0 {
			continue
		}
		for _, k := range holdingNames(h.Symbol) {
			out[k] += total
		}
	}
	return out
}

// addOpenSellCommitmentsAllKeys is addOpenSellCommitments under every name
// form of the order's symbol (dispSym, baseSym, series-stripped dispSym).
func addOpenSellCommitmentsAllKeys(totals map[string]int, book []indiraClient.OrderBook) {
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
		seen := map[string]bool{}
		for _, raw := range []string{ob.Symbol.DispSym, ob.Symbol.BaseSym} {
			v := strings.ToUpper(strings.TrimSpace(raw))
			if v == "" {
				continue
			}
			names := []string{v}
			if i := strings.LastIndex(v, "-"); i > 0 {
				names = append(names, v[:i])
			}
			for _, n := range names {
				if !seen[n] {
					seen[n] = true
					totals[n] += qty
				}
			}
		}
	}
}

// addOpenSellCommitments folds open (cancellable) SELL orders' remaining
// qty into the totals — those shares exist, they are just spoken for.
func addOpenSellCommitments(totals map[string]int, book []indiraClient.OrderBook) {
	for _, ob := range book {
		if !strings.EqualFold(ob.OrdAction, "SELL") || !ob.Cancellable {
			continue
		}
		sym := strings.ToUpper(ob.Symbol.DispSym)
		if sym == "" {
			sym = strings.ToUpper(ob.Symbol.BaseSym)
		}
		if sym == "" {
			continue
		}
		qty := ob.RemainQty
		if qty == 0 {
			qty = ob.Qty - ob.TradedQty
		}
		if qty > 0 {
			totals[sym] += qty
		}
	}
}

func isAuthExpired(err error) bool {
	return err != nil && strings.Contains(err.Error(), "AU004")
}

// ReconResult is the per-user three-way view plus the per-lot broker
// verdicts (2026-10-26). Verdicts are the operator-facing truth: one row per
// Manthan lot (book lots and ledger-only lots), each with a state, a plain
// reason and the broker evidence; Mismatches keep the older class view.
type ReconResult struct {
	UserID         string         `json:"user_id"`
	BrokerLeg      string         `json:"broker_leg"` // OK | AUTH_EXPIRED | NO_CREDENTIAL | ERROR
	BrokerDetail   string         `json:"broker_detail,omitempty"`
	Mismatches     []Mismatch     `json:"mismatches"`
	CleanSymbols   int            `json:"clean_symbols"`
	Verdicts       []Verdict      `json:"verdicts"`
	VerdictSummary map[string]int `json:"verdict_summary"` // state → count
	Warning        string         `json:"warning"`
	GeneratedAt    time.Time      `json:"generated_at"`

	// read is the raw broker read the verdicts were computed from, kept so
	// the reconciler's actions re-use it instead of fetching again (one
	// holdings + one orderbook + one tradebook per user per run).
	read *brokerRead
}

// Snapshot exposes the keyed broker snapshot (nil when the broker leg was
// unavailable). Used by the reconciler's actions and by tests.
func (r *ReconResult) Snapshot() *BrokerSnapshot {
	if r == nil || r.read == nil {
		return nil
	}
	return r.read.snap
}

// brokerRead is one user's raw broker reads for one run.
type brokerRead struct {
	leg, detail string
	auth        *indiraClient.AuthContext
	holdings    []indiraClient.Holding
	book        []indiraClient.OrderBook
	trades      []indiraClient.TradeBook
	ourOrderIDs map[string]bool
	ourShapes   []sellShape // our SELL rows touched today as (symbol, qty)
	tradesOK    bool        // trade book read
	oursOK      bool        // our-order-id attribution read
	live        *liveBook
	snap        *BrokerSnapshot
}

// degrade marks the read as not trustworthy (a tripwire fired) and rebuilds
// the snapshot so every verdict reads UNVERIFIED with the reason attached.
func (r *brokerRead) degrade(detail string) {
	r.leg, r.detail = "UNVERIFIED", detail
	r.snap = BuildSnapshot(r.leg, r.detail, snapshotInputs{}, time.Now())
}

// readBroker performs the per-user broker reads once: holdings, order book,
// trade book. A failed holdings/orderbook read degrades the leg (no truth); a
// failed tradebook / attribution read is recorded (TradesOK / OursOK false)
// so an absent holding can only read UNVERIFIED, never NOT_HELD/SOLD_TODAY.
func (rs *ReconStore) readBroker(ctx context.Context, userID string) *brokerRead {
	r := &brokerRead{}
	auth, legStatus, legDetail := rs.authFor(ctx, userID)
	r.leg, r.detail, r.auth = legStatus, legDetail, auth
	if legStatus != "OK" {
		r.snap = BuildSnapshot(r.leg, r.detail, snapshotInputs{}, time.Now())
		return r
	}
	holdings, err := rs.broker.GetHoldings(ctx, auth)
	if err == nil {
		r.holdings = holdings
		r.book, err = rs.broker.GetOrderBook(ctx, auth)
		if err != nil {
			err = fmt.Errorf("orderbook leg: %w", err)
		}
	}
	switch {
	case isAuthExpired(err):
		r.leg = "AUTH_EXPIRED"
	case err != nil:
		r.leg, r.detail = "ERROR", err.Error()
	}
	if r.leg == "OK" {
		if trades, terr := rs.broker.GetTradeBook(ctx, auth); terr == nil {
			r.trades, r.tradesOK = trades, true
		}
		if ids, oerr := ourBrokerOrderIDs(ctx, rs.fleet.execDB, userID); oerr == nil {
			r.ourOrderIDs, r.oursOK = ids, true
		}
		if rows, serr := rs.ourSellRowsToday(ctx, userID); serr == nil {
			for _, o := range rows {
				names := map[string]bool{}
				for _, n := range symbolNames(o.symbol) {
					names[n] = true
				}
				r.ourShapes = append(r.ourShapes, sellShape{names: names, qty: o.qty})
			}
		} else {
			r.oursOK = false
		}
		r.live = liveBookFrom(r.book)
	}
	r.snap = BuildSnapshot(r.leg, r.detail, snapshotInputs{
		holdings: r.holdings, book: r.book, trades: r.trades,
		ourIDs: r.ourOrderIDs, ourShape: r.ourShapes, tradesOK: r.tradesOK, oursOK: r.oursOK,
	}, time.Now())
	return r
}

// ourBrokerOrderIDs lists the broker order ids of ALL the user's
// manthan_orders SELL rows — a SELL carrying one of them was placed by the
// strategy, not by the client. Shared by the reconciler and the ghost gate.
// No time filter: an old stop's id can still appear in today's book.
func ourBrokerOrderIDs(ctx context.Context, execDB *sql.DB, userID string) (map[string]bool, error) {
	rows, err := execDB.QueryContext(ctx, `
		SELECT broker_order_id FROM manthan_orders
		 WHERE user_id = $1 AND order_side = 'SELL'
		   AND broker_order_id IS NOT NULL AND broker_order_id <> ''`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var id string
		if rows.Scan(&id) == nil && id != "" {
			out[id] = true
		}
	}
	return out, rows.Err()
}

// ledgerLiveOrder is one of our rows that MUST appear in the order book.
type ledgerLiveOrder struct {
	brokerID, symbol string
	qty              int
}

// ourLiveOrdersToday lists our rows with a broker id touched today, in ANY
// status (a REJECTED converted AMO adopted onto our row counts too) — if one
// of them is missing from the order book entirely, the book read was partial
// and nothing may be concluded from it (tripwire 1c).
func (rs *ReconStore) ourLiveOrdersToday(ctx context.Context, userID string) ([]ledgerLiveOrder, error) {
	rows, err := rs.fleet.execDB.QueryContext(ctx, `
		SELECT broker_order_id, UPPER(symbol), qty FROM manthan_orders
		 WHERE user_id = $1
		   AND broker_order_id IS NOT NULL AND broker_order_id <> ''
		   AND broker_order_id NOT LIKE 'PAPER-%'
		   AND updated_at >= date_trunc('day', now() AT TIME ZONE 'Asia/Kolkata') AT TIME ZONE 'Asia/Kolkata'`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ledgerLiveOrder
	for rows.Next() {
		var o ledgerLiveOrder
		if rows.Scan(&o.brokerID, &o.symbol, &o.qty) == nil {
			out = append(out, o)
		}
	}
	return out, rows.Err()
}

// ourSellRowsToday lists our SELL rows touched today (any status, any id) as
// (symbol, qty) shapes — an EXECUTED SELL in the order book with the same
// shape is ours even under a broker id we never saw (AMO conversion).
func (rs *ReconStore) ourSellRowsToday(ctx context.Context, userID string) ([]ledgerLiveOrder, error) {
	rows, err := rs.fleet.execDB.QueryContext(ctx, `
		SELECT COALESCE(broker_order_id, ''), UPPER(symbol), qty FROM manthan_orders
		 WHERE user_id = $1 AND order_side = 'SELL' AND qty > 0
		   AND updated_at >= date_trunc('day', now() AT TIME ZONE 'Asia/Kolkata') AT TIME ZONE 'Asia/Kolkata'`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ledgerLiveOrder
	for rows.Next() {
		var o ledgerLiveOrder
		if rows.Scan(&o.brokerID, &o.symbol, &o.qty) == nil {
			out = append(out, o)
		}
	}
	return out, rows.Err()
}

// lotISINs maps UPPER(symbol) → ISIN from the user's manthan_orders, so a
// holding listed under a name form we do not know is still found.
func (rs *ReconStore) lotISINs(ctx context.Context, userID string) map[string]string {
	out := map[string]string{}
	rows, err := rs.fleet.execDB.QueryContext(ctx, `
		SELECT DISTINCT ON (UPPER(symbol)) UPPER(symbol), isin FROM manthan_orders
		 WHERE user_id = $1 AND isin IS NOT NULL AND isin <> ''
		 ORDER BY UPPER(symbol), id DESC`, userID)
	if err != nil {
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var sym, isin string
		if rows.Scan(&sym, &isin) == nil {
			out[sym] = isin
		}
	}
	return out
}

// inOrderBookAtAll matches by broker id, exchange order id, or (symbol, qty,
// SELL) in ANY status — an AMO gets a new broker id at the 08:50 conversion.
func inOrderBookAtAll(book []indiraClient.OrderBook, o ledgerLiveOrder) bool {
	for _, ob := range book {
		if ob.OrdId == o.brokerID || rawString(ob.ExchOrdId) == o.brokerID {
			return true
		}
		if strings.EqualFold(ob.OrdAction, "SELL") && ob.Qty == o.qty {
			for _, n := range orderNames(ob.Symbol.DispSym, ob.Symbol.BaseSym) {
				for _, m := range symbolNames(o.symbol) {
					if n == m {
						return true
					}
				}
			}
		}
	}
	return false
}

// bookLots loads the user's ACTIVE Manthan lots aggregated per
// (strategy, symbol) with the row ids and the last persisted verdict.
func (rs *ReconStore) bookLots(ctx context.Context, userID string) ([]BookLotInput, error) {
	rows, err := rs.fleet.posDB.QueryContext(ctx, `
		SELECT COALESCE(strategy_id::text, ''), UPPER(symbol), SUM(quantity), MIN(entry_time),
		       array_to_string(array_agg(position_id::text ORDER BY entry_time), ','),
		       COALESCE((array_agg(broker_state ORDER BY broker_checked_at DESC NULLS LAST))[1], ''),
		       MAX(broker_checked_at)
		  FROM positions
		 WHERE user_id = $1 AND status = 'ACTIVE' AND origin = 'MANTHAN'
		 GROUP BY strategy_id, UPPER(symbol)
		 ORDER BY UPPER(symbol), strategy_id`, userID)
	if err != nil {
		return nil, fmt.Errorf("reconcile lots: %w", err)
	}
	defer rows.Close()
	var out []BookLotInput
	for rows.Next() {
		var lot BookLotInput
		var ids string
		var checked sql.NullTime
		if err := rows.Scan(&lot.StrategyID, &lot.Symbol, &lot.Qty, &lot.EntryTime, &ids, &lot.PrevState, &checked); err != nil {
			return nil, err
		}
		lot.UserID = userID
		if ids != "" {
			lot.PositionIDs = strings.Split(ids, ",")
		}
		if checked.Valid {
			lot.PrevCheckedAt = checked.Time
		}
		out = append(out, lot)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// Several strategies of one user on one symbol share ONE pile at the
	// broker: every lot carries the user-symbol sum and the strategy count.
	sum, cnt := map[string]int{}, map[string]int{}
	for _, l := range out {
		sum[l.Symbol] += l.Qty
		cnt[l.Symbol]++
	}
	for i := range out {
		out[i].UserSymbolQty, out[i].Strategies = sum[out[i].Symbol], cnt[out[i].Symbol]
	}
	return out, nil
}

// ledgerOnlyLots lists (strategy, symbol) pairs the execution ledger still
// holds (net filled BUY − SELL > 0) with NO ACTIVE book row for the user —
// the IOLCP class: the book already shows the lot gone but trade-execution
// keeps protecting it. Settlement anchor = newest filled BUY.
func (rs *ReconStore) ledgerOnlyLots(ctx context.Context, userID string, book []BookLotInput) ([]BookLotInput, error) {
	inBook := map[string]bool{}
	for _, l := range book {
		inBook[strings.ToUpper(l.Symbol)] = true
	}
	rows, err := rs.fleet.execDB.QueryContext(ctx, `
		SELECT strategy_id::text, UPPER(symbol),
		       SUM(CASE WHEN order_side='BUY'  AND status='FILLED' THEN filled_qty
		                WHEN order_side='SELL' AND status='FILLED' THEN -filled_qty
		                ELSE 0 END) AS net,
		       MAX(CASE WHEN order_side='BUY' AND status='FILLED'
		                THEN COALESCE(filled_at, created_at) END) AS last_buy
		  FROM manthan_orders
		 WHERE user_id = $1
		 GROUP BY strategy_id, UPPER(symbol)
		HAVING SUM(CASE WHEN order_side='BUY'  AND status='FILLED' THEN filled_qty
		                WHEN order_side='SELL' AND status='FILLED' THEN -filled_qty
		                ELSE 0 END) > 0
		 ORDER BY UPPER(symbol), strategy_id`, userID)
	if err != nil {
		return nil, fmt.Errorf("reconcile ledger lots: %w", err)
	}
	defer rows.Close()
	var out []BookLotInput
	for rows.Next() {
		var lot BookLotInput
		var lastBuy sql.NullTime
		if err := rows.Scan(&lot.StrategyID, &lot.Symbol, &lot.Qty, &lastBuy); err != nil {
			return nil, err
		}
		if inBook[lot.Symbol] {
			continue
		}
		if lastBuy.Valid {
			lot.EntryTime = lastBuy.Time
		}
		lot.UserID, lot.LedgerOnly = userID, true
		out = append(out, lot)
	}
	return out, rows.Err()
}

// Reconcile runs the three-way for one user and judges every lot. The two
// DB legs always render; a dead broker session degrades the result (every
// verdict UNVERIFIED), never blanks it. Read-only: persisting verdicts and
// acting on them is the reconciler's job (ghost_reaper.go).
func (rs *ReconStore) Reconcile(ctx context.Context, userID string) (*ReconResult, error) {
	res := &ReconResult{UserID: userID, Warning: HoldingsWarning, GeneratedAt: time.Now()}

	// Leg 1: ACTIVE book.
	rows, err := rs.fleet.posDB.QueryContext(ctx, `
		SELECT symbol, COUNT(*), SUM(quantity), MIN(entry_time)
		  FROM positions WHERE user_id = $1 AND status = 'ACTIVE'
		 GROUP BY symbol`, userID)
	if err != nil {
		return nil, fmt.Errorf("reconcile book: %w", err)
	}
	var active []BookLot
	for rows.Next() {
		var lot BookLot
		if err := rows.Scan(&lot.Symbol, &lot.Rows, &lot.Qty, &lot.EntryTime); err != nil {
			rows.Close()
			return nil, err
		}
		active = append(active, lot)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Leg 2: symbols EXITED in the book (no ACTIVE remnant) whose SL
	// ledger row is still open.
	exitedOpenSL, err := rs.exitedWithOpenSL(ctx, userID, active)
	if err != nil {
		return nil, err
	}

	// Verdict inputs: Manthan book lots per strategy + ledger-only lots.
	lots, err := rs.bookLots(ctx, userID)
	if err != nil {
		return nil, err
	}
	ledgerLots, err := rs.ledgerOnlyLots(ctx, userID, lots)
	if err != nil {
		return nil, err
	}
	lots = append(lots, ledgerLots...)
	isins := rs.lotISINs(ctx, userID)
	for i := range lots {
		if lots[i].ISIN == "" {
			lots[i].ISIN = isins[lots[i].Symbol]
		}
	}

	// Leg 3: broker — ONE read, shared by the class view and the verdicts.
	res.read = rs.readBroker(ctx, userID)
	if res.read.leg == "OK" {
		// Tripwire 1b: an empty holdings reply while the user has open lots is
		// a lost/partial read until proven otherwise — never "holds nothing".
		if len(res.read.holdings) == 0 && len(lots) > 0 {
			res.read.degrade(fmt.Sprintf("holdings reply empty while %d Manthan lot(s) are open — treated as a partial read, nothing concluded", len(lots)))
		}
	}
	if res.read.leg == "OK" {
		// Tripwire 1c: one of OUR live orders missing from the order book
		// entirely proves the book read was partial.
		if ours, oerr := rs.ourLiveOrdersToday(ctx, userID); oerr == nil {
			for _, o := range ours {
				if !inOrderBookAtAll(res.read.book, o) {
					res.read.degrade(fmt.Sprintf("our live order %s (%s ×%d) is missing from the broker order book — partial read, nothing concluded", o.brokerID, o.symbol, o.qty))
					break
				}
			}
		}
	}
	res.BrokerLeg, res.BrokerDetail = res.read.leg, res.read.detail
	var totals map[string]int
	if res.BrokerLeg == "OK" {
		totals = holdingsTotals(res.read.holdings)
		addOpenSellCommitments(totals, res.read.book)
	}

	now := time.Now()
	res.Mismatches = ThreeWay(active, exitedOpenSL, totals, res.BrokerLeg == "OK", now)
	if res.Mismatches == nil {
		res.Mismatches = []Mismatch{}
	}
	flagged := map[string]bool{}
	for _, m := range res.Mismatches {
		flagged[m.Symbol] = true
	}
	for _, lot := range active {
		if !flagged[strings.ToUpper(lot.Symbol)] {
			res.CleanSymbols++
		}
	}

	res.Verdicts = make([]Verdict, 0, len(lots))
	for _, lot := range lots {
		res.Verdicts = append(res.Verdicts, Classify(lot, res.read.snap, now))
	}
	sortVerdicts(res.Verdicts)
	res.VerdictSummary = SummarizeVerdicts(res.Verdicts)
	return res, nil
}

// exitedWithOpenSL: open SL ledger rows for symbols with no ACTIVE book
// position but at least one EXITED one — the stale-stop class.
func (rs *ReconStore) exitedWithOpenSL(ctx context.Context, userID string, active []BookLot) ([]string, error) {
	activeSet := map[string]bool{}
	for _, lot := range active {
		activeSet[strings.ToUpper(lot.Symbol)] = true
	}

	exited := map[string]bool{}
	rows, err := rs.fleet.posDB.QueryContext(ctx, `
		SELECT DISTINCT symbol FROM positions
		 WHERE user_id = $1 AND status = 'EXITED'`, userID)
	if err != nil {
		return nil, fmt.Errorf("reconcile exited: %w", err)
	}
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			rows.Close()
			return nil, err
		}
		exited[strings.ToUpper(s)] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	openSL, err := rs.fleet.execDB.QueryContext(ctx, `
		SELECT DISTINCT symbol FROM manthan_orders
		 WHERE user_id = $1
		   AND order_type IN ('SL_SELL','SL_SELL_AMO')
		   AND status IN ('SL_PLACED','AMO_PENDING','SL_DEFERRED_BAND')`, userID)
	if err != nil {
		return nil, fmt.Errorf("reconcile open SL: %w", err)
	}
	var out []string
	for openSL.Next() {
		var s string
		if err := openSL.Scan(&s); err != nil {
			openSL.Close()
			return nil, err
		}
		s = strings.ToUpper(s)
		if exited[s] && !activeSet[s] {
			out = append(out, s)
		}
	}
	openSL.Close()
	if err := openSL.Err(); err != nil {
		return nil, err
	}
	sort.Strings(out)
	return out, nil
}

// ── M5.3 broker mirror ──────────────────────────────────────────────────

// MirrorResult wraps one raw broker read.
type MirrorResult struct {
	UserID       string          `json:"user_id"`
	BrokerLeg    string          `json:"broker_leg"`
	BrokerDetail string          `json:"broker_detail,omitempty"`
	Warning      string          `json:"warning,omitempty"`
	Data         json.RawMessage `json:"data,omitempty"`
	FetchedAt    time.Time       `json:"fetched_at"`
}

// Mirror fetches one broker view: "holdings", "orderbook" or "trades".
func (rs *ReconStore) Mirror(ctx context.Context, userID, view string) (*MirrorResult, error) {
	res := &MirrorResult{UserID: userID, FetchedAt: time.Now()}
	auth, legStatus, legDetail := rs.authFor(ctx, userID)
	res.BrokerLeg, res.BrokerDetail = legStatus, legDetail
	if legStatus != "OK" {
		return res, nil
	}

	var payload any
	var err error
	switch view {
	case "holdings":
		res.Warning = HoldingsWarning
		payload, err = rs.broker.GetHoldings(ctx, auth)
	case "orderbook":
		var raw []byte
		raw, err = rs.broker.GetOrderBookRaw(ctx, auth)
		if err == nil {
			res.Data = json.RawMessage(raw)
			return res, nil
		}
	case "trades":
		payload, err = rs.broker.GetTradeBook(ctx, auth)
	default:
		return nil, fmt.Errorf("unknown mirror view %q (want holdings, orderbook or trades)", view)
	}
	if err != nil {
		if isAuthExpired(err) {
			res.BrokerLeg = "AUTH_EXPIRED"
		} else {
			res.BrokerLeg, res.BrokerDetail = "ERROR", err.Error()
		}
		return res, nil
	}
	if payload != nil {
		b, merr := json.Marshal(payload)
		if merr != nil {
			return nil, merr
		}
		res.Data = b
	}
	return res, nil
}
