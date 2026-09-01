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

// ReconResult is the per-user three-way view.
type ReconResult struct {
	UserID       string     `json:"user_id"`
	BrokerLeg    string     `json:"broker_leg"` // OK | AUTH_EXPIRED | NO_CREDENTIAL | ERROR
	BrokerDetail string     `json:"broker_detail,omitempty"`
	Mismatches   []Mismatch `json:"mismatches"`
	CleanSymbols int        `json:"clean_symbols"`
	Warning      string     `json:"warning"`
	GeneratedAt  time.Time  `json:"generated_at"`
}

// Reconcile runs the three-way for one user. The two DB legs always
// render; a dead broker session degrades the result, never blanks it.
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

	// Leg 3: broker.
	var totals map[string]int
	auth, legStatus, legDetail := rs.authFor(ctx, userID)
	res.BrokerLeg, res.BrokerDetail = legStatus, legDetail
	if legStatus == "OK" {
		bt, herr := rs.brokerTotalsWithAuth(ctx, auth)
		switch {
		case isAuthExpired(herr):
			res.BrokerLeg = "AUTH_EXPIRED"
		case herr != nil:
			res.BrokerLeg, res.BrokerDetail = "ERROR", herr.Error()
		default:
			totals = bt
		}
	}

	res.Mismatches = ThreeWay(active, exitedOpenSL, totals, res.BrokerLeg == "OK", time.Now())
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
