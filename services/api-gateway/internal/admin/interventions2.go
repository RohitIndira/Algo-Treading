package admin

// M7 Phase A2 + B — the remaining interventions.
//
// 7.4 Order view / cancel — broker-truth first: the order's LIVE broker
//     status renders before anything acts. Guardrails: cancel of any
//     SL-family ledger order is REFUSED outright (a naked SL cancel is
//     the June liquidation trigger — that sequence exists only inside
//     the supervised square-off); a vanished order (not at the broker)
//     is a 422 verdict, never an escalation (the IOLCP lesson).
//
// 7.5 Ghost cleanup — heal a confirmed ghost from broker-verified
//     evidence only: live holdings must show NOTHING held, and either
//     today's tradebook carries the missing SELL (price attached) or
//     the position is past the 72h settlement window (freeQty=0 hidden
//     rows can't be mistaken for ghosts). Free-hand row editing stays
//     impossible: the synthetic FILLED SELL follows the FIX-5 shape and
//     the book rows close with ADMIN_GHOST_CLEANUP, all TYPED-confirmed.
//
// 7.3 Square-off proxy — TYPED with a rupee blast radius; the actual
//     cancel-SL-then-sell runs inside trade-execution under the position
//     lock (admin_squareoff.go), this side only confirms and audits.
//
// 7.7 Rebalance preview / trigger — runs the operator's own CLI
//     (services/rebalancer) on this host: preview = --dry-run (no
//     publish, no orders), trigger = the real pass for ONE user, TYPED.

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"time"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
)

// actionBroker extends the M5 broker slice with what 7.4 needs.
type actionBroker interface {
	brokerReader
	GetOrderBook(ctx context.Context, auth *indiraClient.AuthContext) ([]indiraClient.OrderBook, error)
	CancelOrder(ctx context.Context, auth *indiraClient.AuthContext, req *indiraClient.CancelOrderRequest) error
}

// cmdRunner executes one local command — injectable so tests never fork.
// extraEnv entries are appended to the inherited environment (child wins
// on duplicates by POSIX last-one rules).
type cmdRunner func(ctx context.Context, dir string, extraEnv []string, bin string, args ...string) ([]byte, error)

func execRunner(ctx context.Context, dir string, extraEnv []string, bin string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Dir = dir
	if len(extraEnv) > 0 {
		cmd.Env = append(os.Environ(), extraEnv...)
	}
	return cmd.CombinedOutput()
}

// Actions bundles the Phase A2/B dependencies.
type Actions struct {
	fleet      *FleetStore
	creds      credentialsFetcher
	broker     actionBroker
	strategies *StrategyControl
	ltp        LTPFeed // nil-safe: blast radius says VALUE UNKNOWN

	teMetricsURL  string
	httpc         *http.Client
	rebalancerBin string   // absolute path to the built CLI ("" disables 7.7)
	rebalancerDir string   // cwd for the run (repo root: .env discovery)
	rebalancerEnv []string // explicit env for the child — the 2026-09-01
	// lesson: the CLI reads REDIS_ADDR/REDIS_URI, the gateway carries
	// REDIS_LOCAL_ADDR, and no .env exists on prod, so the child silently
	// hit the wrong redis and fell back to 30% EMA targets. The gateway
	// now hands it the exact redis it verified the key on.
	run cmdRunner
}

func NewActions(fleet *FleetStore, creds credentialsFetcher, broker actionBroker,
	strategies *StrategyControl, ltp LTPFeed, teMetricsURL, rebalancerBin, rebalancerDir string,
	rebalancerEnv []string) *Actions {
	return &Actions{
		fleet: fleet, creds: creds, broker: broker, strategies: strategies, ltp: ltp,
		teMetricsURL:  strings.TrimRight(teMetricsURL, "/"),
		httpc:         &http.Client{Timeout: 120 * time.Second},
		rebalancerBin: rebalancerBin, rebalancerDir: rebalancerDir, rebalancerEnv: rebalancerEnv,
		run: execRunner,
	}
}

func (a *Actions) authFor(ctx context.Context, userID string) (*indiraClient.AuthContext, error) {
	auth, verdict, detail := fetchAuthFor(ctx, a.creds, userID)
	if verdict != "" {
		return nil, &refusal{code: 422, msg: fmt.Sprintf("broker leg %s for %s: %s", verdict, userID, detail)}
	}
	return auth, nil
}

// ── 7.4 order view / cancel ─────────────────────────────────────────────

// OrderView is broker truth + our ledger row, side by side.
type OrderView struct {
	BrokerOrderID string                  `json:"broker_order_id"`
	BrokerFound   bool                    `json:"broker_found"`
	Broker        *indiraClient.OrderBook `json:"broker,omitempty"`
	Ledger        map[string]any          `json:"ledger,omitempty"` // our manthan_orders row, if any
	Verdict       string                  `json:"verdict"`          // LIVE | TERMINAL | VANISHED
}

func (a *Actions) findBrokerOrder(ctx context.Context, auth *indiraClient.AuthContext, brokerID string) (*indiraClient.OrderBook, error) {
	book, err := a.broker.GetOrderBook(ctx, auth)
	if err != nil {
		return nil, fmt.Errorf("orderbook fetch: %w", err)
	}
	for i := range book {
		if book[i].OrdId == brokerID {
			return &book[i], nil
		}
	}
	return nil, nil
}

// ledgerRowFor returns our manthan_orders view of a broker order id.
func (a *Actions) ledgerRowFor(ctx context.Context, userID, brokerID string) (map[string]any, string, error) {
	var orderType, symbol, status string
	var qty int
	err := a.fleet.execDB.QueryRowContext(ctx, `
		SELECT order_type, symbol, status, qty FROM manthan_orders
		 WHERE user_id = $1 AND broker_order_id = $2
		 ORDER BY id DESC LIMIT 1`, userID, brokerID).Scan(&orderType, &symbol, &status, &qty)
	if err != nil {
		return nil, "", nil // no ledger row — a manual/live-algos order; still viewable
	}
	return map[string]any{"order_type": orderType, "symbol": symbol, "status": status, "qty": qty}, orderType, nil
}

// ViewOrder renders broker truth first.
func (a *Actions) ViewOrder(ctx context.Context, userID, brokerID string) (*OrderView, error) {
	auth, err := a.authFor(ctx, userID)
	if err != nil {
		return nil, err
	}
	v := &OrderView{BrokerOrderID: brokerID}
	row, _, _ := a.ledgerRowFor(ctx, userID, brokerID)
	v.Ledger = row

	ob, err := a.findBrokerOrder(ctx, auth, brokerID)
	if err != nil {
		return nil, err
	}
	if ob == nil {
		v.Verdict = "VANISHED"
		return v, nil
	}
	v.BrokerFound, v.Broker = true, ob
	if ob.Cancellable || ob.Modifiable {
		v.Verdict = "LIVE"
	} else {
		v.Verdict = "TERMINAL"
	}
	return v, nil
}

// CancelOrder cancels one broker order, guardrails first.
func (a *Actions) CancelOrder(ctx context.Context, userID, brokerID string) (*OrderView, error) {
	_, ledgerType, _ := a.ledgerRowFor(ctx, userID, brokerID)
	if strings.HasPrefix(ledgerType, "SL_") {
		return nil, &refusal{code: 422, msg: fmt.Sprintf(
			"order %s is a %s — cancelling a protective stop by hand is the June-liquidation trigger; use the supervised square-off (cancel+sell as one op) or re-arm instead", brokerID, ledgerType)}
	}
	auth, err := a.authFor(ctx, userID)
	if err != nil {
		return nil, err
	}
	ob, err := a.findBrokerOrder(ctx, auth, brokerID)
	if err != nil {
		return nil, err
	}
	if ob == nil {
		return nil, &refusal{code: 422, msg: fmt.Sprintf(
			"order %s is not in the broker's order book — already terminal or never placed; nothing to cancel and NOTHING to escalate (the reconciler syncs our ledger)", brokerID)}
	}
	if !ob.Cancellable {
		return nil, &refusal{code: 422, msg: fmt.Sprintf(
			"broker reports %s as %s and not cancellable", brokerID, ob.Status)}
	}
	req := &indiraClient.CancelOrderRequest{Symbol: ob.Symbol.Symbol, Exc: ob.Symbol.Exc, OrdId: brokerID}
	if req.Exc == "" {
		req.Exc = "NSE"
	}
	if err := a.broker.CancelOrder(ctx, auth, req); err != nil {
		return nil, fmt.Errorf("broker cancel: %w", err)
	}
	return a.ViewOrder(ctx, userID, brokerID)
}

// ── 7.5 ghost cleanup ───────────────────────────────────────────────────

// GhostEvidence is what the broker said at preview/heal time.
type GhostEvidence struct {
	HoldingsAbsent bool       `json:"holdings_absent"`
	BrokerQty      int        `json:"broker_qty"`                // >0 → NOT a ghost
	OpenSellOrder  string     `json:"open_sell_order,omitempty"` // standing SELL at broker → NOT a ghost
	TradebookSell  bool       `json:"tradebook_sell"`
	TradebookPrice float64    `json:"tradebook_price,omitempty"`
	TradebookQty   int        `json:"tradebook_qty,omitempty"`
	OldestEntry    *time.Time `json:"oldest_entry,omitempty"`
	PastSettlement bool       `json:"past_settlement_window"`
}

// GhostPlan is the previewed heal.
type GhostPlan struct {
	UserID       string `json:"user_id"`
	Symbol       string `json:"symbol"`
	BookRows     int    `json:"book_rows"`
	BookQty      int    `json:"book_qty"`
	LedgerNetQty int    `json:"ledger_net_qty"` // manthan_orders net; >0 gets the FIX-5 SELL row
	LedgerOnly   bool   `json:"ledger_only"`    // no ACTIVE book row; only the execution ledger still holds the lot
	// OtherStrategiesHolding — further strategies of this user whose ledger
	// still holds the symbol (each needs its own heal); ledger-only plans.
	OtherStrategiesHolding int           `json:"other_strategies_holding,omitempty"`
	StrategyID             string        `json:"strategy_id,omitempty"`
	Evidence               GhostEvidence `json:"evidence"`
	ExitPrice              float64       `json:"exit_price,omitempty"` // tradebook-backed; 0 = reconciled later
	// ExitReason is what the heal writes on the book rows. Empty → the manual
	// Postman flow's ADMIN_GHOST_CLEANUP. The automated reconciler sets
	// NOT_HELD_AT_BROKER, or MANUAL_EXIT when today's tradebook carries the
	// client's own sell (price attached).
	ExitReason       string `json:"exit_reason,omitempty"`
	ConfirmationText string `json:"confirmation_text"`

	// liveOrderIDs: broker order ids seen live in the order book at preview
	// time — closeStaleStopRows must never close a ledger row whose broker
	// order still stands. Nil = not fetched (heal fetches on its own).
	liveOrderIDs map[string]bool
}

// exitReason resolves the reason the heal writes.
func (p *GhostPlan) exitReason() string {
	if p.ExitReason == "" {
		return ExitReasonAdminGhostCleanup
	}
	return p.ExitReason
}

// closeNote is the plain-language note carried on ledger rows and events.
func (p *GhostPlan) closeNote() string {
	switch p.exitReason() {
	case ExitReasonManualExit:
		return ReasonSoldTodayShort
	case ExitReasonNotHeldAtBroker:
		return ReasonNotHeldClosed
	}
	return "admin ghost cleanup: broker-verified missing SELL recorded (FIX-5 pattern)"
}

func ghostConfirmation(p *GhostPlan) string {
	if p.LedgerOnly {
		// The strategy is part of the text: two strategies of one user can
		// hold the same symbol, and the heal must name the one it closes.
		return fmt.Sprintf("HEAL GHOST %s FOR %s — CLOSE LEDGER ×%d OF STRATEGY %s", p.Symbol, p.UserID, p.LedgerNetQty, shortID(p.StrategyID))
	}
	return fmt.Sprintf("HEAL GHOST %s FOR %s — CLOSE %d BOOK ROWS ×%d", p.Symbol, p.UserID, p.BookRows, p.BookQty)
}

// GhostPreview assembles the broker-verified plan. Every call re-fetches
// live evidence — a stale preview can never authorize a heal (the TYPED
// text is recomputed at heal time and must still match).
func (a *Actions) GhostPreview(ctx context.Context, userID, symbol string) (*GhostPlan, error) {
	return a.ghostPreview(ctx, userID, symbol, nil)
}

// GhostPreviewFromRead is GhostPreview on a broker read the caller already
// holds (the reconciler's one-per-user holdings/orderbook/tradebook fetch).
// Same evidence gate, same refusals; a read whose leg is not OK is refused
// outright. The read must be seconds old — the reconciler calls this inside
// the same run that fetched it.
func (a *Actions) GhostPreviewFromRead(ctx context.Context, userID, symbol string, read *brokerRead) (*GhostPlan, error) {
	if read == nil || read.leg != "OK" {
		leg := "UNAVAILABLE"
		if read != nil && read.leg != "" {
			leg = read.leg
		}
		return nil, &refusal{code: 422, msg: fmt.Sprintf("broker leg %s for %s: no broker truth, no heal", leg, userID)}
	}
	return a.ghostPreview(ctx, userID, symbol, read)
}

func (a *Actions) ghostPreview(ctx context.Context, userID, symbol string, read *brokerRead) (*GhostPlan, error) {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	p := &GhostPlan{UserID: userID, Symbol: symbol}

	// Book side.
	err := a.fleet.posDB.QueryRowContext(ctx, `
		SELECT COUNT(*), COALESCE(SUM(quantity),0), COALESCE(MIN(entry_time), 'epoch'::timestamptz),
		       COALESCE(MAX(strategy_id::text), '')
		  FROM positions
		 WHERE user_id = $1 AND UPPER(symbol) = $2 AND status = 'ACTIVE'`,
		userID, symbol).Scan(&p.BookRows, &p.BookQty, &timeScanner{&p.Evidence.OldestEntry}, &p.StrategyID)
	if err != nil {
		return nil, fmt.Errorf("ghost book: %w", err)
	}
	if p.BookRows == 0 {
		// Ledger-only ghost (IOLCP 2026-10-23): the position book and the
		// rules-engine already show the lot EXITED (manual exit), but
		// manthan_orders never received the SELL row, so trade-execution kept
		// protecting — and trying to exit — a lot the broker does not hold
		// (79 rejected stop re-placements in 20 minutes). Heal from the ledger
		// side under the same broker-verified evidence as a book ghost.
		// Settlement anchor = the NEWEST filled BUY of the strategy: a lot
		// bought yesterday (T+1, hidden by freeQty=0) must never pass as
		// "past settlement" on the strength of a round-trip closed months ago.
		var newest sql.NullTime
		lerr := a.fleet.execDB.QueryRowContext(ctx, `
			SELECT strategy_id, net, last_buy, (SELECT COUNT(*) - 1 FROM (
			           SELECT strategy_id FROM manthan_orders
			            WHERE user_id = $1 AND UPPER(symbol) = $2 AND status = 'FILLED'
			            GROUP BY strategy_id
			           HAVING SUM(CASE WHEN order_side='BUY' THEN filled_qty ELSE -filled_qty END) > 0) o) AS others
			  FROM (
			    SELECT strategy_id,
			           SUM(CASE WHEN order_side='BUY'  AND status='FILLED' THEN filled_qty
			                    WHEN order_side='SELL' AND status='FILLED' THEN -filled_qty
			                    ELSE 0 END) AS net,
			           MAX(CASE WHEN order_side='BUY' AND status='FILLED'
			                    THEN COALESCE(filled_at, created_at) END) AS last_buy
			      FROM manthan_orders
			     WHERE user_id = $1 AND UPPER(symbol) = $2
			     GROUP BY strategy_id) t
			 WHERE net > 0
			 ORDER BY net DESC, strategy_id
			 LIMIT 1`, userID, symbol).Scan(&p.StrategyID, &p.LedgerNetQty, &newest, &p.OtherStrategiesHolding)
		if lerr == sql.ErrNoRows {
			return nil, &refusal{code: 422, msg: fmt.Sprintf("no ACTIVE book rows and no ledger holding for %s/%s — nothing to heal", userID, symbol)}
		}
		if lerr != nil {
			return nil, fmt.Errorf("ghost ledger: %w", lerr)
		}
		if newest.Valid {
			t := newest.Time
			p.Evidence.OldestEntry = &t
		}
		p.LedgerOnly = true
	}
	if p.Evidence.OldestEntry != nil {
		p.Evidence.PastSettlement = time.Since(*p.Evidence.OldestEntry) > settlementWindow
	}

	// Ledger net (drives the FIX-5 row).
	if p.StrategyID != "" && !p.LedgerOnly {
		_ = a.fleet.execDB.QueryRowContext(ctx, `
			SELECT COALESCE(SUM(CASE WHEN order_side='BUY'  AND status='FILLED' THEN filled_qty
			                          WHEN order_side='SELL' AND status='FILLED' THEN -filled_qty
			                          ELSE 0 END), 0)
			  FROM manthan_orders WHERE strategy_id = $1 AND UPPER(symbol) = $2`,
			p.StrategyID, symbol).Scan(&p.LedgerNetQty)
	}

	// Broker side — the verification that makes free-hand edits impossible.
	// Either the caller's fresh read (reconciler: one fetch per user) or a
	// fetch of our own (manual flow).
	var holdings []indiraClient.Holding
	var book []indiraClient.OrderBook
	var trades []indiraClient.TradeBook
	if read != nil {
		holdings, book, trades = read.holdings, read.book, read.trades
	} else {
		auth, err := a.authFor(ctx, userID)
		if err != nil {
			return nil, err
		}
		if holdings, err = a.broker.GetHoldings(ctx, auth); err != nil {
			return nil, fmt.Errorf("ghost holdings: %w", err)
		}
		if book, err = a.broker.GetOrderBook(ctx, auth); err != nil {
			return nil, fmt.Errorf("ghost orderbook check: %w", err)
		}
		trades, _ = a.broker.GetTradeBook(ctx, auth) // best evidence, not required
	}
	// Keyed by EVERY name a holding can carry (baseSym, dispSym, dispSym with
	// its series suffix stripped): a BE/BZ/SM-series lot shows as
	// "SPECIALITY-BE" while our book says "SPECIALITY" — keyed by dispSym
	// alone it read as "holds 0" and a REAL holding could be healed away
	// (2026-10-23 review).
	totals := holdingsTotalsAllKeys(holdings)
	addOpenSellCommitmentsAllKeys(totals, book) // SL-committed qty is HELD (the intraday freeQty=0 trap)
	p.Evidence.BrokerQty = totals[symbol]
	p.Evidence.HoldingsAbsent = p.Evidence.BrokerQty == 0
	if !p.Evidence.HoldingsAbsent {
		return nil, &refusal{code: 422, msg: fmt.Sprintf(
			"broker HOLDS %d of %s — this is not a ghost; reconcile shows it as QTY_MISMATCH instead", p.Evidence.BrokerQty, symbol)}
	}

	// The ALIVUS lesson (2026-09-01 probe): qty fully committed to an OPEN
	// SELL order (a standing SL) has freeQty=0 and VANISHES from holdings —
	// looking exactly like a ghost while the shares are really there. An
	// open sell-side order at the broker is proof of holding: refuse.
	// The same pass records every live order id for the heal's stale-row
	// close (a ledger row whose broker order still stands is never closed).
	p.liveOrderIDs = map[string]bool{}
	for _, ob := range book {
		st := strings.ToUpper(strings.TrimSpace(ob.Status))
		if st != "EXECUTED" && st != "CANCELLED" && st != "CANCELED" && st != "REJECTED" && st != "EXPIRED" {
			p.liveOrderIDs[ob.OrdId] = true
		}
		if strings.EqualFold(ob.OrdAction, "SELL") && ob.Cancellable &&
			(strings.EqualFold(ob.Symbol.DispSym, symbol) || strings.EqualFold(ob.Symbol.BaseSym, symbol)) {
			p.Evidence.OpenSellOrder = ob.OrdId
			return nil, &refusal{code: 422, msg: fmt.Sprintf(
				"open SELL order %s (%s) stands at the broker for %s — the qty is committed there (freeQty=0 hides the holding); NOT a ghost. Square off or wait for that order to resolve",
				ob.OrdId, ob.Status, symbol)}
		}
	}

	// Tradebook: today's SELL executions for the symbol (best evidence).
	for _, tr := range trades {
		if !strings.EqualFold(tr.OrdAction, "SELL") {
			continue
		}
		if tradeSymbolMatches(tr, symbol) {
			p.Evidence.TradebookSell = true
			p.Evidence.TradebookQty += tr.TradedQty
			if tr.TradedPrice > 0 {
				p.Evidence.TradebookPrice = tr.TradedPrice
			}
		}
	}
	if p.Evidence.TradebookSell {
		p.ExitPrice = p.Evidence.TradebookPrice
	}

	if !p.Evidence.TradebookSell && !p.Evidence.PastSettlement {
		return nil, &refusal{code: 422, msg: fmt.Sprintf(
			"%s was entered within the settlement window and today's tradebook shows no SELL — an absent holdings row may be a hidden freeQty=0 row, not a ghost; wait out settlement or verify in the mirror", symbol)}
	}

	p.ConfirmationText = ghostConfirmation(p)
	return p, nil
}

// GhostHeal executes the previewed plan (caller has TYPED-matched the
// recomputed confirmation text). Closes book rows; writes the FIX-5
// synthetic SELL when a Manthan ledger net remains.
func (a *Actions) GhostHeal(ctx context.Context, p *GhostPlan) (map[string]any, error) {
	reason := p.exitReason()
	// The verdict travels with the closure: broker_state names what the broker
	// said (NOT_HELD / SOLD_TODAY), broker_state_reason is the sentence the
	// admin console and the client app render.
	state, note := BrokerStateNotHeld, p.closeNote()
	if reason == ExitReasonManualExit {
		state = BrokerStateSoldToday
	}
	res, err := a.fleet.posDB.ExecContext(ctx, `
		UPDATE positions
		   SET status = 'EXITED', exit_time = now(), exit_reason = $4,
		       exit_price = NULLIF($3::numeric, 0),
		       broker_state = $5, broker_state_reason = $6, broker_checked_at = now(),
		       updated_at = now()
		 WHERE user_id = $1 AND UPPER(symbol) = $2 AND status = 'ACTIVE'`,
		p.UserID, p.Symbol, p.ExitPrice, reason, state, note)
	if err != nil {
		return nil, fmt.Errorf("ghost heal book: %w", err)
	}
	closed, _ := res.RowsAffected()

	ledgerRow, eventWritten := false, false
	var stopRows int64
	var stopRowsLive []string
	if p.StrategyID != "" && p.LedgerNetQty > 0 {
		// One synthetic SELL per (user, strategy, symbol, day): without the
		// strategy in the key a second strategy's heal on the same day
		// silently wrote nothing (2026-10-23 review).
		sig := fmt.Sprintf("adminghost-%s-%s-%s-%s", p.UserID, shortID(p.StrategyID), p.Symbol, time.Now().Format("20060102"))
		var ledgerID int64
		err := a.fleet.execDB.QueryRowContext(ctx, `
			INSERT INTO manthan_orders
				(signal_id, strategy_id, user_id, symbol, order_type, order_side,
				 qty, filled_qty, status, avg_fill_price, exchange, filled_at, last_error)
			VALUES ($1, $2, $3, $4, 'MARKET_SELL', 'SELL', $5, $5, 'FILLED', NULLIF($6::numeric, 0), 'NSE', now(), $7)
			ON CONFLICT (signal_id) DO NOTHING
			RETURNING id`,
			sig, p.StrategyID, p.UserID, p.Symbol, p.LedgerNetQty, p.ExitPrice, note).Scan(&ledgerID)
		if err != nil && err != sql.ErrNoRows {
			return nil, fmt.Errorf("ghost heal ledger: %w", err)
		}
		ledgerRow = ledgerID > 0
		if !ledgerRow {
			// Same key already used today and the net is STILL > 0: nothing was
			// written — say so, and leave the stop rows alone (a parked
			// GHOST_SUSPECT row is what keeps Phase A from re-arming the ghost).
			return nil, &refusal{code: 409, msg: fmt.Sprintf(
				"a ghost-heal SELL for %s/%s/%s was already recorded today (%s) yet the ledger net is still %d — inspect manthan_orders before healing again",
				p.UserID, shortID(p.StrategyID), p.Symbol, sig, p.LedgerNetQty)}
		}
		// Durable trail: resolves the GHOST_SUSPECT / EXIT_CHASE_EXHAUSTED
		// rows in /admin/attention for this position.
		if _, eerr := a.fleet.execDB.ExecContext(ctx, `
			INSERT INTO manthan_order_events (order_id, event_type, old_status, new_status, broker_status, price, qty, detail)
			VALUES ($1, 'GHOST_HEALED', '', 'FILLED', 'ADMIN', $2, $3, $4)`,
			ledgerID, p.ExitPrice, p.LedgerNetQty,
			fmt.Sprintf("%s (%s): broker holds 0 — holdings absent, no open SELL, past settlement=%v, tradebook sell=%v",
				note, reason, p.Evidence.PastSettlement, p.Evidence.TradebookSell)); eerr == nil {
			eventWritten = true
		}
		// Stale protection rows of the lot (parked GHOST_SUSPECT, deferred,
		// placed-but-dead) would keep counting as "cover" and could be
		// promoted again by the trail or Phase A. A row whose broker order is
		// STILL LIVE in the book is never closed here — that would hide a
		// real order from the reconciler and the monitor; it is reported.
		if p.liveOrderIDs != nil {
			stopRows, stopRowsLive = a.closeStaleStopRows(ctx, nil, p.liveOrderIDs, p.StrategyID, p.Symbol, note)
		} else if auth, aerr := a.authFor(ctx, p.UserID); aerr != nil {
			stopRows = -1 // cannot prove the orders dead — close nothing
		} else {
			stopRows, stopRowsLive = a.closeStaleStopRows(ctx, auth, nil, p.StrategyID, p.Symbol, note)
		}
	}

	// Portfolio-slot release: mark the rules-engine's own row EXITED (same
	// semantics as its orphan cleanup). Its 60s orphan-scanner DB→memory
	// sync (SyncExitedFromDB) then books the exit in memory and evicts the
	// Redis cache keys — the slot frees within a tick, no restart needed.
	var slotRows int64
	if p.StrategyID != "" {
		// A PLEDGED row (slot already free, no trailing) that the broker now
		// shows gone is closed the same way.
		res, serr := a.fleet.tradingDB.ExecContext(ctx, `
			UPDATE manthan_positions
			   SET status = 'EXITED', exit_reason = $3,
			       exit_price = NULLIF($4::numeric, 0),
			       protective_last_status = $5, protective_last_reason = $6,
			       exit_time = now(), updated_at = now()
			 WHERE strategy_id = $1 AND UPPER(symbol) = $2 AND status IN ('ACTIVE','PLEDGED')`,
			p.StrategyID, p.Symbol, reason, p.ExitPrice, state, note)
		if serr != nil {
			// The book+ledger heal succeeded — don't fail the whole heal;
			// report so the operator knows the slot needs the manual UPDATE.
			slotRows = -1
		} else {
			slotRows, _ = res.RowsAffected()
		}
	}
	return map[string]any{"book_rows_closed": closed, "ledger_sell_written": ledgerRow, "event_written": eventWritten,
		"stop_rows_closed": stopRows, "stop_rows_left_live_at_broker": stopRowsLive, "ledger_only": p.LedgerOnly,
		"other_strategies_holding": p.OtherStrategiesHolding,
		"exit_price":               p.ExitPrice, "slot_rows_marked": slotRows}, nil
}

// closeStaleStopRows cancels the lot's protection rows in the ledger — only
// those whose broker order is absent from the book or terminal there. Rows
// whose order is still live are left untouched and returned by broker id.
// -1 rows means the ledger read/write itself failed. live, when non-nil, is
// the set of order ids seen live in a book fetched moments ago (the
// reconciler's one read per user); otherwise the book is fetched with auth.
func (a *Actions) closeStaleStopRows(ctx context.Context, auth *indiraClient.AuthContext, live map[string]bool, strategyID, symbol, note string) (int64, []string) {
	rows, err := a.fleet.execDB.QueryContext(ctx, `
		SELECT id, COALESCE(broker_order_id, '')
		  FROM manthan_orders
		 WHERE strategy_id = $1 AND UPPER(symbol) = $2 AND order_side = 'SELL'
		   AND order_type IN ('SL_SELL','SL_SELL_AMO')
		   AND status IN ('PENDING','SL_PLACED','SL_MODIFY_PENDING','SL_DEFERRED_BAND','AMO_PENDING')`,
		strategyID, symbol)
	if err != nil {
		return -1, nil
	}
	type stopRow struct {
		id       int64
		brokerID string
	}
	var candidates []stopRow
	for rows.Next() {
		var r stopRow
		if rows.Scan(&r.id, &r.brokerID) == nil {
			candidates = append(candidates, r)
		}
	}
	rows.Close()
	if len(candidates) == 0 {
		return 0, nil
	}
	needBook := false
	for _, c := range candidates {
		if c.brokerID != "" {
			needBook = true
		}
	}
	if needBook && live == nil {
		if auth == nil {
			return -1, nil // no broker truth — close nothing
		}
		book, berr := a.broker.GetOrderBook(ctx, auth)
		if berr != nil {
			return -1, nil // cannot prove the orders dead — close nothing
		}
		live = map[string]bool{}
		for _, ob := range book {
			st := strings.ToUpper(strings.TrimSpace(ob.Status))
			if st == "EXECUTED" || st == "CANCELLED" || st == "CANCELED" || st == "REJECTED" || st == "EXPIRED" {
				continue
			}
			live[ob.OrdId] = true
		}
	}
	if note == "" {
		note = "admin ghost cleanup"
	}
	var closed int64
	var left []string
	for _, c := range candidates {
		if c.brokerID != "" && live[c.brokerID] {
			left = append(left, c.brokerID)
			continue
		}
		res, uerr := a.fleet.execDB.ExecContext(ctx, `
			UPDATE manthan_orders
			   SET status = 'CANCELLED', broker_status = 'GHOST_HEALED', cancelled_at = now(), updated_at = now(),
			       last_error = $2
			 WHERE id = $1`, c.id, note+": stale stop row closed (broker holds nothing; order not live in the book)")
		if uerr != nil {
			return -1, left
		}
		n, _ := res.RowsAffected()
		closed += n
	}
	return closed, left
}

// tradeSymbolMatches digs the display symbol out of the tradebook's
// loosely-typed symbol object.
func tradeSymbolMatches(tr indiraClient.TradeBook, symbol string) bool {
	m, ok := tr.Symbol.(map[string]any)
	if !ok {
		return false
	}
	for _, k := range []string{"dispSym", "baseSym"} {
		if s, ok := m[k].(string); ok {
			if strings.EqualFold(s, symbol) {
				return true
			}
			if i := strings.LastIndex(s, "-"); i > 0 && strings.EqualFold(s[:i], symbol) {
				return true // "SPECIALITY-BE" is SPECIALITY
			}
		}
	}
	return false
}

// timeScanner scans a nullable timestamp into **time.Time.
type timeScanner struct{ t **time.Time }

func (s *timeScanner) Scan(v any) error {
	if tt, ok := v.(time.Time); ok && tt.Year() > 1971 {
		*s.t = &tt
	}
	return nil
}

// ── 7.3 square-off proxy ────────────────────────────────────────────────

// SquareOffContext is the preview: qty + rupee blast radius + the text.
type SquareOffContext struct {
	Ref              *StrategyRef `json:"strategy"`
	Symbol           string       `json:"symbol"`
	Qty              int          `json:"qty"`
	LTP              float64      `json:"ltp,omitempty"`
	ApproxValue      float64      `json:"approx_value,omitempty"`
	StandingSLs      int          `json:"standing_sls"`
	ConfirmationText string       `json:"confirmation_text"`
}

// SquareOffPreview computes the blast radius from live data.
func (a *Actions) SquareOffPreview(ctx context.Context, ref *StrategyRef, symbol string) (*SquareOffContext, error) {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	sc := &SquareOffContext{Ref: ref, Symbol: symbol}

	err := a.fleet.posDB.QueryRowContext(ctx, `
		SELECT COALESCE(SUM(quantity),0) FROM positions
		 WHERE strategy_id = $1 AND UPPER(symbol) = $2 AND status = 'ACTIVE'`,
		ref.StrategyID, symbol).Scan(&sc.Qty)
	if err != nil {
		return nil, fmt.Errorf("squareoff book: %w", err)
	}
	if sc.Qty <= 0 {
		return nil, &refusal{code: 422, msg: fmt.Sprintf("no ACTIVE position %s on this strategy — nothing to square off", symbol)}
	}
	var token string
	_ = a.fleet.execDB.QueryRowContext(ctx, `
		SELECT COALESCE(exchange_token,'') FROM manthan_orders
		 WHERE strategy_id = $1 AND UPPER(symbol) = $2 AND exchange_token IS NOT NULL AND exchange_token <> ''
		 ORDER BY id DESC LIMIT 1`, ref.StrategyID, symbol).Scan(&token)
	_ = a.fleet.execDB.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM manthan_orders
		 WHERE strategy_id = $1 AND UPPER(symbol) = $2
		   AND order_type IN ('SL_SELL','SL_SELL_AMO')
		   AND status IN ('SL_PLACED','SL_MODIFY_PENDING','AMO_PENDING')`,
		ref.StrategyID, symbol).Scan(&sc.StandingSLs)

	if a.ltp != nil && token != "" {
		if quotes, st := a.ltp.FetchByTokens(ctx, []string{token}); st == "HEALTHY" {
			if q, ok := quotes[token]; ok && q.LTP > 0 {
				sc.LTP = q.LTP
				sc.ApproxValue = q.LTP * float64(sc.Qty)
			}
		}
	}
	val := "VALUE UNKNOWN (LTP FEED DOWN)"
	if sc.ApproxValue > 0 {
		val = fmt.Sprintf("≈₹%.0f", sc.ApproxValue)
	}
	sc.ConfirmationText = fmt.Sprintf("SQUARE OFF %s ×%d FOR %s — CANCEL %d STOPS AND SELL AT MARKET %s",
		symbol, sc.Qty, ref.UserID, sc.StandingSLs, val)
	return sc, nil
}

// SquareOffExecute proxies to trade-execution's supervised operation.
func (a *Actions) SquareOffExecute(ctx context.Context, ref *StrategyRef, symbol string) (string, error) {
	u := fmt.Sprintf("%s/manthan/admin/squareoff?user_id=%s&strategy_id=%s&symbol=%s",
		a.teMetricsURL, url.QueryEscape(ref.UserID), url.QueryEscape(ref.StrategyID), url.QueryEscape(strings.ToUpper(symbol)))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, nil)
	if err != nil {
		return "", err
	}
	resp, err := a.httpc.Do(req)
	if err != nil {
		return "", fmt.Errorf("trade-execution unreachable: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
	if resp.StatusCode != http.StatusOK {
		return "", &refusal{code: 502, msg: fmt.Sprintf("trade-execution refused square-off (%d): %s", resp.StatusCode, strings.TrimSpace(string(body)))}
	}
	return string(body), nil
}

// ── 7.7 rebalance preview / trigger ─────────────────────────────────────

const rebalanceOutputCap = 64 * 1024

// Rebalance runs the operator CLI. dryRun=true never publishes.
func (a *Actions) Rebalance(ctx context.Context, userID string, dryRun bool) (string, error) {
	if a.rebalancerBin == "" {
		return "", &refusal{code: 503, msg: "rebalancer binary not configured on this host (REBALANCER_BIN)"}
	}
	args := []string{}
	if dryRun {
		args = append(args, "--dry-run")
	}
	if userID != "" {
		args = append(args, "--user", userID)
	}
	ctx, cancel := context.WithTimeout(ctx, 110*time.Second)
	defer cancel()
	out, err := a.run(ctx, a.rebalancerDir, a.rebalancerEnv, a.rebalancerBin, args...)
	if len(out) > rebalanceOutputCap {
		out = out[len(out)-rebalanceOutputCap:]
	}
	if err != nil {
		return string(out), fmt.Errorf("rebalancer run failed: %w (output tail attached)", err)
	}
	return string(out), nil
}
