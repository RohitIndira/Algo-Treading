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
	"fmt"
	"io"
	"net/http"
	"net/url"
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
type cmdRunner func(ctx context.Context, dir, bin string, args ...string) ([]byte, error)

func execRunner(ctx context.Context, dir, bin string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Dir = dir
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
	rebalancerBin string // absolute path to the built CLI ("" disables 7.7)
	rebalancerDir string // cwd for the run (repo root: .env discovery)
	run           cmdRunner
}

func NewActions(fleet *FleetStore, creds credentialsFetcher, broker actionBroker,
	strategies *StrategyControl, ltp LTPFeed, teMetricsURL, rebalancerBin, rebalancerDir string) *Actions {
	return &Actions{
		fleet: fleet, creds: creds, broker: broker, strategies: strategies, ltp: ltp,
		teMetricsURL: strings.TrimRight(teMetricsURL, "/"),
		httpc:        &http.Client{Timeout: 120 * time.Second},
		rebalancerBin: rebalancerBin, rebalancerDir: rebalancerDir,
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
	BrokerOrderID string                 `json:"broker_order_id"`
	BrokerFound   bool                   `json:"broker_found"`
	Broker        *indiraClient.OrderBook `json:"broker,omitempty"`
	Ledger        map[string]any         `json:"ledger,omitempty"` // our manthan_orders row, if any
	Verdict       string                 `json:"verdict"`          // LIVE | TERMINAL | VANISHED
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
	HoldingsAbsent   bool       `json:"holdings_absent"`
	BrokerQty        int        `json:"broker_qty"` // >0 → NOT a ghost
	TradebookSell    bool       `json:"tradebook_sell"`
	TradebookPrice   float64    `json:"tradebook_price,omitempty"`
	TradebookQty     int        `json:"tradebook_qty,omitempty"`
	OldestEntry      *time.Time `json:"oldest_entry,omitempty"`
	PastSettlement   bool       `json:"past_settlement_window"`
}

// GhostPlan is the previewed heal.
type GhostPlan struct {
	UserID           string        `json:"user_id"`
	Symbol           string        `json:"symbol"`
	BookRows         int           `json:"book_rows"`
	BookQty          int           `json:"book_qty"`
	LedgerNetQty     int           `json:"ledger_net_qty"` // manthan_orders net; >0 gets the FIX-5 SELL row
	StrategyID       string        `json:"strategy_id,omitempty"`
	Evidence         GhostEvidence `json:"evidence"`
	ExitPrice        float64       `json:"exit_price,omitempty"` // tradebook-backed; 0 = reconciled later
	ConfirmationText string        `json:"confirmation_text"`
}

func ghostConfirmation(p *GhostPlan) string {
	return fmt.Sprintf("HEAL GHOST %s FOR %s — CLOSE %d BOOK ROWS ×%d", p.Symbol, p.UserID, p.BookRows, p.BookQty)
}

// GhostPreview assembles the broker-verified plan. Every call re-fetches
// live evidence — a stale preview can never authorize a heal (the TYPED
// text is recomputed at heal time and must still match).
func (a *Actions) GhostPreview(ctx context.Context, userID, symbol string) (*GhostPlan, error) {
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
		return nil, &refusal{code: 422, msg: fmt.Sprintf("no ACTIVE book rows for %s/%s — nothing to heal", userID, symbol)}
	}
	if p.Evidence.OldestEntry != nil {
		p.Evidence.PastSettlement = time.Since(*p.Evidence.OldestEntry) > settlementWindow
	}

	// Ledger net (drives the FIX-5 row).
	if p.StrategyID != "" {
		_ = a.fleet.execDB.QueryRowContext(ctx, `
			SELECT COALESCE(SUM(CASE WHEN order_side='BUY'  AND status='FILLED' THEN filled_qty
			                          WHEN order_side='SELL' AND status='FILLED' THEN -filled_qty
			                          ELSE 0 END), 0)
			  FROM manthan_orders WHERE strategy_id = $1 AND UPPER(symbol) = $2`,
			p.StrategyID, symbol).Scan(&p.LedgerNetQty)
	}

	// Broker side — the verification that makes free-hand edits impossible.
	auth, err := a.authFor(ctx, userID)
	if err != nil {
		return nil, err
	}
	holdings, err := a.broker.GetHoldings(ctx, auth)
	if err != nil {
		return nil, fmt.Errorf("ghost holdings: %w", err)
	}
	totals := holdingsTotals(holdings)
	p.Evidence.BrokerQty = totals[symbol]
	p.Evidence.HoldingsAbsent = p.Evidence.BrokerQty == 0
	if !p.Evidence.HoldingsAbsent {
		return nil, &refusal{code: 422, msg: fmt.Sprintf(
			"broker HOLDS %d of %s — this is not a ghost; reconcile shows it as QTY_MISMATCH instead", p.Evidence.BrokerQty, symbol)}
	}

	// Tradebook: today's SELL executions for the symbol (best evidence).
	if trades, terr := a.broker.GetTradeBook(ctx, auth); terr == nil {
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
	res, err := a.fleet.posDB.ExecContext(ctx, `
		UPDATE positions
		   SET status = 'EXITED', exit_time = now(), exit_reason = 'ADMIN_GHOST_CLEANUP',
		       exit_price = NULLIF($3, 0)
		 WHERE user_id = $1 AND UPPER(symbol) = $2 AND status = 'ACTIVE'`,
		p.UserID, p.Symbol, p.ExitPrice)
	if err != nil {
		return nil, fmt.Errorf("ghost heal book: %w", err)
	}
	closed, _ := res.RowsAffected()

	ledgerRow := false
	if p.StrategyID != "" && p.LedgerNetQty > 0 {
		sig := fmt.Sprintf("adminghost-%s-%s-%s", p.UserID, p.Symbol, time.Now().Format("20060102"))
		if _, err := a.fleet.execDB.ExecContext(ctx, `
			INSERT INTO manthan_orders
				(signal_id, strategy_id, user_id, symbol, order_type, order_side,
				 qty, filled_qty, status, avg_fill_price, exchange, filled_at, last_error)
			VALUES ($1, $2, $3, $4, 'MARKET_SELL', 'SELL', $5, $5, 'FILLED', NULLIF($6,0), 'NSE', now(),
			        'admin ghost cleanup: broker-verified missing SELL recorded (FIX-5 pattern)')
			ON CONFLICT (signal_id) DO NOTHING`,
			sig, p.StrategyID, p.UserID, p.Symbol, p.LedgerNetQty, p.ExitPrice); err != nil {
			return nil, fmt.Errorf("ghost heal ledger: %w", err)
		}
		ledgerRow = true
	}
	return map[string]any{"book_rows_closed": closed, "ledger_sell_written": ledgerRow, "exit_price": p.ExitPrice}, nil
}

// tradeSymbolMatches digs the display symbol out of the tradebook's
// loosely-typed symbol object.
func tradeSymbolMatches(tr indiraClient.TradeBook, symbol string) bool {
	m, ok := tr.Symbol.(map[string]any)
	if !ok {
		return false
	}
	for _, k := range []string{"dispSym", "baseSym"} {
		if s, ok := m[k].(string); ok && strings.EqualFold(s, symbol) {
			return true
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
	out, err := a.run(ctx, a.rebalancerDir, a.rebalancerBin, args...)
	if len(out) > rebalanceOutputCap {
		out = out[len(out)-rebalanceOutputCap:]
	}
	if err != nil {
		return string(out), fmt.Errorf("rebalancer run failed: %w (output tail attached)", err)
	}
	return string(out), nil
}

