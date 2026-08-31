package admin

// M5.1 — protection state board: every open position with its protection
// state, trail context and distance-to-trigger. The six hand-written
// queries of the Aug 27 SL audit, as one view.
//
// M5.4 — price-scale / corp-action check: trail trigger vs live LTP ratio
// out of band, or book qty ≠ broker qty. The check that proved the GNA
// scare false, run nightly (08:35 IST — after the 08:30 credential sweep,
// before the 08:50 AMO conversion) and on demand.
//
// Sources (same handles the fleet store already holds):
//   positions_db positions            — the open book (qty, entry, dup rows)
//   execution_db manthan_orders       — SL ledger: state + triggers + token
//   trading_db   manthan_positions    — trail state (high-water, current SL)
//   livealgos LTP store (optional)    — live quotes for distance/scale
//
// States, in precedence order per symbol:
//   ARMED         — a standing broker stop (SL_PLACED); trigger shown
//   AMO_PENDING   — overnight AMO queue row awaiting 08:50 conversion
//   DEFERRED_BAND — outside the DPR band by design; intended trigger shown
//                   (software-supervised, hard-20%-rule: defer, never clamp)
//   CAPPED        — the give-up predicate (≥5 failed overnight attempts in
//                   24h, none standing): the system stopped retrying
//   NAKED         — an open position none of the above protects

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/livealgos"
)

// LTPFeed is the slice of livealgos.LTPStore the board needs; an
// interface so tests stub quotes without Redis and main.go can pass an
// untyped nil when the feed is unwired.
type LTPFeed interface {
	FetchByTokens(ctx context.Context, tokens []string) (map[string]livealgos.LTPQuote, livealgos.Status)
}

// ProtectionRow is one open position on the board.
type ProtectionRow struct {
	UserID     string `json:"user_id"`
	StrategyID string `json:"strategy_id"`
	Symbol     string `json:"symbol"`
	State      string `json:"state"` // ARMED | AMO_PENDING | DEFERRED_BAND | CAPPED | NAKED

	Qty      int `json:"qty"`
	BookRows int `json:"book_rows"` // >1 = the duplicate-row defect, surfaced not hidden

	// Trail context (rules-engine's manthan book; zero when absent).
	EntryPrice     float64 `json:"entry_price,omitempty"`
	HighSinceEntry float64 `json:"high_since_entry,omitempty"`
	CurrentSL      float64 `json:"current_sl,omitempty"`

	// Ledger context.
	BrokerTrigger   float64 `json:"broker_trigger,omitempty"`   // ARMED: what the broker holds
	IntendedTrigger float64 `json:"intended_trigger,omitempty"` // DEFERRED/AMO: what we want
	BrokerOrderID   string  `json:"broker_order_id,omitempty"`
	FailedAttempts  int     `json:"failed_attempts_24h,omitempty"` // CAPPED context

	// Live context (zero/absent when the LTP feed is down).
	LTP                  float64 `json:"ltp,omitempty"`
	DistanceToTriggerPct float64 `json:"distance_to_trigger_pct,omitempty"` // (LTP−trigger)/LTP×100
}

// ProtectionBoard is the whole-fleet view.
type ProtectionBoard struct {
	Rows        []ProtectionRow `json:"rows"`
	Counts      map[string]int  `json:"counts"`
	LTPStatus   string          `json:"ltp_status"` // HEALTHY | UNAVAILABLE
	GeneratedAt time.Time       `json:"generated_at"`
}

// ProtectionStore assembles the board.
type ProtectionStore struct {
	fleet *FleetStore
	ltp   LTPFeed // nil-safe: board renders without live distance
}

func NewProtectionStore(fleet *FleetStore, ltp LTPFeed) *ProtectionStore {
	return &ProtectionStore{fleet: fleet, ltp: ltp}
}

// ledgerState is the per-symbol SL-ledger summary for one strategy.
type ledgerState struct {
	state         string // ARMED | AMO_PENDING | DEFERRED_BAND | ""
	brokerTrig    float64
	intendedTrig  float64
	brokerOrderID string
	token         string
}

// Board builds the fleet-wide protection view.
func (p *ProtectionStore) Board(ctx context.Context) (*ProtectionBoard, error) {
	board := &ProtectionBoard{Counts: map[string]int{}, GeneratedAt: time.Now(), LTPStatus: string(livealgos.StatusUnavailable)}

	// 1) The open book, deduped per symbol with the duplicate count kept
	// visible (BALUFORGE ×17 must show as 17, not be silently collapsed).
	rows, err := p.fleet.posDB.QueryContext(ctx, `
		SELECT user_id, strategy_id, symbol, COUNT(*), SUM(quantity)
		  FROM positions WHERE status = 'ACTIVE'
		 GROUP BY user_id, strategy_id, symbol`)
	if err != nil {
		return nil, fmt.Errorf("protection book: %w", err)
	}
	for rows.Next() {
		var r ProtectionRow
		if err := rows.Scan(&r.UserID, &r.StrategyID, &r.Symbol, &r.BookRows, &r.Qty); err != nil {
			rows.Close()
			return nil, err
		}
		r.Symbol = strings.ToUpper(r.Symbol)
		board.Rows = append(board.Rows, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	strategies := map[string]bool{}
	for _, r := range board.Rows {
		strategies[r.StrategyID] = true
	}

	// 2) SL ledger per strategy: open protection rows, precedence in Go.
	ledger := map[string]map[string]*ledgerState{} // strategy → symbol → state
	for sid := range strategies {
		ls, err := p.ledgerFor(ctx, sid)
		if err != nil {
			return nil, err
		}
		ledger[sid] = ls
	}

	// 3) Give-ups (CAPPED), one aggregate over the whole ledger — the
	// exact predicate the attention queue uses.
	capped, err := p.cappedSymbols(ctx)
	if err != nil {
		return nil, err
	}

	// 4) Trail state from the manthan book.
	trail, err := p.trailFor(ctx, strategies)
	if err != nil {
		return nil, err
	}

	// 5) Assemble states + collect tokens for the LTP pass.
	tokens := map[string]string{} // symbol → token (last wins; tokens are per-symbol stable)
	for i := range board.Rows {
		r := &board.Rows[i]
		if t, ok := trail[r.StrategyID+"|"+r.Symbol]; ok {
			r.EntryPrice, r.HighSinceEntry, r.CurrentSL = t.entry, t.high, t.sl
		}
		ls := ledger[r.StrategyID][r.Symbol]
		switch {
		case ls != nil && ls.state != "":
			r.State = ls.state
			r.BrokerTrigger, r.IntendedTrigger, r.BrokerOrderID = ls.brokerTrig, ls.intendedTrig, ls.brokerOrderID
			if ls.token != "" {
				tokens[r.Symbol] = ls.token
			}
		case capped[r.StrategyID+"|"+r.Symbol] > 0:
			r.State = "CAPPED"
			r.FailedAttempts = capped[r.StrategyID+"|"+r.Symbol]
		default:
			r.State = "NAKED"
		}
		board.Counts[r.State]++
	}

	// 6) Live distance where the feed is up. Board stays useful without it.
	p.applyLTP(ctx, board, tokens)

	sort.Slice(board.Rows, func(i, j int) bool {
		a, b := board.Rows[i], board.Rows[j]
		if pa, pb := stateRank(a.State), stateRank(b.State); pa != pb {
			return pa < pb // worst first: the board's job is triage
		}
		if a.UserID != b.UserID {
			return a.UserID < b.UserID
		}
		return a.Symbol < b.Symbol
	})
	return board, nil
}

func stateRank(s string) int {
	switch s {
	case "NAKED":
		return 0
	case "CAPPED":
		return 1
	case "DEFERRED_BAND":
		return 2
	case "AMO_PENDING":
		return 3
	default: // ARMED
		return 4
	}
}

func (p *ProtectionStore) ledgerFor(ctx context.Context, strategyID string) (map[string]*ledgerState, error) {
	rows, err := p.fleet.execDB.QueryContext(ctx, `
		SELECT symbol, status,
		       COALESCE(trigger_price, 0), COALESCE(broker_trigger_price, 0),
		       COALESCE(broker_order_id, ''), COALESCE(exchange_token, '')
		  FROM manthan_orders
		 WHERE strategy_id = $1
		   AND order_type IN ('SL_SELL','SL_SELL_AMO')
		   AND status IN ('SL_PLACED','AMO_PENDING','SL_DEFERRED_BAND')
		 ORDER BY updated_at ASC`, strategyID) // later rows overwrite within a state
	if err != nil {
		return nil, fmt.Errorf("protection ledger: %w", err)
	}
	defer rows.Close()

	out := map[string]*ledgerState{}
	for rows.Next() {
		var sym, status, brokerID, token string
		var trig, brokerTrig float64
		if err := rows.Scan(&sym, &status, &trig, &brokerTrig, &brokerID, &token); err != nil {
			return nil, err
		}
		sym = strings.ToUpper(sym)
		ls := out[sym]
		if ls == nil {
			ls = &ledgerState{}
			out[sym] = ls
		}
		if token != "" {
			ls.token = token
		}
		// Precedence: ARMED beats AMO_PENDING beats DEFERRED_BAND.
		switch status {
		case "SL_PLACED":
			ls.state = "ARMED"
			if brokerTrig > 0 {
				ls.brokerTrig = brokerTrig
			} else {
				ls.brokerTrig = trig
			}
			ls.brokerOrderID = brokerID
		case "AMO_PENDING":
			if ls.state != "ARMED" {
				ls.state, ls.intendedTrig, ls.brokerOrderID = "AMO_PENDING", trig, brokerID
			}
		case "SL_DEFERRED_BAND":
			if ls.state == "" {
				ls.state, ls.intendedTrig = "DEFERRED_BAND", trig
			}
		}
	}
	return out, rows.Err()
}

// cappedSymbols runs the give-up predicate over the whole ledger:
// ≥5 failed overnight-stop attempts in 24h with none standing.
func (p *ProtectionStore) cappedSymbols(ctx context.Context) (map[string]int, error) {
	rows, err := p.fleet.execDB.QueryContext(ctx, `
		SELECT strategy_id, symbol,
		       COUNT(*) FILTER (WHERE status IN ('CANCELLED','REJECTED')) AS failed
		  FROM manthan_orders
		 WHERE order_type = 'SL_SELL_AMO'
		   AND created_at >= now() - interval '24 hours'
		 GROUP BY strategy_id, symbol
		HAVING COUNT(*) FILTER (WHERE status IN ('CANCELLED','REJECTED')) >= 5
		   AND NOT bool_or(status = 'SL_PLACED')`)
	if err != nil {
		return nil, fmt.Errorf("protection giveups: %w", err)
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var sid, sym string
		var n int
		if err := rows.Scan(&sid, &sym, &n); err != nil {
			return nil, err
		}
		out[sid+"|"+strings.ToUpper(sym)] = n
	}
	return out, rows.Err()
}

type trailState struct{ entry, high, sl float64 }

func (p *ProtectionStore) trailFor(ctx context.Context, strategies map[string]bool) (map[string]trailState, error) {
	out := map[string]trailState{}
	for sid := range strategies {
		rows, err := p.fleet.tradingDB.QueryContext(ctx, `
			SELECT symbol, COALESCE(entry_price,0), COALESCE(high_since_entry,0), COALESCE(current_sl,0)
			  FROM manthan_positions
			 WHERE strategy_id = $1::uuid AND status = 'ACTIVE'`, sid)
		if err != nil {
			return nil, fmt.Errorf("protection trail: %w", err)
		}
		for rows.Next() {
			var sym string
			var t trailState
			if err := rows.Scan(&sym, &t.entry, &t.high, &t.sl); err != nil {
				rows.Close()
				return nil, err
			}
			out[sid+"|"+strings.ToUpper(sym)] = t
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// applyLTP fills LTP + distance on rows whose token we know. Trigger for
// distance = the broker's standing trigger when armed, else the trail SL.
func (p *ProtectionStore) applyLTP(ctx context.Context, board *ProtectionBoard, tokens map[string]string) {
	if p.ltp == nil || len(tokens) == 0 {
		// Feed unwired or no tokens known: try trail-only rows anyway is
		// impossible without tokens — status stays UNAVAILABLE unless the
		// fetcher itself reports healthy on an empty ask.
		if p.ltp == nil {
			return
		}
	}
	list := make([]string, 0, len(tokens))
	for _, t := range tokens {
		list = append(list, t)
	}
	quotes, status := p.ltp.FetchByTokens(ctx, list)
	board.LTPStatus = string(status)
	if status != livealgos.StatusHealthy {
		return
	}
	byToken := map[string]livealgos.LTPQuote{}
	for k, q := range quotes {
		byToken[k] = q
	}
	for i := range board.Rows {
		r := &board.Rows[i]
		tok := tokens[r.Symbol]
		if tok == "" {
			continue
		}
		q, ok := byToken[tok]
		if !ok || q.LTP <= 0 {
			continue
		}
		r.LTP = q.LTP
		trigger := r.BrokerTrigger
		if trigger == 0 {
			trigger = r.IntendedTrigger
		}
		if trigger == 0 {
			trigger = r.CurrentSL
		}
		if trigger > 0 {
			r.DistanceToTriggerPct = (q.LTP - trigger) / q.LTP * 100
		}
	}
}

// ── M5.4 price-scale / corp-action check ────────────────────────────────

// ScaleFinding is one out-of-band observation.
type ScaleFinding struct {
	UserID     string  `json:"user_id"`
	StrategyID string  `json:"strategy_id"`
	Symbol     string  `json:"symbol"`
	Kind       string  `json:"kind"` // TRIGGER_SCALE | QTY_DRIFT
	CurrentSL  float64 `json:"current_sl,omitempty"`
	LTP        float64 `json:"ltp,omitempty"`
	BookQty    int     `json:"book_qty,omitempty"`
	BrokerQty  int     `json:"broker_qty,omitempty"`
	Detail     string  `json:"detail"`
}

// ScaleChecker runs the nightly check and remembers the last result for
// the attention queue (in-memory, like the credential sweep — a restart
// forgets, the next run relearns).
type ScaleChecker struct {
	prot  *ProtectionStore
	recon *ReconStore // broker qty leg; nil-safe (LTP-only checks then)

	mu        sync.Mutex
	last      []ScaleFinding
	checkedAt time.Time
}

func NewScaleChecker(prot *ProtectionStore, recon *ReconStore) *ScaleChecker {
	return &ScaleChecker{prot: prot, recon: recon}
}

// evaluateTrigger is the pure band rule: a trail trigger at/above market
// should have FIRED (frozen protection, or the price scale changed under
// us — split/bonus); one below 40% of market is the same break the other
// way. Returns "" when in band.
func evaluateTrigger(currentSL, ltp float64) string {
	if currentSL <= 0 || ltp <= 0 {
		return ""
	}
	ratio := currentSL / ltp
	switch {
	case ratio >= 1.0:
		return fmt.Sprintf("trail trigger %.2f AT/ABOVE LTP %.2f (ratio %.2f) — stop should have fired: protection frozen, or corp-action scale change", currentSL, ltp, ratio)
	case ratio < 0.4:
		return fmt.Sprintf("trail trigger %.2f is <40%% of LTP %.2f (ratio %.2f) — scale break suspected (split/bonus adjustment missing)", currentSL, ltp, ratio)
	}
	return ""
}

// Run executes one full check and stores the findings.
func (s *ScaleChecker) Run(ctx context.Context) ([]ScaleFinding, error) {
	board, err := s.prot.Board(ctx)
	if err != nil {
		return nil, err
	}

	var findings []ScaleFinding

	// Leg 1: trigger vs LTP (only meaningful when the feed answered).
	if board.LTPStatus == string(livealgos.StatusHealthy) {
		for _, r := range board.Rows {
			if msg := evaluateTrigger(r.CurrentSL, r.LTP); msg != "" {
				findings = append(findings, ScaleFinding{
					UserID: r.UserID, StrategyID: r.StrategyID, Symbol: r.Symbol,
					Kind: "TRIGGER_SCALE", CurrentSL: r.CurrentSL, LTP: r.LTP, Detail: msg,
				})
			}
		}
	}

	// Leg 2: book qty vs broker qty, per user with a working credential.
	if s.recon != nil {
		byUser := map[string][]ProtectionRow{}
		for _, r := range board.Rows {
			byUser[r.UserID] = append(byUser[r.UserID], r)
		}
		for uid, rows := range byUser {
			totals, legStatus := s.recon.brokerTotals(ctx, uid)
			if legStatus != "OK" {
				continue // dead credential: the sweep already screams about it
			}
			for _, r := range rows {
				if bq, ok := totals[r.Symbol]; ok && bq != r.Qty {
					findings = append(findings, ScaleFinding{
						UserID: uid, StrategyID: r.StrategyID, Symbol: r.Symbol,
						Kind: "QTY_DRIFT", BookQty: r.Qty, BrokerQty: bq,
						Detail: fmt.Sprintf("book holds %d, broker holds %d — corp action or missed fill; protection sized wrong", r.Qty, bq),
					})
				}
			}
		}
	}

	s.mu.Lock()
	s.last, s.checkedAt = findings, time.Now()
	s.mu.Unlock()
	return findings, nil
}

// LastRun returns a copy of the most recent findings.
func (s *ScaleChecker) LastRun() ([]ScaleFinding, time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]ScaleFinding, len(s.last))
	copy(cp, s.last)
	return cp, s.checkedAt
}

// findingsAsAttention feeds the queue, credential-sweep style.
func (s *ScaleChecker) findingsAsAttention() []AttentionItem {
	findings, at := s.LastRun()
	if at.IsZero() {
		return nil
	}
	var items []AttentionItem
	for _, f := range findings {
		items = append(items, AttentionItem{
			Severity: "HIGH", Kind: "SCALE_" + f.Kind, UserID: f.UserID,
			Strategy: f.StrategyID, Symbol: f.Symbol,
			Detail: fmt.Sprintf("scale check %s: %s", at.Format("15:04"), f.Detail),
			Module: "M5",
		})
	}
	return items
}

// StartDaily runs the check every day at 08:35 IST — after the 08:30
// credential sweep (fresh broker sessions), before the 08:50 AMO
// conversion and the bell.
func (s *ScaleChecker) StartDaily(ctx context.Context, ist *time.Location) {
	go func() {
		for {
			now := time.Now().In(ist)
			next := time.Date(now.Year(), now.Month(), now.Day(), 8, 35, 0, 0, ist)
			if !next.After(now) {
				next = next.Add(24 * time.Hour)
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Until(next)):
			}
			findings, err := s.Run(ctx)
			if err != nil {
				log.Printf("admin: daily scale check failed: %v", err)
				continue
			}
			for _, f := range findings {
				log.Printf("⚠ SCALE CHECK: %s %s %s — %s", f.UserID, f.Symbol, f.Kind, f.Detail)
			}
			log.Printf("admin: daily scale check done — %d finding(s)", len(findings))
		}
	}()
}

