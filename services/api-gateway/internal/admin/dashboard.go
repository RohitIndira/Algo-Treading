package admin

// M12 — Admin Dashboard + Clients analytics (read-only endpoints).
//
// Every book-wide function takes a userID: "" is the whole book (the
// /portfolio/* overview), a client id narrows to that client — reached
// either via ?client_id= on the same /portfolio/* routes or in one shot
// via GET /clients/{id}/dashboard, which composes every panel.
//
// Data sources (all reads, no writes):
//   trading_db.manthan_positions   — the rules-engine position book: entry/exit,
//                                    realized_pnl, industry, mcap_bucket,
//                                    ema_alloc_pct, invested_amt, signal_id
//   trading_db.strategies          — client strategy roster (active flag)
//   trading_db.trade_configs       — total_capital (invested fund per strategy)
//   execution_db.manthan_orders    — symbol → exchange_token for LTP lookups
//   execution_db.signal_inbox      — signal_id → created_at (signal entry time)
//   stockk_market.strategy_nav_daily — per-strategy daily NAV snapshots (the
//                                    backbone of every time series here)
//   stockk_market.benchmark_daily  — Nifty 50 closes for the equity overlay
//
// Position-book status mapping: OPEN = ACTIVE|EXIT_PENDING, CLOSED = EXITED.
// PENDING_ENTRY and EXPIRED rows are excluded from analytics (they never
// held stock) but do appear in the raw positions matrix with their own status.
//
// client_name: the platform stores no display names — client_name mirrors
// client_code (user_id) until a names source exists.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"

	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/livealgos"
)

// DashboardStore owns the SQL + LTP access for the M12 endpoints.
type DashboardStore struct {
	tradingDB *sql.DB // manthan_positions, strategies, trade_configs
	execDB    *sql.DB // manthan_orders (tokens), signal_inbox (signal times)
	perfDB    *sql.DB // strategy_nav_daily, benchmark_daily
	signalsDB *sql.DB // manthan_stocks (52-week highs); nil-safe
	ltp       LTPFeed // nil-safe: prices degrade to entry-price valuation
	ist       *time.Location
	now       func() time.Time
}

func NewDashboardStore(tradingDB, execDB, perfDB, signalsDB *sql.DB, ltp LTPFeed) *DashboardStore {
	loc, err := time.LoadLocation("Asia/Kolkata")
	if err != nil || loc == nil {
		loc = time.FixedZone("IST", 5*3600+30*60)
	}
	return &DashboardStore{
		tradingDB: tradingDB, execDB: execDB, perfDB: perfDB, signalsDB: signalsDB,
		ltp: ltp, ist: loc, now: time.Now,
	}
}

// openStatuses is the SQL fragment for "position currently holds stock".
const openStatuses = "('ACTIVE','EXIT_PENDING')"

// ── NAV series (shared by every time-series endpoint) ──────────────────

type navDay struct {
	Date       time.Time
	NetPnL     float64 // SUM(net_pnl_amount) across matching strategies
	Capital    float64 // SUM(deployed_capital)
	OpenPos    int     // SUM(open_positions)
	Unrealized float64
	Realized   float64
}

// navSeries returns the daily NAV aggregated across strategies, ascending by
// date. userID == "" aggregates the whole book.
func (d *DashboardStore) navSeries(ctx context.Context, userID string, days int) ([]navDay, error) {
	since := d.now().In(d.ist).AddDate(0, 0, -days).Format("2006-01-02")
	q := `SELECT date, SUM(net_pnl_amount), SUM(deployed_capital),
	             SUM(open_positions), SUM(unrealized_amount), SUM(realized_amount)
	      FROM strategy_nav_daily WHERE date >= $1`
	args := []any{since}
	if userID != "" {
		q += ` AND user_id = $2`
		args = append(args, userID)
	}
	q += ` GROUP BY date ORDER BY date`
	rows, err := d.perfDB.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("nav series: %w", err)
	}
	defer rows.Close()
	var out []navDay
	for rows.Next() {
		var n navDay
		if err := rows.Scan(&n.Date, &n.NetPnL, &n.Capital, &n.OpenPos, &n.Unrealized, &n.Realized); err != nil {
			return nil, fmt.Errorf("nav scan: %w", err)
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// daysParam parses ?days= with a default and sane clamps.
func daysParam(r *http.Request, def int) int {
	v, err := strconv.Atoi(r.URL.Query().Get("days"))
	if err != nil || v <= 0 {
		return def
	}
	if v > 730 {
		return 730
	}
	return v
}

func round2f(v float64) float64 { return float64(int64(v*100+0.5*sign(v))) / 100 }

func sign(v float64) float64 {
	if v < 0 {
		return -1
	}
	return 1
}

func pct(part, whole float64) float64 {
	if whole == 0 {
		return 0
	}
	return round2f(part / whole * 100)
}

// ── 1. P&L history ──────────────────────────────────────────────────────

// PnLHistory serves both the portfolio chart (userID == "") and the
// per-client mirror (§4a of the frontend gaps doc).
func (d *DashboardStore) PnLHistory(ctx context.Context, userID string, days int) (any, error) {
	nav, err := d.navSeries(ctx, userID, days)
	if err != nil {
		return nil, err
	}
	type point struct {
		Date          string  `json:"date"`
		PnL           float64 `json:"pnl"`
		PnLPct        float64 `json:"pnl_pct"`
		CumulativePnL float64 `json:"cumulative_pnl"`
	}
	series := make([]point, 0, len(nav))
	prev := 0.0
	for i, n := range nav {
		daily := n.NetPnL - prev
		if i == 0 {
			daily = 0 // first visible day: no prior row to diff against
		}
		series = append(series, point{
			Date: n.Date.Format("2006-01-02"), PnL: round2f(daily),
			PnLPct: pct(daily, n.Capital), CumulativePnL: round2f(n.NetPnL),
		})
		prev = n.NetPnL
	}
	return map[string]any{"series": series}, nil
}

// ── 2. Position-count history ───────────────────────────────────────────

func (d *DashboardStore) PositionHistory(ctx context.Context, userID string, days int) (any, error) {
	nav, err := d.navSeries(ctx, userID, days)
	if err != nil {
		return nil, err
	}
	type point struct {
		Date          string `json:"date"`
		PositionCount int    `json:"position_count"`
		ActiveCount   int    `json:"active_count"`
		PausedCount   int    `json:"paused_count"`
	}
	series := make([]point, 0, len(nav))
	for _, n := range nav {
		// Strategy pause state isn't snapshotted historically, so
		// active == total and paused == 0; honest zero over a guess.
		series = append(series, point{
			Date: n.Date.Format("2006-01-02"), PositionCount: n.OpenPos,
			ActiveCount: n.OpenPos, PausedCount: 0,
		})
	}
	return map[string]any{"series": series}, nil
}

// ── 3. Best & worst trades ──────────────────────────────────────────────

type tradeRef struct {
	Symbol string  `json:"symbol"`
	PnL    float64 `json:"pnl"`
	PnLPct float64 `json:"pnl_pct"`
	Date   string  `json:"date"`
}

// bestWorst returns the extreme CLOSED trades; userID == "" is book-wide.
func (d *DashboardStore) bestWorst(ctx context.Context, userID string) (best, worst *tradeRef, err error) {
	one := func(order string) (*tradeRef, error) {
		q := `SELECT symbol, realized_pnl, invested_amt, exit_time
		      FROM manthan_positions WHERE status = 'EXITED' AND realized_pnl IS NOT NULL`
		args := []any{}
		if userID != "" {
			q += ` AND user_id = $1`
			args = append(args, userID)
		}
		q += ` ORDER BY realized_pnl ` + order + ` LIMIT 1`
		var t tradeRef
		var invested float64
		var exit sql.NullTime
		err := d.tradingDB.QueryRowContext(ctx, q, args...).Scan(&t.Symbol, &t.PnL, &invested, &exit)
		if err == sql.ErrNoRows {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		t.PnL = round2f(t.PnL)
		t.PnLPct = pct(t.PnL, invested)
		if exit.Valid {
			t.Date = exit.Time.In(d.ist).Format("2006-01-02")
		}
		return &t, nil
	}
	if best, err = one("DESC"); err != nil {
		return nil, nil, err
	}
	worst, err = one("ASC")
	return best, worst, err
}

// BestWorstTrades is book-wide for userID == "", else one client's.
func (d *DashboardStore) BestWorstTrades(ctx context.Context, userID string) (any, error) {
	best, worst, err := d.bestWorst(ctx, userID)
	if err != nil {
		return nil, err
	}
	return map[string]any{"best_trade": best, "worst_trade": worst}, nil
}

// ── 4/6/7. Open-book groupings (sector / mcap / ema) ────────────────────

// userScope appends the optional per-client predicate. Every book-wide
// query in this file takes userID == "" for the whole book; a non-empty
// id narrows to that client (the /portfolio/*?client_id= and
// /clients/{id}/dashboard views). Returns the SQL fragment and args.
func userScope(userID string, args []any) (string, []any) {
	if userID == "" {
		return "", args
	}
	args = append(args, userID)
	return fmt.Sprintf(" AND user_id = $%d", len(args)), args
}

// groupOpen aggregates the open book by an expression over manthan_positions.
func (d *DashboardStore) groupOpen(ctx context.Context, expr, userID string) (labels []string, counts []int, values []float64, total float64, err error) {
	scope, args := userScope(userID, nil)
	q := `SELECT COALESCE(NULLIF(` + expr + `, ''), 'Unknown') AS grp,
	             COUNT(*), COALESCE(SUM(invested_amt), 0)
	      FROM manthan_positions WHERE status IN ` + openStatuses + scope + `
	      GROUP BY grp ORDER BY 3 DESC`
	rows, err := d.tradingDB.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, nil, nil, 0, err
	}
	defer rows.Close()
	for rows.Next() {
		var l string
		var c int
		var v float64
		if err := rows.Scan(&l, &c, &v); err != nil {
			return nil, nil, nil, 0, err
		}
		labels, counts, values = append(labels, l), append(counts, c), append(values, v)
		total += v
	}
	return labels, counts, values, total, rows.Err()
}

func (d *DashboardStore) SectorBreakdown(ctx context.Context, userID string) (any, error) {
	labels, _, values, total, err := d.groupOpen(ctx, "industry", userID)
	if err != nil {
		return nil, err
	}
	type sector struct {
		Name       string  `json:"name"`
		Percentage float64 `json:"percentage"`
		Value      float64 `json:"value"`
	}
	out := make([]sector, 0, len(labels))
	for i := range labels {
		out = append(out, sector{Name: labels[i], Percentage: pct(values[i], total), Value: round2f(values[i])})
	}
	return map[string]any{"sectors": out}, nil
}

func (d *DashboardStore) EMAAllocation(ctx context.Context, userID string) (any, error) {
	labels, counts, values, total, err := d.groupOpen(ctx, "TO_CHAR(ema_alloc_pct * 100, 'FM999')", userID)
	if err != nil {
		return nil, err
	}
	type alloc struct {
		EMALevel      string  `json:"ema_level"`
		Percentage    float64 `json:"percentage"`
		PositionCount int     `json:"position_count"`
		Value         float64 `json:"value"`
	}
	out := make([]alloc, 0, len(labels))
	for i := range labels {
		lvl := labels[i]
		if lvl != "Unknown" {
			lvl += "% allocation"
		}
		out = append(out, alloc{EMALevel: lvl, Percentage: pct(values[i], total), PositionCount: counts[i], Value: round2f(values[i])})
	}

	// §6b: with-EMA vs without-EMA cohorts. A direct-buy position (outside
	// the signal flow) carries NULL ema_alloc_pct; every allocator entry
	// has a value. Cohorts are over the open book, like the levels above.
	type cohort struct {
		PositionCount int     `json:"position_count"`
		Value         float64 `json:"value"`
		Percentage    float64 `json:"percentage"`
	}
	var with, without cohort
	scope, args := userScope(userID, nil)
	err = d.tradingDB.QueryRowContext(ctx, `
		SELECT COUNT(*) FILTER (WHERE ema_alloc_pct IS NOT NULL),
		       COALESCE(SUM(invested_amt) FILTER (WHERE ema_alloc_pct IS NOT NULL), 0),
		       COUNT(*) FILTER (WHERE ema_alloc_pct IS NULL),
		       COALESCE(SUM(invested_amt) FILTER (WHERE ema_alloc_pct IS NULL), 0)
		FROM manthan_positions WHERE status IN `+openStatuses+scope, args...).
		Scan(&with.PositionCount, &with.Value, &without.PositionCount, &without.Value)
	if err != nil {
		return nil, err
	}
	tot := with.Value + without.Value
	with.Percentage, without.Percentage = pct(with.Value, tot), pct(without.Value, tot)
	with.Value, without.Value = round2f(with.Value), round2f(without.Value)

	return map[string]any{
		"allocations": out,
		"by_ema_flag": map[string]any{"with_ema": with, "without_ema": without},
	}, nil
}

func (d *DashboardStore) McapPerformance(ctx context.Context, userID string) (any, error) {
	// avg_return_pct is over CLOSED trades (realized truth); count + value
	// describe the OPEN book — the chart shows "how has each cap performed"
	// beside "what's deployed there now".
	scope, args := userScope(userID, nil)
	q := `SELECT COALESCE(NULLIF(mcap_bucket,''),'Unknown') AS cap,
	             COUNT(*) FILTER (WHERE status IN ` + openStatuses + `),
	             COALESCE(SUM(invested_amt) FILTER (WHERE status IN ` + openStatuses + `), 0),
	             COALESCE(AVG(realized_pnl / NULLIF(invested_amt,0) * 100)
	                      FILTER (WHERE status = 'EXITED'), 0)
	      FROM manthan_positions
	      WHERE status IN ('ACTIVE','EXIT_PENDING','EXITED')` + scope + `
	      GROUP BY cap ORDER BY 3 DESC`
	rows, err := d.tradingDB.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	type segment struct {
		Cap           string  `json:"cap"`
		AvgReturnPct  float64 `json:"avg_return_pct"`
		PositionCount int     `json:"position_count"`
		Value         float64 `json:"value"`
	}
	var out []segment
	seen := map[string]bool{}
	for rows.Next() {
		var s segment
		if err := rows.Scan(&s.Cap, &s.PositionCount, &s.Value, &s.AvgReturnPct); err != nil {
			return nil, err
		}
		s.AvgReturnPct, s.Value = round2f(s.AvgReturnPct), round2f(s.Value)
		out = append(out, s)
		seen[s.Cap] = true
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// Canonical buckets always present, zero-filled — the frontend's
	// LARGE tile must render even while nothing large-cap is held.
	for _, cap := range []string{"SMALL", "MID", "LARGE"} {
		if !seen[cap] {
			out = append(out, segment{Cap: cap})
		}
	}
	return map[string]any{"segments": out}, nil
}

// ── LTP helpers ─────────────────────────────────────────────────────────

// symbolTokens maps each symbol to its latest known exchange token from the
// order ledger (same source the protection board uses).
func (d *DashboardStore) symbolTokens(ctx context.Context, symbols []string) map[string]string {
	out := map[string]string{}
	if d.execDB == nil || len(symbols) == 0 {
		return out
	}
	rows, err := d.execDB.QueryContext(ctx, `
		SELECT DISTINCT ON (symbol) symbol, exchange_token
		FROM manthan_orders
		WHERE COALESCE(exchange_token,'') <> ''
		ORDER BY symbol, created_at DESC`)
	if err != nil {
		log.Printf("admin dashboard: token lookup failed: %v", err)
		return out
	}
	defer rows.Close()
	all := map[string]string{}
	for rows.Next() {
		var sym, tok string
		if rows.Scan(&sym, &tok) == nil {
			all[sym] = tok
		}
	}
	for _, s := range symbols {
		if t, ok := all[s]; ok {
			out[s] = t
		}
	}
	return out
}

// fetchQuotes returns symbol → full live quote for whatever the feed
// knows. The market payload carries LTP plus the live 52-week high and
// its date (verified against the real feed 2026-09-22) — no separate
// lookup needed for the down-from-high metrics.
func (d *DashboardStore) fetchQuotes(ctx context.Context, symbols []string) map[string]livealgos.LTPQuote {
	out := map[string]livealgos.LTPQuote{}
	if d.ltp == nil {
		return out
	}
	tokens := d.symbolTokens(ctx, symbols)
	if len(tokens) == 0 {
		return out
	}
	list := make([]string, 0, len(tokens))
	tokToSym := map[string]string{}
	for sym, tok := range tokens {
		list = append(list, tok)
		tokToSym[tok] = sym
	}
	quotes, _ := d.ltp.FetchByTokens(ctx, list)
	for tok, q := range quotes {
		if sym, ok := tokToSym[tok]; ok && q.LTP > 0 {
			out[sym] = q
		}
	}
	return out
}

// w52Highs maps symbol → latest 52-week high from the signals universe
// (data-ingestion refreshes manthan_stocks daily from the sheet). Symbols
// absent from the sheet simply have no entry — callers omit the metric.
func (d *DashboardStore) w52Highs(ctx context.Context) map[string]float64 {
	out := map[string]float64{}
	if d.signalsDB == nil {
		return out
	}
	rows, err := d.signalsDB.QueryContext(ctx, `
		SELECT DISTINCT ON (symbol) symbol, week52_high
		FROM manthan_stocks WHERE week52_high > 0
		ORDER BY symbol, created_at DESC`)
	if err != nil {
		log.Printf("admin dashboard: w52 high lookup failed: %v", err)
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var sym string
		var h float64
		if rows.Scan(&sym, &h) == nil {
			out[sym] = h
		}
	}
	return out
}

// ── 5. Stock allocation (top holdings) ──────────────────────────────────

// StockAllocation is the open book per symbol; userID == "" is book-wide,
// otherwise scoped to one client (§6d of the frontend gaps doc).
func (d *DashboardStore) StockAllocation(ctx context.Context, userID string) (any, error) {
	q := `SELECT symbol, SUM(quantity), COALESCE(SUM(invested_amt),0)
	      FROM manthan_positions WHERE status IN ` + openStatuses
	args := []any{}
	if userID != "" {
		q += ` AND user_id = $1`
		args = append(args, userID)
	}
	q += ` GROUP BY symbol`
	rows, err := d.tradingDB.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	type holding struct {
		Symbol     string  `json:"symbol"`
		Quantity   int     `json:"quantity"`
		Price      float64 `json:"price"`
		Value      float64 `json:"value"`
		Percentage float64 `json:"percentage"`
		PnL        float64 `json:"pnl"`
		priceLive  bool
	}
	hs := make([]holding, 0) // [] not null when the client holds nothing
	var syms []string
	for rows.Next() {
		var h holding
		var invested float64
		if err := rows.Scan(&h.Symbol, &h.Quantity, &invested); err != nil {
			return nil, err
		}
		if h.Quantity > 0 {
			h.Price = invested / float64(h.Quantity) // entry avg until LTP lands
		}
		h.Value, h.PnL = invested, 0
		hs = append(hs, h)
		syms = append(syms, h.Symbol)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	quotes := d.fetchQuotes(ctx, syms)
	total := 0.0
	for i := range hs {
		if q, ok := quotes[hs[i].Symbol]; ok {
			live := q.LTP * float64(hs[i].Quantity)
			hs[i].PnL = round2f(live - hs[i].Value)
			hs[i].Value, hs[i].Price, hs[i].priceLive = live, q.LTP, true
		}
		total += hs[i].Value
	}
	for i := range hs {
		hs[i].Percentage = pct(hs[i].Value, total)
		hs[i].Value = round2f(hs[i].Value)
		hs[i].Price = round2f(hs[i].Price)
	}
	sort.Slice(hs, func(i, j int) bool { return hs[i].Value > hs[j].Value })
	return map[string]any{"holdings": hs, "total": round2f(total)}, nil
}

// ── 8. Positions matrix ─────────────────────────────────────────────────

type matrixRow struct {
	PositionID       string   `json:"position_id"`
	ClientID         string   `json:"client_id"`
	ClientName       string   `json:"client_name"`
	Script           string   `json:"script"`
	Industry         string   `json:"industry"`
	Mcap             string   `json:"mcap"`
	Quantity         int      `json:"quantity"`
	BuyRate          float64  `json:"buy_rate"`
	CurrentPrice     float64  `json:"current_price"`
	EntryDate        string   `json:"entry_date"`
	EntryTime        string   `json:"entry_time"`
	ExitDate         string   `json:"exit_date,omitempty"`
	ExitTime         string   `json:"exit_time,omitempty"`
	SignalEntryDate  string   `json:"signal_entry_date,omitempty"`
	SignalEntryTime  string   `json:"signal_entry_time,omitempty"`
	SignalExitDate   string   `json:"signal_exit_date,omitempty"`
	SignalExitTime   string   `json:"signal_exit_time,omitempty"`
	PnL              float64  `json:"pnl"`
	PnLPct           float64  `json:"pnl_pct"`
	CurrentValue     float64  `json:"current_value"`
	Stoploss         *float64 `json:"stoploss,omitempty"`
	DownFromHighPct  *float64 `json:"down_from_high_pct,omitempty"`
	ClosedDate       string   `json:"closed_date,omitempty"` // alias of exit_date, per spec
	EMAAllocationPct *float64 `json:"ema_allocation_pct,omitempty"`
	HoldingDays      int      `json:"holding_period_days"`
	Status           string   `json:"status"`
	signalID         string
	w52High          float64 // resolved high (feed, else sheet); 0 = unknown
	w52HighDate      string  // feed-supplied date of that high, if any
}

// PositionsFilter narrows the matrix (§7 of the frontend gaps doc).
// Zero values mean "no filter"; unknown values fall back to no filter,
// matching the status parameter's behaviour.
type PositionsFilter struct {
	Status   string // all | open | closed
	Industry string // exact match
	Mcap     string // SMALL | MID | LARGE
	EMA      string // with | without (ema_alloc_pct NULL-ness)
	ClientID string // user_id
}

func (d *DashboardStore) Positions(ctx context.Context, f PositionsFilter) (any, error) {
	statuses := "('ACTIVE','EXIT_PENDING','EXITED')"
	switch f.Status {
	case "open":
		statuses = openStatuses
	case "closed":
		statuses = "('EXITED')"
	}
	q := `SELECT id, user_id, symbol, COALESCE(industry,''), COALESCE(mcap_bucket,''),
	             quantity, entry_price, COALESCE(invested_amt,0), entry_time, exit_time,
	             exit_price, realized_pnl, ema_alloc_pct, status, COALESCE(signal_id::text,''),
	             current_sl
	      FROM manthan_positions WHERE status IN ` + statuses
	args := []any{}
	add := func(cond string, v any) {
		args = append(args, v)
		q += fmt.Sprintf(" AND "+cond, len(args))
	}
	if f.Industry != "" {
		add("industry = $%d", f.Industry)
	}
	switch strings.ToUpper(f.Mcap) {
	case "SMALL", "MID", "LARGE":
		add("mcap_bucket = $%d", strings.ToUpper(f.Mcap))
	}
	switch strings.ToLower(f.EMA) {
	case "with":
		q += " AND ema_alloc_pct IS NOT NULL"
	case "without":
		q += " AND ema_alloc_pct IS NULL"
	}
	if f.ClientID != "" {
		add("user_id = $%d", f.ClientID)
	}
	q += ` ORDER BY entry_time DESC NULLS LAST`
	rows, err := d.tradingDB.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]matrixRow, 0) // [] not null for an empty filter result
	var openSyms []string
	nowT := d.now()
	for rows.Next() {
		var r matrixRow
		var id int64
		var entryT, exitT sql.NullTime
		var exitPx, realized, ema, csl sql.NullFloat64
		if err := rows.Scan(&id, &r.ClientID, &r.Script, &r.Industry, &r.Mcap,
			&r.Quantity, &r.BuyRate, &r.PnL /*invested, reused below*/, &entryT, &exitT,
			&exitPx, &realized, &ema, &r.Status, &r.signalID, &csl); err != nil {
			return nil, err
		}
		if csl.Valid && csl.Float64 > 0 {
			v := round2f(csl.Float64)
			r.Stoploss = &v
		}
		invested := r.PnL
		r.PnL = 0
		r.ClientName = r.ClientID
		r.PositionID = fmt.Sprintf("MP_%d", id)
		if ema.Valid {
			v := round2f(ema.Float64 * 100)
			r.EMAAllocationPct = &v
		}
		if entryT.Valid {
			t := entryT.Time.In(d.ist)
			r.EntryDate, r.EntryTime = t.Format("2006-01-02"), t.Format("15:04")
		}
		if r.Status == "EXITED" {
			r.Status = "CLOSED"
			if exitT.Valid {
				t := exitT.Time.In(d.ist)
				r.ExitDate, r.ExitTime = t.Format("2006-01-02"), t.Format("15:04")
				r.ClosedDate = r.ExitDate
				if entryT.Valid {
					r.HoldingDays = int(exitT.Time.Sub(entryT.Time).Hours() / 24)
				}
			}
			if exitPx.Valid {
				r.CurrentPrice = round2f(exitPx.Float64)
			}
			r.CurrentValue = round2f(r.CurrentPrice * float64(r.Quantity))
			if realized.Valid {
				r.PnL = round2f(realized.Float64)
				r.PnLPct = pct(realized.Float64, invested)
			}
		} else {
			r.Status = "OPEN"
			r.CurrentPrice = r.BuyRate // upgraded to LTP below when known
			if entryT.Valid {
				r.HoldingDays = int(nowT.Sub(entryT.Time).Hours() / 24)
			}
			openSyms = append(openSyms, r.Script)
			_ = invested
		}
		r.BuyRate = round2f(r.BuyRate)
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Live quotes for the open rows: price, and the feed's own live
	// 52-week high (falls back to the sheet universe when the feed
	// misses a symbol).
	quotes := d.fetchQuotes(ctx, openSyms)
	sheetHighs := d.w52Highs(ctx)
	for i := range out {
		if out[i].Status != "OPEN" {
			continue
		}
		high, highDate := sheetHighs[out[i].Script], ""
		if q, ok := quotes[out[i].Script]; ok {
			out[i].CurrentPrice = round2f(q.LTP)
			cost := out[i].BuyRate * float64(out[i].Quantity)
			out[i].PnL = round2f((q.LTP - out[i].BuyRate) * float64(out[i].Quantity))
			out[i].PnLPct = pct(out[i].PnL, cost)
			if q.Week52High > 0 {
				high, highDate = q.Week52High, q.Week52HighDate // live feed beats sheet snapshot
			}
		}
		out[i].CurrentValue = round2f(out[i].CurrentPrice * float64(out[i].Quantity))
		out[i].w52High, out[i].w52HighDate = high, highDate
		if high > 0 && out[i].CurrentPrice > 0 {
			v := pct(out[i].CurrentPrice-high, high) // ≤ 0 when below the high
			out[i].DownFromHighPct = &v
		}
	}

	// Signal entry times from the execution inbox (signal_id → created_at).
	d.applySignalTimes(ctx, out)

	return map[string]any{"positions": out, "total_count": len(out)}, nil
}

func (d *DashboardStore) applySignalTimes(ctx context.Context, rows []matrixRow) {
	if d.execDB == nil {
		return
	}
	want := map[string][]int{}
	for i := range rows {
		if rows[i].signalID != "" {
			want[rows[i].signalID] = append(want[rows[i].signalID], i)
		}
	}
	if len(want) == 0 {
		return
	}
	res, err := d.execDB.QueryContext(ctx, `
		SELECT DISTINCT ON (signal_id) signal_id, created_at
		FROM signal_inbox WHERE order_type = 'ENTRY' ORDER BY signal_id, created_at`)
	if err != nil {
		log.Printf("admin dashboard: signal time lookup failed: %v", err)
		return
	}
	defer res.Close()
	for res.Next() {
		var sid string
		var at time.Time
		if res.Scan(&sid, &at) != nil {
			continue
		}
		for _, i := range want[sid] {
			t := at.In(d.ist)
			rows[i].SignalEntryDate = t.Format("2006-01-02")
			rows[i].SignalEntryTime = t.Format("15:04")
		}
	}
}

// ── 9. Positions summary ────────────────────────────────────────────────

type positionsSummary struct {
	TotalPositions    int     `json:"total_positions"`
	OpenPositions     int     `json:"open_positions"`
	ClosedPositions   int     `json:"closed_positions"`
	ProfitMaking      int     `json:"profit_making"`
	LossMaking        int     `json:"loss_making"`
	AvgProfitPerTrade float64 `json:"avg_profit_per_trade"`
	AvgLossPerTrade   float64 `json:"avg_loss_per_trade"`
	WinRatePct        float64 `json:"win_rate_pct"`
	ProfitFactor      float64 `json:"profit_factor"`
	BreakevenTrades   int     `json:"breakeven_trades"`
}

// positionsAgg computes the summary; userID == "" is book-wide.
func (d *DashboardStore) positionsAgg(ctx context.Context, userID string) (*positionsSummary, error) {
	q := `SELECT
	        COUNT(*) FILTER (WHERE status IN ` + openStatuses + `),
	        COUNT(*) FILTER (WHERE status = 'EXITED'),
	        COUNT(*) FILTER (WHERE status = 'EXITED' AND realized_pnl IS NOT NULL),
	        COUNT(*) FILTER (WHERE status = 'EXITED' AND realized_pnl > 0),
	        COUNT(*) FILTER (WHERE status = 'EXITED' AND realized_pnl < 0),
	        COUNT(*) FILTER (WHERE status = 'EXITED' AND realized_pnl = 0),
	        COALESCE(AVG(realized_pnl) FILTER (WHERE status='EXITED' AND realized_pnl > 0), 0),
	        COALESCE(AVG(realized_pnl) FILTER (WHERE status='EXITED' AND realized_pnl < 0), 0),
	        COALESCE(SUM(realized_pnl) FILTER (WHERE status='EXITED' AND realized_pnl > 0), 0),
	        COALESCE(ABS(SUM(realized_pnl) FILTER (WHERE status='EXITED' AND realized_pnl < 0)), 0)
	      FROM manthan_positions WHERE status IN ('ACTIVE','EXIT_PENDING','EXITED')`
	args := []any{}
	if userID != "" {
		q += ` AND user_id = $1`
		args = append(args, userID)
	}
	var s positionsSummary
	var decided int // closed trades with a recorded realized_pnl — manual
	// exits and ghost heals can close a row without one; win rate is
	// computed over decided trades only, never the NULLs.
	var grossProfit, grossLoss float64
	if err := d.tradingDB.QueryRowContext(ctx, q, args...).Scan(
		&s.OpenPositions, &s.ClosedPositions, &decided, &s.ProfitMaking, &s.LossMaking,
		&s.BreakevenTrades, &s.AvgProfitPerTrade, &s.AvgLossPerTrade,
		&grossProfit, &grossLoss); err != nil {
		return nil, err
	}
	s.TotalPositions = s.OpenPositions + s.ClosedPositions
	s.AvgProfitPerTrade = round2f(s.AvgProfitPerTrade)
	s.AvgLossPerTrade = round2f(s.AvgLossPerTrade)
	if decided > 0 {
		s.WinRatePct = pct(float64(s.ProfitMaking), float64(decided))
	}
	if grossLoss > 0 {
		s.ProfitFactor = round2f(grossProfit / grossLoss)
	} else if grossProfit > 0 {
		s.ProfitFactor = round2f(grossProfit) // no losses yet: PF is unbounded; report gross
	}
	return &s, nil
}

// PortfolioSummary is /portfolio/positions-summary: the matrix aggregate
// plus the portfolio-wide KPIs from §3 of the frontend gaps doc. All the
// KPI inputs already exist (NAV history, strategy capital, open book).
type portfolioSummary struct {
	positionsSummary
	RealizedPnL         float64 `json:"realized_pnl"`
	UnrealizedPnL       float64 `json:"unrealized_pnl"`
	PortfolioValue      float64 `json:"portfolio_value"`
	CAGR                float64 `json:"cagr"`
	XIRR                float64 `json:"xirr"` // == CAGR (no cash-flow ledger)
	AnnualizedReturnPct float64 `json:"annualized_return_pct"`
	MaxDrawdownPct      float64 `json:"max_drawdown_pct"`
	CurrentExposurePct  float64 `json:"current_exposure_pct"`
}

// PortfolioSummary is book-wide for userID == "", else the same KPIs over
// one client's strategies and positions.
func (d *DashboardStore) PortfolioSummary(ctx context.Context, userID string) (any, error) {
	agg, err := d.positionsAgg(ctx, userID)
	if err != nil {
		return nil, err
	}
	out := portfolioSummary{positionsSummary: *agg}

	bases, err := d.clientBases(ctx, userID)
	if err != nil {
		return nil, err
	}
	invested := 0.0
	for _, b := range bases {
		invested += b.InvestedFund
	}
	_, utilized, err := d.openBook(ctx, userID)
	if err != nil {
		return nil, err
	}
	deployed := 0.0
	for _, v := range utilized {
		deployed += v
	}

	nav, vals, err := d.valueSeries(ctx, userID, 3650)
	if err != nil {
		return nil, err
	}
	if n := len(nav); n > 0 {
		last := nav[n-1]
		out.RealizedPnL = round2f(last.Realized)
		out.UnrealizedPnL = round2f(last.Unrealized)
		out.PortfolioValue = round2f(invested + last.NetPnL)
	} else {
		out.PortfolioValue = round2f(invested)
	}
	// TWR index: capital-flow immune (new strategies/capital are not
	// "returns"; a strategy leaving the snapshots is not a "drawdown").
	idx := twrIndex(nav)
	out.CAGR = cagrFromIndex(nav, idx)
	out.XIRR, out.AnnualizedReturnPct = out.CAGR, out.CAGR
	maxDD, _ := drawdownStats(idx)
	out.MaxDrawdownPct = maxDD
	_ = vals
	out.CurrentExposurePct = pct(deployed, invested)
	return out, nil
}

// DownFromHigh lists open positions by distance below their 52-week high
// (§6c). window is accepted for forward compatibility; today the one
// source is the sheet-refreshed 52-week high, so any value maps to 52w.
// userID == "" is book-wide.
func (d *DashboardStore) DownFromHigh(ctx context.Context, userID string) (any, error) {
	// Positions already resolved price + 52-week high (feed, else sheet)
	// for every open row; reuse it rather than hitting Redis/signals twice.
	res, err := d.Positions(ctx, PositionsFilter{Status: "open", ClientID: userID})
	if err != nil {
		return nil, err
	}
	rows := res.(map[string]any)["positions"].([]matrixRow)
	type entry struct {
		PositionID      string  `json:"position_id"`
		Script          string  `json:"script"`
		ClientID        string  `json:"client_id"`
		CurrentPrice    float64 `json:"current_price"`
		HighPrice       float64 `json:"high_price"`
		HighDate        string  `json:"high_date,omitempty"` // from the live feed
		DownFromHighPct float64 `json:"down_from_high_pct"`
	}
	out := make([]entry, 0, len(rows))
	for _, r := range rows {
		if r.w52High <= 0 || r.CurrentPrice <= 0 {
			continue // no 52w high known for this symbol in feed or sheet
		}
		out = append(out, entry{
			PositionID: r.PositionID, Script: r.Script, ClientID: r.ClientID,
			CurrentPrice: r.CurrentPrice, HighPrice: round2f(r.w52High), HighDate: r.w52HighDate,
			DownFromHighPct: pct(r.CurrentPrice-r.w52High, r.w52High),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].DownFromHighPct < out[j].DownFromHighPct })
	return map[string]any{"positions": out, "source": "live feed week_52_high (sheet universe fallback)"}, nil
}

// ── 15. Client dashboard (composite) ────────────────────────────────────

// Standalone ?days= defaults, panel by panel. The composite reuses them so
// that, without an explicit ?days=, each key is exactly what its own
// endpoint would return (the TWR index and drawdown are rebased to the
// first point of the window, so a different window is a different number).
const (
	daysPnLHistory      = 90
	daysPositionHistory = 30
	daysMTMSeries       = 30
	daysEquityCurve     = 90
	daysDrawdown        = 90
)

// ClientDashboard is GET /clients/{id}/dashboard: every panel of the
// portfolio overview page, scoped to one client, in a single response.
// Each key carries exactly the payload its standalone endpoint returns
// (same shapes, same formulas — the frontend reuses its chart parsers).
// days > 0 bounds every time series to that one window; days <= 0 means
// "each panel's own default" (daysPnLHistory & co). The effective window
// per series is echoed under "windows". The KPI blocks (summary,
// positions_summary) are lifetime, as on the standalone endpoints.
// Unknown client → 404.
func (d *DashboardStore) ClientDashboard(ctx context.Context, userID string, days int) (any, error) {
	if userID == "" {
		return nil, errClientUnknown
	}
	summary, err := d.ClientSummary(ctx, userID) // also 404s unknown ids
	if err != nil {
		return nil, err
	}
	window := func(def int) int {
		if days > 0 {
			return days
		}
		return def
	}
	windows := map[string]int{
		"pnl_history":      window(daysPnLHistory),
		"position_history": window(daysPositionHistory),
		"mtm_series":       window(daysMTMSeries),
		"equity_curve":     window(daysEquityCurve),
		"drawdown":         window(daysDrawdown),
	}
	out := map[string]any{
		"client_id":    userID,
		"client_code":  userID,
		"client_name":  userID,
		"windows":      windows,
		"generated_at": d.now().In(d.ist).Format(time.RFC3339),
		"summary":      summary,
	}
	parts := []struct {
		key string
		fn  func(context.Context) (any, error)
	}{
		{"positions_summary", func(c context.Context) (any, error) { return d.PortfolioSummary(c, userID) }},
		{"pnl_history", func(c context.Context) (any, error) { return d.PnLHistory(c, userID, windows["pnl_history"]) }},
		{"position_history", func(c context.Context) (any, error) {
			return d.PositionHistory(c, userID, windows["position_history"])
		}},
		{"mtm_series", func(c context.Context) (any, error) { return d.MTMSeries(c, userID, windows["mtm_series"]) }},
		{"equity_curve", func(c context.Context) (any, error) { return d.EquityCurve(c, userID, windows["equity_curve"]) }},
		{"drawdown", func(c context.Context) (any, error) { return d.Drawdown(c, userID, windows["drawdown"]) }},
		{"sector_breakdown", func(c context.Context) (any, error) { return d.SectorBreakdown(c, userID) }},
		{"stock_allocation", func(c context.Context) (any, error) { return d.StockAllocation(c, userID) }},
		{"mcap_performance", func(c context.Context) (any, error) { return d.McapPerformance(c, userID) }},
		{"ema_allocation", func(c context.Context) (any, error) { return d.EMAAllocation(c, userID) }},
		{"best_worst_trades", func(c context.Context) (any, error) { return d.BestWorstTrades(c, userID) }},
		{"down_from_high", func(c context.Context) (any, error) { return d.DownFromHigh(c, userID) }},
	}
	for _, p := range parts {
		v, err := p.fn(ctx)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", p.key, err)
		}
		out[p.key] = v
	}
	return out, nil
}

// ── HTTP handlers (mounted in http.go) ──────────────────────────────────

// scopeParam reads the optional ?client_id= on the /portfolio/* endpoints.
// Empty keeps the book-wide view (today's behaviour, byte-identical); a
// non-empty id must name a known client or the request 404s — a typo must
// not render as an empty-but-plausible dashboard.
func (h *HTTP) scopeParam(ctx context.Context, ar *AdminRequest) (string, error) {
	raw := ar.Request.URL.Query().Get("client_id")
	if raw == "" {
		return "", nil // absent or client_id= → whole book
	}
	uid := strings.TrimSpace(raw)
	if uid == "" {
		return "", errClientUnknown // whitespace is a malformed id, not "all"
	}
	if err := h.dashboard.requireClient(ctx, uid); err != nil {
		return "", err
	}
	return uid, nil
}

// scoped wraps a per-client store call behind scopeParam.
func (h *HTTP) scoped(w http.ResponseWriter, ar *AdminRequest, fn func(ctx context.Context, userID string) (any, error)) {
	h.dashJSON(w, ar, func(ctx context.Context) (any, error) {
		uid, err := h.scopeParam(ctx, ar)
		if err != nil {
			return nil, err
		}
		return fn(ctx, uid)
	})
}

func (h *HTTP) handleDashPnLHistory(w http.ResponseWriter, ar *AdminRequest) {
	h.scoped(w, ar, func(ctx context.Context, uid string) (any, error) {
		return h.dashboard.PnLHistory(ctx, uid, daysParam(ar.Request, daysPnLHistory))
	})
}

func (h *HTTP) handleDashPositionHistory(w http.ResponseWriter, ar *AdminRequest) {
	h.scoped(w, ar, func(ctx context.Context, uid string) (any, error) {
		return h.dashboard.PositionHistory(ctx, uid, daysParam(ar.Request, daysPositionHistory))
	})
}

func (h *HTTP) handleDashBestWorst(w http.ResponseWriter, ar *AdminRequest) {
	h.scoped(w, ar, h.dashboard.BestWorstTrades)
}

func (h *HTTP) handleDashSectors(w http.ResponseWriter, ar *AdminRequest) {
	h.scoped(w, ar, h.dashboard.SectorBreakdown)
}

func (h *HTTP) handleDashStockAlloc(w http.ResponseWriter, ar *AdminRequest) {
	h.scoped(w, ar, h.dashboard.StockAllocation)
}

func (h *HTTP) handleDashMcap(w http.ResponseWriter, ar *AdminRequest) {
	h.scoped(w, ar, h.dashboard.McapPerformance)
}

func (h *HTTP) handleDashEMA(w http.ResponseWriter, ar *AdminRequest) {
	h.scoped(w, ar, h.dashboard.EMAAllocation)
}

// handleDashPositions: client_id goes through the same scopeParam as its
// siblings (trimmed, unknown → 404). A client with positions but no live
// strategy still resolves — see clientBase.
func (h *HTTP) handleDashPositions(w http.ResponseWriter, ar *AdminRequest) {
	h.scoped(w, ar, func(ctx context.Context, uid string) (any, error) {
		qp := ar.Request.URL.Query()
		return h.dashboard.Positions(ctx, PositionsFilter{
			Status:   qp.Get("status"),
			Industry: qp.Get("industry"),
			Mcap:     qp.Get("mcap"),
			EMA:      qp.Get("ema"),
			ClientID: uid,
		})
	})
}

func (h *HTTP) handleDashPositionsSummary(w http.ResponseWriter, ar *AdminRequest) {
	h.scoped(w, ar, h.dashboard.PortfolioSummary)
}

func (h *HTTP) handleDashDownFromHigh(w http.ResponseWriter, ar *AdminRequest) {
	h.scoped(w, ar, h.dashboard.DownFromHigh)
}

func (h *HTTP) handleClientDashboard(w http.ResponseWriter, ar *AdminRequest) {
	h.dashJSON(w, ar, func(ctx context.Context) (any, error) {
		// 0 → each panel keeps its standalone default; an explicit ?days=
		// applies one window to every series.
		return h.dashboard.ClientDashboard(ctx, mux.Vars(ar.Request)["client_id"], daysParam(ar.Request, 0))
	})
}

func (h *HTTP) handleClientPnLHistory(w http.ResponseWriter, ar *AdminRequest) {
	h.dashJSON(w, ar, func(ctx context.Context) (any, error) {
		uid := mux.Vars(ar.Request)["client_id"]
		if err := h.dashboard.requireClient(ctx, uid); err != nil {
			return nil, err
		}
		return h.dashboard.PnLHistory(ctx, uid, daysParam(ar.Request, daysPnLHistory))
	})
}

func (h *HTTP) handleClientPositionHistory(w http.ResponseWriter, ar *AdminRequest) {
	h.dashJSON(w, ar, func(ctx context.Context) (any, error) {
		uid := mux.Vars(ar.Request)["client_id"]
		if err := h.dashboard.requireClient(ctx, uid); err != nil {
			return nil, err
		}
		return h.dashboard.PositionHistory(ctx, uid, daysParam(ar.Request, daysPositionHistory))
	})
}

func (h *HTTP) handleClientStockAlloc(w http.ResponseWriter, ar *AdminRequest) {
	h.dashJSON(w, ar, func(ctx context.Context) (any, error) {
		uid := mux.Vars(ar.Request)["client_id"]
		if err := h.dashboard.requireClient(ctx, uid); err != nil {
			return nil, err
		}
		return h.dashboard.StockAllocation(ctx, uid)
	})
}

func (h *HTTP) handleClientsList(w http.ResponseWriter, ar *AdminRequest) {
	h.dashJSON(w, ar, h.dashboard.Clients)
}

func (h *HTTP) handleClientSummary(w http.ResponseWriter, ar *AdminRequest) {
	h.dashJSON(w, ar, func(ctx context.Context) (any, error) {
		return h.dashboard.ClientSummary(ctx, mux.Vars(ar.Request)["client_id"])
	})
}

func (h *HTTP) handleClientEquityCurve(w http.ResponseWriter, ar *AdminRequest) {
	h.dashJSON(w, ar, func(ctx context.Context) (any, error) {
		return h.dashboard.EquityCurve(ctx, mux.Vars(ar.Request)["client_id"], daysParam(ar.Request, daysEquityCurve))
	})
}

func (h *HTTP) handleClientDrawdown(w http.ResponseWriter, ar *AdminRequest) {
	h.dashJSON(w, ar, func(ctx context.Context) (any, error) {
		return h.dashboard.Drawdown(ctx, mux.Vars(ar.Request)["client_id"], daysParam(ar.Request, daysDrawdown))
	})
}

func (h *HTTP) handleClientMTM(w http.ResponseWriter, ar *AdminRequest) {
	h.dashJSON(w, ar, func(ctx context.Context) (any, error) {
		return h.dashboard.MTMSeries(ctx, mux.Vars(ar.Request)["client_id"], daysParam(ar.Request, daysMTMSeries))
	})
}

// dashJSON is the shared happy-path wrapper: run the query, envelope the
// result, log-and-500 on failure. ErrClientUnknown maps to a 404 envelope.
func (h *HTTP) dashJSON(w http.ResponseWriter, ar *AdminRequest, fn func(ctx context.Context) (any, error)) {
	data, err := fn(ar.Request.Context())
	if errors.Is(err, errClientUnknown) {
		writeErr(w, http.StatusNotFound, "E_NOT_FOUND", "client not found")
		return
	}
	if err != nil {
		log.Printf("admin dashboard: %s failed: %v", ar.action, err)
		writeErr(w, http.StatusInternalServerError, "E_ADMIN_INTERNAL", ar.action+" failed")
		return
	}
	writeOK(w, data)
}
