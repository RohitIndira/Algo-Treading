package admin

// M12 — Admin Dashboard + Clients analytics (14 read-only endpoints).
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
	"fmt"
	"log"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/gorilla/mux"
)

// DashboardStore owns the SQL + LTP access for the M12 endpoints.
type DashboardStore struct {
	tradingDB *sql.DB // manthan_positions, strategies, trade_configs
	execDB    *sql.DB // manthan_orders (tokens), signal_inbox (signal times)
	perfDB    *sql.DB // strategy_nav_daily, benchmark_daily
	ltp       LTPFeed // nil-safe: prices degrade to entry-price valuation
	ist       *time.Location
	now       func() time.Time
}

func NewDashboardStore(tradingDB, execDB, perfDB *sql.DB, ltp LTPFeed) *DashboardStore {
	loc, err := time.LoadLocation("Asia/Kolkata")
	if err != nil || loc == nil {
		loc = time.FixedZone("IST", 5*3600+30*60)
	}
	return &DashboardStore{
		tradingDB: tradingDB, execDB: execDB, perfDB: perfDB,
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

func (d *DashboardStore) PnLHistory(ctx context.Context, days int) (any, error) {
	nav, err := d.navSeries(ctx, "", days)
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

func (d *DashboardStore) PositionHistory(ctx context.Context, days int) (any, error) {
	nav, err := d.navSeries(ctx, "", days)
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

func (d *DashboardStore) BestWorstTrades(ctx context.Context) (any, error) {
	best, worst, err := d.bestWorst(ctx, "")
	if err != nil {
		return nil, err
	}
	return map[string]any{"best_trade": best, "worst_trade": worst}, nil
}

// ── 4/6/7. Open-book groupings (sector / mcap / ema) ────────────────────

// groupOpen aggregates the open book by an expression over manthan_positions.
func (d *DashboardStore) groupOpen(ctx context.Context, expr string) (labels []string, counts []int, values []float64, total float64, err error) {
	q := `SELECT COALESCE(NULLIF(` + expr + `, ''), 'Unknown') AS grp,
	             COUNT(*), COALESCE(SUM(invested_amt), 0)
	      FROM manthan_positions WHERE status IN ` + openStatuses + `
	      GROUP BY grp ORDER BY 3 DESC`
	rows, err := d.tradingDB.QueryContext(ctx, q)
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

func (d *DashboardStore) SectorBreakdown(ctx context.Context) (any, error) {
	labels, _, values, total, err := d.groupOpen(ctx, "industry")
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

func (d *DashboardStore) EMAAllocation(ctx context.Context) (any, error) {
	labels, counts, values, total, err := d.groupOpen(ctx, "TO_CHAR(ema_alloc_pct * 100, 'FM999')")
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
	return map[string]any{"allocations": out}, nil
}

func (d *DashboardStore) McapPerformance(ctx context.Context) (any, error) {
	// avg_return_pct is over CLOSED trades (realized truth); count + value
	// describe the OPEN book — the chart shows "how has each cap performed"
	// beside "what's deployed there now".
	q := `SELECT COALESCE(NULLIF(mcap_bucket,''),'Unknown') AS cap,
	             COUNT(*) FILTER (WHERE status IN ` + openStatuses + `),
	             COALESCE(SUM(invested_amt) FILTER (WHERE status IN ` + openStatuses + `), 0),
	             COALESCE(AVG(realized_pnl / NULLIF(invested_amt,0) * 100)
	                      FILTER (WHERE status = 'EXITED'), 0)
	      FROM manthan_positions
	      WHERE status IN ('ACTIVE','EXIT_PENDING','EXITED')
	      GROUP BY cap ORDER BY 3 DESC`
	rows, err := d.tradingDB.QueryContext(ctx, q)
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
	for rows.Next() {
		var s segment
		if err := rows.Scan(&s.Cap, &s.PositionCount, &s.Value, &s.AvgReturnPct); err != nil {
			return nil, err
		}
		s.AvgReturnPct, s.Value = round2f(s.AvgReturnPct), round2f(s.Value)
		out = append(out, s)
	}
	return map[string]any{"segments": out}, rows.Err()
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

// fetchLTPs returns symbol → last traded price for whatever the feed knows.
func (d *DashboardStore) fetchLTPs(ctx context.Context, symbols []string) map[string]float64 {
	out := map[string]float64{}
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
			out[sym] = q.LTP
		}
	}
	return out
}

// ── 5. Stock allocation (top holdings) ──────────────────────────────────

func (d *DashboardStore) StockAllocation(ctx context.Context) (any, error) {
	q := `SELECT symbol, SUM(quantity), COALESCE(SUM(invested_amt),0)
	      FROM manthan_positions WHERE status IN ` + openStatuses + `
	      GROUP BY symbol`
	rows, err := d.tradingDB.QueryContext(ctx, q)
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
	var hs []holding
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
	ltps := d.fetchLTPs(ctx, syms)
	total := 0.0
	for i := range hs {
		if ltp, ok := ltps[hs[i].Symbol]; ok {
			live := ltp * float64(hs[i].Quantity)
			hs[i].PnL = round2f(live - hs[i].Value)
			hs[i].Value, hs[i].Price, hs[i].priceLive = live, ltp, true
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
	EMAAllocationPct *float64 `json:"ema_allocation_pct,omitempty"`
	HoldingDays      int      `json:"holding_period_days"`
	Status           string   `json:"status"`
	signalID         string
}

func (d *DashboardStore) Positions(ctx context.Context, statusFilter string) (any, error) {
	q := `SELECT id, user_id, symbol, COALESCE(industry,''), COALESCE(mcap_bucket,''),
	             quantity, entry_price, COALESCE(invested_amt,0), entry_time, exit_time,
	             exit_price, realized_pnl, ema_alloc_pct, status, COALESCE(signal_id::text,'')
	      FROM manthan_positions WHERE status IN ('ACTIVE','EXIT_PENDING','EXITED')`
	switch statusFilter {
	case "open":
		q = q[:len(q)-len("('ACTIVE','EXIT_PENDING','EXITED')")] + openStatuses
	case "closed":
		q = q[:len(q)-len("('ACTIVE','EXIT_PENDING','EXITED')")] + "('EXITED')"
	}
	q += ` ORDER BY entry_time DESC NULLS LAST`
	rows, err := d.tradingDB.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []matrixRow
	var openSyms []string
	nowT := d.now()
	for rows.Next() {
		var r matrixRow
		var id int64
		var entryT, exitT sql.NullTime
		var exitPx, realized, ema sql.NullFloat64
		if err := rows.Scan(&id, &r.ClientID, &r.Script, &r.Industry, &r.Mcap,
			&r.Quantity, &r.BuyRate, &r.PnL /*invested, reused below*/, &entryT, &exitT,
			&exitPx, &realized, &ema, &r.Status, &r.signalID); err != nil {
			return nil, err
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
				if entryT.Valid {
					r.HoldingDays = int(exitT.Time.Sub(entryT.Time).Hours() / 24)
				}
			}
			if exitPx.Valid {
				r.CurrentPrice = round2f(exitPx.Float64)
			}
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

	// Live prices for the open rows.
	ltps := d.fetchLTPs(ctx, openSyms)
	for i := range out {
		if out[i].Status != "OPEN" {
			continue
		}
		if ltp, ok := ltps[out[i].Script]; ok {
			out[i].CurrentPrice = round2f(ltp)
			cost := out[i].BuyRate * float64(out[i].Quantity)
			out[i].PnL = round2f((ltp - out[i].BuyRate) * float64(out[i].Quantity))
			out[i].PnLPct = pct(out[i].PnL, cost)
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

// ── HTTP handlers (mounted in http.go) ──────────────────────────────────

func (h *HTTP) handleDashPnLHistory(w http.ResponseWriter, ar *AdminRequest) {
	h.dashJSON(w, ar, func(ctx context.Context) (any, error) {
		return h.dashboard.PnLHistory(ctx, daysParam(ar.Request, 90))
	})
}

func (h *HTTP) handleDashPositionHistory(w http.ResponseWriter, ar *AdminRequest) {
	h.dashJSON(w, ar, func(ctx context.Context) (any, error) {
		return h.dashboard.PositionHistory(ctx, daysParam(ar.Request, 30))
	})
}

func (h *HTTP) handleDashBestWorst(w http.ResponseWriter, ar *AdminRequest) {
	h.dashJSON(w, ar, h.dashboard.BestWorstTrades)
}

func (h *HTTP) handleDashSectors(w http.ResponseWriter, ar *AdminRequest) {
	h.dashJSON(w, ar, h.dashboard.SectorBreakdown)
}

func (h *HTTP) handleDashStockAlloc(w http.ResponseWriter, ar *AdminRequest) {
	h.dashJSON(w, ar, h.dashboard.StockAllocation)
}

func (h *HTTP) handleDashMcap(w http.ResponseWriter, ar *AdminRequest) {
	h.dashJSON(w, ar, h.dashboard.McapPerformance)
}

func (h *HTTP) handleDashEMA(w http.ResponseWriter, ar *AdminRequest) {
	h.dashJSON(w, ar, h.dashboard.EMAAllocation)
}

func (h *HTTP) handleDashPositions(w http.ResponseWriter, ar *AdminRequest) {
	h.dashJSON(w, ar, func(ctx context.Context) (any, error) {
		return h.dashboard.Positions(ctx, ar.Request.URL.Query().Get("status"))
	})
}

func (h *HTTP) handleDashPositionsSummary(w http.ResponseWriter, ar *AdminRequest) {
	h.dashJSON(w, ar, func(ctx context.Context) (any, error) {
		return h.dashboard.positionsAgg(ctx, "")
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
		return h.dashboard.EquityCurve(ctx, mux.Vars(ar.Request)["client_id"], daysParam(ar.Request, 90))
	})
}

func (h *HTTP) handleClientDrawdown(w http.ResponseWriter, ar *AdminRequest) {
	h.dashJSON(w, ar, func(ctx context.Context) (any, error) {
		return h.dashboard.Drawdown(ctx, mux.Vars(ar.Request)["client_id"], daysParam(ar.Request, 90))
	})
}

func (h *HTTP) handleClientMTM(w http.ResponseWriter, ar *AdminRequest) {
	h.dashJSON(w, ar, func(ctx context.Context) (any, error) {
		return h.dashboard.MTMSeries(ctx, mux.Vars(ar.Request)["client_id"], daysParam(ar.Request, 30))
	})
}

// dashJSON is the shared happy-path wrapper: run the query, envelope the
// result, log-and-500 on failure. ErrClientUnknown maps to a 404 envelope.
func (h *HTTP) dashJSON(w http.ResponseWriter, ar *AdminRequest, fn func(ctx context.Context) (any, error)) {
	data, err := fn(ar.Request.Context())
	if err == errClientUnknown {
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
