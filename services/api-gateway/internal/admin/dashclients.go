package admin

// M12 clients page — per-client analytics built on the same DashboardStore.
// See dashboard.go for the data-source map.

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"time"
)

// errClientUnknown → 404 envelope (client_id with no strategies at all).
var errClientUnknown = errors.New("client unknown")

// clientBase is the strategy-roster view of one client: capital and status.
type clientBase struct {
	UserID       string
	StartedAt    time.Time
	ActiveCount  int
	InvestedFund float64 // SUM(total_capital) over ACTIVE strategies
}

// clientBases lists every user that has (non-deleted) strategies. userID
// filters to one client; "" returns all.
func (d *DashboardStore) clientBases(ctx context.Context, userID string) (map[string]*clientBase, error) {
	q := `SELECT s.user_id, MIN(s.created_at),
	             COUNT(*) FILTER (WHERE s.active),
	             COALESCE(SUM(tc.total_capital) FILTER (WHERE s.active), 0)
	      FROM strategies s
	      LEFT JOIN trade_configs tc ON tc.strategy_id::text = s.strategy_id::text
	      WHERE s.deleted_at IS NULL`
	args := []any{}
	if userID != "" {
		q += ` AND s.user_id = $1`
		args = append(args, userID)
	}
	q += ` GROUP BY s.user_id ORDER BY s.user_id`
	rows, err := d.tradingDB.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]*clientBase{}
	for rows.Next() {
		var b clientBase
		if err := rows.Scan(&b.UserID, &b.StartedAt, &b.ActiveCount, &b.InvestedFund); err != nil {
			return nil, err
		}
		out[b.UserID] = &b
	}
	return out, rows.Err()
}

// openBook returns per-user open-position count + utilized exposure.
func (d *DashboardStore) openBook(ctx context.Context) (map[string]int, map[string]float64, error) {
	rows, err := d.tradingDB.QueryContext(ctx, `
		SELECT user_id, COUNT(*), COALESCE(SUM(invested_amt),0)
		FROM manthan_positions WHERE status IN `+openStatuses+` GROUP BY user_id`)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	counts, utilized := map[string]int{}, map[string]float64{}
	for rows.Next() {
		var u string
		var c int
		var v float64
		if err := rows.Scan(&u, &c, &v); err != nil {
			return nil, nil, err
		}
		counts[u], utilized[u] = c, v
	}
	return counts, utilized, rows.Err()
}

// latestNav returns, per user, the two most recent aggregated NAV days
// (for pnl + daily MTM). Window of 10 days covers weekends/holidays.
func (d *DashboardStore) latestNav(ctx context.Context, userID string) (last, prev map[string]navDay, err error) {
	since := d.now().In(d.ist).AddDate(0, 0, -10).Format("2006-01-02")
	q := `SELECT user_id, date, SUM(net_pnl_amount), SUM(deployed_capital),
	             SUM(open_positions), SUM(unrealized_amount), SUM(realized_amount)
	      FROM strategy_nav_daily WHERE date >= $1`
	args := []any{since}
	if userID != "" {
		q += ` AND user_id = $2`
		args = append(args, userID)
	}
	q += ` GROUP BY user_id, date ORDER BY user_id, date`
	rows, err := d.perfDB.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	last, prev = map[string]navDay{}, map[string]navDay{}
	for rows.Next() {
		var u string
		var n navDay
		if err := rows.Scan(&u, &n.Date, &n.NetPnL, &n.Capital, &n.OpenPos, &n.Unrealized, &n.Realized); err != nil {
			return nil, nil, err
		}
		if cur, ok := last[u]; ok {
			prev[u] = cur
		}
		last[u] = n
	}
	return last, prev, rows.Err()
}

// ── 10. Clients list ────────────────────────────────────────────────────

func (d *DashboardStore) Clients(ctx context.Context) (any, error) {
	bases, err := d.clientBases(ctx, "")
	if err != nil {
		return nil, err
	}
	counts, utilized, err := d.openBook(ctx)
	if err != nil {
		return nil, err
	}
	last, prev, err := d.latestNav(ctx, "")
	if err != nil {
		return nil, err
	}
	type client struct {
		UserID           string  `json:"user_id"`
		ClientCode       string  `json:"client_code"`
		ClientName       string  `json:"client_name"`
		StartedAt        string  `json:"started_at"`
		Positions        int     `json:"positions"`
		InvestedFund     float64 `json:"invested_fund"`
		UtilizedExposure float64 `json:"utilized_exposure"`
		RemainingFund    float64 `json:"remaining_fund"`
		Status           string  `json:"status"`
		PnL              float64 `json:"pnl"`
		PnLPct           float64 `json:"pnl_pct"`
		DailyMTM         float64 `json:"daily_mtm"`
		DailyMTMPct      float64 `json:"daily_mtm_pct"`
		NetWorth         float64 `json:"net_worth"`
	}
	var out []client
	for _, b := range bases {
		c := client{
			UserID: b.UserID, ClientCode: b.UserID, ClientName: b.UserID,
			StartedAt: b.StartedAt.In(d.ist).Format("2006-01-02"),
			Positions: counts[b.UserID], InvestedFund: round2f(b.InvestedFund),
			UtilizedExposure: round2f(utilized[b.UserID]),
			Status:           "STOPPED",
		}
		if b.ActiveCount > 0 {
			c.Status = "ACTIVE"
		}
		c.RemainingFund = round2f(math.Max(0, b.InvestedFund-utilized[b.UserID]))
		if n, ok := last[b.UserID]; ok {
			c.PnL = round2f(n.NetPnL)
			c.PnLPct = pct(n.NetPnL, b.InvestedFund)
			if p, ok := prev[b.UserID]; ok {
				c.DailyMTM = round2f(n.NetPnL - p.NetPnL)
				c.DailyMTMPct = pct(c.DailyMTM, b.InvestedFund)
			}
		}
		c.NetWorth = round2f(b.InvestedFund + c.PnL)
		out = append(out, c)
	}
	// map iteration is random; keep the listing stable
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j].UserID < out[j-1].UserID; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return map[string]any{"clients": out, "count": len(out)}, nil
}

// ── portfolio-value series (shared by curve/drawdown/summary) ───────────

// valueSeries converts a client's NAV days into portfolio values
// (deployed capital + cumulative P&L per day).
func (d *DashboardStore) valueSeries(ctx context.Context, userID string, days int) ([]navDay, []float64, error) {
	nav, err := d.navSeries(ctx, userID, days)
	if err != nil {
		return nil, nil, err
	}
	vals := make([]float64, len(nav))
	for i, n := range nav {
		vals[i] = n.Capital + n.NetPnL
	}
	return nav, vals, nil
}

// requireClient 404s unknown client ids before running series queries.
func (d *DashboardStore) requireClient(ctx context.Context, userID string) error {
	bases, err := d.clientBases(ctx, userID)
	if err != nil {
		return err
	}
	if len(bases) == 0 {
		return errClientUnknown
	}
	return nil
}

// ── 11. Client summary ──────────────────────────────────────────────────

func (d *DashboardStore) ClientSummary(ctx context.Context, userID string) (any, error) {
	bases, err := d.clientBases(ctx, userID)
	if err != nil {
		return nil, err
	}
	b, ok := bases[userID]
	if !ok {
		return nil, errClientUnknown
	}
	agg, err := d.positionsAgg(ctx, userID)
	if err != nil {
		return nil, err
	}
	counts, utilized, err := d.openBook(ctx)
	if err != nil {
		return nil, err
	}
	best, worst, err := d.bestWorst(ctx, userID)
	if err != nil {
		return nil, err
	}
	nav, vals, err := d.valueSeries(ctx, userID, 3650)
	if err != nil {
		return nil, err
	}

	var pnl, realized, unrealized, dailyMTM float64
	if n := len(nav); n > 0 {
		lastN := nav[n-1]
		pnl, realized, unrealized = lastN.NetPnL, lastN.Realized, lastN.Unrealized
		if n > 1 {
			dailyMTM = lastN.NetPnL - nav[n-2].NetPnL
		}
	}

	// CAGR from the first to the last portfolio value. XIRR needs a cash-flow
	// ledger the platform doesn't keep, so it's reported equal to CAGR
	// (single-deposit approximation).
	cagr := 0.0
	if len(vals) > 1 && vals[0] > 0 {
		years := nav[len(nav)-1].Date.Sub(nav[0].Date).Hours() / 24 / 365.25
		if years > 0 {
			cagr = round2f((math.Pow(vals[len(vals)-1]/vals[0], 1/years) - 1) * 100)
		}
	}
	maxDD, _ := drawdownStats(vals)

	return map[string]any{
		"client_name":          userID,
		"client_code":          userID,
		"invested_fund":        round2f(b.InvestedFund),
		"portfolio_value":      round2f(b.InvestedFund + pnl),
		"realized_pnl":         round2f(realized),
		"unrealized_pnl":       round2f(unrealized),
		"pnl_pct":              pct(pnl, b.InvestedFund),
		"daily_mtm":            round2f(dailyMTM),
		"daily_mtm_pct":        pct(dailyMTM, b.InvestedFund),
		"cagr":                 cagr,
		"xirr":                 cagr,
		"max_drawdown_pct":     maxDD,
		"exposure_pct":         pct(utilized[userID], b.InvestedFund),
		"avg_profit_per_trade": agg.AvgProfitPerTrade,
		"avg_loss_per_trade":   agg.AvgLossPerTrade,
		"total_closed_trades":  agg.ClosedPositions,
		"open_positions":       counts[userID],
		"best_trade":           best,
		"worst_trade":          worst,
	}, nil
}

// ── 12. Equity curve vs Nifty ───────────────────────────────────────────

func (d *DashboardStore) EquityCurve(ctx context.Context, userID string, days int) (any, error) {
	if err := d.requireClient(ctx, userID); err != nil {
		return nil, err
	}
	nav, vals, err := d.valueSeries(ctx, userID, days)
	if err != nil {
		return nil, err
	}
	type point struct {
		Date             string   `json:"date"`
		PortfolioIndexed float64  `json:"portfolio_indexed"`
		NiftyIndexed     *float64 `json:"nifty_indexed,omitempty"`
		PortfolioValue   float64  `json:"portfolio_value"`
	}
	series := make([]point, 0, len(nav))
	if len(nav) == 0 {
		return map[string]any{"series": series, "outperformance_pct": 0}, nil
	}

	nifty, err := d.niftyCloses(ctx, nav[0].Date)
	if err != nil {
		return nil, err
	}
	var niftyBase, lastPort, lastNifty float64
	for i, n := range nav {
		p := point{
			Date:           n.Date.Format("2006-01-02"),
			PortfolioValue: round2f(vals[i]),
		}
		if vals[0] > 0 {
			p.PortfolioIndexed = round2f(vals[i] / vals[0] * 100)
			lastPort = p.PortfolioIndexed
		}
		if close, ok := nifty[p.Date]; ok {
			if niftyBase == 0 {
				niftyBase = close
			}
			idx := round2f(close / niftyBase * 100)
			p.NiftyIndexed = &idx
			lastNifty = idx
		}
		series = append(series, p)
	}
	out := round2f(lastPort - lastNifty)
	if lastNifty == 0 {
		out = 0
	}
	return map[string]any{"series": series, "outperformance_pct": out}, nil
}

// niftyCloses maps ISO date → nifty50 close since a start date.
func (d *DashboardStore) niftyCloses(ctx context.Context, since time.Time) (map[string]float64, error) {
	rows, err := d.perfDB.QueryContext(ctx, `
		SELECT date, close_value FROM benchmark_daily
		WHERE benchmark_id = 'nifty50' AND date >= $1 ORDER BY date`,
		since.Format("2006-01-02"))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]float64{}
	for rows.Next() {
		var dt time.Time
		var close float64
		if err := rows.Scan(&dt, &close); err != nil {
			return nil, err
		}
		out[dt.Format("2006-01-02")] = close
	}
	return out, rows.Err()
}

// ── 13. Drawdown ────────────────────────────────────────────────────────

// drawdownStats returns (max drawdown %, per-point drawdown %) over values.
func drawdownStats(vals []float64) (float64, []float64) {
	dd := make([]float64, len(vals))
	peak, maxDD := 0.0, 0.0
	for i, v := range vals {
		if v > peak {
			peak = v
		}
		if peak > 0 {
			dd[i] = round2f((v - peak) / peak * 100)
		}
		if dd[i] < maxDD {
			maxDD = dd[i]
		}
	}
	return maxDD, dd
}

func (d *DashboardStore) Drawdown(ctx context.Context, userID string, days int) (any, error) {
	if err := d.requireClient(ctx, userID); err != nil {
		return nil, err
	}
	nav, vals, err := d.valueSeries(ctx, userID, days)
	if err != nil {
		return nil, err
	}
	maxDD, dd := drawdownStats(vals)
	type point struct {
		Date        string  `json:"date"`
		DrawdownPct float64 `json:"drawdown_pct"`
		PeakValue   float64 `json:"peak_value"`
	}
	series := make([]point, 0, len(nav))
	peak := 0.0
	current := 0.0
	for i, n := range nav {
		if vals[i] > peak {
			peak = vals[i]
		}
		series = append(series, point{
			Date: n.Date.Format("2006-01-02"), DrawdownPct: dd[i], PeakValue: round2f(peak),
		})
		current = dd[i]
	}
	return map[string]any{
		"series": series, "max_drawdown_pct": maxDD, "current_drawdown_pct": current,
	}, nil
}

// ── 14. MTM series ──────────────────────────────────────────────────────

func (d *DashboardStore) MTMSeries(ctx context.Context, userID string, days int) (any, error) {
	if err := d.requireClient(ctx, userID); err != nil {
		return nil, err
	}
	nav, err := d.navSeries(ctx, userID, days)
	if err != nil {
		return nil, err
	}
	type point struct {
		Date           string  `json:"date"`
		MTM            float64 `json:"mtm"`
		MTMPct         float64 `json:"mtm_pct"`
		PositionsCount int     `json:"positions_count"`
		CumulativeMTM  float64 `json:"cumulative_mtm"`
	}
	series := make([]point, 0, len(nav))
	prev, sum := 0.0, 0.0
	for i, n := range nav {
		mtm := n.NetPnL - prev
		if i == 0 {
			mtm = 0
		}
		series = append(series, point{
			Date: n.Date.Format("2006-01-02"), MTM: round2f(mtm),
			MTMPct: pct(mtm, n.Capital), PositionsCount: n.OpenPos,
			CumulativeMTM: round2f(n.NetPnL),
		})
		prev = n.NetPnL
		sum += mtm
	}
	avg := 0.0
	if len(series) > 1 {
		avg = round2f(sum / float64(len(series)-1)) // first point carries no MTM
	}
	return map[string]any{"series": series, "avg_daily_mtm": avg}, nil
}

// silence the unused-import vet if sql ends up unused in a refactor
var _ = sql.ErrNoRows
