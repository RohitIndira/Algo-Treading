package admin

// M12.3 — holding analytics, unique scripts and the multi-client equity
// comparison (frontend change request, 2026-10-07). Same DashboardStore,
// same sources, same scoping rule: userID == "" is the whole book.

import (
	"context"
	"database/sql"
	"math"
	"net/http"
	"sort"
	"strings"
	"time"
)

// holdingDays is the calendar-day holding period in IST: the number of
// midnights between entry and end, never negative — a buy on Monday sold
// on Tuesday morning is 1 day, a same-day round trip is 0. Open rows end
// at "now"; closed rows at exit_time, or updated_at when the exit carried
// no timestamp (manual exits, ghost heals).
func holdingDays(entry, end time.Time, loc *time.Location) int {
	e, x := entry.In(loc), end.In(loc)
	ed := time.Date(e.Year(), e.Month(), e.Day(), 0, 0, 0, 0, loc)
	xd := time.Date(x.Year(), x.Month(), x.Day(), 0, 0, 0, 0, loc)
	if xd.Before(ed) {
		return 0
	}
	return int(math.Round(xd.Sub(ed).Hours() / 24))
}

func meanInts(v []int) float64 {
	if len(v) == 0 {
		return 0
	}
	sum := 0
	for _, x := range v {
		sum += x
	}
	return round2f(float64(sum) / float64(len(v)))
}

// medianInts is the conventional median: middle value, or the mean of the
// two middle values for an even count.
func medianInts(v []int) float64 {
	if len(v) == 0 {
		return 0
	}
	s := append([]int(nil), v...)
	sort.Ints(s)
	n := len(s)
	if n%2 == 1 {
		return float64(s[n/2])
	}
	return round2f(float64(s[n/2-1]+s[n/2]) / 2)
}

// holdRow is one position that held stock, with its resolved holding days.
type holdRow struct {
	user, strategy, symbol, status string
	entry, updated                 time.Time
	exit                           sql.NullTime
	days                           int
	open                           bool
}

// holdRows loads every position that held stock (open + closed) in scope.
func (d *DashboardStore) holdRows(ctx context.Context, userID string) ([]holdRow, error) {
	scope, args := userScope(userID, nil)
	rows, err := d.tradingDB.QueryContext(ctx, `
		SELECT user_id, COALESCE(strategy_id::text,''), symbol, status, entry_time, exit_time, updated_at
		FROM manthan_positions WHERE status IN ('ACTIVE','EXIT_PENDING','EXITED')`+scope+`
		ORDER BY entry_time, symbol`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	now := d.now()
	var out []holdRow
	for rows.Next() {
		var r holdRow
		if err := rows.Scan(&r.user, &r.strategy, &r.symbol, &r.status, &r.entry, &r.exit, &r.updated); err != nil {
			return nil, err
		}
		r.open = r.status != "EXITED"
		end := now
		if !r.open {
			end = r.updated
			if r.exit.Valid {
				end = r.exit.Time
			}
		}
		r.days = holdingDays(r.entry, end, d.ist)
		out = append(out, r)
	}
	return out, rows.Err()
}

// ── 16. Holding analytics ───────────────────────────────────────────────

const holdingBasis = "IST calendar days (midnights between entry and end; same-day = 0, BTST = 1); open rows measured to now, closed rows entry→exit_time (updated_at when exit_time is missing); averages over open+closed rows unless suffixed _open/_closed"

// HoldingAnalytics is /portfolio/holding-analytics[?client_id=].
func (d *DashboardStore) HoldingAnalytics(ctx context.Context, userID string) (any, error) {
	rs, err := d.holdRows(ctx, userID)
	if err != nil {
		return nil, err
	}

	type scriptAgg struct {
		Script            string  `json:"script"`
		OpenCount         int     `json:"open_count"`
		ClosedCount       int     `json:"closed_count"`
		ClientsHolding    int     `json:"clients_holding"`
		AvgHoldingDays    float64 `json:"avg_holding_days"`
		MedianHoldingDays float64 `json:"median_holding_days"`
		days              []int
		holders           map[string]bool
	}
	type clientAgg struct {
		ClientID          string  `json:"client_id"`
		ClientName        string  `json:"client_name"`
		StrategyCount     int     `json:"strategy_count"`
		OpenPositions     int     `json:"open_positions"`
		ClosedPositions   int     `json:"closed_positions"`
		AvgHoldingDays    float64 `json:"avg_holding_days"`
		MedianHoldingDays float64 `json:"median_holding_days"`
		days              []int
		strategies        map[string]bool
	}
	scripts := map[string]*scriptAgg{}
	clients := map[string]*clientAgg{}
	var all, openDays, closedDays []int
	uniqueOpen, uniqueAll := map[string]bool{}, map[string]bool{}
	for _, r := range rs {
		all = append(all, r.days)
		uniqueAll[r.symbol] = true
		sa := scripts[r.symbol]
		if sa == nil {
			sa = &scriptAgg{Script: r.symbol, holders: map[string]bool{}}
			scripts[r.symbol] = sa
		}
		ca := clients[r.user]
		if ca == nil {
			ca = &clientAgg{ClientID: r.user, ClientName: r.user, strategies: map[string]bool{}}
			clients[r.user] = ca
		}
		sa.days = append(sa.days, r.days)
		ca.days = append(ca.days, r.days)
		if r.strategy != "" {
			ca.strategies[r.strategy] = true
		}
		if r.open {
			openDays = append(openDays, r.days)
			uniqueOpen[r.symbol] = true
			sa.OpenCount++
			sa.holders[r.user] = true
			ca.OpenPositions++
		} else {
			closedDays = append(closedDays, r.days)
			sa.ClosedCount++
			ca.ClosedPositions++
		}
	}
	byScript := make([]scriptAgg, 0, len(scripts))
	for _, sa := range scripts {
		sa.ClientsHolding = len(sa.holders)
		sa.AvgHoldingDays, sa.MedianHoldingDays = meanInts(sa.days), medianInts(sa.days)
		byScript = append(byScript, *sa)
	}
	sort.Slice(byScript, func(i, j int) bool {
		if byScript[i].OpenCount != byScript[j].OpenCount {
			return byScript[i].OpenCount > byScript[j].OpenCount
		}
		if byScript[i].AvgHoldingDays != byScript[j].AvgHoldingDays {
			return byScript[i].AvgHoldingDays > byScript[j].AvgHoldingDays
		}
		return byScript[i].Script < byScript[j].Script
	})
	byClient := make([]clientAgg, 0, len(clients))
	for _, ca := range clients {
		ca.StrategyCount = len(ca.strategies)
		ca.AvgHoldingDays, ca.MedianHoldingDays = meanInts(ca.days), medianInts(ca.days)
		byClient = append(byClient, *ca)
	}
	sort.Slice(byClient, func(i, j int) bool { return byClient[i].ClientID < byClient[j].ClientID })

	return map[string]any{
		"overall": map[string]any{
			"avg_holding_days":        meanInts(all),
			"median_holding_days":     medianInts(all),
			"avg_holding_days_open":   meanInts(openDays),
			"avg_holding_days_closed": meanInts(closedDays),
			"open_positions":          len(openDays),
			"closed_positions":        len(closedDays),
			"total_unique_scripts":    len(uniqueOpen), // distinct tickers in the OPEN book
			"unique_scripts_all_time": len(uniqueAll),
		},
		"by_script":   byScript,
		"by_strategy": byClient, // per client, as the spec describes it; strategy_count carried per client
		"basis":       holdingBasis,
	}, nil
}

// ── 17. Unique scripts ──────────────────────────────────────────────────

// UniqueScripts is /portfolio/unique-scripts[?client_id=]: every distinct
// ticker in the open book with who holds it and how much.
func (d *DashboardStore) UniqueScripts(ctx context.Context, userID string) (any, error) {
	scope, args := userScope(userID, nil)
	rows, err := d.tradingDB.QueryContext(ctx, `
		SELECT symbol, COUNT(DISTINCT user_id), COALESCE(SUM(quantity),0), COALESCE(SUM(invested_amt),0),
		       array_to_string(array_agg(DISTINCT user_id ORDER BY user_id), ',')
		FROM manthan_positions WHERE status IN `+openStatuses+scope+`
		GROUP BY symbol`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	type script struct {
		Script         string   `json:"script"`
		ClientsHolding int      `json:"clients_holding"`
		Clients        []string `json:"clients"`
		TotalQuantity  int      `json:"total_quantity"`
		TotalInvested  float64  `json:"total_invested"`
		TotalValue     float64  `json:"total_value"` // live LTP × qty when known, else invested
		PnL            float64  `json:"pnl"`
		Price          float64  `json:"price"` // LTP when known, else average entry
	}
	out := make([]script, 0)
	var syms []string
	for rows.Next() {
		var s script
		var holders string
		if err := rows.Scan(&s.Script, &s.ClientsHolding, &s.TotalQuantity, &s.TotalInvested, &holders); err != nil {
			return nil, err
		}
		s.Clients = strings.Split(holders, ",")
		if holders == "" {
			s.Clients = []string{}
		}
		s.TotalValue = s.TotalInvested
		if s.TotalQuantity > 0 {
			s.Price = s.TotalInvested / float64(s.TotalQuantity)
		}
		out = append(out, s)
		syms = append(syms, s.Script)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	quotes := d.fetchQuotes(ctx, syms)
	total := 0.0
	for i := range out {
		if q, ok := quotes[out[i].Script]; ok && q.LTP > 0 {
			live := q.LTP * float64(out[i].TotalQuantity)
			out[i].PnL = round2f(live - out[i].TotalInvested)
			out[i].TotalValue, out[i].Price = live, q.LTP
		}
		total += out[i].TotalValue
		out[i].TotalValue = round2f(out[i].TotalValue)
		out[i].TotalInvested = round2f(out[i].TotalInvested)
		out[i].Price = round2f(out[i].Price)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].TotalValue != out[j].TotalValue {
			return out[i].TotalValue > out[j].TotalValue
		}
		return out[i].Script < out[j].Script
	})
	return map[string]any{"scripts": out, "count": len(out), "total_value": round2f(total)}, nil
}

// ── 18. Multi-client equity comparison ──────────────────────────────────

var comparisonBenchmarks = []string{"nifty50", "midcap150", "smallcap250"}

const comparisonBasis = "portfolio_indexed = time-weighted return index, 100 at each client's first point in the window; top-level benchmarks indexed to 100 at the earliest client start; per-client outperformance = final portfolio_indexed − benchmark re-indexed to 100 at that client's own start date (fair pairing); beating_nifty = outperformance_nifty_pct > 0"

// ClientEquityComparison is /portfolio/client-equity-comparison?days=
// [&client_id=]: one response with the benchmark curves once, and every
// client's TWR curve plus "is this client beating Nifty" flags. userID ==
// "" compares the whole roster; a client id narrows to that one client.
func (d *DashboardStore) ClientEquityComparison(ctx context.Context, userID string, days int) (any, error) {
	bases, err := d.clientBases(ctx, userID)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(bases))
	for id := range bases {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	type point struct {
		Date             string  `json:"date"`
		PortfolioIndexed float64 `json:"portfolio_indexed"`
		PortfolioValue   float64 `json:"portfolio_value"`
	}
	type benchPoint struct {
		Date    string  `json:"date"`
		Indexed float64 `json:"indexed"`
	}
	type client struct {
		ClientID          string   `json:"client_id"`
		ClientName        string   `json:"client_name"`
		Status            string   `json:"status"`
		InvestedFund      float64  `json:"invested_fund"`
		StartDate         string   `json:"start_date,omitempty"`
		EndDate           string   `json:"end_date,omitempty"`
		Points            int      `json:"points"`
		FinalIndexed      *float64 `json:"final_indexed"`
		NiftyFinalIndexed *float64 `json:"nifty_final_indexed"`
		BeatingNifty      *bool    `json:"beating_nifty"`
		OutNifty          *float64 `json:"outperformance_nifty_pct"`
		OutMidcap         *float64 `json:"outperformance_midcap150_pct"`
		OutSmallcap       *float64 `json:"outperformance_smallcap250_pct"`
		Series            []point  `json:"series"`
	}

	navs := map[string][]navDay{}
	var earliest time.Time
	for _, id := range ids {
		nav, err := d.navSeries(ctx, id, days)
		if err != nil {
			return nil, err
		}
		navs[id] = nav
		if len(nav) > 0 && (earliest.IsZero() || nav[0].Date.Before(earliest)) {
			earliest = nav[0].Date
		}
	}

	closes := map[string]map[string]float64{}
	if !earliest.IsZero() {
		if closes, err = d.benchmarkCloses(ctx, earliest); err != nil {
			return nil, err
		}
	}
	benchDates := map[string][]string{}
	benchmarks := map[string][]benchPoint{}
	for _, b := range comparisonBenchmarks {
		dates := make([]string, 0, len(closes[b]))
		for dt := range closes[b] {
			dates = append(dates, dt)
		}
		sort.Strings(dates)
		benchDates[b] = dates
		series := make([]benchPoint, 0, len(dates))
		for _, dt := range dates {
			base := closes[b][dates[0]]
			if base > 0 {
				series = append(series, benchPoint{Date: dt, Indexed: round2f(closes[b][dt] / base * 100)})
			}
		}
		benchmarks[b] = series
	}
	// benchAt: the close on that date, else the latest close before it
	// (holiday/weekend NAV rows), else the first close after it.
	benchAt := func(b, date string) (float64, bool) {
		dates := benchDates[b]
		if len(dates) == 0 {
			return 0, false
		}
		i := sort.SearchStrings(dates, date)
		switch {
		case i < len(dates) && dates[i] == date:
			return closes[b][date], true
		case i > 0:
			return closes[b][dates[i-1]], true
		default:
			return closes[b][dates[i]], true
		}
	}

	clients := make([]client, 0, len(ids))
	withData, beating := 0, 0
	for _, id := range ids {
		b := bases[id]
		c := client{ClientID: id, ClientName: id, Status: "PAUSED", InvestedFund: round2f(b.InvestedFund), Series: []point{}}
		if b.ActiveCount > 0 {
			c.Status = "ACTIVE"
		}
		nav := navs[id]
		if len(nav) == 0 {
			clients = append(clients, c)
			continue
		}
		idx := twrIndex(nav)
		for i, n := range nav {
			c.Series = append(c.Series, point{
				Date: n.Date.Format("2006-01-02"), PortfolioIndexed: round2f(idx[i]),
				PortfolioValue: round2f(n.Capital + n.NetPnL),
			})
		}
		c.Points = len(c.Series)
		c.StartDate, c.EndDate = c.Series[0].Date, c.Series[len(c.Series)-1].Date
		final := round2f(idx[len(idx)-1])
		c.FinalIndexed = &final
		withData++
		outVs := func(bm string) (*float64, *float64) {
			b0, ok0 := benchAt(bm, c.StartDate)
			b1, ok1 := benchAt(bm, c.EndDate)
			if !ok0 || !ok1 || b0 <= 0 {
				return nil, nil
			}
			bi := round2f(b1 / b0 * 100)
			v := round2f(final - bi)
			return &v, &bi
		}
		c.OutNifty, c.NiftyFinalIndexed = outVs("nifty50")
		c.OutMidcap, _ = outVs("midcap150")
		c.OutSmallcap, _ = outVs("smallcap250")
		if c.OutNifty != nil {
			bn := *c.OutNifty > 0
			c.BeatingNifty = &bn
			if bn {
				beating++
			}
		}
		clients = append(clients, c)
	}
	// Leaderboard: clients with a Nifty comparison, best outperformance first.
	lb := make([]string, 0, len(clients))
	ranked := make([]client, 0, len(clients))
	for _, c := range clients {
		if c.OutNifty != nil {
			ranked = append(ranked, c)
		}
	}
	sort.SliceStable(ranked, func(i, j int) bool { return *ranked[i].OutNifty > *ranked[j].OutNifty })
	for _, c := range ranked {
		lb = append(lb, c.ClientID)
	}
	start := ""
	if !earliest.IsZero() {
		start = earliest.Format("2006-01-02")
	}
	return map[string]any{
		"days":        days,
		"start_date":  start,
		"benchmarks":  benchmarks,
		"clients":     clients,
		"leaderboard": lb,
		"summary": map[string]any{
			"clients_total":         len(ids),
			"clients_with_data":     withData,
			"clients_beating_nifty": beating,
		},
		"basis": comparisonBasis,
	}, nil
}

// ── HTTP handlers (mounted in http.go) ──────────────────────────────────

func (h *HTTP) handleDashHoldingAnalytics(w http.ResponseWriter, ar *AdminRequest) {
	h.scoped(w, ar, h.dashboard.HoldingAnalytics)
}

func (h *HTTP) handleDashUniqueScripts(w http.ResponseWriter, ar *AdminRequest) {
	h.scoped(w, ar, h.dashboard.UniqueScripts)
}

func (h *HTTP) handleDashClientEquityComparison(w http.ResponseWriter, ar *AdminRequest) {
	h.scoped(w, ar, func(ctx context.Context, uid string) (any, error) {
		return h.dashboard.ClientEquityComparison(ctx, uid, daysParam(ar.Request, daysEquityCurve))
	})
}
