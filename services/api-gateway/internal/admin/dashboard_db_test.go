package admin

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"
)

// ── Fixture: synthetic clients in the local trading_db ─────────────────
//
// TDASH_A: 2 open (SMALL/Chemicals with EMA 50%, MID/Pharma without EMA)
//          + closed winner (+500) + closed loser (-200). ₹5L capital.
// TDASH_B: 1 open (LARGE/Banks) + closed winner (+900). ₹10L capital.
// TDASH_C: strategy soft-deleted, 1 open position kept (Banks/LARGE) —
//          in the book but off the strategy roster.
// TDASH_D: active strategy, no positions yet (freshly onboarded).
// NAV: A and B have rows at today-40 (pnl 0) and today-2..today.
// signals_db.manthan_stocks: 52-week highs for the three open symbols.

const (
	dashA = "TDASH_A"
	dashB = "TDASH_B"
	dashC = "TDASH_C"
	dashD = "TDASH_D"
)

type dashDBs struct{ trading, perf, signals *sql.DB }

func openDashTestDBs(t *testing.T) dashDBs {
	t.Helper()
	trading := openAdminTestDB(t)
	open := func(name string) *sql.DB {
		db, err := sql.Open("postgres",
			"host=localhost port=5432 user=postgres password=postgres dbname="+name+" sslmode=disable")
		if err != nil {
			t.Fatalf("open %s: %v", name, err)
		}
		if err := db.Ping(); err != nil {
			t.Skipf("%s not reachable — skipping (%v)", name, err)
		}
		return db
	}
	perf, signals := open("stockk_market"), open("signals_db")
	mig, err := os.ReadFile(filepath.Join("..", "..", "migrations", "001_strategy_nav_daily.sql"))
	if err != nil {
		t.Fatalf("read nav migration: %v", err)
	}
	if _, err := perf.Exec(string(mig)); err != nil {
		t.Fatalf("apply nav migration: %v", err)
	}
	dbs := dashDBs{trading, perf, signals}
	cleanupDashRows(t, dbs)
	t.Cleanup(func() { cleanupDashRows(t, dbs); perf.Close(); signals.Close() })
	return dbs
}

func cleanupDashRows(t *testing.T, dbs dashDBs) {
	t.Helper()
	_, _ = dbs.trading.Exec(`DELETE FROM manthan_positions WHERE user_id LIKE 'TDASH%'`)
	_, _ = dbs.trading.Exec(`DELETE FROM trade_configs WHERE strategy_id IN (SELECT strategy_id FROM strategies WHERE user_id LIKE 'TDASH%')`)
	_, _ = dbs.trading.Exec(`DELETE FROM strategies WHERE user_id LIKE 'TDASH%'`)
	_, _ = dbs.perf.Exec(`DELETE FROM strategy_nav_daily WHERE user_id LIKE 'TDASH%'`)
	_, _ = dbs.signals.Exec(`DELETE FROM manthan_stocks WHERE symbol LIKE 'TDASH%'`)
}

func seedDashFixture(t *testing.T, dbs dashDBs) {
	t.Helper()
	mustExec := func(db *sql.DB, q string, args ...any) {
		t.Helper()
		if _, err := db.Exec(q, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, q)
		}
	}
	strategy := func(user string, deleted bool) string {
		var sid string
		del := "NULL"
		if deleted {
			del = "now()"
		}
		if err := dbs.trading.QueryRow(`INSERT INTO strategies (user_id, strategy_name, active, created_at, deleted_at)
			VALUES ($1, 'Manthan', $2, now() - interval '30 days', `+del+`) RETURNING strategy_id::text`,
			user, !deleted).Scan(&sid); err != nil {
			t.Fatalf("seed strategy %s: %v", user, err)
		}
		return sid
	}
	sidA, sidB, sidC, sidD := strategy(dashA, false), strategy(dashB, false), strategy(dashC, true), strategy(dashD, false)
	mustExec(dbs.trading, `INSERT INTO trade_configs (strategy_id, order_type, product_type, validity, quantity, exchange, total_capital)
		VALUES ($1::uuid, 'MARKET', 'CNC', 'DAY', 0, 'NSE', 500000),
		       ($2::uuid, 'MARKET', 'CNC', 'DAY', 0, 'NSE', 1000000),
		       ($3::uuid, 'MARKET', 'CNC', 'DAY', 0, 'NSE', 300000),
		       ($4::uuid, 'MARKET', 'CNC', 'DAY', 0, 'NSE', 200000)`, sidA, sidB, sidC, sidD)

	pos := `INSERT INTO manthan_positions
		(strategy_id, user_id, symbol, industry, mcap_bucket, index_name, entry_price, quantity, invested_amt,
		 ema_alloc_pct, status, exit_price, realized_pnl, entry_time, exit_time)
		VALUES ($1::uuid, $2, $3, $4, $5, 'NIFTY', $6, $7, $8, $9, $10, $11, $12, now() - interval '5 days', now() - $13::interval)`
	// entry and exit both come from the statement's now(), so holding periods are exact whole days
	mustExec(dbs.trading, pos, sidA, dashA, "TDASHCHEM", "Chemicals", "SMALL", 100.0, 100, 10000.0, 0.5, "ACTIVE", nil, nil, nil)
	mustExec(dbs.trading, pos, sidA, dashA, "TDASHPHRM", "Pharmaceuticals", "MID", 200.0, 150, 30000.0, nil, "ACTIVE", nil, nil, nil)
	mustExec(dbs.trading, pos, sidA, dashA, "TDASHWIN", "Chemicals", "SMALL", 50.0, 100, 5000.0, 0.5, "EXITED", 55.0, 500.0, "1 day")
	mustExec(dbs.trading, pos, sidA, dashA, "TDASHLOSE", "Pharmaceuticals", "MID", 100.0, 20, 2000.0, 0.5, "EXITED", 90.0, -200.0, "2 days")
	mustExec(dbs.trading, pos, sidB, dashB, "TDASHBANK", "Banks", "LARGE", 1000.0, 50, 50000.0, 0.5, "ACTIVE", nil, nil, nil)
	mustExec(dbs.trading, pos, sidB, dashB, "TDASHBWIN", "Banks", "LARGE", 100.0, 100, 10000.0, 0.5, "EXITED", 109.0, 900.0, "1 day")
	mustExec(dbs.trading, pos, sidC, dashC, "TDASHBANK", "Banks", "LARGE", 1000.0, 10, 10000.0, 0.5, "ACTIVE", nil, nil, nil)
	// closed without an exit timestamp (manual exit / ghost heal shape)
	mustExec(dbs.trading, pos, sidB, dashB, "TDASHBNOX", "Banks", "LARGE", 100.0, 10, 1000.0, 0.5, "EXITED", nil, nil, nil)

	// 52-week highs: CHEM 200 (price 100 → -50%), PHRM 250 (200 → -20%), BANK 1250 (1000 → -20%).
	mustExec(dbs.signals, `INSERT INTO manthan_stocks (symbol, status, week52_high) VALUES
		('TDASHCHEM','ACTIVE',200), ('TDASHPHRM','ACTIVE',250), ('TDASHBANK','ACTIVE',1250)`)

	// Benchmarks for the same dates: nifty50 95,100,100.1,100.1 · midcap150
	// 100,100,99,99 · smallcap250 100,100,100.2,100.2. Existing real rows on
	// these (id,date) pairs are snapshotted and restored on cleanup.
	seedBenchmarks(t, dbs.perf)

	// NAV: today-40 (pnl 0) then today-2..today. A: 0,300,300; B: 0,900,1500.
	nav := `INSERT INTO strategy_nav_daily (strategy_id, user_id, date, deployed_capital, net_pnl_amount, net_pnl_pct,
	        realized_amount, unrealized_amount, open_positions) VALUES ($1, $2, $3, $4, $5, 0, $6, $7, $8)`
	today := time.Now()
	day := func(back int) string { return today.AddDate(0, 0, -back).Format("2006-01-02") }
	mustExec(dbs.perf, nav, sidA, dashA, day(40), 500000, 0, 0, 0, 0)
	mustExec(dbs.perf, nav, sidB, dashB, day(40), 1000000, 0, 0, 0, 0)
	for i, p := range []int64{0, 300, 300} {
		mustExec(dbs.perf, nav, sidA, dashA, day(2-i), 500000, p, p, 0, 2)
	}
	for i, p := range []int64{0, 900, 1500} {
		mustExec(dbs.perf, nav, sidB, dashB, day(2-i), 1000000, p, 900, p-900, 1)
	}
}

// benchFixture: close per fixture-date index (0=today-40, 1=today-2,
// 2=today-1, 3=today). Missing indexes are deliberately ABSENT rows so the
// comparison exercises every benchAt path: nifty50 lacks today (carry-back
// at the end date), midcap150 lacks today-2 (first-after at a 30-day
// start), smallcap250 has only today-40 (no rows at all in a 30-day window
// → null outperformance; carry-back across 40 days in a 90-day window).
var benchFixture = map[string]map[int]float64{
	"nifty50":     {0: 95, 1: 100, 2: 100.1},
	"midcap150":   {0: 100, 2: 99, 3: 99},
	"smallcap250": {0: 100},
}

func fixtureDates() [4]string {
	today := time.Now()
	day := func(back int) string { return today.AddDate(0, 0, -back).Format("2006-01-02") }
	return [4]string{day(40), day(2), day(1), day(0)}
}

type benchSnap struct {
	id, date string
	close    float64
	ret      sql.NullFloat64
}

// seedBenchmarks upserts the fixture closes and registers a cleanup that
// puts back whatever was there before (or deletes what wasn't).
func seedBenchmarks(t *testing.T, perf *sql.DB) {
	t.Helper()
	dates := fixtureDates()
	var snaps []benchSnap
	var absent []benchSnap
	for id := range benchFixture {
		for _, dt := range dates {
			var sn benchSnap
			err := perf.QueryRow(`SELECT benchmark_id, date::text, close_value, return_pct FROM benchmark_daily WHERE benchmark_id=$1 AND date=$2`, id, dt).
				Scan(&sn.id, &sn.date, &sn.close, &sn.ret)
			switch err {
			case nil:
				snaps = append(snaps, sn)
			case sql.ErrNoRows:
				absent = append(absent, benchSnap{id: id, date: dt})
			default:
				t.Fatalf("snapshot benchmark: %v", err)
			}
		}
	}
	t.Cleanup(func() {
		for _, a := range absent {
			_, _ = perf.Exec(`DELETE FROM benchmark_daily WHERE benchmark_id=$1 AND date=$2`, a.id, a.date)
		}
		for _, sn := range snaps {
			_, _ = perf.Exec(`INSERT INTO benchmark_daily (benchmark_id, date, close_value, return_pct) VALUES ($1,$2,$3,$4)
				ON CONFLICT (benchmark_id, date) DO UPDATE SET close_value=EXCLUDED.close_value, return_pct=EXCLUDED.return_pct`,
				sn.id, sn.date, sn.close, sn.ret)
		}
	})
	for id, closes := range benchFixture {
		for i, dt := range dates {
			c, present := closes[i]
			if !present {
				if _, err := perf.Exec(`DELETE FROM benchmark_daily WHERE benchmark_id=$1 AND date=$2`, id, dt); err != nil {
					t.Fatalf("clear benchmark: %v", err)
				}
				continue
			}
			if _, err := perf.Exec(`INSERT INTO benchmark_daily (benchmark_id, date, close_value) VALUES ($1,$2,$3)
				ON CONFLICT (benchmark_id, date) DO UPDATE SET close_value = EXCLUDED.close_value`, id, dt, c); err != nil {
				t.Fatalf("seed benchmark: %v", err)
			}
		}
	}
}

// benchOnFixtureDates keeps only the fixture dates from a benchmark series
// (a dev box with a fresh benchmark sync may hold real rows in the window).
func benchOnFixtureDates(series []any) []map[string]any {
	want := map[string]bool{}
	for _, dt := range fixtureDates() {
		want[dt] = true
	}
	out := []map[string]any{}
	for _, p := range series {
		m := p.(map[string]any)
		if want[m["date"].(string)] {
			out = append(out, m)
		}
	}
	return out
}

func newDashStore(dbs dashDBs) *DashboardStore {
	d := NewDashboardStore(dbs.trading, nil, dbs.perf, dbs.signals, nil)
	fixed := time.Now()
	d.now = func() time.Time { return fixed }
	return d
}

// roundTrip normalises any store result through JSON so nested structs and
// maps compare by their wire shape.
func roundTrip(t *testing.T, v any) map[string]any {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v / %s", err, b)
	}
	return out
}

// canon is the comparable wire form: struct field order and map key order
// both collapse to sorted keys.
func canon(v any) string {
	b, _ := json.Marshal(v)
	var x any
	_ = json.Unmarshal(b, &x)
	c, _ := json.Marshal(x)
	return string(c)
}

func okv(t *testing.T) func(any, error) any {
	return func(v any, err error) any {
		t.Helper()
		if err != nil {
			t.Fatalf("store call: %v", err)
		}
		return v
	}
}

func TestDashboard_ClientScoping(t *testing.T) {
	dbs := openDashTestDBs(t)
	seedDashFixture(t, dbs)
	d := newDashStore(dbs)
	ctx := context.Background()
	ok := okv(t)

	// Sector: A sees only its two sectors, summing to 100%.
	sec := roundTrip(t, ok(d.SectorBreakdown(ctx, dashA)))["sectors"].([]any)
	if len(sec) != 2 {
		t.Fatalf("A sectors = %d, want 2: %v", len(sec), sec)
	}
	total := 0.0
	for _, s := range sec {
		m := s.(map[string]any)
		if m["name"] == "Banks" {
			t.Fatalf("A sees B's sector: %v", sec)
		}
		total += m["percentage"].(float64)
	}
	if total < 99.9 || total > 100.1 {
		t.Fatalf("A sector pct sum = %v", total)
	}
	all := roundTrip(t, ok(d.SectorBreakdown(ctx, "")))["sectors"].([]any)
	names := map[string]bool{}
	for _, s := range all {
		names[s.(map[string]any)["name"].(string)] = true
	}
	if !names["Banks"] || !names["Chemicals"] {
		t.Fatalf("book-wide sectors missing fixture rows: %v", names)
	}

	// Mcap: A has SMALL(1 open, avg +10%) and MID(1 open, avg -10%); LARGE zero-filled.
	segs := roundTrip(t, ok(d.McapPerformance(ctx, dashA)))["segments"].([]any)
	got := map[string]map[string]any{}
	for _, s := range segs {
		m := s.(map[string]any)
		got[m["cap"].(string)] = m
	}
	if got["SMALL"]["position_count"].(float64) != 1 || got["SMALL"]["avg_return_pct"].(float64) != 10 {
		t.Fatalf("A SMALL = %v", got["SMALL"])
	}
	if got["MID"]["position_count"].(float64) != 1 || got["MID"]["avg_return_pct"].(float64) != -10 {
		t.Fatalf("A MID = %v", got["MID"])
	}
	if got["LARGE"]["position_count"].(float64) != 0 || got["LARGE"]["value"].(float64) != 0 {
		t.Fatalf("A LARGE should be zero-filled, got %v", got["LARGE"])
	}

	// EMA: levels AND cohorts both scoped. A: Unknown(30k, 75%) + "50% allocation"(10k, 25%).
	ema := roundTrip(t, ok(d.EMAAllocation(ctx, dashA)))
	levels := ema["allocations"].([]any)
	if len(levels) != 2 {
		t.Fatalf("A ema levels = %v", levels)
	}
	l0, l1 := levels[0].(map[string]any), levels[1].(map[string]any)
	if l0["ema_level"] != "Unknown" || l0["value"].(float64) != 30000 || l0["percentage"].(float64) != 75 ||
		l1["ema_level"] != "50% allocation" || l1["value"].(float64) != 10000 || l1["percentage"].(float64) != 25 {
		t.Fatalf("A ema levels = %v", levels)
	}
	flag := ema["by_ema_flag"].(map[string]any)
	with, without := flag["with_ema"].(map[string]any), flag["without_ema"].(map[string]any)
	if with["position_count"].(float64) != 1 || with["value"].(float64) != 10000 ||
		without["position_count"].(float64) != 1 || without["value"].(float64) != 30000 {
		t.Fatalf("A ema cohorts = %v", flag)
	}

	// Best/worst: A's own extremes, never B's +900.
	bw := roundTrip(t, ok(d.BestWorstTrades(ctx, dashA)))
	if bw["best_trade"].(map[string]any)["pnl"].(float64) != 500 ||
		bw["worst_trade"].(map[string]any)["pnl"].(float64) != -200 {
		t.Fatalf("A best/worst = %v", bw)
	}

	// Positions summary scoped: 2 open, 2 closed, 1 win, 1 loss, PF 2.5,
	// portfolio value 5L + 300, exposure 40k/5L = 8%.
	ps := roundTrip(t, ok(d.PortfolioSummary(ctx, dashA)))
	wantPS := map[string]float64{
		"open_positions": 2, "closed_positions": 2, "profit_making": 1, "loss_making": 1,
		"win_rate_pct": 50, "profit_factor": 2.5, "portfolio_value": 500300,
		"current_exposure_pct": 8, "realized_pnl": 300,
	}
	for k, want := range wantPS {
		if ps[k].(float64) != want {
			t.Fatalf("A positions_summary.%s = %v, want %v (full: %v)", k, ps[k], want, ps)
		}
	}
	psB := roundTrip(t, ok(d.PortfolioSummary(ctx, dashB)))
	if psB["open_positions"].(float64) != 1 || psB["portfolio_value"].(float64) != 1001500 {
		t.Fatalf("B positions_summary = %v", psB)
	}

	// Stock allocation scoped: A's two open symbols only.
	sa := roundTrip(t, ok(d.StockAllocation(ctx, dashA)))
	if hs := sa["holdings"].([]any); len(hs) != 2 || sa["total"].(float64) != 40000 {
		t.Fatalf("A stock allocation = %v", sa)
	}

	// Down-from-high scoped, using the seeded 52w highs (no live feed here):
	// A → [CHEM -50, PHRM -20] ascending; B → [BANK -20]; book-wide ⊇ all three.
	dfhA := roundTrip(t, ok(d.DownFromHigh(ctx, dashA)))["positions"].([]any)
	if len(dfhA) != 2 ||
		dfhA[0].(map[string]any)["script"] != "TDASHCHEM" || dfhA[0].(map[string]any)["down_from_high_pct"].(float64) != -50 ||
		dfhA[1].(map[string]any)["script"] != "TDASHPHRM" || dfhA[1].(map[string]any)["down_from_high_pct"].(float64) != -20 {
		t.Fatalf("A down_from_high = %v", dfhA)
	}
	dfhB := roundTrip(t, ok(d.DownFromHigh(ctx, dashB)))["positions"].([]any)
	if len(dfhB) != 1 || dfhB[0].(map[string]any)["script"] != "TDASHBANK" || dfhB[0].(map[string]any)["client_id"] != dashB {
		t.Fatalf("B down_from_high = %v", dfhB)
	}
	dfhAll := roundTrip(t, ok(d.DownFromHigh(ctx, "")))["positions"].([]any)
	seen := map[string]int{}
	for _, e := range dfhAll {
		seen[e.(map[string]any)["script"].(string)]++
	}
	if seen["TDASHCHEM"] != 1 || seen["TDASHPHRM"] != 1 || seen["TDASHBANK"] != 2 /* B and C */ {
		t.Fatalf("book-wide down_from_high fixture coverage = %v", seen)
	}

	// Unknown / empty client → errClientUnknown from the composite.
	if _, err := d.ClientDashboard(ctx, "TDASH_NOPE", 30); err != errClientUnknown {
		t.Fatalf("unknown client err = %v, want errClientUnknown", err)
	}
	if _, err := d.ClientDashboard(ctx, "", 30); err != errClientUnknown {
		t.Fatalf("empty client err = %v, want errClientUnknown", err)
	}
}

// Windows: days threads through to every series, and the composite
// defaults each panel to its standalone window when days <= 0.
func TestDashboard_WindowsAndCompositeParity(t *testing.T) {
	dbs := openDashTestDBs(t)
	seedDashFixture(t, dbs)
	d := newDashStore(dbs)
	ctx := context.Background()
	ok := okv(t)

	points := func(v any) int { return len(roundTrip(t, v)["series"].([]any)) }
	if n := points(ok(d.PnLHistory(ctx, dashA, 30))); n != 3 {
		t.Fatalf("pnl 30d points = %d, want 3", n)
	}
	if n := points(ok(d.PnLHistory(ctx, dashA, 90))); n != 4 {
		t.Fatalf("pnl 90d points = %d, want 4 (includes today-40)", n)
	}

	// days <= 0 → per-panel defaults.
	comp := roundTrip(t, ok(d.ClientDashboard(ctx, dashA, 0)))
	wantWin := map[string]float64{"pnl_history": 90, "position_history": 30, "mtm_series": 30, "equity_curve": 90, "drawdown": 90}
	win := comp["windows"].(map[string]any)
	for k, w := range wantWin {
		if win[k].(float64) != w {
			t.Fatalf("default windows[%s] = %v, want %v", k, win[k], w)
		}
	}
	if points(comp["pnl_history"]) != 4 || points(comp["position_history"]) != 3 {
		t.Fatalf("default-window series lengths: pnl=%d pos=%d", points(comp["pnl_history"]), points(comp["position_history"]))
	}
	for _, k := range []string{"client_id", "client_code", "client_name", "windows", "generated_at", "summary",
		"positions_summary", "pnl_history", "position_history", "mtm_series", "equity_curve", "drawdown",
		"sector_breakdown", "stock_allocation", "mcap_performance", "ema_allocation", "best_worst_trades", "down_from_high",
		"holding_analytics", "unique_scripts"} {
		if _, present := comp[k]; !present {
			t.Fatalf("composite missing %q", k)
		}
	}

	// Parity: each key equals its standalone call at the same window —
	// once with defaults, once with an explicit 30.
	check := func(comp map[string]any, w map[string]int) {
		t.Helper()
		standalone := map[string]any{
			"summary":           ok(d.ClientSummary(ctx, dashA)),
			"positions_summary": ok(d.PortfolioSummary(ctx, dashA)),
			"pnl_history":       ok(d.PnLHistory(ctx, dashA, w["pnl_history"])),
			"position_history":  ok(d.PositionHistory(ctx, dashA, w["position_history"])),
			"mtm_series":        ok(d.MTMSeries(ctx, dashA, w["mtm_series"])),
			"equity_curve":      ok(d.EquityCurve(ctx, dashA, w["equity_curve"])),
			"drawdown":          ok(d.Drawdown(ctx, dashA, w["drawdown"])),
			"sector_breakdown":  ok(d.SectorBreakdown(ctx, dashA)),
			"stock_allocation":  ok(d.StockAllocation(ctx, dashA)),
			"mcap_performance":  ok(d.McapPerformance(ctx, dashA)),
			"ema_allocation":    ok(d.EMAAllocation(ctx, dashA)),
			"best_worst_trades": ok(d.BestWorstTrades(ctx, dashA)),
			"down_from_high":    ok(d.DownFromHigh(ctx, dashA)),
			"holding_analytics": ok(d.HoldingAnalytics(ctx, dashA)),
			"unique_scripts":    ok(d.UniqueScripts(ctx, dashA)),
		}
		for k, v := range standalone {
			if canon(v) != canon(comp[k]) {
				t.Fatalf("composite[%s] differs from standalone:\n got=%s\nwant=%s", k, canon(comp[k]), canon(v))
			}
		}
	}
	check(comp, map[string]int{"pnl_history": 90, "position_history": 30, "mtm_series": 30, "equity_curve": 90, "drawdown": 90})
	comp30 := roundTrip(t, ok(d.ClientDashboard(ctx, dashA, 30)))
	for k := range wantWin {
		if comp30["windows"].(map[string]any)[k].(float64) != 30 {
			t.Fatalf("explicit windows[%s] = %v, want 30", k, comp30["windows"].(map[string]any)[k])
		}
	}
	check(comp30, map[string]int{"pnl_history": 30, "position_history": 30, "mtm_series": 30, "equity_curve": 30, "drawdown": 30})
	if points(comp30["pnl_history"]) != 3 {
		t.Fatalf("explicit 30d pnl points = %d, want 3", points(comp30["pnl_history"]))
	}
}

// A client whose strategies were all deleted (positions kept) is in the
// book-wide numbers, so it must resolve on every per-client view — with
// zero capital — while staying off the /clients roster.
func TestDashboard_DeletedStrategyClientResolves(t *testing.T) {
	dbs := openDashTestDBs(t)
	seedDashFixture(t, dbs)
	d := newDashStore(dbs)
	ctx := context.Background()
	ok := okv(t)

	if err := d.requireClient(ctx, dashC); err != nil {
		t.Fatalf("requireClient(C) = %v, want nil", err)
	}
	sum := roundTrip(t, ok(d.ClientSummary(ctx, dashC)))
	if sum["invested_fund"].(float64) != 0 || sum["open_positions"].(float64) != 1 || sum["exposure_pct"].(float64) != 0 {
		t.Fatalf("C summary = %v", sum)
	}
	ps := roundTrip(t, ok(d.PortfolioSummary(ctx, dashC)))
	if ps["open_positions"].(float64) != 1 || ps["portfolio_value"].(float64) != 0 {
		t.Fatalf("C positions_summary = %v", ps)
	}
	if _, err := d.ClientDashboard(ctx, dashC, 0); err != nil {
		t.Fatalf("C dashboard: %v", err)
	}
	clients := roundTrip(t, ok(d.Clients(ctx)))["clients"].([]any)
	for _, c := range clients {
		if c.(map[string]any)["user_id"] == dashC {
			t.Fatalf("C must not be on the strategy roster: %v", c)
		}
	}
}

// Empty collections are [] on the wire, never null — a freshly onboarded
// client must not crash a .map() in the frontend.
func TestDashboard_EmptyClientListsAreArrays(t *testing.T) {
	dbs := openDashTestDBs(t)
	seedDashFixture(t, dbs)
	d := newDashStore(dbs)
	ctx := context.Background()
	ok := okv(t)

	raw := func(v any) string { b, _ := json.Marshal(v); return string(b) }
	if s := raw(ok(d.StockAllocation(ctx, dashD))); s != `{"holdings":[],"total":0}` {
		t.Fatalf("D stock allocation = %s", s)
	}
	if s := raw(ok(d.Positions(ctx, PositionsFilter{ClientID: dashD}))); s != `{"positions":[],"total_count":0}` {
		t.Fatalf("D positions = %s", s)
	}
	comp := roundTrip(t, ok(d.ClientDashboard(ctx, dashD, 0)))
	if h := comp["stock_allocation"].(map[string]any)["holdings"]; h == nil {
		t.Fatalf("D composite holdings is null")
	}
	if s := comp["sector_breakdown"].(map[string]any)["sectors"]; s == nil {
		t.Fatalf("D composite sectors is null")
	}
}

// HTTP: the composite route, and ?client_id= on every /portfolio/* route
// compared against the store (scoped and book-wide), 404 envelopes, and
// read-only admins allowed through (TierRead).
func TestHTTP_ClientDashboardAndScope(t *testing.T) {
	dbs := openDashTestDBs(t)
	seedDashFixture(t, dbs)
	seedAdmin(t, dbs.trading, "TADM_DASH", true)
	seedAdmin(t, dbs.trading, "TADM_DASH_RO", true)
	if _, err := dbs.trading.Exec(`UPDATE admin_users SET role='read_only' WHERE user_id='TADM_DASH_RO'`); err != nil {
		t.Fatalf("set read_only: %v", err)
	}

	h := NewHTTP(NewService(NewStore(dbs.trading)))
	store := newDashStore(dbs)
	h.SetDashboard(store)
	r := newRouterFor(t, h)
	token := elevateViaHTTP(t, r, "TADM_DASH")
	roToken := elevateViaHTTP(t, r, "TADM_DASH_RO")
	ctx := context.Background()
	ok := okv(t)

	get := func(tok, path string) (int, map[string]any) {
		t.Helper()
		req := httptest.NewRequest("GET", path, nil)
		req.Header.Set(TokenHeader, tok)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)
		var env map[string]any
		_ = json.Unmarshal(rec.Body.Bytes(), &env)
		return rec.Code, env
	}
	expect404 := func(path string) {
		t.Helper()
		code, env := get(token, path)
		if code != http.StatusNotFound || env["infoID"] != "E_NOT_FOUND" {
			t.Fatalf("%s: HTTP %d %v, want 404 E_NOT_FOUND", path, code, env)
		}
	}

	// Composite route.
	code, env := get(token, "/api/v1/admin/clients/"+dashA+"/dashboard?days=30")
	if code != http.StatusOK {
		t.Fatalf("dashboard: HTTP %d %v", code, env)
	}
	data := env["data"].(map[string]any)
	if data["client_id"] != dashA || data["windows"].(map[string]any)["pnl_history"].(float64) != 30 {
		t.Fatalf("dashboard payload header = %v / %v", data["client_id"], data["windows"])
	}
	if canon(data) != canon(ok(store.ClientDashboard(ctx, dashA, 30))) {
		t.Fatalf("dashboard over HTTP differs from store")
	}
	expect404("/api/v1/admin/clients/TDASH_NOPE/dashboard")
	// read-only admin may read it.
	if code, env := get(roToken, "/api/v1/admin/clients/"+dashB+"/dashboard"); code != http.StatusOK {
		t.Fatalf("read_only dashboard: HTTP %d %v", code, env)
	}

	// Every /portfolio/* route: scoped == store(B), unscoped == store("").
	routes := []struct {
		path   string
		scoped func(uid string) (any, error)
	}{
		{"/portfolio/pnl-history?days=30", func(u string) (any, error) { return store.PnLHistory(ctx, u, 30) }},
		{"/portfolio/position-history?days=30", func(u string) (any, error) { return store.PositionHistory(ctx, u, 30) }},
		{"/portfolio/best-worst-trades", func(u string) (any, error) { return store.BestWorstTrades(ctx, u) }},
		{"/portfolio/sector-breakdown", func(u string) (any, error) { return store.SectorBreakdown(ctx, u) }},
		{"/portfolio/stock-allocation", func(u string) (any, error) { return store.StockAllocation(ctx, u) }},
		{"/portfolio/mcap-performance", func(u string) (any, error) { return store.McapPerformance(ctx, u) }},
		{"/portfolio/ema-allocation", func(u string) (any, error) { return store.EMAAllocation(ctx, u) }},
		{"/portfolio/positions", func(u string) (any, error) { return store.Positions(ctx, PositionsFilter{ClientID: u}) }},
		{"/portfolio/positions-summary", func(u string) (any, error) { return store.PortfolioSummary(ctx, u) }},
		{"/portfolio/down-from-high", func(u string) (any, error) { return store.DownFromHigh(ctx, u) }},
		{"/portfolio/holding-analytics", func(u string) (any, error) { return store.HoldingAnalytics(ctx, u) }},
		{"/portfolio/unique-scripts", func(u string) (any, error) { return store.UniqueScripts(ctx, u) }},
		{"/portfolio/client-equity-comparison?days=30", func(u string) (any, error) { return store.ClientEquityComparison(ctx, u, 30) }},
	}
	for _, rt := range routes {
		sep := "?"
		if len(rt.path) > 0 && containsRune(rt.path, '?') {
			sep = "&"
		}
		code, env := get(token, "/api/v1/admin"+rt.path+sep+"client_id="+dashB)
		if code != http.StatusOK {
			t.Fatalf("%s scoped: HTTP %d %v", rt.path, code, env)
		}
		if canon(env["data"]) != canon(ok(rt.scoped(dashB))) {
			t.Fatalf("%s scoped differs from store(B):\n got=%s\nwant=%s", rt.path, canon(env["data"]), canon(ok(rt.scoped(dashB))))
		}
		code, env = get(token, "/api/v1/admin"+rt.path)
		if code != http.StatusOK {
			t.Fatalf("%s book-wide: HTTP %d %v", rt.path, code, env)
		}
		if canon(env["data"]) != canon(ok(rt.scoped(""))) {
			t.Fatalf("%s book-wide differs from store(\"\")", rt.path)
		}
		expect404("/api/v1/admin" + rt.path + sep + "client_id=TDASH_NOPE")
		expect404("/api/v1/admin" + rt.path + sep + "client_id=%20")
		// deleted-strategy client resolves on the scoped view too
		if code, env := get(token, "/api/v1/admin"+rt.path+sep+"client_id="+dashC); code != http.StatusOK {
			t.Fatalf("%s client C: HTTP %d %v", rt.path, code, env)
		}
	}

	if code, _ := get(roToken, "/api/v1/admin/portfolio/client-equity-comparison"); code != http.StatusOK {
		t.Fatalf("read_only comparison: HTTP %d", code)
	}

	// Scoped sector check is semantically right, not just store-equal.
	_, env = get(token, "/api/v1/admin/portfolio/sector-breakdown?client_id="+dashB)
	secs := env["data"].(map[string]any)["sectors"].([]any)
	if len(secs) != 1 || secs[0].(map[string]any)["name"] != "Banks" {
		t.Fatalf("scoped sectors = %v", secs)
	}
}

func containsRune(s string, r rune) bool {
	for _, c := range s {
		if c == r {
			return true
		}
	}
	return false
}

// Change 1: buy_date mirrors entry_date; holding_period_days always resolves
// (open → now, closed → exit_time, closed without exit_time → updated_at).
func TestDashboard_PositionsBuyDateAndHoldingDays(t *testing.T) {
	dbs := openDashTestDBs(t)
	seedDashFixture(t, dbs)
	d := newDashStore(dbs)
	ctx := context.Background()
	ok := okv(t)

	rows := roundTrip(t, ok(d.Positions(ctx, PositionsFilter{})))["positions"].([]any)
	want := map[string]float64{"TDASHCHEM": 5, "TDASHPHRM": 5, "TDASHWIN": 4, "TDASHLOSE": 3, "TDASHBANK": 5, "TDASHBWIN": 4, "TDASHBNOX": 5}
	seen := 0
	for _, r := range rows {
		m := r.(map[string]any)
		w, isFixture := want[m["script"].(string)]
		if !isFixture {
			continue
		}
		seen++
		if m["buy_date"] != m["entry_date"] || m["buy_date"] == "" {
			t.Fatalf("%s buy_date=%v entry_date=%v", m["script"], m["buy_date"], m["entry_date"])
		}
		if hd, present := m["holding_period_days"]; !present || hd.(float64) != w {
			t.Fatalf("%s holding_period_days=%v want %v (status %v, exit %v)", m["script"], hd, w, m["status"], m["exit_date"])
		}
	}
	if seen < len(want) {
		t.Fatalf("saw %d fixture rows, want %d", seen, len(want))
	}
}

// Endpoint 2: holding analytics, scoped and book-wide.
func TestDashboard_HoldingAnalytics(t *testing.T) {
	dbs := openDashTestDBs(t)
	seedDashFixture(t, dbs)
	d := newDashStore(dbs)
	ctx := context.Background()
	ok := okv(t)

	// A: open 5,5 + closed 4,3 → avg 4.25, median 4.5; 2 open scripts, 4 all-time.
	a := roundTrip(t, ok(d.HoldingAnalytics(ctx, dashA)))
	ov := a["overall"].(map[string]any)
	wantOv := map[string]float64{"avg_holding_days": 4.25, "median_holding_days": 4.5, "avg_holding_days_open": 5,
		"avg_holding_days_closed": 3.5, "open_positions": 2, "closed_positions": 2, "total_unique_scripts": 2, "unique_scripts_all_time": 4}
	for k, w := range wantOv {
		if ov[k].(float64) != w {
			t.Fatalf("A overall.%s = %v, want %v (%v)", k, ov[k], w, ov)
		}
	}
	bs := a["by_script"].([]any)
	if len(bs) != 4 {
		t.Fatalf("A by_script = %v", bs)
	}
	first := bs[0].(map[string]any) // open rows first; CHEM/PHRM both open_count 1, avg 5 → alphabetical
	if first["script"] != "TDASHCHEM" || first["open_count"].(float64) != 1 || first["avg_holding_days"].(float64) != 5 || first["clients_holding"].(float64) != 1 {
		t.Fatalf("A by_script[0] = %v", first)
	}
	bst := a["by_strategy"].([]any)
	if len(bst) != 1 {
		t.Fatalf("A by_strategy = %v", bst)
	}
	ca := bst[0].(map[string]any)
	if ca["client_id"] != dashA || ca["strategy_count"].(float64) != 1 || ca["open_positions"].(float64) != 2 ||
		ca["closed_positions"].(float64) != 2 || ca["avg_holding_days"].(float64) != 4.25 {
		t.Fatalf("A by_strategy[0] = %v", ca)
	}

	// Book-wide: TDASHBANK held by B and C; B appears with the no-exit-time row counted (5 days).
	all := roundTrip(t, ok(d.HoldingAnalytics(ctx, "")))
	var bank, bClient map[string]any
	for _, x := range all["by_script"].([]any) {
		if m := x.(map[string]any); m["script"] == "TDASHBANK" {
			bank = m
		}
	}
	for _, x := range all["by_strategy"].([]any) {
		if m := x.(map[string]any); m["client_id"] == dashB {
			bClient = m
		}
	}
	if bank == nil || bank["clients_holding"].(float64) != 2 || bank["open_count"].(float64) != 2 {
		t.Fatalf("book-wide TDASHBANK = %v", bank)
	}
	if bClient == nil || bClient["closed_positions"].(float64) != 2 || bClient["avg_holding_days"].(float64) != round2f((5+4+5)/3.0) {
		t.Fatalf("book-wide B = %v", bClient)
	}
}

// Endpoint 3: unique scripts in the open book.
func TestDashboard_UniqueScripts(t *testing.T) {
	dbs := openDashTestDBs(t)
	seedDashFixture(t, dbs)
	d := newDashStore(dbs)
	ctx := context.Background()
	ok := okv(t)

	all := roundTrip(t, ok(d.UniqueScripts(ctx, "")))
	var bank map[string]any
	for _, x := range all["scripts"].([]any) {
		if m := x.(map[string]any); m["script"] == "TDASHBANK" {
			bank = m
		}
	}
	if bank == nil || bank["clients_holding"].(float64) != 2 || bank["total_quantity"].(float64) != 60 ||
		bank["total_value"].(float64) != 60000 || bank["total_invested"].(float64) != 60000 || bank["price"].(float64) != 1000 {
		t.Fatalf("TDASHBANK = %v", bank)
	}
	if cl := bank["clients"].([]any); len(cl) != 2 || cl[0] != dashB || cl[1] != dashC {
		t.Fatalf("TDASHBANK clients = %v", cl)
	}
	a := roundTrip(t, ok(d.UniqueScripts(ctx, dashA)))
	if a["count"].(float64) != 2 || a["total_value"].(float64) != 40000 {
		t.Fatalf("A unique scripts = %v", a)
	}
	if s := a["scripts"].([]any)[0].(map[string]any); s["script"] != "TDASHPHRM" || s["total_value"].(float64) != 30000 {
		t.Fatalf("A top script = %v", s)
	}
	dd := roundTrip(t, ok(d.UniqueScripts(ctx, dashD)))
	if dd["count"].(float64) != 0 || dd["scripts"] == nil {
		t.Fatalf("D unique scripts = %v", dd)
	}
}

// Endpoint 4: multi-client comparison against the seeded benchmarks.
func TestDashboard_ClientEquityComparison(t *testing.T) {
	dbs := openDashTestDBs(t)
	seedDashFixture(t, dbs)
	d := newDashStore(dbs)
	ctx := context.Background()
	ok := okv(t)
	dates := fixtureDates()

	res := roundTrip(t, ok(d.ClientEquityComparison(ctx, "", 30)))
	if res["start_date"] != dates[1] {
		t.Fatalf("start_date = %v, want %v", res["start_date"], dates[1])
	}
	// nifty has today-2 and today-1 only (today is absent by design).
	nifty := benchOnFixtureDates(res["benchmarks"].(map[string]any)["nifty50"].([]any))
	if len(nifty) != 2 || nifty[0]["indexed"].(float64) != 100 || nifty[1]["indexed"].(float64) != 100.1 {
		t.Fatalf("nifty series = %v", nifty)
	}
	if sc := benchOnFixtureDates(res["benchmarks"].(map[string]any)["smallcap250"].([]any)); len(sc) != 0 {
		t.Fatalf("smallcap must have no rows in a 30d window, got %v", sc)
	}
	byID := map[string]map[string]any{}
	for _, c := range res["clients"].([]any) {
		m := c.(map[string]any)
		byID[m["client_id"].(string)] = m
	}
	a, b, dd := byID[dashA], byID[dashB], byID[dashD]
	if a == nil || b == nil || dd == nil {
		t.Fatalf("clients missing: %v", keys(byID))
	}
	if _, present := byID[dashC]; present {
		t.Fatalf("C has no live strategy and must not be compared")
	}
	// A: TWR 100.06. Nifty: start today-2 = 100 (exact), end today → carried
	// back to today-1 = 100.1 → not beating (-0.04). Midcap: start today-2
	// absent → first-after today-1 = 99, end 99 → indexed 100 → +0.06.
	// Smallcap: no rows in window → null.
	if a["final_indexed"].(float64) != 100.06 || a["nifty_final_indexed"].(float64) != 100.1 ||
		a["beating_nifty"].(bool) || a["outperformance_nifty_pct"].(float64) != -0.04 || a["outperformance_midcap150_pct"].(float64) != 0.06 ||
		a["outperformance_smallcap250_pct"] != nil ||
		a["points"].(float64) != 3 || a["start_date"] != dates[1] || a["end_date"] != dates[3] {
		t.Fatalf("A = %v", a)
	}
	// B: 100.15 → beating (+0.05).
	if b["final_indexed"].(float64) != 100.15 || !b["beating_nifty"].(bool) || b["outperformance_nifty_pct"].(float64) != 0.05 {
		t.Fatalf("B = %v", b)
	}
	// D: on the roster, no NAV → empty series and null flags.
	if dd["points"].(float64) != 0 || dd["beating_nifty"] != nil || dd["final_indexed"] != nil || len(dd["series"].([]any)) != 0 {
		t.Fatalf("D = %v", dd)
	}
	sum := res["summary"].(map[string]any)
	if sum["clients_beating_nifty"].(float64) < 1 || sum["clients_with_data"].(float64) < 2 {
		t.Fatalf("summary = %v", sum)
	}
	lb := res["leaderboard"].([]any)
	posB, posA := -1, -1
	for i, id := range lb {
		if id == dashB {
			posB = i
		}
		if id == dashA {
			posA = i
		}
	}
	if posB < 0 || posA < 0 || posB > posA {
		t.Fatalf("leaderboard = %v (B must rank above A)", lb)
	}

	// 90-day window reaches the today-40 rows: 4 points, benchmarks rebased at day-40.
	res90 := roundTrip(t, ok(d.ClientEquityComparison(ctx, "", 90)))
	if res90["start_date"] != dates[0] || len(benchOnFixtureDates(res90["benchmarks"].(map[string]any)["nifty50"].([]any))) != 3 {
		t.Fatalf("90d start/benchmarks = %v / %v", res90["start_date"], res90["benchmarks"])
	}
	for _, c := range res90["clients"].([]any) {
		if m := c.(map[string]any); m["client_id"] == dashA {
			// nifty 95 → (carry-back) 100.1 = 105.37 indexed; A 100.06 → -5.31.
			// smallcap: only today-40 exists → start exact, end carried back
			// 40 days → indexed 100 → +0.06.
			if m["points"].(float64) != 4 || m["nifty_final_indexed"].(float64) != 105.37 || m["outperformance_nifty_pct"].(float64) != -5.31 ||
				m["outperformance_smallcap250_pct"].(float64) != 0.06 {
				t.Fatalf("A 90d = %v", m)
			}
		}
	}

	// Scoped to one client: only that client on the roster.
	only := roundTrip(t, ok(d.ClientEquityComparison(ctx, dashB, 30)))
	if cl := only["clients"].([]any); len(cl) != 1 || cl[0].(map[string]any)["client_id"] != dashB || !cl[0].(map[string]any)["beating_nifty"].(bool) {
		t.Fatalf("scoped comparison = %v", cl)
	}
}

func keys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
