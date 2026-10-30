package manthan

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"go.uber.org/zap"
)

// openSignalsDB connects to the local signals_db (skips when absent). The
// ref_* tables come from migration 013, which the developer applies once.
func openSignalsDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("postgres",
		"host=localhost port=5432 user=postgres password=postgres dbname=signals_db sslmode=disable")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Skipf("signals_db not reachable — skipping (%v)", err)
	}
	// Registered BEFORE the fixture cleanup so it runs AFTER it (cleanups are
	// LIFO): closing the pool first would leave the fixture rows behind.
	t.Cleanup(func() { _ = db.Close() })
	var ok bool
	if err := db.QueryRow(`SELECT to_regclass('ref_buy_triggers') IS NOT NULL`).Scan(&ok); err != nil || !ok {
		t.Skip("migration 013 not applied locally — skipping")
	}
	return db
}

// Fixture: four TDBR* companies on one EOD day.
//
//	TDBRA  full data, preferred PAT falls back to standalone, mapped industry, triggered today
//	TDBRB  PAT only in an old period (> max age) → PAT must read as 0
//	TDBRC  no fundamentals at all, unmapped industry (API name used), 52w from vendor
//	TDBRD  yesterday's trigger only → not in today's BuySignals
func seedDBReader(t *testing.T, db *sql.DB, today string) {
	t.Helper()
	cleanup := func() {
		for _, q := range []string{
			`DELETE FROM ref_buy_triggers WHERE symbol LIKE 'TDBR%'`,
			`DELETE FROM ref_ath_close WHERE symbol LIKE 'TDBR%'`,
			`DELETE FROM ref_price_levels WHERE symbol LIKE 'TDBR%'`,
			`DELETE FROM ref_fundamentals WHERE isin LIKE 'INETDBR%'`,
			`DELETE FROM ref_equity_eod WHERE isin LIKE 'INETDBR%'`,
			`DELETE FROM ref_industry_map WHERE api_industry = 'TDBR Api Industry'`,
			`DELETE FROM ref_index_allocation WHERE index_name = 'NIFTY50' AND trade_date = '2099-01-01'`,
		} {
			_, _ = db.Exec(q)
		}
	}
	cleanup()
	t.Cleanup(cleanup)
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := db.Exec(q, args...); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	// Use a far-future EOD day so the fixture is the "latest" row even on a
	// developer box that has real data loaded.
	eod := "2099-01-01"
	exec(`INSERT INTO ref_industry_map (api_industry, manthan_industry) VALUES ('TDBR Api Industry', 'TDBR Accord Industry')`)
	for _, r := range []struct {
		sym, ind    string
		close, mcap float64
	}{
		{"TDBRA", "TDBR Api Industry", 100, 5000},
		{"TDBRB", "TDBR Api Industry", 50, 800},
		{"TDBRC", "Unmapped Widgets", 20, 600},
		{"TDBRD", "TDBR Api Industry", 10, 700},
	} {
		exec(`INSERT INTO ref_equity_eod (trade_date, isin, nse_symbol, company_name, industry, close_price, market_cap_cr)
		      VALUES ($1, $2, $3, $4, $5, $6, $7)`, eod, "INE"+r.sym, r.sym, r.sym+" Ltd", r.ind, r.close, r.mcap)
	}
	// TDBRA: consolidated null, preferred/standalone 150 Cr, eps 4 → PE 25; fscore 70 in 2098-12
	exec(`INSERT INTO ref_fundamentals (period, isin, basis, nse_symbol, pat_cr, pat_basis, eps_ttm, f_score)
	      VALUES ('2098-12', 'INETDBRA', 'preferred', 'TDBRA', 150, 'standalone', 4, 70),
	             ('2098-12', 'INETDBRA', 'consolidated', 'TDBRA', NULL, NULL, NULL, 70),
	             ('2098-09', 'INETDBRA', 'preferred', 'TDBRA', 120, 'standalone', 3.5, 65)`)
	// TDBRB: only a 2090 period → too old
	exec(`INSERT INTO ref_fundamentals (period, isin, basis, nse_symbol, pat_cr, pat_basis, eps_ttm, f_score)
	      VALUES ('2090-03', 'INETDBRB', 'preferred', 'TDBRB', 10, 'consolidated', 1, 80)`)
	exec(`INSERT INTO ref_ath_close (symbol, isin, ath_close, ath_date, week52_high_close, week52_low_close, source)
	      VALUES ('TDBRA', 'INETDBRA', 98.5, '2098-12-20', 98.5, 60, 'seed:test'),
	             ('TDBRC', 'INETDBRC', 19, '2098-11-01', NULL, NULL, 'seed:test')`)
	exec(`INSERT INTO ref_price_levels (symbol, co_code, week52_high, week52_low, ath_intraday) VALUES ('TDBRC', 1, 21, 12, 22)`)
	exec(`INSERT INTO ref_buy_triggers (symbol, trade_date, ltp, ath_close, source) VALUES ('TDBRA', $1, 100.2, 98.5, 'live:ext-redis')`, today)
	exec(`INSERT INTO ref_buy_triggers (symbol, trade_date, ltp, ath_close, source) VALUES ('TDBRD', $1::date - 1, 10.5, 10, 'live:ext-redis')`, today)
	exec(`INSERT INTO ref_index_allocation (index_name, trade_date, close, ema21, ema50, ema100, score, allocation)
	      VALUES ('NIFTY50', '2099-01-01', 100, 90, 80, 70, 6, 1.0)`)
}

func TestDBReader_ReadAll(t *testing.T) {
	db := openSignalsDB(t)
	ist, _ := time.LoadLocation("Asia/Kolkata")
	now := time.Date(2099, 1, 2, 11, 0, 0, 0, ist)
	today := now.Format("2006-01-02")
	seedDBReader(t, db, today)

	r := NewDBReader(db, "", zap.NewNop())
	r.now = func() time.Time { return now }
	if err := r.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	res, err := r.ReadAll(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	pe := map[string]*PEAndNetProfitRow{}
	for _, p := range res.PEAndNetProfit {
		pe[p.NSESymbol] = p
	}
	a := pe["TDBRA"]
	// LatestPrice is today's breakout LTP (100.2), PE stays on the close (100/4).
	if a == nil || a.PAT != 150 || a.TTMEPS != 4 || a.TTMPE != 25 || a.LatestMarketCap != 5000 || a.ISIN != "INETDBRA" || a.LatestPrice != 100.2 {
		t.Fatalf("TDBRA pe row: %+v", a)
	}
	if b := pe["TDBRB"]; b == nil || b.PAT != 0 || b.TTMPE != 0 {
		t.Fatalf("TDBRB stale PAT must read as 0: %+v", b)
	}
	if c := pe["TDBRC"]; c == nil || c.PAT != 0 || c.LatestPrice != 20 { // not triggered → EOD close
		t.Fatalf("TDBRC no fundamentals: %+v", c)
	}

	fs := map[string]float64{}
	for _, f := range res.FScores {
		fs[f.ISIN] = f.Score
	}
	if fs["INETDBRA"] != 70 {
		t.Fatalf("fscore latest period: %v", fs)
	}
	if _, ok := fs["INETDBRB"]; ok {
		t.Fatalf("stale fscore must be dropped: %v", fs)
	}

	ind := map[string]string{}
	for _, i := range res.Industries {
		ind[i.NSECode] = i.Industry
	}
	if ind["TDBRA"] != "TDBR Accord Industry" || ind["TDBRC"] != "Unmapped Widgets" {
		t.Fatalf("industry mapping: %v", ind)
	}

	lth := map[string]*LifeTimeHighRow{}
	for _, l := range res.LifeTimeHighs {
		lth[l.ScripName] = l
	}
	if lth["TDBRA"] == nil || lth["TDBRA"].AllTimeHigh != 98.5 || lth["TDBRA"].Week52High != 98.5 {
		t.Fatalf("TDBRA lth: %+v", lth["TDBRA"])
	}
	if lth["TDBRC"] == nil || lth["TDBRC"].AllTimeHigh != 19 || lth["TDBRC"].Week52High != 21 || lth["TDBRC"].Week52Low != 12 {
		t.Fatalf("TDBRC must take 52w from vendor, ATH stays ours: %+v", lth["TDBRC"])
	}

	bs := map[string]*BuySignalRow{}
	for _, b := range res.BuySignals {
		bs[b.ScripName] = b
	}
	if len(bs) < 1 || bs["TDBRA"] == nil || bs["TDBRA"].ATHEntry != "Buy" || !bs["TDBRA"].IsAllTimeHigh || bs["TDBRA"].Industry != "TDBR Accord Industry" {
		t.Fatalf("today's trigger: %+v", bs["TDBRA"])
	}
	if bs["TDBRD"] != nil {
		t.Fatal("yesterday's trigger leaked into today's BuySignals")
	}

	alloc := map[string]float64{}
	for _, g := range res.IndicesGrades {
		alloc[g.IndexName] = g.Allocation
	}
	// The fixture pins NIFTY50 to 1.0 on a far-future date; the other two
	// come from whatever the dev box holds (real rows or the 1.0 default) —
	// only presence is asserted for them.
	if len(res.IndicesGrades) != 3 || alloc["NIFTY50"] != 1.0 {
		t.Fatalf("all three indices must be present and NIFTY50 must read the fixture row: %v", alloc)
	}
	for _, name := range []string{"NFTYMCP150", "NTYSLCP250"} {
		if a, ok := alloc[name]; !ok || a <= 0 || a > 1 {
			t.Fatalf("%s allocation missing or out of range: %v", name, alloc)
		}
	}

	syms, err := r.BuySymbols(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, s := range syms {
		found = found || s == "TDBRA"
		if s == "TDBRD" {
			t.Fatal("BuySymbols leaked yesterday's trigger")
		}
	}
	if !found {
		t.Fatalf("BuySymbols missing TDBRA: %v", syms)
	}
}

// The pipeline must accept the DB reader's output end to end: TDBRA passes
// every filter and becomes ELIGIBLE with the mapped industry and our ATH.
func TestDBReader_FeedsPipeline(t *testing.T) {
	db := openSignalsDB(t)
	ist, _ := time.LoadLocation("Asia/Kolkata")
	now := time.Date(2099, 1, 2, 11, 0, 0, 0, ist)
	seedDBReader(t, db, now.Format("2006-01-02"))

	r := NewDBReader(db, "preferred", zap.NewNop())
	r.now = func() time.Time { return now }
	raw, err := r.ReadAll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// Keep only the fixture's trigger so real triggers on a dev box do not
	// disturb the assertion.
	var mine []*BuySignalRow
	for _, b := range raw.BuySignals {
		if b.ScripName == "TDBRA" {
			mine = append(mine, b)
		}
	}
	raw.BuySignals = mine

	res := NewPipeline(zap.NewNop()).Run(raw)
	var got *ManthanStock
	for _, s := range res.Eligible {
		if s.Symbol == "TDBRA" {
			got = s
		}
	}
	if got == nil {
		t.Fatalf("TDBRA not eligible; drops=%+v filtered=%d", res.Drops, len(res.FilteredOut))
	}
	if got.Industry != "TDBR Accord Industry" || got.ATHClose != 98.5 || got.PAT != 150 || got.PE != 25 || got.FScore != 70 || got.MCapBucket != "SMALL" {
		t.Fatalf("eligible row: %+v", got)
	}
}
