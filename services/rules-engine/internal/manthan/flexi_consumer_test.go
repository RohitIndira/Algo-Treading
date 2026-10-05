package manthan

// Consumer-level flexi tests (processSignal wiring):
//
//	TestConsumer_OffMode_NoFlexiWork          — off: no universe fetch, no evals, decisions as today
//	TestConsumer_OnMode_NilSignalsDBFailsClosed — on + no signals DB: base caps, counter, no evals
//	TestConsumer_DryRun_EndToEnd_LocalDB       — seeded signals_db + real publisher on trading_db:
//	                                             4 WOULD_ALLOCATE rows for S4450's day, 5th no row,
//	                                             live decisions untouched, shadow book restart-safe

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"go.uber.org/zap"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/manthan/types"
)

// fakePublisher records everything the consumer hands it. Satisfies OrderPublisher.
type fakePublisher struct {
	entries []ManthanOrder
	skips   []SkipReason
	evals   []types.FlexiEval
}

func (f *fakePublisher) PublishEntryOrder(_ context.Context, o ManthanOrder) error {
	f.entries = append(f.entries, o)
	return nil
}
func (f *fakePublisher) PublishSLModify(context.Context, SLModifyOrder) error { return nil }
func (f *fakePublisher) PublishSLExit(context.Context, SLExitOrder) error     { return nil }
func (f *fakePublisher) PublishSignalSkip(_ context.Context, _, _ string, s SkipReason) {
	f.skips = append(f.skips, s)
}
func (f *fakePublisher) PublishFlexiEval(_ context.Context, _, _ string, ev types.FlexiEval) {
	f.evals = append(f.evals, ev)
}
func (f *fakePublisher) PersistPositionOpen(context.Context, ManthanOrder) error { return nil }
func (f *fakePublisher) PersistTrail(context.Context, string, types.Position) error {
	return nil
}
func (f *fakePublisher) PersistExit(context.Context, string, string, float64, float64, string) error {
	return nil
}
func (f *fakePublisher) PersistFillConfirmed(context.Context, string, string, float64, int32, float64) error {
	return nil
}

func testStrategy() types.UserStrategy {
	return types.UserStrategy{StrategyID: flexiTestStrategy, UserID: "S4450", TradingMode: "PAPER",
		TotalCapital: 500000, MaxPositions: 25, PerStockBase: 20000, StopLossPct: 20, TrailingSLPct: 2,
		CreatedAt: flexiT0.Add(-24 * time.Hour)}
}

// newTestConsumer builds a Consumer without a Kafka reader (processSignal
// never touches it) and seeds the strategy's portfolio with the given book.
func newTestConsumer(pub OrderPublisher, signalsDB *sql.DB, book *types.Portfolio) (*Consumer, *PortfolioManager) {
	pm := NewPortfolioManager(zap.NewNop())
	st := testStrategy()
	p := pm.GetOrCreate(st)
	p.Mu.Lock()
	for k, v := range book.Positions {
		p.Positions[k] = v
	}
	p.Mu.Unlock()
	c := &Consumer{
		signalsDB:    signalsDB,
		allocator:    NewAllocator(zap.NewNop()),
		orderGen:     NewOrderGenerator(zap.NewNop()),
		portfolioMgr: pm,
		slMgr:        NewTrailingSLManager(zap.NewNop()),
		publisher:    pub,
		strategyFn:   func() []types.UserStrategy { return []types.UserStrategy{st} },
		emaFn:        func() map[string]float64 { return fullEMA },
		logger:       zap.NewNop(),
		flexiNow:     func() time.Time { return time.Date(2026, 9, 30, 9, 30, 0, 0, flexiIST) },
	}
	return c, pm
}

func TestConsumer_OffMode_NoFlexiWork(t *testing.T) {
	pub := &fakePublisher{}
	// SetFlexi never called (zero config) — the production default when the
	// env is unset — AND with an explicit off config: both must be inert.
	for _, setOff := range []bool{false, true} {
		c, _ := newTestConsumer(pub, nil, flexiPortfolio(12, 5, 0))
		if setOff {
			c.SetFlexi(flexiCfg("off", nil), nil)
		}
		c.processSignal(context.Background(), flexiSig("sa", types.BucketSmall))
		c.processSignal(context.Background(), flexiSig("ma", types.BucketMid))
	}
	if len(pub.evals) != 0 {
		t.Fatalf("off-mode published flexi evals: %+v", pub.evals)
	}
	if len(pub.skips) != 2 || pub.skips[0].Reason != "mcap bucket cap 50% reached for SMALL" || pub.skips[1].Reason != "mcap bucket cap 50% reached for SMALL" {
		t.Fatalf("skips: %+v", pub.skips)
	}
	if len(pub.entries) != 2 || pub.entries[0].Symbol != "ma" || pub.entries[0].Flexi != nil {
		t.Fatalf("entries: %+v", pub.entries)
	}
}

func TestConsumer_OnMode_NilSignalsDBFailsClosed(t *testing.T) {
	pub := &fakePublisher{}
	c, _ := newTestConsumer(pub, nil, flexiPortfolio(12, 5, 0))
	c.SetFlexi(flexiCfg("on", map[string]string{types.EnvFlexiStrategyAllowlist: flexiTestStrategy}), nil)
	c.processSignal(context.Background(), flexiSig("sa", types.BucketSmall))
	if len(pub.entries) != 0 || len(pub.skips) != 1 || pub.skips[0].Reason != "mcap bucket cap 50% reached for SMALL" {
		t.Fatalf("must fall back to base caps: entries=%+v skips=%+v", pub.entries, pub.skips)
	}
	if len(pub.evals) != 0 {
		t.Fatalf("no evals without a universe: %+v", pub.evals)
	}
	s := c.FlexiStats()
	if s["plan_not_applied."+types.FlexiGuardUniverseError] != 1 || s["universe_fetch_error"] != 1 {
		t.Fatalf("stats: %v", s)
	}
	// Not-allowlisted strategy: no fetch at all.
	pub2 := &fakePublisher{}
	c2, _ := newTestConsumer(pub2, nil, flexiPortfolio(12, 5, 0))
	c2.SetFlexi(flexiCfg("on", map[string]string{types.EnvFlexiStrategyAllowlist: "some-other-strategy"}), nil)
	c2.processSignal(context.Background(), flexiSig("sa", types.BucketSmall))
	if s := c2.FlexiStats(); s["universe_fetch_error"] != 0 || s["universe_fetch_ok"] != 0 {
		t.Fatalf("non-allowlisted fleet must not fetch the universe: %v", s)
	}
}

// End-to-end dry-run on the local dev DBs. Seeds a synthetic run_date in
// signals_db (never a real day), uses the REAL publisher against trading_db
// so FLEXI_EVAL rows drive the shadow book across messages, and cleans up.
func TestConsumer_DryRun_EndToEnd_LocalDB(t *testing.T) {
	const runDate = "2099-01-01"
	const strategyID = "bbbbbbbb-f1e1-4f1e-9f1e-eeeeeeeeeeee"
	tdb := openFlexiTradingDB(t)
	defer tdb.Close()
	if err := probeFlexiSchema(context.Background(), tdb); err != nil {
		t.Skipf("migration 014 not applied locally: %v", err)
	}
	sdb, err := sql.Open("postgres",
		"host=localhost port=5432 user=postgres password=postgres dbname=signals_db sslmode=disable")
	if err != nil {
		t.Fatalf("open signals_db: %v", err)
	}
	defer sdb.Close()
	if err := sdb.Ping(); err != nil {
		t.Skipf("signals_db not reachable — skipping (%v)", err)
	}

	cleanup := func() {
		_, _ = sdb.Exec(`DELETE FROM manthan_signals WHERE run_date = $1::date`, runDate)
		_, _ = sdb.Exec(`DELETE FROM manthan_stocks WHERE run_date = $1::date`, runDate)
		_, _ = tdb.Exec(`DELETE FROM manthan_signal_decisions WHERE strategy_id = $1`, strategyID)
	}
	cleanup()
	defer cleanup()

	// Universe: 9 SMALL + 3 MID eligible, 2 FILTER_REJECTED (structural
	// reasons), 0 LARGE eligible → LARGE provably void. 14 stocks ≥ 5.
	seen := flexiT0
	seed := func(sym, bucket, status, reason string, eligible bool) {
		if _, err := sdb.Exec(`
			INSERT INTO manthan_stocks (run_date, symbol, industry, mcap_bucket, status, reason, latest_price)
			VALUES ($1::date, $2, $3, $4, $5, NULLIF($6,''), 100)`, runDate, sym, "sec-"+sym, bucket, status, reason); err != nil {
			t.Fatalf("seed stock %s: %v", sym, err)
		}
		if eligible {
			if _, err := sdb.Exec(`
				INSERT INTO manthan_signals (run_date, symbol, industry, mcap_bucket, index_name, latest_price, first_seen_at)
				VALUES ($1::date, $2, $3, $4, 'NTYSLCP250', 100, $5)`, runDate, sym, "sec-"+sym, bucket, seen); err != nil {
				t.Fatalf("seed signal %s: %v", sym, err)
			}
		}
	}
	for _, s := range flexiNames("s", 9) {
		seed(s, "SMALL", "ELIGIBLE", "", true)
	}
	for _, s := range flexiNames("m", 3) {
		seed(s, "MID", "ELIGIBLE", "", true)
	}
	seed("bigco", "LARGE", "FILTER_REJECTED", "MCap out of range", false)
	seed("pecos", "MID", "FILTER_REJECTED", "PE above threshold", false)

	writers := &fakeWriter{}
	pub := &ManthanPublisher{db: tdb, tradeWriter: &fakeWriter{}, portfolioWriter: writers, logger: zap.NewNop()}
	st := testStrategy()
	st.StrategyID = strategyID
	book := flexiPortfolio(12, 5, 0)
	cfg := flexiCfg("dry_run", map[string]string{types.EnvFlexiStrategyAllowlist: strategyID})

	mkConsumer := func() *Consumer {
		pm := NewPortfolioManager(zap.NewNop())
		p := pm.GetOrCreate(st)
		p.Mu.Lock()
		for k, v := range book.Positions {
			p.Positions[k] = v
		}
		p.Mu.Unlock()
		c := &Consumer{
			signalsDB: sdb, allocator: NewAllocator(zap.NewNop()), orderGen: NewOrderGenerator(zap.NewNop()),
			portfolioMgr: pm, slMgr: NewTrailingSLManager(zap.NewNop()), publisher: pub,
			strategyFn: func() []types.UserStrategy { return []types.UserStrategy{st} },
			emaFn:      func() map[string]float64 { return fullEMA }, logger: zap.NewNop(),
			flexiNow: func() time.Time { return time.Date(2026, 9, 30, 9, 30, 0, 0, flexiIST) },
		}
		c.SetFlexi(cfg, tdb)
		return c
	}
	c := mkConsumer()
	sig := func(sym string) types.ManthanSignal {
		s := flexiSig(sym, types.BucketSmall)
		s.RunDate = runDate
		return s
	}
	for _, s := range flexiNames("s", 5) {
		c.processSignal(context.Background(), sig(s))
	}

	var wouldAllocate, total, entries int
	if err := tdb.QueryRow(`SELECT count(*) FILTER (WHERE payload->>'outcome' = 'WOULD_ALLOCATE'), count(*)
		FROM manthan_signal_decisions WHERE strategy_id = $1 AND signal_type = 'FLEXI_EVAL'`, strategyID).Scan(&wouldAllocate, &total); err != nil {
		t.Fatal(err)
	}
	if wouldAllocate != 4 || total != 4 {
		t.Fatalf("expected exactly 4 WOULD_ALLOCATE rows (S4450 day: ceiling 16 − held 12), got %d/%d", wouldAllocate, total)
	}
	if err := tdb.QueryRow(`SELECT count(*) FROM manthan_signal_decisions WHERE strategy_id = $1 AND signal_type = 'ENTRY_BUY' AND status <> 'REJECTED'`, strategyID).Scan(&entries); err != nil {
		t.Fatal(err)
	}
	if entries != 0 {
		t.Fatalf("dry_run must not dispatch an entry, found %d", entries)
	}
	var rejected int
	_ = tdb.QueryRow(`SELECT count(*) FROM manthan_signal_decisions WHERE strategy_id = $1 AND status = 'REJECTED' AND rejection_reason = 'mcap bucket cap 50% reached for SMALL'`, strategyID).Scan(&rejected)
	if rejected != 5 {
		t.Fatalf("live decisions must be today's base blocks for all 5 signals, got %d", rejected)
	}
	stats := c.FlexiStats()
	if stats["eval.WOULD_ALLOCATE"] != 4 || stats["plan_not_applied."+types.FlexiGuardNoSpare] != 1 || stats["universe_fetch_ok"]+stats["universe_fetch_slow"] != 5 {
		t.Fatalf("stats: %v", stats)
	}
	// Payload carries the evidence the Phase-1 sweep re-derives from.
	var ceiling, donors, claimMID, spare, stocksRows, voidLarge, oppSmall string
	if err := tdb.QueryRow(`
		SELECT payload->'plan'->'ceiling'->>'SMALL', payload->'plan'->'donors'->>0,
		       payload->'plan'->'base_claim'->>'MID', payload->'plan'->>'spare_before',
		       payload->'plan'->'universe'->>'stocks_rows', payload->'plan'->'void'->>'LARGE',
		       jsonb_array_length(payload->'plan'->'opp_symbols'->'SMALL')::text
		FROM manthan_signal_decisions WHERE strategy_id = $1 AND symbol = 'sa'`, strategyID).
		Scan(&ceiling, &donors, &claimMID, &spare, &stocksRows, &voidLarge, &oppSmall); err != nil {
		t.Fatal(err)
	}
	if ceiling != "16" || donors != "LARGE" || claimMID != "3" || spare != "4" || stocksRows != "14" || voidLarge != "true" || oppSmall != "9" {
		t.Fatalf("plan payload: ceiling=%s donors=%s base_claim[MID]=%s spare=%s stocks_rows=%s void[LARGE]=%s opp[SMALL]=%s",
			ceiling, donors, claimMID, spare, stocksRows, voidLarge, oppSmall)
	}
	if !strings.Contains(string(writers.msgs[0].Value), `"type":"FLEXI_EVAL"`) {
		t.Fatalf("Kafka event: %s", writers.msgs[0].Value)
	}

	// Restart-neutral: a fresh consumer (empty in-memory state) reloads the
	// shadow book from the rows and the 6th SMALL still gets no plan / no row.
	c2 := mkConsumer()
	c2.processSignal(context.Background(), sig("sf"))
	_ = tdb.QueryRow(`SELECT count(*) FROM manthan_signal_decisions WHERE strategy_id = $1 AND signal_type = 'FLEXI_EVAL'`, strategyID).Scan(&total)
	if total != 4 {
		t.Fatalf("after restart the shadow must still hold 4: rows=%d", total)
	}
	// Redelivery of an already-evaluated symbol: no new row.
	c2.processSignal(context.Background(), sig("sa"))
	_ = tdb.QueryRow(`SELECT count(*) FROM manthan_signal_decisions WHERE strategy_id = $1 AND signal_type = 'FLEXI_EVAL'`, strategyID).Scan(&total)
	if total != 4 {
		t.Fatalf("redelivery must not add a row: rows=%d", total)
	}
	// A MID signal is not in play (fits under base) → allocates for real, no eval.
	ms := flexiSig("ma", types.BucketMid)
	ms.RunDate = runDate
	c2.processSignal(context.Background(), ms)
	var midEntries int
	_ = tdb.QueryRow(`SELECT count(*) FROM manthan_signal_decisions WHERE strategy_id = $1 AND symbol = 'ma' AND signal_type = 'ENTRY_BUY' AND flexi_grant IS NULL`, strategyID).Scan(&midEntries)
	if midEntries != 1 {
		t.Fatalf("MID must allocate under base with NULL flexi_grant, got %d", midEntries)
	}
}
