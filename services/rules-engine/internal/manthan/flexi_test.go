package manthan

// Flexi caps — allocator / publisher / consumer tests (design brief
// 2026-09-30). Unit tests run everywhere; DB-backed ones use the existing
// local trading_db / signals_db pattern and skip when unreachable or when
// migration 014 is not applied.
//
//	TestAllocator_OffModeGoldenIdentity     — off ≡ AllocateWithFlexi(nil) ≡ mode-off input: same
//	                                          AllocateResult, same ManthanOrder JSON, no evals;
//	                                          full book + mid-batch break stay SILENT (zero Skipped)
//	TestAllocator_PortfolioFullSkipRows      — P0.2 (flexi enabled only): "portfolio full (N/N slots)"
//	                                          for a full book and the mid-batch break; held symbols
//	                                          keep "already holding"
//	TestAllocator_DryRun_BatchMirrorsRealAllocation — a real allocation earlier in a batch is
//	                                          visible to later evals' shadow clone
//	TestFlexiSchemaProbe_ErrorClassification — definitive "not migrated" vs transient DB error
//	TestAllocator_OnMode_S4450Day            — 4 SMALL grants (ceiling 16) with Flexi set, 5th
//	                                          blocked with the LEGACY string, MID fills under base;
//	                                          MID-first arrival → same end state; SL follows the stock
//	TestAllocator_OnMode_SectorCapPrecedence — sector string + BLOCKED_SECTOR inside a flexed bucket
//	TestAllocator_OnMode_TailFailAfterCeiling— ceiling admits, EMA 0 rejects → WOULD_FAIL_TAIL, no grant
//	TestAllocator_DryRun_RealDecisionsUnchanged — dry_run decisions == off; evals predict on-mode;
//	                                          shadow book drives WOULD_BLOCK_FLEXI_CEILING and never
//	                                          restricts the live decision
//	TestManthanOrder_FlexiNeverOnWire        — json:"-" keeps trade-signals bytes identical
//	TestFlexiEval_DeterministicID            — FLEXI_EVAL id ≠ entry id ≠ SKIP id, stable
//	TestEcosystemConfig_ForwardsFlexiKeys    — every MANTHAN_FLEXI_* key is in the PM2 whitelist
//	TestFlexiSchemaProbe_LocalDB             — probe passes on a migrated DB (skips otherwise)
//	TestPublishFlexiEval_RowAndShadowBook    — FLEXI_EVAL/EVALUATED row (payload set), idempotent,
//	                                          loadShadowBook returns WOULD_ALLOCATE rows only
//	TestFetchFlexiUniverse_LocalSignalsDB    — nil handle fails closed; real fetch is consistent

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"go.uber.org/zap"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/manthan/types"
)

// ── fixtures ──────────────────────────────────────────────────────────

const flexiTestStrategy = "a6cb5b08-dddd-4e54-ad2d-d5ae55edb3c9"
const flexiRunDate = "2026-09-30"

var flexiT0 = time.Date(2026, 9, 30, 3, 25, 0, 0, time.UTC)
var flexiIST = time.FixedZone("IST", 19800)

func flexiPortfolio(small, mid, large int) *types.Portfolio {
	p := &types.Portfolio{
		UserID: "S4450", StrategyID: flexiTestStrategy,
		InitialCapital: 500000, CurrentCapital: 500000, MaxPositions: 25, PerStockBase: 20000,
		StopLossPct: 20,
		Positions:   map[string]*types.Position{}, Cooldown: map[string]*types.CooldownEntry{},
	}
	add := func(prefix, bucket string, n int) {
		for i := 0; i < n; i++ {
			sym := prefix + string(rune('A'+i))
			p.Positions[sym] = &types.Position{Symbol: sym, Industry: "sec-" + sym, MCapBucket: bucket,
				State: types.StateActive, Active: true, Quantity: 10, EntryPrice: 100}
		}
	}
	add("S", types.BucketSmall, small)
	add("M", types.BucketMid, mid)
	add("L", types.BucketLarge, large)
	return p
}

func flexiSig(sym, bucket string) types.ManthanSignal {
	return types.ManthanSignal{RunDate: flexiRunDate, Symbol: sym, ISIN: "INE" + sym, Industry: "sec-" + sym,
		MCapBucket: bucket, IndexName: "NTYSLCP250", LatestPrice: 100, ATHClose: 120, Week52High: 120,
		FirstSeenAt: flexiT0.Format(time.RFC3339)}
}

func flexiNames(prefix string, n int) []string {
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, prefix+string(rune('a'+i)))
	}
	return out
}

func flexiUniverse(small, mid, large []string) *types.FlexiUniverse {
	u := &types.FlexiUniverse{RunDate: flexiRunDate, StocksRows: 40, StocksByStatus: map[string]int{"ELIGIBLE": 12},
		InfraUnknown: map[string]bool{}, QueriedAt: flexiT0}
	for b, list := range map[string][]string{types.BucketSmall: small, types.BucketMid: mid, types.BucketLarge: large} {
		for _, s := range list {
			u.Eligible = append(u.Eligible, types.FlexiUniverseRow{Symbol: s, Industry: "sec-" + s, Bucket: b, FirstSeenAt: flexiT0})
		}
	}
	return u
}

func flexiCfg(mode string, over map[string]string) types.FlexiConfig {
	m := map[string]string{types.EnvFlexiMode: mode}
	for k, v := range over {
		m[k] = v
	}
	return types.ParseFlexiConfig(func(k string) string { return m[k] })
}

func flexiInput(cfg types.FlexiConfig, u *types.FlexiUniverse, shadow []types.ShadowEntry) *FlexiInput {
	return &FlexiInput{Cfg: cfg, Universe: u, StrategyCreatedAt: flexiT0.Add(-24 * time.Hour),
		NowIST: time.Date(2026, 9, 30, 9, 30, 0, 0, flexiIST), Shadow: shadow}
}

var fullEMA = map[string]float64{"NTYSLCP250": 1.0, "NFTYMCP150": 1.0, "NIFTY50": 1.0}

// applyAllocations mirrors the consumer: every allocation becomes a
// PENDING_ENTRY position (AddPosition semantics).
func applyAllocations(p *types.Portfolio, res *AllocateResult) {
	p.Mu.Lock()
	defer p.Mu.Unlock()
	for _, a := range res.Allocations {
		p.Positions[a.Symbol] = &types.Position{Symbol: a.Symbol, Industry: a.Industry, MCapBucket: a.MCapBucket,
			EntryPrice: a.EntryPrice, Quantity: a.Quantity, State: types.StatePendingEntry}
	}
}

func heldBy(p *types.Portfolio) map[string]int {
	out := map[string]int{}
	for _, pos := range p.Positions {
		if pos.Occupies() {
			out[pos.MCapBucket]++
		}
	}
	return out
}

// ── golden off-mode identity ──────────────────────────────────────────

func TestAllocator_OffModeGoldenIdentity(t *testing.T) {
	a := NewAllocator(zap.NewNop())
	strategy := types.UserStrategy{StrategyID: flexiTestStrategy, UserID: "S4450", TradingMode: "LIVE",
		TotalCapital: 500000, MaxPositions: 25, StopLossPct: 20, TrailingSLPct: 2}
	// A mixed batch: one held, one sector-capped, one bucket-capped, one EMA 0,
	// one price 0, one qty 0, two allocatable (SMALL under base? no — SMALL is
	// at 12, so a MID and a LARGE allocate).
	mk := func() *types.Portfolio {
		p := flexiPortfolio(12, 5, 0)
		for i := 0; i < 6; i++ { // saturate sector "Banks" with MID names
			sym := "BANK" + string(rune('A'+i))
			p.Positions[sym] = &types.Position{Symbol: sym, Industry: "Banks", MCapBucket: types.BucketMid, State: types.StateActive, Active: true}
		}
		return p
	}
	sigs := []types.ManthanSignal{
		flexiSig("SA", types.BucketSmall), // already holding
		func() types.ManthanSignal { s := flexiSig("BANKZ", types.BucketMid); s.Industry = "Banks"; return s }(),
		flexiSig("sa", types.BucketSmall), // bucket cap 50%
		func() types.ManthanSignal { s := flexiSig("ma", types.BucketMid); s.IndexName = "UNKNOWN"; return s }(),
		func() types.ManthanSignal { s := flexiSig("mb", types.BucketMid); s.LatestPrice = 0; return s }(),
		func() types.ManthanSignal { s := flexiSig("mc", types.BucketMid); s.LatestPrice = 99999; return s }(),
		flexiSig("md", types.BucketMid),
		flexiSig("la", types.BucketLarge),
	}
	ema := map[string]float64{"NTYSLCP250": 0.3, "NFTYMCP150": 0.3, "NIFTY50": 0.3}

	off := a.Allocate(sigs, mk(), ema)
	viaNil := a.AllocateWithFlexi(sigs, mk(), ema, nil)
	viaOffCfg := a.AllocateWithFlexi(sigs, mk(), ema, flexiInput(flexiCfg("off", nil), flexiUniverse(flexiNames("s", 9), nil, nil), nil))
	// An unparseable mode is forced off and must be identical too.
	viaBadCfg := a.AllocateWithFlexi(sigs, mk(), ema, flexiInput(flexiCfg("maybe", nil), flexiUniverse(flexiNames("s", 9), nil, nil), nil))

	for name, got := range map[string]*AllocateResult{"nil": viaNil, "off-cfg": viaOffCfg, "bad-cfg": viaBadCfg} {
		if !reflect.DeepEqual(off, got) {
			t.Fatalf("%s: AllocateResult differs from Allocate()\noff=%+v\ngot=%+v", name, off, got)
		}
		if len(got.FlexiEvals) != 0 || got.FlexiNotApplied != "" {
			t.Fatalf("%s: off-mode must produce no flexi output: %+v", name, got)
		}
		for _, al := range got.Allocations {
			if al.Flexi != nil {
				t.Fatalf("%s: Flexi must be nil in off-mode", name)
			}
		}
	}

	// Golden skip strings — the exact bytes downstream dashboards key on.
	wantSkips := map[string]string{
		"SA":    "already holding",
		"BANKZ": "sector cap 25% reached for Banks",
		"sa":    "mcap bucket cap 50% reached for SMALL",
		"ma":    "EMA allocation 0% for index UNKNOWN",
		"mb":    "latest_price is 0",
		"mc":    "quantity = 0 (per_call ₹6000 < effective price ₹100329.00)",
	}
	got := map[string]string{}
	for _, s := range off.Skipped {
		got[s.Symbol] = s.Reason
	}
	if !reflect.DeepEqual(wantSkips, got) {
		t.Fatalf("golden skip strings changed:\nwant %v\ngot  %v", wantSkips, got)
	}
	if len(off.Allocations) != 2 || off.Allocations[0].Symbol != "md" || off.Allocations[1].Symbol != "la" {
		t.Fatalf("golden allocations changed: %+v", off.Allocations)
	}
	// LARGE gets the 10% stop, MID the configured 20% — the bucket rule, unchanged.
	if off.Allocations[0].InitialSL != 80 || off.Allocations[1].InitialSL != 90 {
		t.Fatalf("initial SLs changed: MID %.2f LARGE %.2f", off.Allocations[0].InitialSL, off.Allocations[1].InitialSL)
	}

	// ManthanOrder JSON (trade-signals bytes) identical off vs nil-input.
	gen := NewOrderGenerator(zap.NewNop())
	o1 := gen.GenerateEntryOrders(strategy, off.Allocations)
	o2 := gen.GenerateEntryOrders(strategy, viaNil.Allocations)
	if len(o1) != len(o2) {
		t.Fatalf("order counts differ")
	}
	for i := range o1 {
		o1[i].Timestamp, o2[i].Timestamp = time.Time{}, time.Time{}
		b1, _ := json.Marshal(o1[i])
		b2, _ := json.Marshal(o2[i])
		if string(b1) != string(b2) {
			t.Fatalf("ManthanOrder JSON differs:\n%s\n%s", b1, b2)
		}
		if strings.Contains(string(b1), "flexi") || strings.Contains(string(b1), "Flexi") {
			t.Fatalf("flexi leaked onto the wire: %s", b1)
		}
	}

	// Full book (25/25) and the mid-batch break: pre-flexi the allocator
	// returned / broke SILENTLY — zero Skipped, so the consumer writes no
	// REJECTED row and no SIGNAL_SKIPPED event. That must hold in off-mode
	// (nil input, explicit off, unparseable mode). The P0.2 audit row is
	// gated on an enabled flexi input (TestAllocator_PortfolioFullSkipRows).
	fullSigs := []types.ManthanSignal{flexiSig("x1", types.BucketSmall), flexiSig("x2", types.BucketMid), flexiSig("SA", types.BucketSmall)}
	almostSigs := []types.ManthanSignal{flexiSig("m1", types.BucketMid), flexiSig("m2", types.BucketMid), flexiSig("m3", types.BucketMid)}
	offInputs := map[string]*FlexiInput{
		"nil":     nil,
		"off-cfg": flexiInput(flexiCfg("off", nil), flexiUniverse(flexiNames("s", 9), nil, nil), nil),
		"bad-cfg": flexiInput(flexiCfg("maybe", nil), flexiUniverse(flexiNames("s", 9), nil, nil), nil),
	}
	for name, in := range offInputs {
		full := a.AllocateWithFlexi(fullSigs, flexiPortfolio(12, 12, 1), fullEMA, in)
		if len(full.Allocations) != 0 || len(full.Skipped) != 0 || len(full.FlexiEvals) != 0 || full.FlexiNotApplied != "" {
			t.Fatalf("%s: full book must stay SILENT in off-mode (today's bytes): %+v", name, full)
		}
		if !reflect.DeepEqual(full, &AllocateResult{}) {
			t.Fatalf("%s: full-book result must be the zero AllocateResult: %+v", name, full)
		}
		brk := a.AllocateWithFlexi(almostSigs, flexiPortfolio(12, 11, 1), fullEMA, in) // 24/25 → m1 fills, m2/m3 dropped silently
		if len(brk.Allocations) != 1 || brk.Allocations[0].Symbol != "m1" || len(brk.Skipped) != 0 {
			t.Fatalf("%s: mid-batch break must be silent in off-mode: allocs=%+v skipped=%+v", name, brk.Allocations, brk.Skipped)
		}
	}
	if !reflect.DeepEqual(a.Allocate(fullSigs, flexiPortfolio(12, 12, 1), fullEMA), &AllocateResult{}) {
		t.Fatal("Allocate() on a full book must return the zero AllocateResult")
	}
}

// ── P0.2 ──────────────────────────────────────────────────────────────

// The "portfolio full" audit row exists ONLY while flexi is enabled (dry_run
// or on). Off-mode stays silent — pinned in TestAllocator_OffModeGoldenIdentity.
func TestAllocator_PortfolioFullSkipRows(t *testing.T) {
	a := NewAllocator(zap.NewNop())
	u := flexiUniverse(flexiNames("s", 9), flexiNames("m", 3), nil)
	for _, mode := range []string{"dry_run", "on"} {
		fx := flexiInput(flexiCfg(mode, nil), u, nil)

		// Full book: every fresh signal gets the row; a HELD symbol keeps the
		// "already holding" reason (first-write-wins audit must not mislabel it).
		full := flexiPortfolio(12, 12, 1) // 25/25
		full.Positions["MA"].Active, full.Positions["MA"].State = false, types.StatePendingEntry
		sigs := []types.ManthanSignal{flexiSig("x1", types.BucketSmall), flexiSig("x2", types.BucketMid),
			flexiSig("SA", types.BucketSmall), flexiSig("MA", types.BucketMid)}
		res := a.AllocateWithFlexi(sigs, full, fullEMA, fx)
		if len(res.Allocations) != 0 || len(res.Skipped) != 4 || len(res.FlexiEvals) != 0 {
			t.Fatalf("%s full book: allocations %d skipped %d evals %d", mode, len(res.Allocations), len(res.Skipped), len(res.FlexiEvals))
		}
		want := map[string]string{
			"x1": "portfolio full (25/25 slots)",
			"x2": "portfolio full (25/25 slots)",
			"SA": "already holding",
			"MA": "already holding (PENDING_ENTRY)",
		}
		got := map[string]string{}
		for _, s := range res.Skipped {
			if s.Signal.Symbol != s.Symbol {
				t.Fatalf("skip must carry its signal: %+v", s)
			}
			got[s.Symbol] = s.Reason
		}
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("%s full-book skips:\nwant %v\ngot  %v", mode, want, got)
		}

		// Mid-batch break: 24/25 held, MID signals m1 m2 SA m3 → m1 allocated,
		// m2/m3 "portfolio full", the held SA "already holding".
		almost := flexiPortfolio(12, 11, 1)
		res = a.AllocateWithFlexi([]types.ManthanSignal{flexiSig("m1", types.BucketMid), flexiSig("m2", types.BucketMid),
			flexiSig("SA", types.BucketSmall), flexiSig("m3", types.BucketMid)}, almost, fullEMA, fx)
		if len(res.Allocations) != 1 || res.Allocations[0].Symbol != "m1" {
			t.Fatalf("%s: expected exactly m1 allocated, got %+v", mode, res.Allocations)
		}
		got = map[string]string{}
		for _, s := range res.Skipped {
			got[s.Symbol] = s.Reason
		}
		want = map[string]string{"m2": "portfolio full (25/25 slots)", "SA": "already holding", "m3": "portfolio full (25/25 slots)"}
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("%s mid-loop skips:\nwant %v\ngot  %v", mode, want, got)
		}
	}

	// And the control: the very same books through Allocate() (off) are silent.
	if res := a.Allocate([]types.ManthanSignal{flexiSig("x1", types.BucketSmall)}, flexiPortfolio(12, 12, 1), fullEMA); len(res.Skipped) != 0 {
		t.Fatalf("off-mode full book must not emit skip rows: %+v", res.Skipped)
	}
}

// ── on-mode ───────────────────────────────────────────────────────────

func TestAllocator_OnMode_S4450Day(t *testing.T) {
	a := NewAllocator(zap.NewNop())
	cfg := flexiCfg("on", map[string]string{types.EnvFlexiDonors: "LARGE", types.EnvFlexiStrategyAllowlist: flexiTestStrategy})
	smallOpp, midOpp := flexiNames("s", 9), flexiNames("m", 3)

	run := func(order []types.ManthanSignal) (*types.Portfolio, []string, int) {
		p := flexiPortfolio(12, 5, 0)
		var reasons []string
		grants := 0
		for _, sig := range order {
			// A fresh universe per arrival: taken names disappear from Opp
			// (data-ingestion never re-lists what the book holds → held_by_S).
			u := flexiUniverse(smallOpp, midOpp, nil)
			res := a.AllocateWithFlexi([]types.ManthanSignal{sig}, p, fullEMA, flexiInput(cfg, u, nil))
			for _, s := range res.Skipped {
				reasons = append(reasons, s.Symbol+": "+s.Reason)
			}
			for _, al := range res.Allocations {
				if al.Flexi != nil {
					grants++
					if al.Flexi.Recipient != types.BucketSmall || al.Flexi.Ceiling != 16 || al.Flexi.Base != 12 ||
						len(al.Flexi.Donors) != 1 || al.Flexi.Donors[0] != types.BucketLarge || al.Flexi.Plan == nil {
						t.Fatalf("grant record wrong: %+v", al.Flexi)
					}
					// The rule follows the STOCK: bucket + 20% stop untouched.
					if al.MCapBucket != types.BucketSmall || al.InitialSL != 80 {
						t.Fatalf("borrowed SMALL entry must keep SMALL bucket and 20%% SL: %+v", al)
					}
					if al.PerCallBase != 20000 || al.Quantity != int32(20000/(100*(1+types.TotalTxnCostPct()))) {
						t.Fatalf("sizing changed on a borrowed slot: %+v", al)
					}
				}
			}
			applyAllocations(p, res)
		}
		return p, reasons, grants
	}

	smallSigs := make([]types.ManthanSignal, 0, 9)
	for _, s := range smallOpp {
		smallSigs = append(smallSigs, flexiSig(s, types.BucketSmall))
	}
	midSigs := make([]types.ManthanSignal, 0, 3)
	for _, s := range midOpp {
		midSigs = append(midSigs, flexiSig(s, types.BucketMid))
	}

	// SMALL first.
	p, reasons, grants := run(append(append([]types.ManthanSignal{}, smallSigs...), midSigs...))
	held := heldBy(p)
	if held[types.BucketSmall] != 16 || held[types.BucketMid] != 8 || grants != 4 {
		t.Fatalf("SMALL-first end state %v grants %d, want SMALL 16 / MID 8 / 4 grants; skips=%v", held, grants, reasons)
	}
	// 5 SMALL skipped; the 5th (Spare 0 → no plan) and later carry the LEGACY string.
	legacy := 0
	for _, r := range reasons {
		if strings.HasSuffix(r, "mcap bucket cap 50% reached for SMALL") {
			legacy++
		}
	}
	if legacy != 5 {
		t.Fatalf("expected 5 legacy SMALL blocks after the 4 grants, got %d: %v", legacy, reasons)
	}
	if types.CountOccupied(p.Positions) != 24 {
		t.Fatalf("1 idle seat must remain: occupied %d", types.CountOccupied(p.Positions))
	}

	// MID first → identical end state.
	p2, _, grants2 := run(append(append([]types.ManthanSignal{}, midSigs...), smallSigs...))
	held2 := heldBy(p2)
	if held2[types.BucketSmall] != 16 || held2[types.BucketMid] != 8 || grants2 != 4 {
		t.Fatalf("MID-first end state %v grants %d differs from SMALL-first", held2, grants2)
	}

	// Late LARGE arrival after SMALL borrowed: LARGE non-void → no donor →
	// SMALL back to base (blocked, legacy string); LARGE takes the idle seat.
	u := flexiUniverse(append(append([]string{}, smallOpp...), "sz"), midOpp, []string{"la"})
	res := a.AllocateWithFlexi([]types.ManthanSignal{flexiSig("sz", types.BucketSmall)}, p, fullEMA, flexiInput(cfg, u, nil))
	// 24/25 held → the free-slot guard fires first; either way: no plan, legacy string.
	if len(res.Skipped) != 1 || res.Skipped[0].Reason != "mcap bucket cap 50% reached for SMALL" || res.FlexiNotApplied != types.FlexiGuardNoFreeSlots {
		t.Fatalf("late LARGE (book 24/25): %+v not_applied=%q", res.Skipped, res.FlexiNotApplied)
	}
	// Same arrival on a book with room: it is the donor test that fails.
	res = a.AllocateWithFlexi([]types.ManthanSignal{flexiSig("sz", types.BucketSmall)}, flexiPortfolio(12, 5, 0), fullEMA, flexiInput(cfg, u, nil))
	if len(res.Skipped) != 1 || res.Skipped[0].Reason != "mcap bucket cap 50% reached for SMALL" || res.FlexiNotApplied != types.FlexiGuardNoVoidDonor {
		t.Fatalf("LARGE present → no void donor: %+v not_applied=%q", res.Skipped, res.FlexiNotApplied)
	}
	res = a.AllocateWithFlexi([]types.ManthanSignal{flexiSig("la", types.BucketLarge)}, p, fullEMA, flexiInput(cfg, u, nil))
	if len(res.Allocations) != 1 || res.Allocations[0].Flexi != nil || res.Allocations[0].InitialSL != 90 {
		t.Fatalf("late LARGE must take the idle seat under base with its 10%% stop: %+v / %+v", res.Allocations, res.Skipped)
	}
	applyAllocations(p, res)
	// Book now 25/25 → next signal gets the companion row, not silence.
	res = a.AllocateWithFlexi([]types.ManthanSignal{flexiSig("mz", types.BucketMid)}, p, fullEMA, flexiInput(cfg, u, nil))
	if len(res.Skipped) != 1 || res.Skipped[0].Reason != "portfolio full (25/25 slots)" {
		t.Fatalf("full book after flexi day: %+v", res.Skipped)
	}
}

func TestAllocator_OnMode_SectorCapPrecedence(t *testing.T) {
	a := NewAllocator(zap.NewNop())
	p := flexiPortfolio(12, 5, 0)
	// Six SMALL positions already in "Banks" (replace 6 of the 12 sectors).
	i := 0
	for _, pos := range p.Positions {
		if pos.MCapBucket == types.BucketSmall && i < 6 {
			pos.Industry = "Banks"
			i++
		}
	}
	sig := flexiSig("sa", types.BucketSmall)
	sig.Industry = "Banks"
	u := flexiUniverse(flexiNames("s", 9), flexiNames("m", 3), nil)
	res := a.AllocateWithFlexi([]types.ManthanSignal{sig}, p, fullEMA, flexiInput(flexiCfg("on", nil), u, nil))
	if len(res.Skipped) != 1 || res.Skipped[0].Reason != "sector cap 25% reached for Banks" {
		t.Fatalf("sector cap must win inside a flexed bucket: %+v", res.Skipped)
	}
	if len(res.FlexiEvals) != 1 || res.FlexiEvals[0].Outcome != types.FlexiBlockedSector {
		t.Fatalf("eval must be BLOCKED_SECTOR: %+v", res.FlexiEvals)
	}
}

func TestAllocator_OnMode_TailFailAfterCeiling(t *testing.T) {
	a := NewAllocator(zap.NewNop())
	p := flexiPortfolio(12, 5, 0)
	u := flexiUniverse(flexiNames("s", 9), flexiNames("m", 3), nil)
	res := a.AllocateWithFlexi([]types.ManthanSignal{flexiSig("sa", types.BucketSmall)}, p,
		map[string]float64{"NTYSLCP250": 0}, flexiInput(flexiCfg("on", nil), u, nil))
	if len(res.Allocations) != 0 || len(res.Skipped) != 1 || res.Skipped[0].Reason != "EMA allocation 0% for index NTYSLCP250" {
		t.Fatalf("tail must still reject: %+v / %+v", res.Allocations, res.Skipped)
	}
	if len(res.FlexiEvals) != 1 || res.FlexiEvals[0].Outcome != types.FlexiWouldFailTail || res.FlexiEvals[0].Mode != types.FlexiOn {
		t.Fatalf("eval: %+v", res.FlexiEvals)
	}
	// And the happy path records GRANTED with the simulated sizing.
	res = a.AllocateWithFlexi([]types.ManthanSignal{flexiSig("sa", types.BucketSmall)}, p, fullEMA, flexiInput(flexiCfg("on", nil), u, nil))
	if len(res.FlexiEvals) != 1 || res.FlexiEvals[0].Outcome != types.FlexiGranted || res.FlexiEvals[0].Sim == nil ||
		res.FlexiEvals[0].Sim.Quantity != res.Allocations[0].Quantity || res.FlexiEvals[0].Sim.InitialSL != res.Allocations[0].InitialSL {
		t.Fatalf("GRANTED eval must mirror the real allocation: %+v vs %+v", res.FlexiEvals, res.Allocations)
	}
}

// ── dry-run ───────────────────────────────────────────────────────────

func TestAllocator_DryRun_RealDecisionsUnchanged(t *testing.T) {
	a := NewAllocator(zap.NewNop())
	cfg := flexiCfg("dry_run", nil)
	u := flexiUniverse(flexiNames("s", 9), flexiNames("m", 3), nil)
	sigs := []types.ManthanSignal{flexiSig("sa", types.BucketSmall), flexiSig("ma", types.BucketMid)}

	off := a.Allocate(sigs, flexiPortfolio(12, 5, 0), fullEMA)
	dry := a.AllocateWithFlexi(sigs, flexiPortfolio(12, 5, 0), fullEMA, flexiInput(cfg, u, nil))
	if !reflect.DeepEqual(off.Allocations, dry.Allocations) || !reflect.DeepEqual(off.Skipped, dry.Skipped) {
		t.Fatalf("dry_run changed a real decision:\noff=%+v %+v\ndry=%+v %+v", off.Allocations, off.Skipped, dry.Allocations, dry.Skipped)
	}
	if dry.Skipped[0].Reason != "mcap bucket cap 50% reached for SMALL" {
		t.Fatalf("real SMALL decision must be the base block: %+v", dry.Skipped)
	}
	// Prediction: SMALL would allocate (held 12 < ceiling 16); MID not in play → no eval.
	if len(dry.FlexiEvals) != 1 || dry.FlexiEvals[0].Symbol != "sa" || dry.FlexiEvals[0].Outcome != types.FlexiWouldAllocate {
		t.Fatalf("evals: %+v", dry.FlexiEvals)
	}
	ev := dry.FlexiEvals[0]
	if ev.Mode != types.FlexiDryRun || ev.EvalHeld != 12 || ev.EvalLimit != 16 || ev.Sim == nil || ev.Sim.Quantity <= 0 ||
		ev.BaseOutcome != "mcap bucket cap 50% reached for SMALL" || ev.Plan == nil || ev.Plan.Ceiling[types.BucketSmall] != 16 {
		t.Fatalf("eval detail: %+v", ev)
	}
	for _, al := range dry.Allocations {
		if al.Flexi != nil {
			t.Fatal("dry_run must never set AllocationResult.Flexi")
		}
	}

	// Shadow book, S4450 day: 4 earlier WOULD_ALLOCATEs → for the 5th SMALL
	// the shadow world has Spare 0 → NO plan (exactly what on-mode does:
	// legacy block, no eval). The daily WOULD_ALLOCATE count therefore equals
	// what on-mode would have bought.
	shadow := []types.ShadowEntry{}
	for _, s := range flexiNames("s", 4) {
		shadow = append(shadow, types.ShadowEntry{Symbol: s, Industry: "sec-" + s, Bucket: types.BucketSmall})
	}
	p := flexiPortfolio(12, 5, 0)
	res := a.AllocateWithFlexi([]types.ManthanSignal{flexiSig("se", types.BucketSmall)}, p, fullEMA, flexiInput(cfg, u, shadow))
	if len(res.Skipped) != 1 || res.Skipped[0].Reason != "mcap bucket cap 50% reached for SMALL" {
		t.Fatalf("real decision with shadow: %+v", res.Skipped)
	}
	if len(res.FlexiEvals) != 0 || res.FlexiNotApplied != types.FlexiGuardNoSpare {
		t.Fatalf("5th SMALL after 4 shadow grants must be a no-plan (no_spare), got evals=%+v not_applied=%q", res.FlexiEvals, res.FlexiNotApplied)
	}
	// The shadow must NOT restrict a live decision: a MID signal in a sector
	// the shadow "occupies" still allocates under base, and the shadow's
	// slot usage never makes the live book look full.
	midSig := flexiSig("ma", types.BucketMid)
	midSig.Industry = "sec-sa" // same sector as a shadow entry
	res = a.AllocateWithFlexi([]types.ManthanSignal{midSig}, p, fullEMA, flexiInput(cfg, u, shadow))
	if len(res.Allocations) != 1 || res.Allocations[0].Symbol != "ma" {
		t.Fatalf("shadow leaked into a live decision: %+v / %+v", res.Allocations, res.Skipped)
	}

	// WOULD_BLOCK_FLEXI_CEILING is the receiver-cap-binding case: SMALL 12
	// real + 8 shadow = 20 = floor(25·80%), MID/LARGE void, 5 more SMALL
	// names → ceiling recorded at 20 with Extra 0 → the next SMALL predicts a
	// block AT the flexi ceiling, while the live decision is the base block.
	shadow8 := []types.ShadowEntry{}
	for _, s := range flexiNames("s", 8) {
		shadow8 = append(shadow8, types.ShadowEntry{Symbol: s, Industry: "sec-" + s, Bucket: types.BucketSmall})
	}
	u13 := flexiUniverse(flexiNames("s", 13), nil, nil)
	p = flexiPortfolio(12, 0, 0)
	res = a.AllocateWithFlexi([]types.ManthanSignal{flexiSig("si", types.BucketSmall)}, p, fullEMA, flexiInput(cfg, u13, shadow8))
	if len(res.Skipped) != 1 || res.Skipped[0].Reason != "mcap bucket cap 50% reached for SMALL" {
		t.Fatalf("real decision (cap-binding): %+v", res.Skipped)
	}
	if len(res.FlexiEvals) != 1 || res.FlexiEvals[0].Outcome != types.FlexiWouldBlockFlexiCeiling ||
		res.FlexiEvals[0].EvalHeld != 20 || res.FlexiEvals[0].EvalLimit != 20 {
		t.Fatalf("shadow must drive WOULD_BLOCK_FLEXI_CEILING at 20/20: %+v", res.FlexiEvals)
	}
	// Shadow entries already in Positions are dropped (no double count):
	// 13 real + 7 shadow (sa deduped) still reads 20.
	p.Positions["sa"] = &types.Position{Symbol: "sa", Industry: "sec-sa", MCapBucket: types.BucketSmall, State: types.StatePendingEntry}
	res = a.AllocateWithFlexi([]types.ManthanSignal{flexiSig("si", types.BucketSmall)}, p, fullEMA, flexiInput(cfg, u13, shadow8))
	if len(res.FlexiEvals) != 1 || res.FlexiEvals[0].EvalHeld != 20 || len(res.FlexiEvals[0].Plan.ShadowSymbols) != 7 {
		t.Fatalf("held 13 real + 7 shadow (sa deduped) must read 20: %+v", res.FlexiEvals)
	}
	// The same book in ON-mode with 20 REAL SMALL positions blocks with the
	// flexi string (this is the only place that string appears).
	p20 := flexiPortfolio(20, 0, 0)
	res = a.AllocateWithFlexi([]types.ManthanSignal{flexiSig("si", types.BucketSmall)}, p20, fullEMA, flexiInput(flexiCfg("on", nil), u13, nil))
	if len(res.Skipped) != 1 || res.Skipped[0].Reason != "mcap bucket cap reached for SMALL (flexi ceiling 20 = base 12 + 8 borrowed from [LARGE MID])" {
		t.Fatalf("on-mode cap-binding block string: %+v", res.Skipped)
	}
	if len(res.FlexiEvals) != 1 || res.FlexiEvals[0].Outcome != types.FlexiBlockedFlexiCeiling {
		t.Fatalf("on-mode eval must be BLOCKED_FLEXI_CEILING: %+v", res.FlexiEvals)
	}

	// Dry-run tail failure is predicted, not a WOULD_ALLOCATE.
	res = a.AllocateWithFlexi([]types.ManthanSignal{flexiSig("sa", types.BucketSmall)}, flexiPortfolio(12, 5, 0),
		map[string]float64{"NTYSLCP250": 0}, flexiInput(cfg, u, nil))
	if len(res.FlexiEvals) != 1 || res.FlexiEvals[0].Outcome != types.FlexiWouldFailTail {
		t.Fatalf("tail fail: %+v", res.FlexiEvals)
	}
	// Under base → NO_DIFFERENCE (SMALL at 10 with 9 opp → ceiling 16 in play, but 10 < 12).
	res = a.AllocateWithFlexi([]types.ManthanSignal{flexiSig("sa", types.BucketSmall)}, flexiPortfolio(10, 5, 0), fullEMA, flexiInput(cfg, u, nil))
	if len(res.Allocations) != 1 || len(res.FlexiEvals) != 1 || res.FlexiEvals[0].Outcome != types.FlexiNoDifference {
		t.Fatalf("NO_DIFFERENCE expected: %+v / %+v", res.Allocations, res.FlexiEvals)
	}
	// Guarded plan → no evals, reason recorded, decision still base.
	res = a.AllocateWithFlexi([]types.ManthanSignal{flexiSig("sa", types.BucketSmall)}, flexiPortfolio(12, 5, 0), fullEMA,
		flexiInput(cfg, &types.FlexiUniverse{RunDate: flexiRunDate, StocksRows: 0}, nil))
	if len(res.FlexiEvals) != 0 || res.FlexiNotApplied != types.FlexiGuardUniverseRows || res.Skipped[0].Reason != "mcap bucket cap 50% reached for SMALL" {
		t.Fatalf("guarded: %+v", res)
	}
}

// A multi-signal dry-run batch (CatchUpNewStrategy shape): a REAL allocation
// earlier in the batch must be visible to a later eval's shadow clone.
// SMALL 11 held, 9 opp: Free 8, BaseClaim MID 3 + SMALL 1, Spare 4 →
// ceiling 16. Batch [sa, sb]: sa fits under base (NO_DIFFERENCE, real
// allocation → SMALL 12); sb's eval must then see 12 (≥ base, < 16 →
// WOULD_ALLOCATE) while the live decision is the base block. Without the
// mirror sb would read 11 and be mislabelled NO_DIFFERENCE.
func TestAllocator_DryRun_BatchMirrorsRealAllocation(t *testing.T) {
	a := NewAllocator(zap.NewNop())
	u := flexiUniverse(flexiNames("s", 9), flexiNames("m", 3), nil)
	res := a.AllocateWithFlexi([]types.ManthanSignal{flexiSig("sa", types.BucketSmall), flexiSig("sb", types.BucketSmall)},
		flexiPortfolio(11, 5, 0), fullEMA, flexiInput(flexiCfg("dry_run", nil), u, nil))
	if len(res.Allocations) != 1 || res.Allocations[0].Symbol != "sa" || res.Allocations[0].Flexi != nil {
		t.Fatalf("real decisions: %+v", res.Allocations)
	}
	if len(res.Skipped) != 1 || res.Skipped[0].Symbol != "sb" || res.Skipped[0].Reason != "mcap bucket cap 50% reached for SMALL" {
		t.Fatalf("sb must be the live base block: %+v", res.Skipped)
	}
	if len(res.FlexiEvals) != 2 {
		t.Fatalf("evals: %+v", res.FlexiEvals)
	}
	sa, sb := res.FlexiEvals[0], res.FlexiEvals[1]
	if sa.Symbol != "sa" || sa.Outcome != types.FlexiNoDifference || sa.EvalHeld != 11 || sa.EvalLimit != 16 {
		t.Fatalf("sa eval: %+v", sa)
	}
	if sb.Symbol != "sb" || sb.Outcome != types.FlexiWouldAllocate || sb.EvalHeld != 12 || sb.EvalLimit != 16 {
		t.Fatalf("sb eval must see sa's real seat in the shadow clone: outcome=%s held=%d limit=%d", sb.Outcome, sb.EvalHeld, sb.EvalLimit)
	}
}

// ── wire / ids ────────────────────────────────────────────────────────

func TestManthanOrder_FlexiNeverOnWire(t *testing.T) {
	strategy := types.UserStrategy{StrategyID: flexiTestStrategy, UserID: "S4450", TradingMode: "LIVE", MaxPositions: 25, StopLossPct: 20, TrailingSLPct: 2}
	alloc := types.AllocationResult{Symbol: "sa", Industry: "sec-sa", MCapBucket: types.BucketSmall, IndexName: "NTYSLCP250",
		EMAAllocPct: 1, PerCallBase: 20000, PerCallActual: 20000, EntryPrice: 100, Quantity: 199, InitialSL: 80, RunDate: flexiRunDate}
	gen := NewOrderGenerator(zap.NewNop())
	plain := gen.GenerateEntryOrders(strategy, []types.AllocationResult{alloc})[0]
	alloc.Flexi = &types.FlexiGrant{Recipient: types.BucketSmall, Donors: []string{types.BucketLarge}, Base: 12, Ceiling: 16, HeldBefore: 12}
	flexed := gen.GenerateEntryOrders(strategy, []types.AllocationResult{alloc})[0]
	if flexed.Flexi == nil {
		t.Fatal("order must carry Flexi in-process")
	}
	plain.Timestamp, flexed.Timestamp = time.Time{}, time.Time{}
	b1, _ := json.Marshal(plain)
	b2, _ := json.Marshal(flexed)
	if string(b1) != string(b2) {
		t.Fatalf("trade-signals bytes differ with Flexi set:\n%s\n%s", b1, b2)
	}
	if plain.OrderID != flexed.OrderID || plain.OrderID != deterministicSignalID(flexiTestStrategy, "sa", flexiRunDate) {
		t.Fatal("entry id must be unchanged by flexi")
	}
	if plain.StopLossPct != 20 || plain.MCapBucket != types.BucketSmall {
		t.Fatalf("stock bucket / SL pct changed: %+v", plain)
	}
}

func TestFlexiEval_DeterministicID(t *testing.T) {
	entry := deterministicSignalID(flexiTestStrategy, "sa", flexiRunDate)
	skip := deterministicSignalID(flexiTestStrategy, "sa", flexiRunDate, "SKIP")
	ev := deterministicSignalID(flexiTestStrategy, "sa", flexiRunDate, "FLEXI_EVAL")
	if entry == skip || entry == ev || skip == ev {
		t.Fatal("ids must not collide across discriminators")
	}
	if ev != deterministicSignalID(flexiTestStrategy, "sa", flexiRunDate, "FLEXI_EVAL") {
		t.Fatal("FLEXI_EVAL id must be stable")
	}
}

// ── deploy gate ───────────────────────────────────────────────────────

// The PM2 env block is a whitelist: a key missing here means the feature can
// never be enabled on the box even with a correct .env. Skips when the
// deployment file is not present (e.g. a partial checkout).
func TestEcosystemConfig_ForwardsFlexiKeys(t *testing.T) {
	path := filepath.Join("..", "..", "..", "..", "deployments", "separate-namespace", "ecosystem.config.js")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Skipf("ecosystem.config.js not found (%v)", err)
	}
	src := string(raw)
	start := strings.Index(src, "name: 'rules-engine'")
	if start < 0 {
		t.Fatal("rules-engine app block not found")
	}
	end := strings.Index(src[start:], "name: 'trade-execution'")
	if end < 0 {
		end = len(src) - start
	}
	block := src[start : start+end]
	for _, k := range types.FlexiEnvKeys {
		if !strings.Contains(block, k+":") || !strings.Contains(block, "ENV."+k) {
			t.Errorf("rules-engine env block does not forward %s as ENV.%s || ''", k, k)
		}
	}
}

// ── DB-backed (local trading_db / signals_db; skip when unreachable) ───

func openFlexiTradingDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("postgres",
		"host=localhost port=5432 user=postgres password=postgres dbname=trading_db sslmode=disable")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Skipf("trading_db not reachable — skipping DB tests (%v)", err)
	}
	return db
}

func TestFlexiSchemaProbe_LocalDB(t *testing.T) {
	db := openFlexiTradingDB(t)
	defer db.Close()
	if err := probeFlexiSchema(context.Background(), nil); err == nil {
		t.Fatal("nil handle must fail the probe")
	}
	err := probeFlexiSchema(context.Background(), db)
	if err != nil {
		if strings.Contains(err.Error(), "014") {
			t.Skipf("migration 014 not applied locally: %v", err)
		}
		t.Fatalf("probe: %v", err)
	}
}

func TestPublishFlexiEval_RowAndShadowBook(t *testing.T) {
	db := openFlexiTradingDB(t)
	defer db.Close()
	if err := probeFlexiSchema(context.Background(), db); err != nil {
		t.Skipf("migration 014 not applied locally: %v", err)
	}
	const strategyID = "aaaaaaaa-f1e1-4f1e-9f1e-eeeeeeeeeeee"
	const runDate = "2026-09-30"
	cleanup := func() {
		_, _ = db.Exec(`DELETE FROM manthan_signal_decisions WHERE strategy_id = $1`, strategyID)
	}
	cleanup()
	defer cleanup()

	w := &fakeWriter{}
	p := &ManthanPublisher{db: db, portfolioWriter: w, logger: zap.NewNop()}
	ctx := context.Background()

	plan := &types.FlexiPlan{Mode: types.FlexiDryRun, Base: 12, Total: 25, Donors: []string{"LARGE"}, Ceiling: map[string]int{"SMALL": 16}}
	mk := func(sym, outcome string) types.FlexiEval {
		return types.FlexiEval{Mode: types.FlexiDryRun, Outcome: outcome, Symbol: sym, RunDate: runDate, Bucket: "SMALL",
			Industry: "sec-" + sym, IndexName: "NTYSLCP250", ISIN: "INE" + sym, LatestPrice: 101.5,
			BaseOutcome: "mcap bucket cap 50% reached for SMALL", EvalHeld: 12, EvalLimit: 16, Plan: plan,
			Config: map[string]any{"mode": "dry_run"}}
	}
	p.PublishFlexiEval(ctx, "UTEST", strategyID, mk("FLXA", types.FlexiWouldAllocate))
	p.PublishFlexiEval(ctx, "UTEST", strategyID, mk("FLXA", types.FlexiWouldAllocate)) // redelivery → no dup row
	p.PublishFlexiEval(ctx, "UTEST", strategyID, mk("FLXB", types.FlexiWouldBlockFlexiCeiling))
	p.PublishFlexiEval(ctx, "UTEST", strategyID, mk("FLXC", types.FlexiWouldAllocate))

	var n int
	var status, sigType, outcome, rd, reason string
	var payload, kafkaPayload []byte
	if err := db.QueryRow(`
		SELECT count(*) FROM manthan_signal_decisions WHERE strategy_id = $1 AND signal_type = 'FLEXI_EVAL'`, strategyID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("expected 3 FLEXI_EVAL rows (dedup on redelivery), got %d", n)
	}
	if err := db.QueryRow(`
		SELECT status, signal_type, payload->>'outcome', payload->>'run_date', rejection_reason, payload, kafka_payload
		FROM manthan_signal_decisions WHERE strategy_id = $1 AND symbol = 'FLXA'`, strategyID).
		Scan(&status, &sigType, &outcome, &rd, &reason, &payload, &kafkaPayload); err != nil {
		t.Fatal(err)
	}
	if status != "EVALUATED" || sigType != "FLEXI_EVAL" || outcome != types.FlexiWouldAllocate || rd != runDate {
		t.Fatalf("row: status=%s type=%s outcome=%s run_date=%s", status, sigType, outcome, rd)
	}
	if kafkaPayload != nil {
		t.Fatal("kafka_payload must stay NULL on a FLEXI_EVAL row (never re-published)")
	}
	if !strings.Contains(reason, "FLEXI_EVAL dry_run WOULD_ALLOCATE SMALL held 12/ceiling 16") {
		t.Fatalf("rejection_reason summary: %q", reason)
	}
	var evt map[string]any
	if err := json.Unmarshal(payload, &evt); err != nil || evt["type"] != "FLEXI_EVAL" || evt["mode"] != "dry_run" {
		t.Fatalf("payload: %s (%v)", payload, err)
	}
	if evt["plan"].(map[string]any)["ceiling"].(map[string]any)["SMALL"].(float64) != 16 {
		t.Fatalf("plan not stored in payload: %s", payload)
	}
	if w.count() != 3 { // Kafka event only when the row was NEWLY inserted — the redelivery of FLXA is not re-published
		t.Fatalf("expected 3 portfolio.allocations events (one per new row), got %d", w.count())
	}
	// First-write-wins: a same-day re-evaluation with a DIFFERENT outcome
	// neither rewrites the row nor emits an event (documented fidelity limit).
	p.PublishFlexiEval(ctx, "UTEST", strategyID, mk("FLXA", types.FlexiWouldBlockFlexiCeiling))
	if err := db.QueryRow(`SELECT payload->>'outcome' FROM manthan_signal_decisions WHERE strategy_id = $1 AND symbol = 'FLXA'`, strategyID).Scan(&outcome); err != nil {
		t.Fatal(err)
	}
	if outcome != types.FlexiWouldAllocate || w.count() != 3 {
		t.Fatalf("first-write-wins violated: outcome=%s events=%d", outcome, w.count())
	}
	// Without a DB handle the event is still best-effort published (at-least-once).
	noDB := &fakeWriter{}
	(&ManthanPublisher{portfolioWriter: noDB, logger: zap.NewNop()}).PublishFlexiEval(ctx, "UTEST", strategyID, mk("FLXD", types.FlexiWouldAllocate))
	if noDB.count() != 1 {
		t.Fatalf("no-DB publisher must still emit the event, got %d", noDB.count())
	}
	if !strings.Contains(string(w.msgs[0].Value), `"type":"FLEXI_EVAL"`) || string(w.msgs[0].Key) != strategyID+":FLXA" {
		t.Fatalf("event: key=%s value=%s", w.msgs[0].Key, w.msgs[0].Value)
	}

	// Shadow book: WOULD_ALLOCATE rows only, excluding this message's own symbol.
	c := &Consumer{flexiDB: db, flexiCfg: flexiCfg("dry_run", nil), flexiStats: newFlexiStats(), logger: zap.NewNop()}
	shadow, err := c.loadShadowBook(ctx, strategyID, runDate, map[string]bool{"FLXC": true})
	if err != nil {
		t.Fatalf("loadShadowBook: %v", err)
	}
	if len(shadow) != 1 || shadow[0].Symbol != "FLXA" || shadow[0].Bucket != "SMALL" || shadow[0].Industry != "sec-FLXA" {
		t.Fatalf("shadow = %+v, want only FLXA", shadow)
	}
	if got, _ := c.loadShadowBook(ctx, strategyID, "2026-09-29", nil); len(got) != 0 {
		t.Fatalf("other run_date must be empty: %+v", got)
	}
	if _, err := (&Consumer{flexiCfg: flexiCfg("dry_run", nil), flexiStats: newFlexiStats(), logger: zap.NewNop()}).
		loadShadowBook(ctx, strategyID, runDate, nil); err == nil {
		t.Fatal("nil decisions DB must error (fail closed)")
	}

	// On-mode entry row with flexi_grant, through the real insert path.
	order := ManthanOrder{OrderID: deterministicSignalID(strategyID, "FLXG", runDate), UserID: "UTEST", StrategyID: strategyID,
		Symbol: "FLXG", ISIN: "INEFLXG", EntryPrice: 100, EMAAllocPct: 100, Quantity: 10, InvestedAmt: 1000, StopLoss: 80,
		Industry: "sec-FLXG", MCapBucket: "SMALL", IndexName: "NTYSLCP250", TradingMode: "PAPER",
		Flexi: &types.FlexiGrant{Recipient: "SMALL", Donors: []string{"LARGE"}, Base: 12, Ceiling: 16, HeldBefore: 12, Plan: plan}}
	if err := p.dbInsertEntryDecision(ctx, order); err != nil {
		t.Fatalf("entry insert with flexi_grant: %v", err)
	}
	var grant []byte
	var kp []byte
	if err := db.QueryRow(`SELECT flexi_grant, kafka_payload FROM manthan_signal_decisions WHERE signal_id = $1`, order.OrderID).Scan(&grant, &kp); err != nil {
		t.Fatal(err)
	}
	var g types.FlexiGrant
	if err := json.Unmarshal(grant, &g); err != nil || g.Ceiling != 16 || g.Recipient != "SMALL" {
		t.Fatalf("flexi_grant: %s (%v)", grant, err)
	}
	if strings.Contains(string(kp), "flexi") || strings.Contains(string(kp), "Flexi") {
		t.Fatalf("kafka_payload must not carry flexi: %s", kp)
	}
	if err := p.dbInsertEntryDecision(ctx, order); err != ErrDuplicateDecision {
		t.Fatalf("second insert must dedupe: %v", err)
	}
	// Plain entry (no grant) still uses the base statement.
	order.Flexi, order.Symbol, order.OrderID = nil, "FLXP", deterministicSignalID(strategyID, "FLXP", runDate)
	if err := p.dbInsertEntryDecision(ctx, order); err != nil {
		t.Fatalf("plain entry insert: %v", err)
	}
	var isNull bool
	if err := db.QueryRow(`SELECT flexi_grant IS NULL FROM manthan_signal_decisions WHERE signal_id = $1`, order.OrderID).Scan(&isNull); err != nil || !isNull {
		t.Fatalf("plain entry must have NULL flexi_grant (%v)", err)
	}
}

func TestFetchFlexiUniverse_LocalSignalsDB(t *testing.T) {
	// nil handle → fail closed, fast.
	c := &Consumer{flexiCfg: flexiCfg("dry_run", nil), flexiStats: newFlexiStats(), logger: zap.NewNop()}
	u := c.fetchFlexiUniverse(context.Background(), flexiRunDate)
	if u == nil || u.Err == nil {
		t.Fatal("nil signals DB must yield Err (universe unknown)")
	}
	if _, why := types.BuildFlexiPlan(flexiInput(c.flexiCfg, u, nil), types.NewCapCheck(25, nil), 25, 0, nil, nil,
		[]types.ManthanSignal{flexiSig("sa", types.BucketSmall)}); why != types.FlexiGuardUniverseError {
		t.Fatalf("plan must fail closed on universe error: %s", why)
	}

	db, err := sql.Open("postgres",
		"host=localhost port=5432 user=postgres password=postgres dbname=signals_db sslmode=disable")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		t.Skipf("signals_db not reachable — skipping (%v)", err)
	}
	var runDate sql.NullString
	if err := db.QueryRow(`SELECT to_char(max(run_date),'YYYY-MM-DD') FROM manthan_stocks`).Scan(&runDate); err != nil || !runDate.Valid {
		t.Skipf("no manthan_stocks rows locally (%v)", err)
	}
	c = &Consumer{signalsDB: db, flexiCfg: flexiCfg("dry_run", nil), flexiStats: newFlexiStats(), logger: zap.NewNop()}
	u = c.fetchFlexiUniverse(context.Background(), runDate.String)
	if u.Err != nil {
		t.Fatalf("fetch: %v", u.Err)
	}
	var stocks, signals int
	_ = db.QueryRow(`SELECT count(*) FROM manthan_stocks WHERE run_date = $1::date`, runDate.String).Scan(&stocks)
	_ = db.QueryRow(`SELECT count(*) FROM manthan_signals WHERE run_date = $1::date`, runDate.String).Scan(&signals)
	if u.StocksRows != stocks || len(u.Eligible) != signals || u.RunDate != runDate.String {
		t.Fatalf("universe %d/%d vs DB %d/%d", u.StocksRows, len(u.Eligible), stocks, signals)
	}
	for _, r := range u.Eligible {
		if r.Bucket != types.NormalizeBucket(r.Bucket) {
			t.Fatalf("bucket not normalised: %q", r.Bucket)
		}
	}
	// Empty run_date and a garbage date both fail closed rather than panic.
	if u := c.fetchFlexiUniverse(context.Background(), ""); u.Err == nil {
		t.Fatal("empty run_date must fail closed")
	}
	if u := c.fetchFlexiUniverse(context.Background(), "not-a-date"); u.Err == nil {
		t.Fatal("unparseable run_date must fail closed")
	}
	// Stats recorded the queries.
	if s := c.FlexiStats(); s["universe_query_count"] < 3 {
		t.Fatalf("stats: %v", s)
	}

	// Deadline → fail closed: an already-expired per-statement budget makes
	// the tx context expire before BEGIN (database/sql checks ctx first), so
	// Err is set, no plan can be built, and observeFlexiUniverse counts +
	// Warns (rate-limited) instead of staying silent.
	slow := &Consumer{signalsDB: db, flexiCfg: flexiCfg("dry_run", nil), flexiStats: newFlexiStats(), logger: zap.NewNop()}
	slow.flexiCfg.DBTimeout = time.Nanosecond
	u = slow.fetchFlexiUniverse(context.Background(), runDate.String)
	if u.Err == nil || !strings.Contains(u.Err.Error(), "begin tx") {
		t.Fatalf("expired budget must fail closed at BEGIN: %v", u.Err)
	}
	slow.observeFlexiUniverse(u)
	if _, why := types.BuildFlexiPlan(flexiInput(slow.flexiCfg, u, nil), types.NewCapCheck(25, nil), 25, 0, nil, nil,
		[]types.ManthanSignal{flexiSig("sa", types.BucketSmall)}); why != types.FlexiGuardUniverseError {
		t.Fatalf("plan must fail closed on a timed-out universe: %s", why)
	}
	if s := slow.FlexiStats(); s["universe_fetch_error"] != 1 || s["universe_query_count"] != 1 {
		t.Fatalf("timeout must be counted: %v", s)
	}
	// A statement budget of 1 ms with a 4 ms tx budget against a local DB
	// either succeeds or fails closed — never panics, never returns nil.
	slow.flexiCfg.DBTimeout = time.Millisecond
	if u := slow.fetchFlexiUniverse(context.Background(), runDate.String); u == nil {
		t.Fatal("fetch must never return nil")
	}
}

// probeFlexiSchema classifies its failures: a nil handle / missing
// constraint / un-widened list is DEFINITIVE (ErrFlexiSchemaNotMigrated — Wire
// forces off immediately and names the migration); a connectivity error is
// not (Wire retries with backoff and blames the DB).
func TestFlexiSchemaProbe_ErrorClassification(t *testing.T) {
	if err := probeFlexiSchema(context.Background(), nil); !errors.Is(err, ErrFlexiSchemaNotMigrated) {
		t.Fatalf("nil handle must be definitive: %v", err)
	}
	// An unreachable server (nothing listens on port 1) is a DB error, not a
	// schema answer. sql.Open does not dial, so this is deterministic.
	dead, err := sql.Open("postgres", "host=127.0.0.1 port=1 user=x password=x dbname=x sslmode=disable connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	defer dead.Close()
	perr := probeFlexiSchema(context.Background(), dead)
	if perr == nil || errors.Is(perr, ErrFlexiSchemaNotMigrated) {
		t.Fatalf("unreachable DB must be a non-definitive error: %v", perr)
	}
	// The retry wrapper gives up after flexiProbeAttempts and never converts
	// a DB error into "not migrated"; a cancelled ctx aborts the backoff.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rerr := probeFlexiSchemaWithRetry(ctx, dead, zap.NewNop())
	if rerr == nil || errors.Is(rerr, ErrFlexiSchemaNotMigrated) {
		t.Fatalf("retry wrapper: %v", rerr)
	}
}
