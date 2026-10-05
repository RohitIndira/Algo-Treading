package types

// Pure-layer tests for flexi caps (design brief 2026-09-30 §2):
//
//	TestFlexiConfig_Parse                 — defaults, modes, every invalid value forces off
//	TestBuildFlexiPlan_S4450Trace         — the worked example: SMALL ceiling 16, MID reserved 3, 1 idle
//	TestBuildFlexiPlan_AdversarialMID     — MID Opp 10 → Spare 0 → no plan (SMALL stays at 12)
//	TestBuildFlexiPlan_OrderIndependence  — SMALL-first vs MID-first → identical end state
//	TestBuildFlexiPlan_OrderDependenceWhenDonorFlipsIntraday — the precondition: a donor that
//	                                        turns void only because its last name was BOUGHT
//	                                        makes the end state arrival-order dependent
//	                                        (under-borrowing, never a wrong grant)
//	TestBuildFlexiPlan_MonotoneTrajectory — sequential grants never lower the ceiling
//	TestBuildFlexiPlan_RestartNeutral     — SMALL rehydrated at 15 → same ceiling as the trajectory
//	TestBuildFlexiPlan_Guards             — every fail-closed / structural guard returns nil
//	TestBuildFlexiPlan_MaxReceiverPct     — ceiling ≤ floor(N·pct/100)
//	TestBuildFlexiPlan_CreationGate       — pre-creation names are not opportunities
//	TestCapCheck_FlexiStrings             — legacy string at base, flexi string at ceiling, sector first

import (
	"testing"
	"time"
)

func envMap(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestFlexiConfig_Parse(t *testing.T) {
	// Unset → off, defaults intact.
	c := ParseFlexiConfig(envMap(nil))
	if c.Mode != FlexiOff || c.Enabled() {
		t.Fatalf("unset must be off, got %s", c.Mode)
	}
	if c.MaxReceiverPct != 80 || c.MinIdleSlots != 1 || c.MinUniverseRows != 5 || c.CutoffIST != "15:20" ||
		c.DBTimeout != 300*time.Millisecond || len(c.Priority) != 3 || len(c.Donors) != 3 {
		t.Fatalf("defaults wrong: %+v", c)
	}
	if !c.AppliesTo("anything") {
		t.Fatal("empty allowlist must apply to all")
	}
	if c.ReceiverCap(25) != 20 || c.ReceiverCap(10) != 8 || c.ReceiverCap(50) != 40 {
		t.Fatalf("ReceiverCap: 25→%d 10→%d 50→%d", c.ReceiverCap(25), c.ReceiverCap(10), c.ReceiverCap(50))
	}
	ist := time.FixedZone("IST", 19800)
	if c.CutoffReached(time.Date(2026, 9, 30, 15, 19, 0, 0, ist)) {
		t.Error("15:19 must be before the 15:20 cutoff")
	}
	if !c.CutoffReached(time.Date(2026, 9, 30, 15, 20, 0, 0, ist)) {
		t.Error("15:20 must be at the cutoff")
	}

	// dry_run with the S4450 allowlist.
	c = ParseFlexiConfig(envMap(map[string]string{
		EnvFlexiMode:              "dry_run",
		EnvFlexiStrategyAllowlist: "A6CB5B08-DDDD-4E54-AD2D-D5AE55EDB3C9, ",
		EnvFlexiDonors:            "large",
		EnvFlexiPriority:          "mid, large, small",
		EnvFlexiMaxReceiverPct:    "90",
		EnvFlexiMinIdleSlots:      "0",
		EnvFlexiCutoffIST:         "14:00",
		EnvFlexiDBTimeoutMs:       "150",
	}))
	if c.Mode != FlexiDryRun || !c.Enabled() || c.ForcedOff {
		t.Fatalf("dry_run not parsed: %+v", c)
	}
	if !c.AppliesTo("a6cb5b08-dddd-4e54-ad2d-d5ae55edb3c9") || c.AppliesTo("other") {
		t.Fatal("allowlist must be case-insensitive and exclusive")
	}
	if len(c.Donors) != 1 || c.Donors[0] != "LARGE" {
		t.Fatalf("donors: %v", c.Donors)
	}
	if c.Priority[0] != "MID" || c.Priority[1] != "LARGE" || c.Priority[2] != "SMALL" {
		t.Fatalf("priority: %v", c.Priority)
	}
	if c.MaxReceiverPct != 90 || c.MinIdleSlots != 0 || c.CutoffIST != "14:00" || c.DBTimeout != 150*time.Millisecond {
		t.Fatalf("values: %+v", c)
	}
	if c.CutoffReached(time.Date(2026, 9, 30, 13, 59, 0, 0, ist)) || !c.CutoffReached(time.Date(2026, 9, 30, 14, 0, 0, 0, ist)) {
		t.Error("14:00 cutoff not honoured")
	}

	// on
	if c := ParseFlexiConfig(envMap(map[string]string{EnvFlexiMode: "ON"})); c.Mode != FlexiOn {
		t.Fatalf("on: %s", c.Mode)
	}

	// Clamp, not reject, for out-of-range pct.
	c = ParseFlexiConfig(envMap(map[string]string{EnvFlexiMode: "on", EnvFlexiMaxReceiverPct: "30"}))
	if c.Mode != FlexiOn || c.MaxReceiverPct != 50 {
		t.Fatalf("pct 30 must clamp to 50 and keep mode: %+v", c)
	}
	c = ParseFlexiConfig(envMap(map[string]string{EnvFlexiMode: "on", EnvFlexiMaxReceiverPct: "150"}))
	if c.MaxReceiverPct != 100 {
		t.Fatalf("pct 150 must clamp to 100: %d", c.MaxReceiverPct)
	}

	// Every invalid value forces off when a non-off mode was requested.
	bad := []map[string]string{
		{EnvFlexiMode: "yes"},
		{EnvFlexiMode: "on", EnvFlexiPriority: "LARGE,MID"},          // not a permutation
		{EnvFlexiMode: "on", EnvFlexiPriority: "LARGE,MID,MID"},      // duplicate
		{EnvFlexiMode: "on", EnvFlexiPriority: "LARGE,MID,SMALL,XL"}, // unknown
		{EnvFlexiMode: "on", EnvFlexiDonors: "HUGE"},
		{EnvFlexiMode: "on", EnvFlexiDonors: ","},
		{EnvFlexiMode: "dry_run", EnvFlexiMaxReceiverPct: "eighty"},
		{EnvFlexiMode: "dry_run", EnvFlexiMinIdleSlots: "-1"},
		{EnvFlexiMode: "dry_run", EnvFlexiMinUniverseRows: "0"},
		{EnvFlexiMode: "dry_run", EnvFlexiCutoffIST: "3pm"},
		{EnvFlexiMode: "dry_run", EnvFlexiStrategyAllowlist: " , "},
		{EnvFlexiMode: "dry_run", EnvFlexiDBTimeoutMs: "0"},
	}
	for _, m := range bad {
		c := ParseFlexiConfig(envMap(m))
		if c.Mode != FlexiOff || c.Enabled() {
			t.Errorf("%v must force off, got %s", m, c.Mode)
		}
		if len(c.Warnings) == 0 {
			t.Errorf("%v must record a warning", m)
		}
	}
	// An invalid extra with mode off stays off silently (no ForcedOff).
	if c := ParseFlexiConfig(envMap(map[string]string{EnvFlexiPriority: "bogus"})); c.ForcedOff {
		t.Error("off + invalid extra must not report ForcedOff")
	}
}

// ── fixtures ──────────────────────────────────────────────────────────

func onCfg(over map[string]string) FlexiConfig {
	m := map[string]string{EnvFlexiMode: "on"}
	for k, v := range over {
		m[k] = v
	}
	return ParseFlexiConfig(envMap(m))
}

// book builds a portfolio Positions map with n held names per bucket, each in
// its own sector (so the sector cap never interferes unless asked).
func book(small, mid, large int) map[string]*Position {
	pos := map[string]*Position{}
	add := func(prefix, bucket string, n int) {
		for i := 0; i < n; i++ {
			sym := prefix + string(rune('A'+i))
			pos[sym] = &Position{Symbol: sym, Industry: "sec-" + sym, MCapBucket: bucket, State: StateActive, Active: true}
		}
	}
	add("S", BucketSmall, small)
	add("M", BucketMid, mid)
	add("L", BucketLarge, large)
	return pos
}

// universe builds a snapshot with the given eligible symbols per bucket, all
// first seen at t0 (2026-09-30 08:55 UTC) and 40 manthan_stocks rows.
var t0 = time.Date(2026, 9, 30, 3, 25, 0, 0, time.UTC)

func universe(runDate string, small, mid, large []string) *FlexiUniverse {
	u := &FlexiUniverse{RunDate: runDate, StocksRows: 40, StocksByStatus: map[string]int{"ELIGIBLE": 12, "FILTER_REJECTED": 28},
		InfraUnknown: map[string]bool{}, QueriedAt: t0}
	for _, s := range small {
		u.Eligible = append(u.Eligible, FlexiUniverseRow{Symbol: s, Industry: "sec-" + s, Bucket: BucketSmall, FirstSeenAt: t0})
	}
	for _, s := range mid {
		u.Eligible = append(u.Eligible, FlexiUniverseRow{Symbol: s, Industry: "sec-" + s, Bucket: BucketMid, FirstSeenAt: t0})
	}
	for _, s := range large {
		u.Eligible = append(u.Eligible, FlexiUniverseRow{Symbol: s, Industry: "sec-" + s, Bucket: BucketLarge, FirstSeenAt: t0})
	}
	return u
}

func names(prefix string, n int) []string {
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, prefix+string(rune('a'+i)))
	}
	return out
}

func sigOf(sym, bucket string) ManthanSignal {
	return ManthanSignal{RunDate: "2026-09-30", Symbol: sym, Industry: "sec-" + sym, MCapBucket: bucket,
		IndexName: "NTYSLCP250", LatestPrice: 100}
}

func input(cfg FlexiConfig, u *FlexiUniverse) *FlexiInput {
	return &FlexiInput{Cfg: cfg, Universe: u,
		StrategyCreatedAt: t0.Add(-24 * time.Hour),
		NowIST:            time.Date(2026, 9, 30, 9, 30, 0, 0, time.FixedZone("IST", 19800))}
}

// S4450 today: N=25, Held SMALL 12 / MID 5 / LARGE 0, Opp LARGE 0 / MID 3 / SMALL 9.
// Free = 8−1 = 7; BaseClaim[MID] = 3; Spare = 4; Extra[MID] = 0;
// Extra[SMALL] = min(9, 4, 12, 8) = 4 → SMALL ceiling 16, 1 idle.
func s4450() (map[string]*Position, *FlexiUniverse, []ManthanSignal) {
	pos := book(12, 5, 0)
	smallOpp := names("s", 9)
	u := universe("2026-09-30", smallOpp, names("m", 3), nil)
	return pos, u, []ManthanSignal{sigOf(smallOpp[0], BucketSmall)}
}

func TestBuildFlexiPlan_S4450Trace(t *testing.T) {
	pos, u, sigs := s4450()
	caps := NewCapCheck(25, pos)
	plan, why := BuildFlexiPlan(input(onCfg(nil), u), caps, 25, CountOccupied(pos), pos, nil, sigs)
	if plan == nil {
		t.Fatalf("expected a plan, guard=%s", why)
	}
	if plan.Base != 12 || plan.Free != 7 || plan.HeldTotal != 17 || plan.ReceiverCap != 20 {
		t.Fatalf("base/free/held/cap = %d/%d/%d/%d", plan.Base, plan.Free, plan.HeldTotal, plan.ReceiverCap)
	}
	if !plan.Void[BucketLarge] || plan.Void[BucketMid] || plan.Void[BucketSmall] {
		t.Fatalf("void: %v", plan.Void)
	}
	if len(plan.Donors) != 1 || plan.Donors[0] != BucketLarge {
		t.Fatalf("donors: %v", plan.Donors)
	}
	if plan.PoolFreeBefore != 12 {
		t.Fatalf("pool_free = %d, want 12", plan.PoolFreeBefore)
	}
	if plan.BaseClaim[BucketMid] != 3 || plan.BaseClaim[BucketSmall] != 0 {
		t.Fatalf("base_claim: %v", plan.BaseClaim)
	}
	if plan.SpareBefore != 4 {
		t.Fatalf("spare = %d, want 4", plan.SpareBefore)
	}
	if plan.Extra[BucketMid] != 0 || plan.Extra[BucketSmall] != 4 {
		t.Fatalf("extra: %v", plan.Extra)
	}
	if plan.Ceiling[BucketSmall] != 16 {
		t.Fatalf("SMALL ceiling = %d, want 16", plan.Ceiling[BucketSmall])
	}
	if _, ok := plan.Ceiling[BucketMid]; ok {
		t.Fatalf("MID must have no ceiling entry (stays at base): %v", plan.Ceiling)
	}
	if !plan.InPlay(BucketSmall) || plan.InPlay(BucketMid) || plan.InPlay(BucketLarge) {
		t.Fatal("only SMALL is in play")
	}
	if plan.Opportunities[BucketSmall] != 9 || plan.Opportunities[BucketMid] != 3 || plan.Opportunities[BucketLarge] != 0 {
		t.Fatalf("opportunities: %v", plan.Opportunities)
	}
	if len(plan.OppSymbols[BucketMid]) != 3 {
		t.Fatalf("opp_symbols MID: %v", plan.OppSymbols[BucketMid])
	}
	// Invariant: Σ base_claim + Σ extra ≤ free.
	sum := 0
	for _, v := range plan.BaseClaim {
		sum += v
	}
	for _, v := range plan.Extra {
		sum += v
	}
	if sum > plan.Free {
		t.Fatalf("Σ base_claim + Σ extra = %d > free %d", sum, plan.Free)
	}
}

// The critique's adversarial universe: MID has 10 eligible names → MID's own
// base room (7) is fully reserved → Spare 0 → NO plan → SMALL blocked at 12
// with the legacy string. Order of arrival cannot change this.
func TestBuildFlexiPlan_AdversarialMID(t *testing.T) {
	pos := book(12, 5, 0)
	u := universe("2026-09-30", names("s", 9), names("m", 10), nil)
	caps := NewCapCheck(25, pos)
	plan, why := BuildFlexiPlan(input(onCfg(nil), u), caps, 25, CountOccupied(pos), pos, nil, []ManthanSignal{sigOf("sa", BucketSmall)})
	if plan != nil {
		t.Fatalf("expected no plan, got ceiling %v", plan.Ceiling)
	}
	if why != FlexiGuardNoSpare {
		t.Fatalf("guard = %s, want %s", why, FlexiGuardNoSpare)
	}
	if ok, reason := caps.CanAdd("sec-sa", BucketSmall); ok || reason != "mcap bucket cap 50% reached for SMALL" {
		t.Fatalf("SMALL must be blocked with the legacy string, got ok=%v %q", ok, reason)
	}
}

// simulateDay replays arrivals in the given order against a fresh S4450 book
// through the exact CanAdd/ceiling path the allocator uses (one plan per
// arrival, positions added on success). Returns the final held counts.
func simulateDay(t *testing.T, cfg FlexiConfig, pos map[string]*Position, u *FlexiUniverse, arrivals []ManthanSignal) (map[string]int, []int) {
	t.Helper()
	var ceilings []int
	for _, sig := range arrivals {
		caps := NewCapCheck(25, pos)
		plan, _ := BuildFlexiPlan(input(cfg, u), caps, 25, CountOccupied(pos), pos, nil, []ManthanSignal{sig})
		if plan != nil {
			caps.BucketCeiling, caps.FlexiDonors = plan.Ceiling, plan.Donors
			if sig.MCapBucket == BucketSmall && plan.InPlay(BucketSmall) {
				ceilings = append(ceilings, plan.Ceiling[BucketSmall])
			}
		}
		if CountOccupied(pos) >= 25 {
			continue
		}
		if ok, _ := caps.CanAdd(sig.Industry, sig.MCapBucket); ok {
			pos[sig.Symbol] = &Position{Symbol: sig.Symbol, Industry: sig.Industry, MCapBucket: sig.MCapBucket, State: StatePendingEntry}
		}
	}
	held := map[string]int{}
	for _, p := range pos {
		if p.Occupies() {
			held[p.MCapBucket]++
		}
	}
	return held, ceilings
}

func TestBuildFlexiPlan_OrderIndependence(t *testing.T) {
	smallSigs := make([]ManthanSignal, 0, 9)
	for _, s := range names("s", 9) {
		smallSigs = append(smallSigs, sigOf(s, BucketSmall))
	}
	midSigs := make([]ManthanSignal, 0, 3)
	for _, s := range names("m", 3) {
		midSigs = append(midSigs, sigOf(s, BucketMid))
	}
	u := universe("2026-09-30", names("s", 9), names("m", 3), nil)

	for _, cfg := range []FlexiConfig{onCfg(nil), onCfg(map[string]string{EnvFlexiDonors: "LARGE"})} {
		smallFirst, _ := simulateDay(t, cfg, book(12, 5, 0), u, append(append([]ManthanSignal{}, smallSigs...), midSigs...))
		midFirst, _ := simulateDay(t, cfg, book(12, 5, 0), u, append(append([]ManthanSignal{}, midSigs...), smallSigs...))
		for _, b := range CanonicalBuckets {
			if smallFirst[b] != midFirst[b] {
				t.Fatalf("donors=%v: end state differs for %s: SMALL-first %v vs MID-first %v", cfg.Donors, b, smallFirst, midFirst)
			}
		}
		if smallFirst[BucketSmall] != 16 || smallFirst[BucketMid] != 8 {
			t.Fatalf("donors=%v: end state %v, want SMALL 16 / MID 8 (1 idle)", cfg.Donors, smallFirst)
		}
	}

	// Adversarial (MID Opp 10): end state MID 12 / SMALL 12 either way.
	midTen := make([]ManthanSignal, 0, 10)
	for _, s := range names("m", 10) {
		midTen = append(midTen, sigOf(s, BucketMid))
	}
	u2 := universe("2026-09-30", names("s", 9), names("m", 10), nil)
	a, _ := simulateDay(t, onCfg(nil), book(12, 5, 0), u2, append(append([]ManthanSignal{}, smallSigs...), midTen...))
	b, _ := simulateDay(t, onCfg(nil), book(12, 5, 0), u2, append(append([]ManthanSignal{}, midTen...), smallSigs...))
	if a[BucketSmall] != 12 || a[BucketMid] != 12 || b[BucketSmall] != 12 || b[BucketMid] != 12 {
		t.Fatalf("adversarial end states: SMALL-first %v, MID-first %v; want 12/12 both", a, b)
	}
}

// The order-independence claim holds GIVEN the donor set at first arrival.
// Counter-example (file header): Opp LARGE = 1, DONORS=LARGE (or default).
// SMALL-first: LARGE non-void → no donor → SMALL blocked at 12 all day; the
// LARGE name is then bought and LARGE flips to void, too late for SMALL.
// LARGE-first: the LARGE name is bought, LARGE is void, SMALL borrows 3
// (Free 6, BaseClaim MID 3, Spare 3). Under-borrowing only — every grant in
// the LARGE-first run is valid — but the end states differ (S12 vs S15).
func TestBuildFlexiPlan_OrderDependenceWhenDonorFlipsIntraday(t *testing.T) {
	smallSigs := make([]ManthanSignal, 0, 9)
	for _, s := range names("s", 9) {
		smallSigs = append(smallSigs, sigOf(s, BucketSmall))
	}
	midSigs := make([]ManthanSignal, 0, 3)
	for _, s := range names("m", 3) {
		midSigs = append(midSigs, sigOf(s, BucketMid))
	}
	largeSig := []ManthanSignal{sigOf("la", BucketLarge)}
	u := universe("2026-09-30", names("s", 9), names("m", 3), []string{"la"})

	for _, cfg := range []FlexiConfig{onCfg(map[string]string{EnvFlexiDonors: "LARGE"}), onCfg(nil)} {
		smallFirst, smallFirstCeil := simulateDay(t, cfg, book(12, 5, 0), u,
			append(append(append([]ManthanSignal{}, smallSigs...), midSigs...), largeSig...))
		largeFirst, largeFirstCeil := simulateDay(t, cfg, book(12, 5, 0), u,
			append(append(append([]ManthanSignal{}, largeSig...), smallSigs...), midSigs...))

		if smallFirst[BucketSmall] != 12 || smallFirst[BucketMid] != 8 || smallFirst[BucketLarge] != 1 || len(smallFirstCeil) != 0 {
			t.Fatalf("donors=%v SMALL-first: %v ceilings=%v, want S12/M8/L1 and no plan (LARGE non-void until bought)", cfg.Donors, smallFirst, smallFirstCeil)
		}
		if largeFirst[BucketSmall] != 15 || largeFirst[BucketMid] != 8 || largeFirst[BucketLarge] != 1 {
			t.Fatalf("donors=%v LARGE-first: %v, want S15/M8/L1 (LARGE void after its only name was bought)", cfg.Donors, largeFirst)
		}
		// Each LARGE-first grant was individually valid: constant ceiling 15,
		// ≤ ReceiverCap 20, and one idle seat kept (24/25).
		for _, c := range largeFirstCeil {
			if c != 15 {
				t.Fatalf("donors=%v LARGE-first ceilings %v, want constant 15", cfg.Donors, largeFirstCeil)
			}
		}
		if total := largeFirst[BucketSmall] + largeFirst[BucketMid] + largeFirst[BucketLarge]; total != 24 {
			t.Fatalf("donors=%v LARGE-first occupied %d, want 24 (MIN_IDLE 1)", cfg.Donors, total)
		}
	}
}

// After each SMALL grant: Held+1, Opp−1, Free−1, Spare−1, Extra−1 → the
// ceiling stays 16 until Spare hits 0; the trajectory is non-decreasing.
func TestBuildFlexiPlan_MonotoneTrajectory(t *testing.T) {
	smallSigs := make([]ManthanSignal, 0, 9)
	for _, s := range names("s", 9) {
		smallSigs = append(smallSigs, sigOf(s, BucketSmall))
	}
	u := universe("2026-09-30", names("s", 9), names("m", 3), nil)
	held, ceilings := simulateDay(t, onCfg(nil), book(12, 5, 0), u, smallSigs)
	if held[BucketSmall] != 16 {
		t.Fatalf("SMALL ended at %d, want 16", held[BucketSmall])
	}
	if len(ceilings) != 4 {
		t.Fatalf("expected 4 in-play plans (one per grant), got %d: %v", len(ceilings), ceilings)
	}
	for i, c := range ceilings {
		if c != 16 {
			t.Fatalf("ceiling[%d] = %d, want constant 16: %v", i, c, ceilings)
		}
		if i > 0 && c < ceilings[i-1] {
			t.Fatalf("ceiling decreased at step %d: %v", i, ceilings)
		}
	}
}

// A restart with SMALL rehydrated at 15 (3 borrowed) recomputes from the same
// Positions: PoolFree = 12 − 3 = 9, and the ceiling is the same 16 the
// no-restart trajectory produced at that point.
func TestBuildFlexiPlan_RestartNeutral(t *testing.T) {
	pos := book(15, 5, 0)
	u := universe("2026-09-30", names("s", 6), names("m", 3), nil) // 3 of the 9 SMALL were taken
	caps := NewCapCheck(25, pos)
	plan, why := BuildFlexiPlan(input(onCfg(nil), u), caps, 25, CountOccupied(pos), pos, nil, []ManthanSignal{sigOf("sa", BucketSmall)})
	if plan == nil {
		t.Fatalf("no plan after restart: %s", why)
	}
	if plan.PoolFreeBefore != 9 {
		t.Fatalf("pool_free = %d, want 9 (12 − 3 already borrowed)", plan.PoolFreeBefore)
	}
	if plan.Ceiling[BucketSmall] != 16 {
		t.Fatalf("SMALL ceiling after restart = %d, want 16", plan.Ceiling[BucketSmall])
	}
	if ok, _ := caps.CanAdd("sec-sa", BucketSmall); !ok { // base check (plan not applied here)
		// under base caps 15 ≥ 12 blocks — expected; apply the plan:
		caps.BucketCeiling, caps.FlexiDonors = plan.Ceiling, plan.Donors
		if ok, reason := caps.CanAdd("sec-sa", BucketSmall); !ok {
			t.Fatalf("16th SMALL must be admitted under the ceiling: %s", reason)
		}
	}
}

func TestBuildFlexiPlan_Guards(t *testing.T) {
	pos, u, sigs := s4450()
	caps := NewCapCheck(25, pos)
	held := CountOccupied(pos)
	run := func(fx *FlexiInput, c *CapCheck, h int, p map[string]*Position, s []ManthanSignal) string {
		plan, why := BuildFlexiPlan(fx, c, 25, h, p, nil, s)
		if plan != nil {
			t.Fatalf("expected nil plan, got %v", plan.Ceiling)
		}
		return why
	}
	want := func(got, exp string) {
		t.Helper()
		if got != exp {
			t.Errorf("guard = %s, want %s", got, exp)
		}
	}

	want(run(nil, caps, held, pos, sigs), FlexiGuardModeOff)
	want(run(input(ParseFlexiConfig(envMap(nil)), u), caps, held, pos, sigs), FlexiGuardModeOff)
	want(run(input(onCfg(nil), u), caps, held, pos, nil), FlexiGuardNoSignals)
	want(run(input(onCfg(nil), nil), caps, held, pos, sigs), FlexiGuardUniverseNil)

	bad := *u
	bad.Err = errString("timeout")
	want(run(input(onCfg(nil), &bad), caps, held, pos, sigs), FlexiGuardUniverseError)

	few := *u
	few.StocksRows = 4
	want(run(input(onCfg(nil), &few), caps, held, pos, sigs), FlexiGuardUniverseRows)

	// Self-consistency: symbol absent / bucket flipped / non-canonical string.
	want(run(input(onCfg(nil), u), caps, held, pos, []ManthanSignal{sigOf("GHOST", BucketSmall)}), FlexiGuardNotInSnapshot)
	want(run(input(onCfg(nil), u), caps, held, pos, []ManthanSignal{sigOf("sa", BucketMid)}), FlexiGuardBucketMismatch)
	want(run(input(onCfg(nil), u), caps, held, pos, []ManthanSignal{sigOf("sa", "Small ")}), FlexiGuardBucketNonCanon)

	// run_date guards.
	other := sigOf("sa", BucketSmall)
	other.RunDate = "2026-09-29"
	want(run(input(onCfg(nil), u), caps, held, pos, []ManthanSignal{other}), FlexiGuardRunDateMismatch)
	empty := sigOf("sa", BucketSmall)
	empty.RunDate = ""
	want(run(input(onCfg(nil), u), caps, held, pos, []ManthanSignal{empty}), FlexiGuardRunDateEmpty)

	// Cutoff reached.
	late := input(onCfg(nil), u)
	late.NowIST = time.Date(2026, 9, 30, 15, 20, 0, 0, time.FixedZone("IST", 19800))
	want(run(late, caps, held, pos, sigs), FlexiGuardCutoff)

	// INFRA_UNKNOWN on the only void bucket → not a donor → no plan.
	infra := *u
	infra.InfraUnknown = map[string]bool{BucketLarge: true}
	want(run(input(onCfg(nil), &infra), caps, held, pos, sigs), FlexiGuardNoVoidDonor)

	// Donor list excludes the void bucket → no donor.
	want(run(input(onCfg(map[string]string{EnvFlexiDonors: "MID"}), u), caps, held, pos, sigs), FlexiGuardNoVoidDonor)

	// LARGE has an eligible name → nothing is void.
	nonVoid := universe("2026-09-30", names("s", 9), names("m", 3), []string{"la"})
	want(run(input(onCfg(nil), nonVoid), caps, held, pos, sigs), FlexiGuardNoVoidDonor)

	// Book full to MIN_IDLE → no free slots.
	full := book(12, 12, 0)
	want(run(input(onCfg(nil), u), NewCapCheck(25, full), CountOccupied(full), full, sigs), FlexiGuardNoFreeSlots)

	// Pool exhausted ("already borrowed" subtraction): DONORS=LARGE only,
	// SMALL already 12 over base (24 held, N=25, MIN_IDLE 0 → Free 1) →
	// PoolFree = 12 − 12 = 0 → no plan, even though a free slot exists.
	borrowed := book(24, 0, 0)
	uB := universe("2026-09-30", names("s", 5), nil, nil)
	want(run(input(onCfg(map[string]string{EnvFlexiDonors: "LARGE", EnvFlexiMinIdleSlots: "0"}), uB),
		NewCapCheck(25, borrowed), CountOccupied(borrowed), borrowed, sigs), FlexiGuardPoolExhausted)

	// No receiver extra: SMALL has exactly its base room left → Overflow 0.
	fits := book(3, 5, 0) // SMALL 3 held + 9 opp = 12 = base → nothing to borrow
	want(run(input(onCfg(nil), u), NewCapCheck(25, fits), CountOccupied(fits), fits, sigs), FlexiGuardNoReceiverExtra)
}

type errString string

func (e errString) Error() string { return string(e) }

// MAX_RECEIVER_PCT bounds any bucket's effective ceiling: N=25, SMALL 12 held
// with 20 opportunities, MID and LARGE both void, MIN_IDLE 1 → Free 12,
// Spare 12, Pool 24, but ReceiverCap 20 → Extra = 8, ceiling 20. At 100% the
// bound is 25 → Extra 12 → ceiling 24 (1 idle kept).
func TestBuildFlexiPlan_MaxReceiverPct(t *testing.T) {
	pos := book(12, 0, 0)
	u := universe("2026-09-30", names("s", 20), nil, nil)
	sigs := []ManthanSignal{sigOf("sa", BucketSmall)}

	plan, why := BuildFlexiPlan(input(onCfg(nil), u), NewCapCheck(25, pos), 25, CountOccupied(pos), pos, nil, sigs)
	if plan == nil {
		t.Fatalf("no plan: %s", why)
	}
	if plan.Ceiling[BucketSmall] != 20 || plan.Extra[BucketSmall] != 8 {
		t.Fatalf("80%%: ceiling %d extra %d, want 20/8", plan.Ceiling[BucketSmall], plan.Extra[BucketSmall])
	}
	if len(plan.Donors) != 2 {
		t.Fatalf("donors: %v", plan.Donors)
	}

	plan, why = BuildFlexiPlan(input(onCfg(map[string]string{EnvFlexiMaxReceiverPct: "100"}), u), NewCapCheck(25, pos), 25, CountOccupied(pos), pos, nil, sigs)
	if plan == nil {
		t.Fatalf("no plan at 100%%: %s", why)
	}
	if plan.Ceiling[BucketSmall] != 24 {
		t.Fatalf("100%%: ceiling %d, want 24 (MIN_IDLE 1 keeps one seat)", plan.Ceiling[BucketSmall])
	}

	// MIN_IDLE 0 at 100% → the literal "up to the total cap".
	plan, _ = BuildFlexiPlan(input(onCfg(map[string]string{EnvFlexiMaxReceiverPct: "100", EnvFlexiMinIdleSlots: "0"}), u), NewCapCheck(25, pos), 25, CountOccupied(pos), pos, nil, sigs)
	if plan == nil || plan.Ceiling[BucketSmall] != 25 {
		t.Fatalf("100%% / idle 0: %v", plan)
	}

	// Ceiling never exceeds floor(N·pct/100) on a 10-slot book: base 5, cap 8.
	small10 := book(5, 0, 0)
	u10 := universe("2026-09-30", names("s", 9), nil, nil)
	plan, why = BuildFlexiPlan(input(onCfg(nil), u10), NewCapCheck(10, small10), 10, CountOccupied(small10), small10, nil, sigs)
	if plan == nil {
		t.Fatalf("10-slot: %s", why)
	}
	if plan.Ceiling[BucketSmall] != 8 {
		t.Fatalf("10-slot ceiling %d, want 8", plan.Ceiling[BucketSmall])
	}
}

// A MID name first seen BEFORE the strategy was created is not this
// strategy's opportunity (creation gate) → MID is void for it. NULL
// first_seen_at fails open (counts), like the consumer's gate.
func TestBuildFlexiPlan_CreationGate(t *testing.T) {
	pos := book(12, 5, 0)
	u := universe("2026-09-30", names("s", 9), nil, nil)
	u.Eligible = append(u.Eligible, FlexiUniverseRow{Symbol: "OLDMID", Bucket: BucketMid, FirstSeenAt: t0.Add(-72 * time.Hour)})
	fx := input(onCfg(nil), u) // strategy created t0 − 24h → OLDMID predates it
	plan, why := BuildFlexiPlan(fx, NewCapCheck(25, pos), 25, CountOccupied(pos), pos, nil, []ManthanSignal{sigOf("sa", BucketSmall)})
	if plan == nil {
		t.Fatalf("no plan: %s", why)
	}
	if !plan.Void[BucketMid] || plan.Opportunities[BucketMid] != 0 {
		t.Fatalf("pre-creation MID name must not count: void=%v opp=%v", plan.Void, plan.Opportunities)
	}
	if len(plan.Donors) != 2 { // LARGE + MID
		t.Fatalf("donors: %v", plan.Donors)
	}

	// Unstamped row → fails open → MID non-void.
	u.Eligible[len(u.Eligible)-1].FirstSeenAt = time.Time{}
	plan, why = BuildFlexiPlan(fx, NewCapCheck(25, pos), 25, CountOccupied(pos), pos, nil, []ManthanSignal{sigOf("sa", BucketSmall)})
	if plan == nil {
		t.Fatalf("no plan: %s", why)
	}
	if plan.Void[BucketMid] {
		t.Fatal("NULL first_seen_at must count as an opportunity (fail open)")
	}

	// Held names are never opportunities (held_by_S).
	u2 := universe("2026-09-30", names("s", 9), []string{"MA"}, nil) // "MA" is held in book()
	plan, why = BuildFlexiPlan(input(onCfg(nil), u2), NewCapCheck(25, pos), 25, CountOccupied(pos), pos, nil, []ManthanSignal{sigOf("sa", BucketSmall)})
	if plan == nil {
		t.Fatalf("no plan: %s", why)
	}
	if !plan.Void[BucketMid] {
		t.Fatal("a held MID name is not an opportunity → MID void")
	}
}

func TestCapCheck_FlexiStrings(t *testing.T) {
	c := NewCapCheck(25, book(12, 5, 0))
	// nil ceiling map → legacy bytes.
	if ok, reason := c.CanAdd("fresh", BucketSmall); ok || reason != "mcap bucket cap 50% reached for SMALL" {
		t.Fatalf("legacy string expected, got ok=%v %q", ok, reason)
	}
	// Ceiling applied → admitted, then blocked AT the ceiling with the flexi string.
	c.BucketCeiling = map[string]int{BucketSmall: 13}
	c.FlexiDonors = []string{BucketLarge}
	if ok, reason := c.CanAdd("fresh", BucketSmall); !ok {
		t.Fatalf("13th SMALL must pass under ceiling 13: %s", reason)
	}
	c.Add("fresh", BucketSmall)
	ok, reason := c.CanAdd("fresh2", BucketSmall)
	if ok || reason != "mcap bucket cap reached for SMALL (flexi ceiling 13 = base 12 + 1 borrowed from [LARGE])" {
		t.Fatalf("flexi string mismatch: ok=%v %q", ok, reason)
	}
	// A ceiling ≤ base is ignored (never lowers a cap, keeps legacy bytes).
	c2 := NewCapCheck(25, book(12, 5, 0))
	c2.BucketCeiling = map[string]int{BucketSmall: 10, BucketMid: 12}
	if ok, reason := c2.CanAdd("x", BucketSmall); ok || reason != "mcap bucket cap 50% reached for SMALL" {
		t.Fatalf("ceiling below base must be ignored: %v %q", ok, reason)
	}
	if ok, _ := c2.CanAdd("x", BucketMid); !ok {
		t.Fatal("MID 5 < 12 must pass regardless of a ceiling entry equal to base")
	}
	// Sector cap runs FIRST, even inside a flexed bucket.
	c3 := NewCapCheck(25, nil)
	for i := 0; i < 6; i++ {
		c3.Add("Banks", BucketSmall)
	}
	c3.BucketCeiling = map[string]int{BucketSmall: 16}
	if ok, reason := c3.CanAdd("Banks", BucketSmall); ok || reason != "sector cap 25% reached for Banks" {
		t.Fatalf("sector must win: ok=%v %q", ok, reason)
	}
	// Clone is independent.
	cl := c3.Clone()
	cl.Add("Pharma", BucketSmall)
	if c3.BucketCount[BucketSmall] != 6 || cl.BucketCount[BucketSmall] != 7 || cl.BucketCeiling[BucketSmall] != 16 {
		t.Fatal("Clone must copy counters and ceiling without aliasing")
	}
}
