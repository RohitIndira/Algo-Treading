package admin

// Reaper orchestration tests. The EVIDENCE GATE itself is not re-tested
// here — the reaper deliberately has none of its own; it calls the same
// GhostPreview/GhostHeal the manual flow uses (live-validated 2026-09-03
// against 14 production cases: 7 WOULD-CLOSE ghosts, 7 correctly-refused
// real positions including freeQty=0 / standing-SL / settlement-window
// traps). These tests prove the reaper NEVER acts outside what the gate
// returns, in every mode and failure shape.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// ── fakes ───────────────────────────────────────────────────────────────

type fakeGate struct {
	previews map[string]*GhostPlan // "user/symbol" → plan (nil entry = refuse)
	refusals map[string]error
	healErr  error

	previewCalls []string
	healCalls    []string
	healPlans    []*GhostPlan
}

func (f *fakeGate) GhostPreview(_ context.Context, u, s string) (*GhostPlan, error) {
	key := u + "/" + s
	f.previewCalls = append(f.previewCalls, key)
	if err, ok := f.refusals[key]; ok {
		return nil, err
	}
	if p, ok := f.previews[key]; ok {
		return p, nil
	}
	return nil, errors.New("no ACTIVE book rows — nothing to heal")
}

func (f *fakeGate) GhostHeal(_ context.Context, p *GhostPlan) (map[string]any, error) {
	f.healCalls = append(f.healCalls, p.UserID+"/"+p.Symbol)
	f.healPlans = append(f.healPlans, p)
	if f.healErr != nil {
		return nil, f.healErr
	}
	return map[string]any{"book_rows_closed": int64(p.BookRows)}, nil
}

type fakeScanner struct {
	results map[string]*ReconResult
	err     error
}

func (f *fakeScanner) Reconcile(_ context.Context, u string) (*ReconResult, error) {
	if f.err != nil {
		return nil, f.err
	}
	if r, ok := f.results[u]; ok {
		return r, nil
	}
	return &ReconResult{UserID: u, BrokerLeg: "OK", Mismatches: []Mismatch{}}, nil
}

type fakeUsers struct{ users []string }

func (f *fakeUsers) ActiveUserIDs(context.Context) ([]string, error) { return f.users, nil }

type fakeAudit struct{ rows []AuditEntry }

func (f *fakeAudit) Audit(_ context.Context, e AuditEntry) error {
	f.rows = append(f.rows, e)
	return nil
}

func (f *fakeAudit) results() []string {
	out := make([]string, 0, len(f.rows))
	for _, r := range f.rows {
		out = append(out, r.Result)
	}
	return out
}

func newTestReaper(mode string, g *fakeGate, s *fakeScanner, u *fakeUsers, a *fakeAudit) *GhostReaper {
	ist := time.FixedZone("IST", 5*3600+30*60)
	r := NewGhostReaper(mode, g, s, u, a, ist)
	r.now = func() time.Time { return time.Date(2026, 9, 3, 12, 0, 0, 0, ist) }
	r.inWindow = func(time.Time) bool { return true } // Thursday noon — inside the action window anyway
	return r
}

func ghostMismatch(sym string) Mismatch { return Mismatch{Class: "GHOST", Symbol: sym} }

// ── tests ───────────────────────────────────────────────────────────────

// Test: gate refusal (any reason — broker holds, settlement window, open
// SELL, fetch failure) → heal is NEVER called; refusal is audited.
func TestReaper_RefusedCandidateNeverHealed(t *testing.T) {
	gate := &fakeGate{refusals: map[string]error{
		"U1/KEI": errors.New("broker HOLDS 3 of KEI — this is not a ghost"),
	}}
	scan := &fakeScanner{results: map[string]*ReconResult{
		"U1": {BrokerLeg: "OK", Mismatches: []Mismatch{ghostMismatch("KEI")}},
	}}
	audit := &fakeAudit{}
	rep := newTestReaper(ReaperModeEnabled, gate, scan, &fakeUsers{[]string{"U1"}}, audit).
		SweepOnce(context.Background())

	if len(gate.healCalls) != 0 {
		t.Fatalf("heal called on refused candidate: %v", gate.healCalls)
	}
	if rep.Refused != 1 || rep.Healed != 0 {
		t.Fatalf("report wrong: %+v", rep)
	}
	if len(audit.rows) != 1 || audit.rows[0].Result != "REFUSED" {
		t.Fatalf("expected one REFUSED audit row, got %v", audit.results())
	}
}

// Test: dry_run mode — plan passes the gate but heal is NOT called;
// WOULD_CLOSE is audited with the evidence.
func TestReaper_DryRunNeverHeals(t *testing.T) {
	gate := &fakeGate{previews: map[string]*GhostPlan{
		"U1/SHANTIGOLD": {UserID: "U1", Symbol: "SHANTIGOLD", BookRows: 1, BookQty: 149,
			ConfirmationText: "HEAL GHOST SHANTIGOLD FOR U1 — CLOSE 1 BOOK ROWS ×149"},
	}}
	scan := &fakeScanner{results: map[string]*ReconResult{
		"U1": {BrokerLeg: "OK", Mismatches: []Mismatch{ghostMismatch("SHANTIGOLD")}},
	}}
	audit := &fakeAudit{}
	rep := newTestReaper(ReaperModeDryRun, gate, scan, &fakeUsers{[]string{"U1"}}, audit).
		SweepOnce(context.Background())

	if len(gate.healCalls) != 0 {
		t.Fatalf("dry_run healed: %v", gate.healCalls)
	}
	if rep.WouldClose != 1 || rep.Healed != 0 {
		t.Fatalf("report wrong: %+v", rep)
	}
	if audit.rows[0].Result != "WOULD_CLOSE" {
		t.Fatalf("expected WOULD_CLOSE audit, got %v", audit.results())
	}
}

// Test: enabled mode — a passing plan is healed exactly once and audited.
func TestReaper_EnabledHealsPassingPlan(t *testing.T) {
	gate := &fakeGate{previews: map[string]*GhostPlan{
		"U1/SHANTIGOLD": {UserID: "U1", Symbol: "SHANTIGOLD", BookRows: 1, BookQty: 149},
	}}
	scan := &fakeScanner{results: map[string]*ReconResult{
		"U1": {BrokerLeg: "OK", Mismatches: []Mismatch{ghostMismatch("SHANTIGOLD")}},
	}}
	audit := &fakeAudit{}
	rep := newTestReaper(ReaperModeEnabled, gate, scan, &fakeUsers{[]string{"U1"}}, audit).
		SweepOnce(context.Background())

	if len(gate.healCalls) != 1 || gate.healCalls[0] != "U1/SHANTIGOLD" {
		t.Fatalf("expected exactly one heal, got %v", gate.healCalls)
	}
	if rep.Healed != 1 {
		t.Fatalf("report wrong: %+v", rep)
	}
	if audit.rows[0].Result != "HEALED" {
		t.Fatalf("expected HEALED audit, got %v", audit.results())
	}
}

// Test: idempotency — after a heal, the next sweep's reconcile no longer
// lists the symbol (book rows EXITED) and, defensively, the gate would
// refuse. Second sweep must change nothing.
func TestReaper_SecondSweepIsNoOp(t *testing.T) {
	gate := &fakeGate{previews: map[string]*GhostPlan{
		"U1/SHANTIGOLD": {UserID: "U1", Symbol: "SHANTIGOLD", BookRows: 1, BookQty: 149},
	}}
	scan := &fakeScanner{results: map[string]*ReconResult{
		"U1": {BrokerLeg: "OK", Mismatches: []Mismatch{ghostMismatch("SHANTIGOLD")}},
	}}
	audit := &fakeAudit{}
	r := newTestReaper(ReaperModeEnabled, gate, scan, &fakeUsers{[]string{"U1"}}, audit)

	rep1 := r.SweepOnce(context.Background())
	if rep1.Healed != 1 {
		t.Fatalf("first sweep: %+v", rep1)
	}

	// After the heal: reconcile shows no GHOST any more…
	scan.results["U1"] = &ReconResult{BrokerLeg: "OK", Mismatches: []Mismatch{}}
	// …and even if it somehow did, the gate now refuses.
	delete(gate.previews, "U1/SHANTIGOLD")

	rep2 := r.SweepOnce(context.Background())
	if rep2.Healed != 0 || rep2.Candidates != 0 {
		t.Fatalf("second sweep not a no-op: %+v", rep2)
	}
	if len(gate.healCalls) != 1 {
		t.Fatalf("heal called again: %v", gate.healCalls)
	}
}

// Test: uncertain data — broker leg not OK means NO previews, NO heals for
// that user; the skip is audited.
func TestReaper_BrokerLegDownSkipsUser(t *testing.T) {
	gate := &fakeGate{}
	scan := &fakeScanner{results: map[string]*ReconResult{
		"U1": {BrokerLeg: "AUTH_EXPIRED", Mismatches: []Mismatch{ghostMismatch("SHANTIGOLD")}},
	}}
	audit := &fakeAudit{}
	rep := newTestReaper(ReaperModeEnabled, gate, scan, &fakeUsers{[]string{"U1"}}, audit).
		SweepOnce(context.Background())

	if len(gate.previewCalls) != 0 || len(gate.healCalls) != 0 {
		t.Fatalf("acted without broker truth: previews=%v heals=%v", gate.previewCalls, gate.healCalls)
	}
	if rep.SkippedUser != 1 {
		t.Fatalf("report wrong: %+v", rep)
	}
	if audit.rows[0].Result != "SKIPPED" || !strings.Contains(audit.rows[0].Detail, "AUTH_EXPIRED") {
		t.Fatalf("expected SKIPPED audit with leg status, got %+v", audit.rows[0])
	}
}

// Test: a heal error is audited HEAL_FAILED, the sweep continues, and the
// candidate is NOT counted healed.
func TestReaper_HealErrorAuditedAndContinues(t *testing.T) {
	gate := &fakeGate{
		previews: map[string]*GhostPlan{
			"U1/A": {UserID: "U1", Symbol: "A", BookRows: 1},
			"U1/B": {UserID: "U1", Symbol: "B", BookRows: 1},
		},
		healErr: errors.New("db write failed"),
	}
	scan := &fakeScanner{results: map[string]*ReconResult{
		"U1": {BrokerLeg: "OK", Mismatches: []Mismatch{ghostMismatch("A"), ghostMismatch("B")}},
	}}
	audit := &fakeAudit{}
	rep := newTestReaper(ReaperModeEnabled, gate, scan, &fakeUsers{[]string{"U1"}}, audit).
		SweepOnce(context.Background())

	if rep.Healed != 0 || rep.Errors != 2 {
		t.Fatalf("report wrong: %+v", rep)
	}
	if len(gate.healCalls) != 2 {
		t.Fatalf("sweep did not continue past first heal error: %v", gate.healCalls)
	}
	for _, r := range audit.rows {
		if r.Result != "HEAL_FAILED" {
			t.Fatalf("expected HEAL_FAILED rows, got %v", audit.results())
		}
	}
}

// Test: per-sweep heal cap — confirmed ghosts beyond maxHealsPerSweep are
// deferred (audited), never silently dropped, and heal on later sweeps.
func TestReaper_CapDefersExcess(t *testing.T) {
	gate := &fakeGate{previews: map[string]*GhostPlan{}}
	var mismatches []Mismatch
	const over = 2 // two more candidates than the per-sweep cap
	for i := 0; i < maxHealsPerSweep+over; i++ {
		s := fmt.Sprintf("SYM%02d", i)
		gate.previews["U1/"+s] = &GhostPlan{UserID: "U1", Symbol: s, BookRows: 1}
		mismatches = append(mismatches, ghostMismatch(s))
	}
	scan := &fakeScanner{results: map[string]*ReconResult{
		"U1": {BrokerLeg: "OK", Mismatches: mismatches},
	}}
	audit := &fakeAudit{}
	rep := newTestReaper(ReaperModeEnabled, gate, scan, &fakeUsers{[]string{"U1"}}, audit).
		SweepOnce(context.Background())

	if rep.Healed != maxHealsPerSweep {
		t.Fatalf("expected %d heals, got %+v", maxHealsPerSweep, rep)
	}
	if rep.DeferredCap != over {
		t.Fatalf("expected %d deferred, got %+v", over, rep)
	}
	deferred := 0
	for _, r := range audit.rows {
		if r.Result == "DEFERRED_CAP" {
			deferred++
		}
	}
	if deferred != over {
		t.Fatalf("deferred candidates not audited: %v", audit.results())
	}
}

// Test: non-GHOST mismatch classes (QTY_MISMATCH, DUPLICATE_BOOK_ROWS…)
// are never previewed or healed.
func TestReaper_IgnoresNonGhostClasses(t *testing.T) {
	gate := &fakeGate{}
	scan := &fakeScanner{results: map[string]*ReconResult{
		"U1": {BrokerLeg: "OK", Mismatches: []Mismatch{
			{Class: "QTY_MISMATCH", Symbol: "IDEA"},
			{Class: "DUPLICATE_BOOK_ROWS", Symbol: "BALUFORGE"},
			{Class: "UNLEDGERED_EXIT", Symbol: "IOLCP"},
		}},
	}}
	audit := &fakeAudit{}
	rep := newTestReaper(ReaperModeEnabled, gate, scan, &fakeUsers{[]string{"U1"}}, audit).
		SweepOnce(context.Background())

	if len(gate.previewCalls) != 0 || rep.Candidates != 0 {
		t.Fatalf("non-GHOST classes were processed: %v", gate.previewCalls)
	}
}

// Test: next-sweep scheduling — intraday 15-minute slots (09:20 + k×15) until
// 15:45, then the 15:50 close sweep, then the next trading day.
func TestReaper_NextSweepTime(t *testing.T) {
	ist := time.FixedZone("IST", 5*3600+30*60)
	r := NewGhostReaper(ReaperModeDryRun, &fakeGate{}, &fakeScanner{}, &fakeUsers{}, &fakeAudit{}, ist)

	r.now = func() time.Time { return time.Date(2026, 9, 3, 12, 0, 0, 0, ist) } // Thursday
	if got := r.nextSweep(); got.Hour() != 12 || got.Minute() != 5 || got.Day() != 3 {
		t.Fatalf("expected today 12:05 IST (next 15-min slot), got %v", got)
	}
	r.now = func() time.Time { return time.Date(2026, 9, 3, 16, 0, 0, 0, ist) }
	if got := r.nextSweep(); got.Day() != 4 {
		t.Fatalf("expected tomorrow 15:50 IST, got %v", got)
	}
}

type fakeLedger struct{ parks []LedgerGhost }

func (f *fakeLedger) GhostSuspectCandidates(context.Context) ([]LedgerGhost, error) {
	return f.parks, nil
}

// A trade-execution GHOST_SUSPECT park (broker RMS "Set= 0") is a candidate
// even when the reconciler lists no GHOST for it (ledger-only ghost); it still
// goes through the gate, and a user without broker truth is skipped entirely.
func TestReaper_LedgerSuspectsAreCandidates(t *testing.T) {
	gate := &fakeGate{previews: map[string]*GhostPlan{"U1/IOLCP": {UserID: "U1", Symbol: "IOLCP", LedgerOnly: true, LedgerNetQty: 121}}}
	scan := &fakeScanner{results: map[string]*ReconResult{
		"U1": {UserID: "U1", BrokerLeg: "OK"},
		"U2": {UserID: "U2", BrokerLeg: "AUTH_EXPIRED"},
	}}
	users := &fakeUsers{users: []string{"U1", "U2"}}
	audit := &fakeAudit{}
	r := newTestReaper(ReaperModeEnabled, gate, scan, users, audit)
	r.SetLedgerSource(&fakeLedger{parks: []LedgerGhost{{"U1", "IOLCP"}, {"U2", "DIFFNKG"}}})
	rep := r.SweepOnce(context.Background())
	if rep.Candidates != 1 || rep.Healed != 1 || rep.SkippedUser != 1 {
		t.Fatalf("report: %s", rep.String())
	}
	if len(gate.healCalls) != 1 || gate.healCalls[0] != "U1/IOLCP" {
		t.Fatalf("healed: %v", gate.healCalls)
	}
}

// Sweep cadence: 15-minute slots 09:20–15:45 IST, the 15:50 close sweep, then
// 09:20 on the next trading day; weekends are skipped.
func TestReaper_NextSweepCadence(t *testing.T) {
	ist := time.FixedZone("IST", 5*3600+30*60)
	r := newTestReaper(ReaperModeEnabled, &fakeGate{}, &fakeScanner{}, &fakeUsers{}, &fakeAudit{})
	at := func(h, m int) { r.now = func() time.Time { return time.Date(2026, 10, 26, h, m, 0, 0, ist) } } // Monday
	at(11, 21)
	if n := r.nextSweep().In(ist); n.Hour() != 11 || n.Minute() != 35 {
		t.Fatalf("11:21 → %s, want 11:35", n.Format("15:04"))
	}
	at(15, 46)
	if n := r.nextSweep().In(ist); n.Hour() != 15 || n.Minute() != 50 {
		t.Fatalf("15:46 → %s, want 15:50", n.Format("15:04"))
	}
	at(16, 0)
	if n := r.nextSweep().In(ist); n.Day() != 27 || n.Hour() != 9 || n.Minute() != 20 {
		t.Fatalf("16:00 Mon → %s, want Tue 09:20", n.Format("Mon 15:04"))
	}
	r.now = func() time.Time { return time.Date(2026, 10, 24, 12, 0, 0, 0, ist) } // Saturday
	if n := r.nextSweep().In(ist); n.Weekday() != time.Monday || n.Hour() != 9 || n.Minute() != 20 {
		t.Fatalf("Sat noon → %s, want Mon 09:20", n.Format("Mon 15:04"))
	}
}

// ── book-reconciler (verdict-driven) paths ──────────────────────────────

type fakeActor struct {
	persisted []Verdict
	pledged   []string // "user/symbol"
	unpledged []string
	pledgeErr error
	// parkedNow mirrors trading_db truth: "user/symbol" currently PLEDGED.
	// Pledge/Unpledge report changed=false when the row is already there /
	// already gone, exactly like the real Actions.
	parkedNow map[string]bool
}

func (f *fakeActor) isParked(k string) bool { return f.parkedNow != nil && f.parkedNow[k] }

func (f *fakeActor) PersistVerdicts(_ context.Context, vs []Verdict, _ string) (int64, error) {
	f.persisted = append(f.persisted, vs...)
	return int64(len(vs)), nil
}
func (f *fakeActor) Pledge(_ context.Context, v Verdict, _ *liveBook) (map[string]any, error) {
	if f.pledgeErr != nil {
		return nil, f.pledgeErr
	}
	k := v.UserID + "/" + v.Symbol
	changed := !f.isParked(k)
	if f.parkedNow == nil {
		f.parkedNow = map[string]bool{}
	}
	f.parkedNow[k] = true
	f.pledged = append(f.pledged, k)
	return map[string]any{"changed": changed}, nil
}
func (f *fakeActor) Unpledge(_ context.Context, v Verdict) (map[string]any, error) {
	k := v.UserID + "/" + v.Symbol
	changed := f.isParked(k)
	delete(f.parkedNow, k)
	f.unpledged = append(f.unpledged, k)
	return map[string]any{"changed": changed}, nil
}

// verdict builds a lot judged `state` whose previous persisted verdict was
// `prev`, read 20 minutes ago — so prev == state is a CONFIRMED verdict and
// prev != state is a first sighting.
func verdict(u, sym, state, prev string) Verdict {
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.FixedZone("IST", 5*3600+30*60))
	v := Verdict{UserID: u, StrategyID: "S", Symbol: sym, State: state, PrevState: prev, BookQty: 10,
		Reason: state + " reason", PositionIDs: []string{"p-" + sym}, CheckedAt: now}
	if prev != "" {
		v.PrevCheckedAt = now.Add(-20 * time.Minute)
	}
	return v
}

// confirmedVerdict is a lot whose previous read already said the same.
func confirmedVerdict(u, sym, state string) Verdict { return verdict(u, sym, state, state) }

// PLEDGED: the first sighting only waits (audited VERDICT_PLEDGED); the
// confirming read ≥15 min later parks the lot (audited PLEDGED); later passes
// re-assert the park silently (changed=false → no audit).
func TestReconciler_PledgeNeedsConfirmingRead(t *testing.T) {
	gate, audit, actor := &fakeGate{}, &fakeAudit{}, &fakeActor{}
	scan := &fakeScanner{results: map[string]*ReconResult{
		"FIV99": {UserID: "FIV99", BrokerLeg: "OK", Verdicts: []Verdict{verdict("FIV99", "SYRMA", BrokerStatePledged, BrokerStateHeld)}},
	}}
	r := newTestReaper(ReaperModeEnabled, gate, scan, &fakeUsers{[]string{"FIV99"}}, audit)
	r.SetBookActor(actor)

	rep := r.SweepOnce(context.Background())
	if rep.Pledged != 0 || rep.AwaitConfirm != 1 || len(actor.pledged) != 0 {
		t.Fatalf("first sighting must wait: %s %v", rep.String(), actor.pledged)
	}
	if len(actor.persisted) != 1 || actor.persisted[0].State != BrokerStatePledged {
		t.Fatalf("verdict not persisted: %+v", actor.persisted)
	}
	if got := audit.results(); len(got) != 1 || got[0] != "VERDICT_PLEDGED" {
		t.Fatalf("audit: %v", got)
	}

	// Confirming read: same state, previous read 20 min ago.
	scan.results["FIV99"].Verdicts[0] = confirmedVerdict("FIV99", "SYRMA", BrokerStatePledged)
	rep = r.SweepOnce(context.Background())
	if rep.Pledged != 1 || len(actor.pledged) != 1 || actor.pledged[0] != "FIV99/SYRMA" {
		t.Fatalf("confirmed pledge not applied: %s %v", rep.String(), actor.pledged)
	}
	if len(gate.previewCalls) != 0 {
		t.Fatalf("a pledged lot must never reach the ghost gate: %v", gate.previewCalls)
	}
	if got := audit.results(); got[len(got)-1] != "PLEDGED" {
		t.Fatalf("audit: %v", got)
	}

	// Next pass: re-asserted (actor called again), nothing changed → silent.
	before := len(audit.rows)
	rep = r.SweepOnce(context.Background())
	if rep.Pledged != 0 || len(actor.pledged) != 2 || len(audit.rows) != before {
		t.Fatalf("re-assert must be silent: %s pledged=%v audits=%d→%d", rep.String(), actor.pledged, before, len(audit.rows))
	}
}

// A half-applied park (actor errored) is retried on the next pass.
func TestReconciler_PledgeRetriedAfterError(t *testing.T) {
	gate, audit := &fakeGate{}, &fakeAudit{}
	actor := &fakeActor{pledgeErr: errors.New("trading_db down")}
	scan := &fakeScanner{results: map[string]*ReconResult{
		"FIV99": {UserID: "FIV99", BrokerLeg: "OK", Verdicts: []Verdict{confirmedVerdict("FIV99", "SYRMA", BrokerStatePledged)}},
	}}
	r := newTestReaper(ReaperModeEnabled, gate, scan, &fakeUsers{[]string{"FIV99"}}, audit)
	r.SetBookActor(actor)
	rep := r.SweepOnce(context.Background())
	if rep.Errors != 1 || rep.Pledged != 0 || audit.results()[0] != "PLEDGE_FAILED" {
		t.Fatalf("failed pledge: %s %v", rep.String(), audit.results())
	}
	actor.pledgeErr = nil
	rep = r.SweepOnce(context.Background())
	if rep.Pledged != 1 || !actor.isParked("FIV99/SYRMA") {
		t.Fatalf("pledge not retried: %s", rep.String())
	}
}

// HELD after PLEDGED → Unpledge once; HELD after HELD → nothing.
func TestReconciler_UnpledgeOnReturnToHeld(t *testing.T) {
	gate, audit, actor := &fakeGate{}, &fakeAudit{}, &fakeActor{}
	scan := &fakeScanner{results: map[string]*ReconResult{
		"FIV99": {UserID: "FIV99", BrokerLeg: "OK", Verdicts: []Verdict{
			verdict("FIV99", "SYRMA", BrokerStateHeld, BrokerStatePledged),
			verdict("FIV99", "KEI", BrokerStateHeld, BrokerStateHeld),
		}},
	}}
	actor.parkedNow = map[string]bool{"FIV99/SYRMA": true}
	r := newTestReaper(ReaperModeEnabled, gate, scan, &fakeUsers{[]string{"FIV99"}}, audit)
	r.SetBookActor(actor)
	rep := r.SweepOnce(context.Background())
	if rep.Unpledged != 1 || actor.unpledged[0] != "FIV99/SYRMA" {
		t.Fatalf("unpledge wrong: %s %v", rep.String(), actor.unpledged)
	}
	// KEI (never parked) was asked too — re-assert — but is silent.
	if got := audit.results(); len(got) != 1 || got[0] != "UNPLEDGED" {
		t.Fatalf("audit: %v", got)
	}
}

// Outside the 09:20–15:25 IST action window (the 15:50 sweep, an overnight
// login) verdicts are persisted and audited, but nothing is closed, parked
// or unparked.
func TestReconciler_OutOfWindowPersistsOnly(t *testing.T) {
	gate := &fakeGate{previews: map[string]*GhostPlan{"FIV99/AERO": {UserID: "FIV99", Symbol: "AERO", BookRows: 1}}}
	audit, actor := &fakeAudit{}, &fakeActor{parkedNow: map[string]bool{"FIV99/WOCK": true}}
	scan := &fakeScanner{results: map[string]*ReconResult{
		"FIV99": {UserID: "FIV99", BrokerLeg: "OK", Verdicts: []Verdict{
			confirmedVerdict("FIV99", "AERO", BrokerStateNotHeld),
			confirmedVerdict("FIV99", "SYRMA", BrokerStatePledged),
			verdict("FIV99", "WOCK", BrokerStateHeld, BrokerStatePledged),
			verdict("FIV99", "SHREE", BrokerStateSoldToday, ""),
		}, Mismatches: []Mismatch{ghostMismatch("GHOSTY")}},
	}}
	r := newTestReaper(ReaperModeEnabled, gate, scan, &fakeUsers{[]string{"FIV99"}}, audit)
	r.SetBookActor(actor)
	r.inWindow = func(time.Time) bool { return false }
	rep := r.SweepOnce(context.Background())
	if len(gate.healCalls)+len(gate.previewCalls)+len(actor.pledged)+len(actor.unpledged) != 0 {
		t.Fatalf("acted outside the window: heals=%v previews=%v pledged=%v unpledged=%v", gate.healCalls, gate.previewCalls, actor.pledged, actor.unpledged)
	}
	if rep.Persisted != 4 || rep.OutOfWindow != 4 {
		t.Fatalf("report: %s", rep.String())
	}
	// Default window: a trading-day 12:00 is in, 15:50 and Saturday are out.
	ist := time.FixedZone("IST", 5*3600+30*60)
	r2 := NewGhostReaper(ReaperModeEnabled, gate, scan, &fakeUsers{}, audit, ist)
	if !r2.inWindow(time.Date(2026, 10, 26, 12, 0, 0, 0, ist)) || r2.inWindow(time.Date(2026, 10, 26, 15, 50, 0, 0, ist)) ||
		r2.inWindow(time.Date(2026, 10, 26, 9, 19, 0, 0, ist)) || r2.inWindow(time.Date(2026, 10, 24, 12, 0, 0, 0, ist)) {
		t.Fatal("default action window wrong")
	}
}

// NOT_HELD needs the confirming read too; the in-memory fallback covers
// ledger-only lots (no persisted history).
func TestReconciler_NotHeldHysteresis(t *testing.T) {
	gate := &fakeGate{previews: map[string]*GhostPlan{
		"FIV99/AERO":  {UserID: "FIV99", Symbol: "AERO", BookRows: 1},
		"FIV99/IOLCP": {UserID: "FIV99", Symbol: "IOLCP", LedgerOnly: true, LedgerNetQty: 121},
	}}
	audit, actor := &fakeAudit{}, &fakeActor{}
	ledgerOnly := Verdict{UserID: "FIV99", StrategyID: "S", Symbol: "IOLCP", State: BrokerStateNotHeld, LedgerOnly: true, BookQty: 121,
		CheckedAt: time.Date(2026, 9, 3, 12, 0, 0, 0, time.FixedZone("IST", 5*3600+30*60))}
	scan := &fakeScanner{results: map[string]*ReconResult{
		"FIV99": {UserID: "FIV99", BrokerLeg: "OK", Verdicts: []Verdict{verdict("FIV99", "AERO", BrokerStateNotHeld, ""), ledgerOnly}},
	}}
	r := newTestReaper(ReaperModeEnabled, gate, scan, &fakeUsers{[]string{"FIV99"}}, audit)
	r.SetBookActor(actor)
	rep := r.SweepOnce(context.Background())
	if rep.Healed != 0 || rep.AwaitConfirm != 2 || len(gate.previewCalls) != 0 {
		t.Fatalf("first sighting must not close: %s", rep.String())
	}
	// 10 minutes later: still too soon for the ledger-only lot; the book lot's
	// persisted history now says NOT_HELD 10 min ago → also too soon.
	later := ledgerOnly.CheckedAt.Add(10 * time.Minute)
	ledgerOnly.CheckedAt = later
	aero := verdict("FIV99", "AERO", BrokerStateNotHeld, BrokerStateNotHeld)
	aero.CheckedAt, aero.PrevCheckedAt = later, later.Add(-10*time.Minute)
	scan.results["FIV99"].Verdicts = []Verdict{aero, ledgerOnly}
	rep = r.SweepOnce(context.Background())
	if rep.Healed != 0 || rep.AwaitConfirm != 2 {
		t.Fatalf("10 min is not a confirming read: %s", rep.String())
	}
	// 16 minutes after the first sighting: both close.
	later = ledgerOnly.CheckedAt.Add(6 * time.Minute)
	ledgerOnly.CheckedAt = later
	aero.CheckedAt, aero.PrevCheckedAt = later, later.Add(-16*time.Minute)
	scan.results["FIV99"].Verdicts = []Verdict{aero, ledgerOnly}
	rep = r.SweepOnce(context.Background())
	if rep.Healed != 2 || len(gate.healCalls) != 2 {
		t.Fatalf("confirmed NOT_HELD must close: %s %v", rep.String(), gate.healCalls)
	}
}

// Caps: a fleet sweep heals at most 20, a single-user run at most 5.
func TestReconciler_PerRunCaps(t *testing.T) {
	gate := &fakeGate{previews: map[string]*GhostPlan{}}
	var vs []Verdict
	for i := 0; i < 8; i++ {
		sym := fmt.Sprintf("SYM%02d", i)
		gate.previews["FIV99/"+sym] = &GhostPlan{UserID: "FIV99", Symbol: sym, BookRows: 1}
		vs = append(vs, confirmedVerdict("FIV99", sym, BrokerStateNotHeld))
	}
	scan := &fakeScanner{results: map[string]*ReconResult{"FIV99": {UserID: "FIV99", BrokerLeg: "OK", Verdicts: vs}}}
	audit, actor := &fakeAudit{}, &fakeActor{}
	r := newTestReaper(ReaperModeEnabled, gate, scan, &fakeUsers{[]string{"FIV99"}}, audit)
	r.SetBookActor(actor)
	rep := r.SweepUser(context.Background(), "FIV99", VerdictSourceLogin)
	if rep.Healed != maxHealsPerUserRun || rep.DeferredCap != 8-maxHealsPerUserRun {
		t.Fatalf("single-user cap: %s", rep.String())
	}
	gate.healCalls = nil
	rep = r.SweepOnce(context.Background())
	if rep.Healed != 8 || rep.DeferredCap != 0 {
		t.Fatalf("fleet cap must allow 8: %s", rep.String())
	}
}

// The legacy GHOST class and the ledger parks never nominate a symbol that
// already carries a verdict of another state (e.g. QTY_MISMATCH, UNVERIFIED).
func TestReconciler_SeenFromEveryVerdict(t *testing.T) {
	gate := &fakeGate{previews: map[string]*GhostPlan{
		"FIV99/IDEA":  {UserID: "FIV99", Symbol: "IDEA", BookRows: 1},
		"FIV99/FRESH": {UserID: "FIV99", Symbol: "FRESH", BookRows: 1},
		"FIV99/ORPH":  {UserID: "FIV99", Symbol: "ORPH", LedgerOnly: true},
	}}
	audit, actor := &fakeAudit{}, &fakeActor{}
	scan := &fakeScanner{results: map[string]*ReconResult{
		"FIV99": {UserID: "FIV99", BrokerLeg: "OK",
			Verdicts:   []Verdict{verdict("FIV99", "IDEA", BrokerStateQtyMismatch, ""), verdict("FIV99", "FRESH", BrokerStateUnverified, "")},
			Mismatches: []Mismatch{ghostMismatch("IDEA"), ghostMismatch("FRESH"), ghostMismatch("ORPH")}},
	}}
	r := newTestReaper(ReaperModeEnabled, gate, scan, &fakeUsers{[]string{"FIV99"}}, audit)
	r.SetBookActor(actor)
	r.SetLedgerSource(&fakeLedger{parks: []LedgerGhost{{"FIV99", "FRESH"}, {"FIV99", "ORPH"}}})
	rep := r.SweepOnce(context.Background())
	if len(gate.previewCalls) != 1 || gate.previewCalls[0] != "FIV99/ORPH" || rep.Candidates != 1 {
		t.Fatalf("only the unjudged symbol may be nominated: %v (%s)", gate.previewCalls, rep.String())
	}
}

// Session-gap sequence: PLEDGED → (dead session) UNVERIFIED persisted over it
// → HELD. The reverse path must still run, and a lot that was never parked
// (HELD after UNVERIFIED) must not be audited as unpledged.
func TestReconciler_UnpledgeSurvivesUnverifiedGap(t *testing.T) {
	gate, audit, actor := &fakeGate{}, &fakeAudit{}, &fakeActor{}
	scan := &fakeScanner{results: map[string]*ReconResult{
		"FIV99": {UserID: "FIV99", BrokerLeg: "OK", Verdicts: []Verdict{confirmedVerdict("FIV99", "SYRMA", BrokerStatePledged)}},
	}}
	r := newTestReaper(ReaperModeEnabled, gate, scan, &fakeUsers{[]string{"FIV99"}}, audit)
	r.SetBookActor(actor)
	r.SweepOnce(context.Background()) // pledged

	// Dead session: UNVERIFIED persisted, nothing else.
	scan.results["FIV99"] = &ReconResult{UserID: "FIV99", BrokerLeg: "AUTH_EXPIRED",
		Verdicts: []Verdict{verdict("FIV99", "SYRMA", BrokerStateUnverified, BrokerStatePledged)}}
	r.SweepOnce(context.Background())

	// Session back, shares free: prev_state reads UNVERIFIED, yet the lot is
	// still parked in trading_db → Unpledge runs and is audited once.
	scan.results["FIV99"] = &ReconResult{UserID: "FIV99", BrokerLeg: "OK", Verdicts: []Verdict{
		verdict("FIV99", "SYRMA", BrokerStateHeld, BrokerStateUnverified),
		verdict("FIV99", "KEI", BrokerStateHeld, BrokerStateUnverified), // never parked
	}}
	rep := r.SweepOnce(context.Background())
	if rep.Unpledged != 1 || actor.isParked("FIV99/SYRMA") {
		t.Fatalf("unpledge after gap: %s parked=%v", rep.String(), actor.parkedNow)
	}
	unpledgedAudits := 0
	for _, res := range audit.results() {
		if res == "UNPLEDGED" {
			unpledgedAudits++
		}
	}
	if unpledgedAudits != 1 {
		t.Fatalf("expected exactly one UNPLEDGED audit (SYRMA), got %v", audit.results())
	}
	// A PLEDGED verdict after the gap on an already-parked lot: no second
	// PLEDGED audit.
	scan.results["FIV99"] = &ReconResult{UserID: "FIV99", BrokerLeg: "OK", Verdicts: []Verdict{
		confirmedVerdict("FIV99", "WOCK", BrokerStatePledged),
	}}
	r.SweepOnce(context.Background())
	scan.results["FIV99"] = &ReconResult{UserID: "FIV99", BrokerLeg: "OK", Verdicts: []Verdict{
		confirmedVerdict("FIV99", "WOCK", BrokerStatePledged),
	}}
	before := len(audit.rows)
	r.SweepOnce(context.Background())
	if len(audit.rows) != before {
		t.Fatalf("re-pledge after a gap must be silent: %v", audit.results()[before:])
	}
}

// dry_run: verdicts ARE persisted (observations), actions are audited as
// WOULD_* and the actor is never asked to change anything.
func TestReconciler_DryRunPersistsButNeverActs(t *testing.T) {
	gate := &fakeGate{previews: map[string]*GhostPlan{"FIV99/AERO": {UserID: "FIV99", Symbol: "AERO", BookRows: 1}}}
	audit, actor := &fakeAudit{}, &fakeActor{}
	scan := &fakeScanner{results: map[string]*ReconResult{
		"FIV99": {UserID: "FIV99", BrokerLeg: "OK", Verdicts: []Verdict{
			confirmedVerdict("FIV99", "SYRMA", BrokerStatePledged),
			verdict("FIV99", "WOCK", BrokerStateHeld, BrokerStatePledged),
			confirmedVerdict("FIV99", "AERO", BrokerStateNotHeld),
		}},
	}}
	r := newTestReaper(ReaperModeDryRun, gate, scan, &fakeUsers{[]string{"FIV99"}}, audit)
	r.SetBookActor(actor)
	rep := r.SweepOnce(context.Background())
	if len(actor.pledged) != 0 || len(actor.unpledged) != 0 || len(gate.healCalls) != 0 {
		t.Fatalf("dry_run acted: pledged=%v unpledged=%v heals=%v", actor.pledged, actor.unpledged, gate.healCalls)
	}
	if len(actor.persisted) != 3 || rep.Persisted != 3 {
		t.Fatalf("verdicts must be persisted in dry_run: %d", len(actor.persisted))
	}
	want := map[string]bool{"WOULD_PLEDGE": true, "WOULD_UNPLEDGE": true, "WOULD_CLOSE": true}
	for _, res := range audit.results() {
		delete(want, res)
	}
	if len(want) != 0 {
		t.Fatalf("missing dry-run audits %v in %v", want, audit.results())
	}
}

// NOT_HELD verdict → the gate decides; a passing plan is healed with exit
// reason NOT_HELD_AT_BROKER. SOLD_TODAY → MANUAL_EXIT at the verdict's price.
func TestReconciler_ClosureReasons(t *testing.T) {
	gate := &fakeGate{previews: map[string]*GhostPlan{
		"FIV99/AERO":  {UserID: "FIV99", Symbol: "AERO", BookRows: 1},
		"FIV99/SHREE": {UserID: "FIV99", Symbol: "SHREE", BookRows: 1},
	}}
	audit, actor := &fakeAudit{}, &fakeActor{}
	sold := verdict("FIV99", "SHREE", BrokerStateSoldToday, "")
	sold.ExitPrice = 105
	scan := &fakeScanner{results: map[string]*ReconResult{
		"FIV99": {UserID: "FIV99", BrokerLeg: "OK", Verdicts: []Verdict{confirmedVerdict("FIV99", "AERO", BrokerStateNotHeld), sold}},
	}}
	r := newTestReaper(ReaperModeEnabled, gate, scan, &fakeUsers{[]string{"FIV99"}}, audit)
	r.SetBookActor(actor)
	rep := r.SweepOnce(context.Background())
	if rep.Healed != 2 || len(gate.healCalls) != 2 {
		t.Fatalf("heals: %s %v", rep.String(), gate.healCalls)
	}
	byReason := map[string]float64{}
	for _, p := range gate.healPlans {
		byReason[p.exitReason()] = p.ExitPrice
	}
	if _, ok := byReason[ExitReasonNotHeldAtBroker]; !ok {
		t.Fatalf("NOT_HELD closure must carry NOT_HELD_AT_BROKER: %v", byReason)
	}
	if price, ok := byReason[ExitReasonManualExit]; !ok || price != 105 {
		t.Fatalf("SOLD_TODAY closure must be MANUAL_EXIT at 105: %v", byReason)
	}
}

// A gate refusal (e.g. settlement window, broker holds) on a NOT_HELD verdict
// means no heal — same guard as the legacy GHOST path.
func TestReconciler_NotHeldRefusedByGate(t *testing.T) {
	gate := &fakeGate{refusals: map[string]error{"FIV99/AERO": errors.New("within settlement window")}}
	audit, actor := &fakeAudit{}, &fakeActor{}
	scan := &fakeScanner{results: map[string]*ReconResult{
		"FIV99": {UserID: "FIV99", BrokerLeg: "OK", Verdicts: []Verdict{confirmedVerdict("FIV99", "AERO", BrokerStateNotHeld)}},
	}}
	r := newTestReaper(ReaperModeEnabled, gate, scan, &fakeUsers{[]string{"FIV99"}}, audit)
	r.SetBookActor(actor)
	rep := r.SweepOnce(context.Background())
	if rep.Refused != 1 || rep.Healed != 0 || len(gate.healCalls) != 0 {
		t.Fatalf("refusal not honored: %s", rep.String())
	}
}

// QTY_MISMATCH and UNVERIFIED: verdict persisted and audited on change, no
// action of any kind.
func TestReconciler_MismatchAndUnverifiedAreVerdictOnly(t *testing.T) {
	gate, audit, actor := &fakeGate{}, &fakeAudit{}, &fakeActor{}
	scan := &fakeScanner{results: map[string]*ReconResult{
		"FIV99": {UserID: "FIV99", BrokerLeg: "OK", Verdicts: []Verdict{
			verdict("FIV99", "IDEA", BrokerStateQtyMismatch, BrokerStateHeld),
			verdict("FIV99", "FRESH", BrokerStateUnverified, ""),
		}},
	}}
	r := newTestReaper(ReaperModeEnabled, gate, scan, &fakeUsers{[]string{"FIV99"}}, audit)
	r.SetBookActor(actor)
	rep := r.SweepOnce(context.Background())
	if rep.Mismatches != 1 || rep.Unverified != 1 || rep.Candidates != 0 {
		t.Fatalf("report: %s", rep.String())
	}
	if len(gate.previewCalls)+len(actor.pledged)+len(actor.unpledged) != 0 {
		t.Fatalf("acted on a verdict-only state")
	}
	if got := audit.results(); len(got) != 2 || got[0] != "VERDICT_QTY_MISMATCH" || got[1] != "VERDICT_UNVERIFIED" {
		t.Fatalf("audit: %v", got)
	}
}

// Broker leg down: UNVERIFIED verdicts are still persisted (the reason must
// show), but nothing is pledged, unpledged or healed.
func TestReconciler_BrokerDownPersistsUnverifiedOnly(t *testing.T) {
	gate, audit, actor := &fakeGate{}, &fakeAudit{}, &fakeActor{}
	scan := &fakeScanner{results: map[string]*ReconResult{
		"FIV99": {UserID: "FIV99", BrokerLeg: "AUTH_EXPIRED", Verdicts: []Verdict{
			verdict("FIV99", "SYRMA", BrokerStateUnverified, BrokerStatePledged),
		}, Mismatches: []Mismatch{ghostMismatch("SYRMA")}},
	}}
	r := newTestReaper(ReaperModeEnabled, gate, scan, &fakeUsers{[]string{"FIV99"}}, audit)
	r.SetBookActor(actor)
	rep := r.SweepOnce(context.Background())
	if rep.SkippedUser != 1 || rep.Persisted != 1 || len(actor.pledged)+len(actor.unpledged)+len(gate.previewCalls) != 0 {
		t.Fatalf("broker-down handling: %s", rep.String())
	}
}

// Login trigger: SweepUser runs for that user only (source login), and the
// debounce swallows a second trigger inside the window.
func TestReconciler_LoginTriggerDebounced(t *testing.T) {
	gate, audit, actor := &fakeGate{}, &fakeAudit{}, &fakeActor{}
	scan := &fakeScanner{results: map[string]*ReconResult{
		"FIV99": {UserID: "FIV99", BrokerLeg: "OK", Verdicts: []Verdict{confirmedVerdict("FIV99", "SYRMA", BrokerStatePledged)}},
		"S4450": {UserID: "S4450", BrokerLeg: "OK", Verdicts: []Verdict{confirmedVerdict("S4450", "KEI", BrokerStatePledged)}},
	}}
	r := newTestReaper(ReaperModeEnabled, gate, scan, &fakeUsers{[]string{"FIV99", "S4450"}}, audit)
	r.SetBookActor(actor)
	r.sleep = func(time.Duration) {}
	done := make(chan string, 4)
	r.runUsr = func(ctx context.Context, uid string) {
		rep := r.SweepUser(ctx, uid, VerdictSourceLogin)
		done <- uid + ":" + fmt.Sprint(rep.Pledged)
	}
	r.RunUserAsync("FIV99")
	select {
	case got := <-done:
		if got != "FIV99:1" {
			t.Fatalf("login run: %s", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("login run did not happen")
	}
	if len(actor.pledged) != 1 || actor.pledged[0] != "FIV99/SYRMA" {
		t.Fatalf("login run must touch only FIV99: %v", actor.pledged)
	}
	// Second trigger inside the debounce window: nothing runs.
	r.RunUserAsync("FIV99")
	select {
	case got := <-done:
		t.Fatalf("debounce failed: %s", got)
	case <-time.After(200 * time.Millisecond):
	}
	// Past the window, it runs again.
	r.now = func() time.Time { return time.Date(2026, 9, 3, 12, 5, 0, 0, time.FixedZone("IST", 5*3600+30*60)) }
	r.RunUserAsync("FIV99")
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("post-debounce run did not happen")
	}
	r.RunUserAsync("") // nil-safe / empty user: no panic, no run
	var nilReaper *GhostReaper
	nilReaper.RunUserAsync("FIV99")
}
