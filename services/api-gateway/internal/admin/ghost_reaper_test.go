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
	for _, s := range []string{"A", "B", "C", "D", "E", "F", "G"} {
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
	if rep.DeferredCap != 7-maxHealsPerSweep {
		t.Fatalf("expected %d deferred, got %+v", 7-maxHealsPerSweep, rep)
	}
	deferred := 0
	for _, r := range audit.rows {
		if r.Result == "DEFERRED_CAP" {
			deferred++
		}
	}
	if deferred != 7-maxHealsPerSweep {
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

// Test: next-sweep scheduling lands on 15:50 IST, today if still ahead,
// tomorrow otherwise.
func TestReaper_NextSweepTime(t *testing.T) {
	ist := time.FixedZone("IST", 5*3600+30*60)
	r := NewGhostReaper(ReaperModeDryRun, &fakeGate{}, &fakeScanner{}, &fakeUsers{}, &fakeAudit{}, ist)

	r.now = func() time.Time { return time.Date(2026, 9, 3, 12, 0, 0, 0, ist) }
	if got := r.nextSweep(); got.Hour() != 15 || got.Minute() != 50 || got.Day() != 3 {
		t.Fatalf("expected today 15:50 IST, got %v", got)
	}
	r.now = func() time.Time { return time.Date(2026, 9, 3, 16, 0, 0, 0, ist) }
	if got := r.nextSweep(); got.Day() != 4 {
		t.Fatalf("expected tomorrow 15:50 IST, got %v", got)
	}
}
