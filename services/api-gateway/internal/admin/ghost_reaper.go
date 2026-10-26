package admin

// GhostReaper — scheduled sweep that funnels the reconciler's GHOST findings
// through the EXACT SAME GhostPreview → GhostHeal evidence gate the manual
// admin flow uses. It contains NO ghost-detection logic of its own: the
// reconciler (ThreeWay) nominates candidates, GhostPreview re-fetches live
// broker evidence and refuses anything uncertain, GhostHeal executes only a
// plan that passed. One source of truth for the gate — if the manual flow
// would refuse, the reaper refuses, structurally.
//
// Modes (MANTHAN_GHOST_REAPER_MODE, read in main.go — the reaper is never
// even constructed when the variable is unset):
//
//	"dry_run"  — preview-only. Every candidate is audited as WOULD_CLOSE or
//	             REFUSED with the live evidence; nothing is modified. Run
//	             this for several days and compare against manual
//	             GhostPreview results before enabling.
//	"enabled"  — candidates that pass the live gate are healed, capped at
//	             maxHealsPerSweep per day.
//
// Sweep times (2026-10-26): every 15 minutes from 09:20 to 15:45 IST on a
// trading day, plus 15:50 — after the close, before EOD Phase A arming
// (16:35). A ghost confirmed by the broker right after a client logs in
// (FIV99 2026-10-26: nine positions the account no longer held) is therefore
// closed within the session and the strategy's slots are free for new
// signals the same day, without a human in the loop.
//
// Candidate sources: the reconciler's GHOST class (ACTIVE book lot, broker
// holds 0) and trade-execution's GHOST_SUSPECT parks (the broker's RMS
// answered "Set= 0" to a stop or an exit — the strongest evidence there is).
// Both go through the same GhostPreview gate.
//
// Rollback: MANTHAN_GHOST_REAPER_MODE=off (or dry_run) and restart
// api-gateway. Nothing else in the system references the reaper.
//
// Idempotency: a healed ghost's book rows become EXITED, so the next
// sweep's Reconcile no longer classes it GHOST (no ACTIVE lot) AND
// GhostPreview would refuse it ("no ACTIVE book rows") — double protection
// against double-closing. The FIX-5 ledger insert is ON CONFLICT DO NOTHING
// and gated on LedgerNetQty > 0 inside GhostHeal itself.

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
)

const (
	ReaperModeDryRun  = "dry_run"
	ReaperModeEnabled = "enabled"
	ReaperModeOff     = "off"

	// maxHealsPerSweep bounds the blast radius of one automated pass. This
	// is an operational rate bound, not evidence logic — any confirmed
	// ghosts beyond the cap are audited DEFERRED_CAP and heal on later
	// sweeps (safe because sweeps are idempotent).
	maxHealsPerSweep = 20

	reaperSweepHourIST   = 15
	reaperSweepMinuteIST = 50

	// Intraday cadence: first sweep 09:20 (after the 09:15 planner has
	// placed the day's stops), then every reaperIntradayEvery until
	// reaperIntradayLastMin, then the 15:50 close sweep.
	reaperIntradayFirstMin = 9*60 + 20
	reaperIntradayLastMin  = 15*60 + 45
	reaperIntradayEvery    = 15 * time.Minute

	// reaperAdminID marks reaper-authored audit rows. admin_audit.admin_id
	// has no FK (the token-rejection path already writes "UNKNOWN").
	reaperAdminID = "SYSTEM:ghost-reaper"
)

// Seams: the production types satisfy these; tests substitute fakes. The
// reaper depends on behavior, never on how the gate decides.
type ghostGate interface {
	GhostPreview(ctx context.Context, userID, symbol string) (*GhostPlan, error)
	GhostHeal(ctx context.Context, p *GhostPlan) (map[string]any, error)
}

type ghostScanner interface {
	Reconcile(ctx context.Context, userID string) (*ReconResult, error)
}

type reaperAuditor interface {
	Audit(ctx context.Context, e AuditEntry) error
}

type reaperUserSource interface {
	ActiveUserIDs(ctx context.Context) ([]string, error)
}

// LedgerGhost is a (user, symbol) trade-execution parked as GHOST_SUSPECT.
type LedgerGhost struct {
	UserID string
	Symbol string
}

// reaperLedgerSource lists trade-execution's GHOST_SUSPECT parks — positions
// whose stop or exit the broker's RMS rejected with "Set= 0".
type reaperLedgerSource interface {
	GhostSuspectCandidates(ctx context.Context) ([]LedgerGhost, error)
}

// AuditStore exposes the console's audit writer so main.go can hand it to
// the reaper without a second DB handle.
func (h *HTTP) AuditStore() *Store { return h.svc.store }

// GhostReaper wires the existing pieces together on a timer.
type GhostReaper struct {
	mode   string
	gate   ghostGate
	scan   ghostScanner
	users  reaperUserSource
	ledger reaperLedgerSource // optional
	audit  reaperAuditor
	ist    *time.Location
	now    func() time.Time // test seam; defaults to time.Now
}

// SetLedgerSource adds trade-execution's GHOST_SUSPECT parks as a candidate
// source (nil-safe; without it only the reconciler nominates).
func (g *GhostReaper) SetLedgerSource(l reaperLedgerSource) { g.ledger = l }

func NewGhostReaper(mode string, gate ghostGate, scan ghostScanner,
	users reaperUserSource, audit reaperAuditor, ist *time.Location) *GhostReaper {
	return &GhostReaper{
		mode: mode, gate: gate, scan: scan, users: users, audit: audit,
		ist: ist, now: time.Now,
	}
}

// SweepReport summarizes one pass for logs and tests.
type SweepReport struct {
	Users       int
	SkippedUser int // broker leg unavailable — no truth, no action
	Candidates  int
	WouldClose  int // dry_run: plans that passed the gate
	Healed      int
	Refused     int
	DeferredCap int
	Errors      int
}

func (r SweepReport) String() string {
	return fmt.Sprintf("users=%d skipped=%d candidates=%d would_close=%d healed=%d refused=%d deferred=%d errors=%d",
		r.Users, r.SkippedUser, r.Candidates, r.WouldClose, r.Healed, r.Refused, r.DeferredCap, r.Errors)
}

// Start blocks, sweeping on the intraday cadence plus 15:50 IST until ctx is
// cancelled. Run in a goroutine from main.go.
func (g *GhostReaper) Start(ctx context.Context) {
	log.Printf("[ghost-reaper] started mode=%s (every %s 09:20–15:45 IST on trading days + %02d:%02d IST)",
		g.mode, reaperIntradayEvery, reaperSweepHourIST, reaperSweepMinuteIST)
	for {
		next := g.nextSweep()
		select {
		case <-ctx.Done():
			log.Printf("[ghost-reaper] stopped")
			return
		case <-time.After(next.Sub(g.now())):
			rep := g.SweepOnce(ctx)
			log.Printf("[ghost-reaper] sweep complete mode=%s %s", g.mode, rep.String())
		}
	}
}

// nextSweep returns the next sweep instant strictly after now: the next
// 15-minute slot between 09:20 and 15:45 IST, the 15:50 close sweep, else
// 09:20 on the next trading day.
func (g *GhostReaper) nextSweep() time.Time {
	n := g.now().In(g.ist)
	for day := 0; day < 14; day++ {
		d := n.AddDate(0, 0, day)
		if !indiraClient.IsTradingDay(d) {
			continue
		}
		midnight := time.Date(d.Year(), d.Month(), d.Day(), 0, 0, 0, 0, g.ist)
		for m := reaperIntradayFirstMin; m <= reaperIntradayLastMin; m += int(reaperIntradayEvery / time.Minute) {
			if t := midnight.Add(time.Duration(m) * time.Minute); t.After(n) {
				return t
			}
		}
		if t := midnight.Add(time.Duration(reaperSweepHourIST*60+reaperSweepMinuteIST) * time.Minute); t.After(n) {
			return t
		}
	}
	return n.Add(24 * time.Hour) // no trading day in two weeks (never in practice) — try tomorrow
}

// SweepOnce runs one full pass. Safe to call repeatedly (idempotent — see
// package comment). Every candidate outcome lands in the admin audit trail.
func (g *GhostReaper) SweepOnce(ctx context.Context) SweepReport {
	var rep SweepReport

	users, err := g.users.ActiveUserIDs(ctx)
	if err != nil {
		log.Printf("[ghost-reaper] user enumeration failed — aborting sweep: %v", err)
		rep.Errors++
		return rep
	}
	rep.Users = len(users)

	// trade-execution's GHOST_SUSPECT parks, grouped per user.
	ledgerByUser := map[string][]string{}
	if g.ledger != nil {
		if parks, lerr := g.ledger.GhostSuspectCandidates(ctx); lerr != nil {
			log.Printf("[ghost-reaper] ledger ghost-suspect listing failed — reconciler candidates only: %v", lerr)
			rep.Errors++
		} else {
			for _, p := range parks {
				ledgerByUser[p.UserID] = append(ledgerByUser[p.UserID], strings.ToUpper(p.Symbol))
			}
		}
	}

	for _, uid := range users {
		res, err := g.scan.Reconcile(ctx, uid)
		if err != nil {
			log.Printf("[ghost-reaper] reconcile failed user=%s — skipping: %v", uid, err)
			rep.Errors++
			continue
		}
		// No broker truth → no action, ever. An unreachable/expired broker
		// leg means the GHOST classifications are ledger-side guesses.
		if res.BrokerLeg != "OK" {
			rep.SkippedUser++
			g.auditRow(ctx, uid, "", "SKIPPED",
				fmt.Sprintf("broker leg %s — no action without broker truth", res.BrokerLeg), nil)
			continue
		}

		seen := map[string]bool{}
		for _, m := range res.Mismatches {
			if m.Class != "GHOST" || seen[m.Symbol] {
				continue
			}
			seen[m.Symbol] = true
			rep.Candidates++
			g.processCandidate(ctx, uid, m.Symbol, &rep)
		}
		for _, sym := range ledgerByUser[uid] {
			if seen[sym] {
				continue
			}
			seen[sym] = true
			rep.Candidates++
			g.processCandidate(ctx, uid, sym, &rep)
		}
	}
	return rep
}

func (g *GhostReaper) processCandidate(ctx context.Context, uid, symbol string, rep *SweepReport) {
	// The gate re-fetches live evidence; ANY refusal (settlement window,
	// broker holds, open SELL, fetch failure) means no action.
	plan, err := g.gate.GhostPreview(ctx, uid, symbol)
	if err != nil {
		rep.Refused++
		g.auditRow(ctx, uid, symbol, "REFUSED", err.Error(), nil)
		return
	}

	if g.mode != ReaperModeEnabled {
		rep.WouldClose++
		g.auditRow(ctx, uid, symbol, "WOULD_CLOSE",
			"dry_run: live evidence gate passed — manual flow would allow heal",
			map[string]any{"evidence": plan.Evidence, "plan": plan.ConfirmationText})
		return
	}

	if rep.Healed >= maxHealsPerSweep {
		rep.DeferredCap++
		g.auditRow(ctx, uid, symbol, "DEFERRED_CAP",
			fmt.Sprintf("gate passed but sweep already healed %d — will heal on a later sweep", rep.Healed), nil)
		return
	}

	result, herr := g.gate.GhostHeal(ctx, plan)
	if herr != nil {
		rep.Errors++
		g.auditRow(ctx, uid, symbol, "HEAL_FAILED", herr.Error(),
			map[string]any{"evidence": plan.Evidence})
		return
	}
	rep.Healed++
	g.auditRow(ctx, uid, symbol, "HEALED", "auto-healed from broker-verified evidence",
		map[string]any{"evidence": plan.Evidence, "result": result})
}

func (g *GhostReaper) auditRow(ctx context.Context, uid, symbol, result, detail string, params any) {
	e := AuditEntry{
		AdminID: reaperAdminID, Action: "GHOST_REAPER", Tier: string(TierTyped),
		TargetUser: uid, TargetRef: symbol,
		Params: params, Result: result, Detail: detail,
	}
	if err := g.audit.Audit(ctx, e); err != nil {
		// A heal that happened but failed to audit must be loud — the
		// action is done, only the record write failed.
		log.Printf("[ghost-reaper] AUDIT WRITE FAILED user=%s symbol=%s result=%s: %v",
			uid, symbol, result, err)
	}
}
