package admin

// GhostReaper — the book reconciler's scheduled sweep (the name predates the
// verdict model; "ghost" was the first class it closed). It matches every
// client's Manthan book against the client's live broker book and acts on the
// verdicts (book_verdict.go), through the EXACT SAME evidence gate the manual
// admin flow uses:
//
//	NOT_HELD / SOLD_TODAY → GhostPreviewFromRead → GhostHeal. The reaper has NO
//	    ghost-detection logic of its own: the reconciler nominates, the gate
//	    re-checks the live evidence and refuses anything uncertain, the heal
//	    executes only a plan that passed. Exit reason NOT_HELD_AT_BROKER
//	    (price unknown) or MANUAL_EXIT at today's tradebook price.
//	PLEDGED             → Actions.Pledge (slot released, stops parked, no exits)
//	HELD after PLEDGED  → Actions.Unpledge (protection resumes)
//	QTY_MISMATCH / UNVERIFIED / HELD → verdict + reason persisted, nothing else.
//
// Every lot's latest verdict is persisted on its positions_db rows
// (PersistVerdicts) so the admin console and the client app render the same
// sentence; every state change and every action lands in admin_audit.
//
// Triggers (2026-10-26, operator rule):
//   - every 15 minutes 09:20–15:45 IST on a trading day, plus 15:50 — after
//     the close, before EOD Phase A arming (16:35);
//   - immediately (≈10 s later, debounced 60 s per user) when a client's
//     broker session is refreshed — RunUserAsync from the credentials hooks;
//   - on demand: POST /admin/users/{id}/reconcile/run.
//
// Modes (MANTHAN_GHOST_REAPER_MODE, read in main.go):
//
//	"dry_run"  — verdicts are persisted (they are observations), but every
//	             action is audited as WOULD_* and nothing else changes.
//	"enabled"  — actions run; heals capped at maxHealsPerSweep per pass.
//
// Rollback: MANTHAN_GHOST_REAPER_MODE=off (or dry_run) and restart
// api-gateway. Nothing else in the system references the reaper.
//
// Idempotency: a healed lot's book rows become EXITED (no further verdict), a
// pledged lot's trading_db row is already PLEDGED (UPDATE … WHERE status =
// 'ACTIVE' is a no-op), an unpledged lot's parked rows are already closed.
// Actions are also only taken on a state TRANSITION (prev_state ≠ state),
// except closures, which the gate itself makes idempotent.

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
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

	// Login trigger: wait for user-config to commit the token and the probe
	// to verify it, then run once for that user; repeated logins/refreshes
	// inside the debounce window run once.
	loginReconcileDelay    = 10 * time.Second
	loginReconcileDebounce = 60 * time.Second
	loginReconcileTimeout  = 2 * time.Minute

	// reaperAdminID marks reaper-authored audit rows. admin_audit.admin_id
	// has no FK (the token-rejection path already writes "UNKNOWN").
	reaperAdminID = "SYSTEM:ghost-reaper"

	auditActionReaper    = "GHOST_REAPER"
	auditActionReconcile = "BOOK_RECONCILE"
)

// Seams: the production types satisfy these; tests substitute fakes. The
// reaper depends on behavior, never on how the gate decides.
type ghostGate interface {
	GhostPreview(ctx context.Context, userID, symbol string) (*GhostPlan, error)
	GhostHeal(ctx context.Context, p *GhostPlan) (map[string]any, error)
}

// readGate is the gate on a pre-fetched broker read (Actions implements it;
// a fake that does not simply gets GhostPreview).
type readGate interface {
	GhostPreviewFromRead(ctx context.Context, userID, symbol string, read *brokerRead) (*GhostPlan, error)
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

// bookActor applies the non-closure verdicts (Actions implements it).
type bookActor interface {
	PersistVerdicts(ctx context.Context, verdicts []Verdict, source string) (int64, error)
	Pledge(ctx context.Context, v Verdict, live map[string]bool) (map[string]any, error)
	Unpledge(ctx context.Context, v Verdict) (map[string]any, error)
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
	actor  bookActor          // optional: without it only closures run
	audit  reaperAuditor
	ist    *time.Location
	now    func() time.Time                      // test seam; defaults to time.Now
	sleep  func(time.Duration)                   // test seam for the login delay
	runUsr func(ctx context.Context, uid string) // test seam: what RunUserAsync executes

	mu       sync.Mutex
	lastUser map[string]time.Time // login debounce
}

// SetLedgerSource adds trade-execution's GHOST_SUSPECT parks as a candidate
// source (nil-safe; without it only the reconciler nominates).
func (g *GhostReaper) SetLedgerSource(l reaperLedgerSource) { g.ledger = l }

// SetBookActor enables verdict persistence and the PLEDGED / unpledge
// actions. Without it the reaper is the pre-verdict ghost sweep.
func (g *GhostReaper) SetBookActor(a bookActor) { g.actor = a }

func NewGhostReaper(mode string, gate ghostGate, scan ghostScanner,
	users reaperUserSource, audit reaperAuditor, ist *time.Location) *GhostReaper {
	g := &GhostReaper{
		mode: mode, gate: gate, scan: scan, users: users, audit: audit,
		ist: ist, now: time.Now, sleep: time.Sleep,
		lastUser: map[string]time.Time{},
	}
	g.runUsr = func(ctx context.Context, uid string) {
		rep := g.SweepUser(ctx, uid, VerdictSourceLogin)
		log.Printf("[ghost-reaper] post-login reconcile user=%s mode=%s %s", uid, g.mode, rep.String())
	}
	return g
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

	Verdicts   int // lots judged
	Persisted  int64
	Pledged    int // PLEDGED applied (or WOULD_PLEDGE in dry_run)
	Unpledged  int
	Mismatches int // QTY_MISMATCH verdicts (surfaced only)
	Unverified int
}

func (r SweepReport) String() string {
	return fmt.Sprintf("users=%d skipped=%d candidates=%d would_close=%d healed=%d refused=%d deferred=%d errors=%d verdicts=%d persisted=%d pledged=%d unpledged=%d mismatches=%d unverified=%d",
		r.Users, r.SkippedUser, r.Candidates, r.WouldClose, r.Healed, r.Refused, r.DeferredCap, r.Errors,
		r.Verdicts, r.Persisted, r.Pledged, r.Unpledged, r.Mismatches, r.Unverified)
}

// Start blocks, sweeping on the intraday cadence plus 15:50 IST until ctx is
// cancelled. Run in a goroutine from main.go.
func (g *GhostReaper) Start(ctx context.Context) {
	log.Printf("[ghost-reaper] started mode=%s (every %s 09:20–15:45 IST on trading days + %02d:%02d IST; login-triggered runs after %s)",
		g.mode, reaperIntradayEvery, reaperSweepHourIST, reaperSweepMinuteIST, loginReconcileDelay)
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

// RunUserAsync is the login hook: a client just pushed a fresh broker token,
// so match that client's book against the broker shortly after (the probe
// verifies the token first), once per debounce window. nil-receiver safe so
// wiring is optional.
func (g *GhostReaper) RunUserAsync(userID string) {
	if g == nil || userID == "" {
		return
	}
	go func() {
		g.sleep(loginReconcileDelay)
		if !g.debounce(userID) {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), loginReconcileTimeout)
		defer cancel()
		g.runUsr(ctx, userID)
	}()
}

// debounce reports whether a login-triggered run may start for the user now.
func (g *GhostReaper) debounce(userID string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := g.now()
	if last, ok := g.lastUser[userID]; ok && now.Sub(last) < loginReconcileDebounce {
		return false
	}
	g.lastUser[userID] = now
	return true
}

// SweepOnce runs one full pass over every active user. Safe to call
// repeatedly (idempotent — see package comment). Every candidate outcome
// lands in the admin audit trail.
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
		g.sweepUser(ctx, uid, VerdictSourceSweep, ledgerByUser[uid], &rep)
	}
	return rep
}

// SweepUser runs the full comparison + actions for ONE user (login trigger,
// manual run). Ledger GHOST_SUSPECT parks for the user are included.
func (g *GhostReaper) SweepUser(ctx context.Context, userID, source string) SweepReport {
	var rep SweepReport
	rep.Users = 1
	var parks []string
	if g.ledger != nil {
		if all, err := g.ledger.GhostSuspectCandidates(ctx); err == nil {
			for _, p := range all {
				if p.UserID == userID {
					parks = append(parks, strings.ToUpper(p.Symbol))
				}
			}
		}
	}
	g.sweepUser(ctx, userID, source, parks, &rep)
	return rep
}

func (g *GhostReaper) sweepUser(ctx context.Context, uid, source string, ledgerParks []string, rep *SweepReport) {
	res, err := g.scan.Reconcile(ctx, uid)
	if err != nil {
		log.Printf("[ghost-reaper] reconcile failed user=%s — skipping: %v", uid, err)
		rep.Errors++
		return
	}
	// No broker truth → no action, ever. An unreachable/expired broker
	// leg means every verdict is UNVERIFIED and the GHOST classifications
	// are ledger-side guesses. The UNVERIFIED verdicts are still persisted
	// (they are the truth about what we know) so the reason shows.
	if res.BrokerLeg != "OK" {
		rep.SkippedUser++
		rep.Verdicts += len(res.Verdicts)
		rep.Unverified += len(res.Verdicts)
		g.persist(ctx, res.Verdicts, source, rep)
		g.auditRow(ctx, auditActionReaper, uid, "", "SKIPPED",
			fmt.Sprintf("broker leg %s — no action without broker truth", res.BrokerLeg), nil)
		return
	}

	rep.Verdicts += len(res.Verdicts)
	g.persist(ctx, res.Verdicts, source, rep)

	seen := map[string]bool{}
	for _, v := range res.Verdicts {
		switch v.State {
		case BrokerStateNotHeld, BrokerStateSoldToday:
			if seen[v.Symbol] {
				continue
			}
			seen[v.Symbol] = true
			rep.Candidates++
			g.processCandidate(ctx, uid, v.Symbol, res, &v, rep)
		case BrokerStatePledged:
			if v.Changed() {
				g.pledge(ctx, v, res, rep)
			}
		case BrokerStateHeld:
			// Reverse path. prev_state alone is not enough: a sweep during a
			// dead session persists UNVERIFIED over PLEDGED, and a lot judged
			// for the first time after a restart has no prev_state — so the
			// actor is asked whenever the lot might still be parked; it
			// reports whether anything actually changed.
			switch v.PrevState {
			case BrokerStatePledged, BrokerStateUnverified, "":
				g.unpledge(ctx, v, rep)
			}
		case BrokerStateQtyMismatch:
			rep.Mismatches++
			if v.Changed() {
				g.auditRow(ctx, auditActionReconcile, uid, v.Symbol, "VERDICT_"+v.State, v.Reason, map[string]any{"evidence": v.Evidence})
			}
		case BrokerStateUnverified:
			rep.Unverified++
			if v.Changed() {
				g.auditRow(ctx, auditActionReconcile, uid, v.Symbol, "VERDICT_"+v.State, v.Reason, map[string]any{"evidence": v.Evidence})
			}
		}
	}
	// Legacy class view + trade-execution's parks: the gate decides.
	for _, m := range res.Mismatches {
		if m.Class != "GHOST" || seen[m.Symbol] {
			continue
		}
		seen[m.Symbol] = true
		rep.Candidates++
		g.processCandidate(ctx, uid, m.Symbol, res, nil, rep)
	}
	for _, sym := range ledgerParks {
		if seen[sym] {
			continue
		}
		seen[sym] = true
		rep.Candidates++
		g.processCandidate(ctx, uid, sym, res, nil, rep)
	}
}

func (g *GhostReaper) persist(ctx context.Context, verdicts []Verdict, source string, rep *SweepReport) {
	if g.actor == nil || len(verdicts) == 0 {
		return
	}
	n, err := g.actor.PersistVerdicts(ctx, verdicts, source)
	if err != nil {
		rep.Errors++
		log.Printf("[ghost-reaper] verdict persist failed: %v", err)
	}
	rep.Persisted += n
}

func (g *GhostReaper) pledge(ctx context.Context, v Verdict, res *ReconResult, rep *SweepReport) {
	if g.actor == nil {
		return
	}
	if g.mode != ReaperModeEnabled {
		rep.Pledged++
		g.auditRow(ctx, auditActionReconcile, v.UserID, v.Symbol, "WOULD_PLEDGE", "dry_run: "+v.Reason,
			map[string]any{"evidence": v.Evidence, "strategy_id": v.StrategyID})
		return
	}
	var live map[string]bool
	if res != nil && res.read != nil {
		live = map[string]bool{}
		for _, ob := range res.read.book {
			st := strings.ToUpper(strings.TrimSpace(ob.Status))
			if st != "EXECUTED" && st != "CANCELLED" && st != "CANCELED" && st != "REJECTED" && st != "EXPIRED" {
				live[ob.OrdId] = true
			}
		}
	}
	result, err := g.actor.Pledge(ctx, v, live)
	if err != nil {
		rep.Errors++
		g.auditRow(ctx, auditActionReconcile, v.UserID, v.Symbol, "PLEDGE_FAILED", err.Error(),
			map[string]any{"evidence": v.Evidence, "result": result})
		return
	}
	if changed, _ := result["changed"].(bool); !changed && v.PrevState == BrokerStateUnverified {
		return // already pledged before the session gap — nothing to say twice
	}
	rep.Pledged++
	g.auditRow(ctx, auditActionReconcile, v.UserID, v.Symbol, "PLEDGED", v.Reason,
		map[string]any{"evidence": v.Evidence, "result": result, "strategy_id": v.StrategyID})
}

func (g *GhostReaper) unpledge(ctx context.Context, v Verdict, rep *SweepReport) {
	if g.actor == nil {
		return
	}
	if g.mode != ReaperModeEnabled {
		if v.PrevState == BrokerStatePledged {
			rep.Unpledged++
			g.auditRow(ctx, auditActionReconcile, v.UserID, v.Symbol, "WOULD_UNPLEDGE", "dry_run: "+v.Reason,
				map[string]any{"evidence": v.Evidence, "strategy_id": v.StrategyID})
		}
		return
	}
	result, err := g.actor.Unpledge(ctx, v)
	if err != nil {
		rep.Errors++
		g.auditRow(ctx, auditActionReconcile, v.UserID, v.Symbol, "UNPLEDGE_FAILED", err.Error(),
			map[string]any{"evidence": v.Evidence, "result": result})
		return
	}
	if changed, _ := result["changed"].(bool); !changed && v.PrevState != BrokerStatePledged {
		return // was never parked — the common HELD case after a session gap
	}
	rep.Unpledged++
	g.auditRow(ctx, auditActionReconcile, v.UserID, v.Symbol, "UNPLEDGED", v.Reason,
		map[string]any{"evidence": v.Evidence, "result": result, "strategy_id": v.StrategyID})
}

// processCandidate runs one NOT_HELD / SOLD_TODAY / GHOST candidate through
// the gate. v (optional) carries the verdict that nominated it: its exit
// reason and tradebook price travel into the plan.
func (g *GhostReaper) processCandidate(ctx context.Context, uid, symbol string, res *ReconResult, v *Verdict, rep *SweepReport) {
	// The gate re-checks the live evidence (on this run's broker read when
	// the gate supports it); ANY refusal (settlement window, broker holds,
	// open SELL, fetch failure) means no action.
	var plan *GhostPlan
	var err error
	if rg, ok := g.gate.(readGate); ok && res != nil && res.read != nil {
		plan, err = rg.GhostPreviewFromRead(ctx, uid, symbol, res.read)
	} else {
		plan, err = g.gate.GhostPreview(ctx, uid, symbol)
	}
	if err != nil {
		rep.Refused++
		g.auditRow(ctx, auditActionReaper, uid, symbol, "REFUSED", err.Error(), nil)
		return
	}
	// The automated closure names what happened in plain words; the manual
	// Postman flow keeps ADMIN_GHOST_CLEANUP.
	plan.ExitReason = ExitReasonNotHeldAtBroker
	if plan.Evidence.TradebookSell && plan.ExitPrice > 0 {
		plan.ExitReason = ExitReasonManualExit
	}
	if v != nil && v.State == BrokerStateSoldToday && v.ExitPrice > 0 {
		plan.ExitReason, plan.ExitPrice = ExitReasonManualExit, v.ExitPrice
	}

	if g.mode != ReaperModeEnabled {
		rep.WouldClose++
		g.auditRow(ctx, auditActionReaper, uid, symbol, "WOULD_CLOSE",
			"dry_run: live evidence gate passed — manual flow would allow heal ("+plan.exitReason()+")",
			map[string]any{"evidence": plan.Evidence, "plan": plan.ConfirmationText, "exit_reason": plan.exitReason()})
		return
	}

	if rep.Healed >= maxHealsPerSweep {
		rep.DeferredCap++
		g.auditRow(ctx, auditActionReaper, uid, symbol, "DEFERRED_CAP",
			fmt.Sprintf("gate passed but sweep already healed %d — will heal on a later sweep", rep.Healed), nil)
		return
	}

	result, herr := g.gate.GhostHeal(ctx, plan)
	if herr != nil {
		rep.Errors++
		g.auditRow(ctx, auditActionReaper, uid, symbol, "HEAL_FAILED", herr.Error(),
			map[string]any{"evidence": plan.Evidence})
		return
	}
	rep.Healed++
	g.auditRow(ctx, auditActionReaper, uid, symbol, "HEALED", plan.closeNote()+" ("+plan.exitReason()+")",
		map[string]any{"evidence": plan.Evidence, "result": result, "exit_reason": plan.exitReason(), "exit_price": plan.ExitPrice})
}

func (g *GhostReaper) auditRow(ctx context.Context, action, uid, symbol, result, detail string, params any) {
	e := AuditEntry{
		AdminID: reaperAdminID, Action: action, Tier: string(TierTyped),
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
