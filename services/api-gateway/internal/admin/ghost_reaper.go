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
//	PLEDGED             → Actions.Pledge (slot released, stops parked, no exits),
//	                      re-asserted on every pass (idempotent) so a half-applied
//	                      park is completed on the next sweep
//	HELD                → Actions.Unpledge, re-asserted on every pass; it is a
//	                      no-op (no audit) unless the lot was still parked
//	QTY_MISMATCH / UNVERIFIED → verdict + reason persisted, nothing else.
//
// Safety rails (2026-10-26 review):
//   - Hysteresis: a NOT_HELD closure or a PLEDGED park needs the same verdict
//     on two consecutive reads ≥ 15 min apart (persisted checked_at; an
//     in-memory fallback for ledger-only lots). Login runs count as a read.
//   - Action window: closures, parks and unparks run only 09:20–15:25 IST on
//     a trading day; outside it (the 15:50 sweep, overnight logins) verdicts
//     are persisted only.
//   - Caps: a fleet sweep heals at most 20 lots, a login/manual run 5.
//   - A broker leg that is not OK (dead session, empty holdings with open
//     lots, one of our live orders missing from the order book, trade book
//     unreadable) persists UNVERIFIED without overwriting the last truth
//     (PersistVerdicts) and takes no action.
//
// Every lot's latest verdict is persisted on its positions_db rows so the
// admin console and the client app render the same sentence; every state
// change and every action lands in admin_audit.
//
// Triggers: every 15 minutes 09:20–15:45 IST on a trading day plus 15:50;
// ~10 s after a client's broker session is refreshed (debounced 60 s per
// user) via the credentials hooks; on demand via POST
// /admin/users/{id}/reconcile/run (TYPED).
//
// Modes (MANTHAN_GHOST_REAPER_MODE, read in main.go): "dry_run" persists
// verdicts and audits every action as WOULD_*; "enabled" acts. Rollback:
// MANTHAN_GHOST_REAPER_MODE=off (or dry_run) and restart api-gateway.

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

	// maxHealsPerSweep bounds the blast radius of one automated fleet pass;
	// maxHealsPerUserRun bounds a login / manual single-user run. Confirmed
	// closures beyond the cap are audited DEFERRED_CAP and heal later.
	maxHealsPerSweep   = 20
	maxHealsPerUserRun = 5

	reaperSweepHourIST   = 15
	reaperSweepMinuteIST = 50

	// Intraday cadence: first sweep 09:20 (after the 09:15 planner has
	// placed the day's stops), then every reaperIntradayEvery until
	// reaperIntradayLastMin, then the 15:50 close sweep.
	reaperIntradayFirstMin = 9*60 + 20
	reaperIntradayLastMin  = 15*60 + 45
	reaperIntradayEvery    = 15 * time.Minute

	// Action window: automated closures / parks / unparks only inside it.
	actionWindowFirstMin = 9*60 + 20
	actionWindowLastMin  = 15*60 + 25

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
	Pledge(ctx context.Context, v Verdict, live *liveBook) (map[string]any, error)
	Unpledge(ctx context.Context, v Verdict) (map[string]any, error)
}

// LedgerGhost is a (user, symbol) trade-execution parked as GHOST_SUSPECT.
type LedgerGhost struct {
	UserID string
	Symbol string
}

// reaperLedgerSource lists trade-execution's GHOST_SUSPECT parks — positions
// whose stop or exit the broker's RMS answered "Set= 0" (zero sellable).
type reaperLedgerSource interface {
	GhostSuspectCandidates(ctx context.Context) ([]LedgerGhost, error)
}

// AuditStore exposes the console's audit writer so main.go can hand it to
// the reaper without a second DB handle.
func (h *HTTP) AuditStore() *Store { return h.svc.store }

// firstSeen is the in-memory hysteresis record for lots without a persisted
// history (ledger-only lots).
type firstSeen struct {
	state string
	at    time.Time
}

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
	// inWindow decides whether actions may run right now (test seam).
	inWindow func(time.Time) bool

	// startedAt: verdicts persisted before this process started (another
	// mode, yesterday) never confirm today's — a mode flip starts the
	// two-read clock afresh.
	startedAt time.Time

	mu       sync.Mutex
	lastUser map[string]time.Time // login debounce
	seen     map[string]firstSeen // hysteresis fallback: user|strategy|symbol
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
		ist: ist, now: time.Now, sleep: time.Sleep, startedAt: time.Now(),
		lastUser: map[string]time.Time{}, seen: map[string]firstSeen{},
	}
	g.inWindow = g.defaultInWindow
	g.runUsr = func(ctx context.Context, uid string) {
		rep := g.SweepUser(ctx, uid, VerdictSourceLogin)
		log.Printf("[ghost-reaper] post-login reconcile user=%s mode=%s %s", uid, g.mode, rep.String())
	}
	return g
}

// defaultInWindow: 09:20–15:25 IST on a trading day.
func (g *GhostReaper) defaultInWindow(t time.Time) bool {
	n := t.In(g.ist)
	if !indiraClient.IsTradingDay(n) {
		return false
	}
	m := n.Hour()*60 + n.Minute()
	return m >= actionWindowFirstMin && m <= actionWindowLastMin
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

	Verdicts     int // lots judged
	Persisted    int64
	Pledged      int // PLEDGED applied (or WOULD_PLEDGE in dry_run)
	Unpledged    int
	Mismatches   int // QTY_MISMATCH verdicts (surfaced only)
	Unverified   int
	AwaitConfirm int // NOT_HELD / PLEDGED seen once — waiting for the confirming read
	OutOfWindow  int // actionable verdicts left alone because actions are closed now

	cap int
}

func (r SweepReport) String() string {
	return fmt.Sprintf("users=%d skipped=%d candidates=%d would_close=%d healed=%d refused=%d deferred=%d errors=%d verdicts=%d persisted=%d pledged=%d unpledged=%d mismatches=%d unverified=%d await_confirm=%d out_of_window=%d",
		r.Users, r.SkippedUser, r.Candidates, r.WouldClose, r.Healed, r.Refused, r.DeferredCap, r.Errors,
		r.Verdicts, r.Persisted, r.Pledged, r.Unpledged, r.Mismatches, r.Unverified, r.AwaitConfirm, r.OutOfWindow)
}

// Start blocks, sweeping on the intraday cadence plus 15:50 IST until ctx is
// cancelled. Run in a goroutine from main.go.
func (g *GhostReaper) Start(ctx context.Context) {
	log.Printf("[ghost-reaper] started mode=%s (every %s 09:20–15:45 IST on trading days + %02d:%02d IST; actions 09:20–15:25 IST only; login-triggered runs after %s)",
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

// confirmed implements the hysteresis: the SAME state on two reads ≥ 15 min
// apart, on the same IST day, both read by THIS process (a verdict persisted
// under another mode / before a restart never confirms — the flip day starts
// the clock afresh; see the ops note in the package comment). Persisted
// history first; otherwise the in-memory record (ledger-only lots), which
// this call also updates.
//
// Ops: to restart the clock by hand for the flip day —
//
//	UPDATE positions SET broker_checked_at = NULL
//	 WHERE status = 'ACTIVE' AND broker_state IN ('NOT_HELD','SOLD_TODAY','PLEDGED');
func (g *GhostReaper) confirmed(v Verdict) bool {
	if !v.PrevCheckedAt.IsZero() {
		return v.Confirmed() && !v.PrevCheckedAt.Before(g.startedAt)
	}
	key := v.UserID + "|" + v.StrategyID + "|" + v.Symbol
	g.mu.Lock()
	defer g.mu.Unlock()
	fs, ok := g.seen[key]
	if !ok || fs.state != v.State {
		g.seen[key] = firstSeen{state: v.State, at: v.CheckedAt}
		return false
	}
	return v.CheckedAt.Sub(fs.at) >= verdictConfirmGap
}

// SweepOnce runs one full pass over every active user. Safe to call
// repeatedly (idempotent — see package comment). Every candidate outcome
// lands in the admin audit trail.
func (g *GhostReaper) SweepOnce(ctx context.Context) SweepReport {
	rep := SweepReport{cap: maxHealsPerSweep}

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
// manual run) under the single-user closure cap. Ledger GHOST_SUSPECT parks
// for the user are included.
func (g *GhostReaper) SweepUser(ctx context.Context, userID, source string) SweepReport {
	rep := SweepReport{cap: maxHealsPerUserRun}
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
	// No broker truth → no action, ever. The UNVERIFIED verdicts are still
	// persisted (without overwriting the last truth) so the reason shows.
	if res.BrokerLeg != "OK" {
		rep.SkippedUser++
		rep.Verdicts += len(res.Verdicts)
		rep.Unverified += len(res.Verdicts)
		g.persist(ctx, res.Verdicts, source, rep)
		g.auditRow(ctx, auditActionReaper, uid, "", "SKIPPED",
			fmt.Sprintf("broker leg %s — no action without broker truth%s", res.BrokerLeg, detailSuffix(res.BrokerDetail)), nil)
		return
	}

	rep.Verdicts += len(res.Verdicts)
	g.persist(ctx, res.Verdicts, source, rep)
	open := g.inWindow(g.now())

	// Every judged symbol is "seen": the legacy GHOST loop and the ledger
	// parks may nominate only symbols that carry NO verdict of their own.
	seen := map[string]bool{}
	for _, v := range res.Verdicts {
		seen[v.Symbol] = true
	}
	for _, v := range res.Verdicts {
		switch v.State {
		case BrokerStateNotHeld:
			if !g.confirmed(v) {
				rep.AwaitConfirm++
				if v.Changed() {
					g.auditRow(ctx, auditActionReconcile, uid, v.Symbol, "VERDICT_NOT_HELD", v.Reason+" — awaiting the confirming read", map[string]any{"evidence": v.Evidence})
				}
				continue
			}
			if !open {
				rep.OutOfWindow++
				continue
			}
			rep.Candidates++
			g.processCandidate(ctx, uid, v.Symbol, res, &v, rep)
		case BrokerStateSoldToday:
			if !g.confirmed(v) {
				rep.AwaitConfirm++
				if v.Changed() {
					g.auditRow(ctx, auditActionReconcile, uid, v.Symbol, "VERDICT_SOLD_TODAY", v.Reason+" — awaiting the confirming read", map[string]any{"evidence": v.Evidence})
				}
				continue
			}
			if !open {
				rep.OutOfWindow++
				continue
			}
			rep.Candidates++
			g.processCandidate(ctx, uid, v.Symbol, res, &v, rep)
		case BrokerStatePledged:
			if !g.confirmed(v) {
				rep.AwaitConfirm++
				if v.Changed() {
					g.auditRow(ctx, auditActionReconcile, uid, v.Symbol, "VERDICT_PLEDGED", v.Reason+" — awaiting the confirming read", map[string]any{"evidence": v.Evidence})
				}
				continue
			}
			if !open {
				rep.OutOfWindow++
				continue
			}
			g.pledge(ctx, v, res, rep) // re-asserted every pass; audited only when something changed
		case BrokerStateHeld:
			if !open {
				continue
			}
			g.unpledge(ctx, v, rep) // re-asserted every pass; no-op unless still parked
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
	// Every nomination now comes from a verdict: the verdicts cover every
	// origin='MANTHAN' book lot and every ledger-only lot, and they alone
	// carry the two-read hysteresis. The legacy GHOST class (which also lists
	// USER_MANUAL lots) and trade-execution's GHOST_SUSPECT parks are
	// informational here — a park whose symbol carries no verdict has no
	// ledger net left to heal.
	_ = ledgerParks
}

func detailSuffix(d string) string {
	if d == "" {
		return ""
	}
	return ": " + d
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
		// Confirmed and inside the window: this is exactly when enabled mode
		// would park — say so on every pass (dry_run exists to be read).
		rep.Pledged++
		g.auditRow(ctx, auditActionReconcile, v.UserID, v.Symbol, "WOULD_PLEDGE", "dry_run: "+v.Reason,
			map[string]any{"evidence": v.Evidence, "strategy_id": v.StrategyID})
		return
	}
	var live *liveBook
	if res != nil && res.read != nil {
		live = res.read.live
	}
	result, err := g.actor.Pledge(ctx, v, live)
	if err != nil {
		rep.Errors++
		g.auditRow(ctx, auditActionReconcile, v.UserID, v.Symbol, "PLEDGE_FAILED", err.Error(),
			map[string]any{"evidence": v.Evidence, "result": result})
		return
	}
	if changed, _ := result["changed"].(bool); !changed {
		return // already fully parked — nothing to say again
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
	if changed, _ := result["changed"].(bool); !changed {
		return // was not parked anywhere — the common HELD case
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
	// open SELL, our SELL today, empty holdings, fetch failure) means no action.
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
	if plan.Evidence.TradebookSell {
		plan.ExitReason = ExitReasonManualExit
	}
	if v != nil && v.State == BrokerStateSoldToday {
		plan.ExitReason = ExitReasonManualExit
		if v.ExitPrice > 0 {
			plan.ExitPrice = v.ExitPrice
		}
	}

	if g.mode != ReaperModeEnabled {
		rep.WouldClose++
		g.auditRow(ctx, auditActionReaper, uid, symbol, "WOULD_CLOSE",
			"dry_run: live evidence gate passed — manual flow would allow heal ("+plan.exitReason()+")",
			map[string]any{"evidence": plan.Evidence, "plan": plan.ConfirmationText, "exit_reason": plan.exitReason()})
		return
	}

	if rep.Healed >= rep.cap {
		rep.DeferredCap++
		g.auditRow(ctx, auditActionReaper, uid, symbol, "DEFERRED_CAP",
			fmt.Sprintf("gate passed but this run already healed %d (cap %d) — will heal on a later run", rep.Healed, rep.cap), nil)
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
