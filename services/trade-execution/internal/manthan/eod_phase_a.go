// EOD Phase A — daily 16:35 IST scheduler that pre-stages a protective SL
// for every open Manthan position as an AMO (After-Market Order) on Indira.
//
// Why this exists:
//
//	Indira auto-cancels DAY-validity SL orders at 15:30 IST every trading
//	day (no GTT/GTC support). Without action, every open position sits
//	unprotected from 15:30 until the next morning's 09:14 cron re-arms a
//	fresh SL. That 17-hour gap is the worst-case exposure for an overnight
//	Manthan position; a gap-down at open with no SL queued can blow
//	through the original entry by an arbitrary amount before our morning
//	cron has a chance to react.
//
// What this does:
//
//	At 16:35 IST (comfortably after the 15:30 broker auto-cancel) we walk every OPEN
//	position from manthan_positions, compute the SL trigger from the
//	carried TSL trail (falling back to entry_fill_price × 0.92 when no
//	trail exists yet), DPR-clamp and tick-align it, then submit to Indira
//	as an AMO+SL via BrokerAdapter.PlaceAMOSLSell. The AMO sits in
//	Indira's overnight queue and is released at 09:00 IST the next trading
//	day — meaning the position is protected from the moment the market
//	opens, not from 09:14 onward.
//
// What this does NOT do (deferred to next commit):
//   - Layer 4 retry queue for users whose JWT was expired at 16:35
//   - Earlier JWT-expiry alert at 14:30 IST so users can re-login in time
//     The 09:14 cron remains the per-day fallback for any positions this
//     cycle couldn't arm — see protective_replay.go for the modification
//     that makes the morning cron skip AMOs that already converted to live SL.
//
// Concurrency:
//
//	Runs in the same goroutine as scheduleDaily — sequential per position.
//	We do NOT parallelise broker calls; Indira rate-limits AMO submission
//	and the volume is small (≤ MaxPositions per user, ≤ 25 in production).
//	InsertAMOOrder is idempotent via a partial UNIQUE on (parent_order_id,
//	trade_date) so re-running the cycle after a crash / deploy is safe.
package manthan

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
	"go.uber.org/zap"
)

// EODFallbackSLPct is the SL distance used when a position has no carried
// TSL trail yet (e.g. entry filled less than one TSL-tick window before
// 16:35 IST). Kept in sync with the 09:14 cron's `* 0.92` magic so both
// paths produce the same trigger for the same entry — tomorrow's morning
// cron won't see a mismatch between what EOD pre-staged and what it would
// have placed.
const EODFallbackSLPct = 0.92

// StartEODPhaseA registers the 16:35 IST daily cron. Idempotent if Start()
// already started the 09:14 cron — both use scheduleDaily under the hood.
//
// Catch-up: if the service boots between 16:35 and 17:30 IST on a trading
// day, runs once immediately. The broker accepts AMO submissions until
// ~08:55 IST the next morning; 17:30 IST is a conservative startup window
// that still leaves comfortable broker-side processing margin.
func (p *ProtectiveReplay) StartEODPhaseA(ctx context.Context) {
	p.logger.Info("EOD Phase A scheduler started",
		zap.String("schedule", "16:35 IST · AMO+SL submission for every OPEN position"))

	now := p.now()
	if indiraClient.IsTradingDay(now) {
		minute := now.Hour()*60 + now.Minute()
		if minute >= 16*60+35 && minute < 17*60+30 {
			p.logger.Info("Startup-recovery: running missed EOD Phase A cycle")
			go p.runEODPhaseA(ctx)
		}
	}

	go p.scheduleDaily(ctx, 16, 35, p.runEODPhaseA)
}

// runEODPhaseA is the EOD entry-point.
//
// Per-user freeQty is fetched once and reused for all that user's positions.
// We do NOT consult LTP or CheckCircuit — market is closed; both would be
// stale and AMO orders don't need them. Trigger comes from carried TSL
// state (pos.LatestTrigger) or — when absent — from the entry fill price
// with EODFallbackSLPct applied.
func (p *ProtectiveReplay) runEODPhaseA(ctx context.Context) {
	cycleCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	positions, err := p.repo.ListPositionsNeedingProtection(cycleCtx)
	if err != nil {
		p.logger.Error("EOD Phase A: list positions failed", zap.Error(err))
		return
	}
	if len(positions) == 0 {
		p.logger.Info("EOD Phase A: no positions need overnight protection — exiting")
		return
	}

	tradeDate := p.nextTradingDate(p.now())
	p.logger.Info("EOD Phase A: cycle starting",
		zap.Int("positions", len(positions)),
		zap.Time("for_trade_date", tradeDate))

	// Per-user cache: auth + freeQty (one holdings round-trip per user).
	type userCache struct {
		auth    *BrokerAuth
		freeQty map[string]int
		fetched bool
	}
	cache := map[string]*userCache{}

	var placed, alreadyArmed, skipped, failed int
	for _, pos := range positions {
		uc := cache[pos.UserID]
		if uc == nil {
			uc = &userCache{auth: p.getAuth(pos.UserID)}
			cache[pos.UserID] = uc
		}

		// EDGE: no auth — skip + alert + enqueue Layer-4 retry. The
		// ArmRetryWorker picks this up the moment the user re-logs in
		// (USER_CREDENTIALS_UPDATED Kafka event) or, failing that, at the
		// next 5-minute poll. Layer 4 enqueue is best-effort; if the queue
		// insert fails the 09:14 morning cron is still the per-day fallback.
		if uc.auth == nil {
			p.logger.Warn("EOD Phase A: skipping — no broker auth",
				zap.String("user_id", pos.UserID),
				zap.String("symbol", pos.Symbol))
			p.publishProtectionSkipped(cycleCtx, FirePlan{
				Pos:        pos,
				Action:     FireSkip,
				SkipReason: "EOD: no broker auth — re-login required (queued for Layer-4 retry)",
			})
			if err := p.repo.EnqueueArmRetry(cycleCtx, pos.UserID, pos.EntryOrderID, tradeDate,
				"no broker auth at 16:35 IST"); err != nil {
				p.logger.Warn("EOD Phase A: enqueue arm-retry failed",
					zap.String("user_id", pos.UserID),
					zap.Int64("entry_order_id", pos.EntryOrderID),
					zap.Error(err))
			}
			skipped++
			continue
		}

		// Lazy-load EOD-sellable qty once per user. NOT freeQty — Indian T+1
		// settlement means today's CNC buys have freeQty=0 at 16:35 even
		// though the AMO will execute fine tomorrow morning post-settle.
		// See fetchEODSellableQtyMap docstring for the full rationale.
		if !uc.fetched {
			// EOD stays lenient on a failed fetch (ok=false → nil map): the
			// 08:50 AMO conversion is the authoritative validator, and the
			// freeQty gate below only warns. Morning fire is the strict path.
			uc.freeQty, _ = p.fetchEODSellableQtyMap(cycleCtx, *uc.auth, pos.UserID)
			uc.fetched = true
		}

		safeTrigger, safeLimit, info, skipReason := p.planEODTrigger(cycleCtx, pos, uc.freeQty)
		if skipReason != "" {
			p.publishProtectionSkipped(cycleCtx, FirePlan{
				Pos:        pos,
				Auth:       *uc.auth,
				Info:       info,
				Action:     FireSkip,
				SkipReason: "EOD: " + skipReason,
			})
			skipped++
			continue
		}

		// Insert the AMO row first — idempotent via partial UNIQUE index
		// on (parent_order_id, trade_date) for active statuses. A
		// re-run after crash / deploy returns alreadyExists=true.
		rowID, alreadyExists, err := p.repo.InsertAMOOrder(cycleCtx, pos.EntryOrderID, pos, tradeDate, safeTrigger, safeLimit)
		if err != nil {
			p.logger.Error("EOD Phase A: InsertAMOOrder failed",
				zap.String("symbol", pos.Symbol),
				zap.String("user_id", pos.UserID),
				zap.Error(err))
			failed++
			continue
		}
		if alreadyExists {
			p.logger.Info("EOD Phase A: AMO already armed earlier in window — skipping",
				zap.String("symbol", pos.Symbol),
				zap.String("user_id", pos.UserID),
				zap.Time("trade_date", tradeDate))
			alreadyArmed++
			continue
		}

		// Place AMO+SL with the broker. The `amo: true` flag puts this on
		// the next-session queue, NOT today's book, so it survives the
		// 15:30 cancel sweep.
		brokerID, err := p.broker.PlaceAMOSLSell(cycleCtx, *uc.auth, info, pos.NetQty, safeTrigger, safeLimit)
		if err != nil {
			_ = p.repo.UpdateOrderRejected(cycleCtx, rowID, err.Error())
			_ = p.repo.InsertEvent(cycleCtx, rowID, "EOD_AMO_FAILED", "AMO_PENDING", "REJECTED",
				"", safeTrigger, pos.NetQty, err.Error())
			p.logger.Error("EOD Phase A: broker rejected AMO+SL",
				zap.String("symbol", pos.Symbol),
				zap.String("user_id", pos.UserID),
				zap.Error(err))
			// Layer 4 hook: if the broker rejection is JWT-related,
			// enqueue for retry. The cached `uc.auth` was non-nil at the
			// start of this iteration, so the JWT must have rotated/
			// expired during the call. Other rejections (DPR breach,
			// freeze, etc.) are not recoverable by re-login and we let
			// the alreadyExists branch of InsertAMOOrder handle dedup.
			if errors.Is(err, indiraClient.ErrAuthExpired) {
				if qerr := p.repo.EnqueueArmRetry(cycleCtx, pos.UserID, pos.EntryOrderID, tradeDate,
					"JWT expired mid-AMO submission"); qerr != nil {
					p.logger.Warn("EOD Phase A: enqueue arm-retry after broker reject failed",
						zap.String("user_id", pos.UserID),
						zap.Int64("entry_order_id", pos.EntryOrderID),
						zap.Error(qerr))
				}
			}
			failed++
			continue
		}

		_ = p.repo.UpdateSLBrokerID(cycleCtx, rowID, brokerID, pos.EntryOrderID)
		_ = p.repo.InsertEvent(cycleCtx, rowID, "EOD_AMO_PLACED", "AMO_PENDING", "SL_PLACED",
			"", safeTrigger, pos.NetQty,
			fmt.Sprintf("amo_id=%s trigger=%.2f limit=%.2f trade_date=%s",
				brokerID, safeTrigger, safeLimit, tradeDate.Format("2006-01-02")))

		p.logger.Info("EOD Phase A: AMO+SL queued for next session",
			zap.String("symbol", pos.Symbol),
			zap.String("user_id", pos.UserID),
			zap.String("broker_id", brokerID),
			zap.Float64("trigger", safeTrigger),
			zap.Float64("limit", safeLimit),
			zap.Float64("entry_fill", pos.EntryFillPrice),
			zap.Float64("carried_trail", pos.LatestTrigger),
			zap.Time("trade_date", tradeDate))
		placed++
	}

	p.logger.Info("EOD Phase A: cycle complete",
		zap.Int("placed", placed),
		zap.Int("already_armed", alreadyArmed),
		zap.Int("skipped", skipped),
		zap.Int("failed", failed))
}

// planEODTrigger computes the SL trigger + limit for one EOD position,
// applying the same DPR clamp + tick alignment the 09:14 cron uses.
// Returns a non-empty skipReason when the position can't be protected
// (no symbol info, no holdings, freeQty short, neither trail nor entry
// fill price available).
func (p *ProtectiveReplay) planEODTrigger(
	ctx context.Context, pos PositionNeedingProtection, freeQtyByUserSym map[string]int,
) (trigger, limit float64, info *SymbolInfo, skipReason string) {
	info = p.resolveInfo(ctx, pos)
	if info == nil {
		return 0, 0, nil, "could not resolve symbol info (no indira_symbol / exchange_token)"
	}

	// SSOT is manthan_orders — ListPositionsNeedingProtection already
	// computed pos.NetQty as SUM(FILLED BUY qty) − SUM(FILLED SELL qty).
	// Broker's /v2/holdings is NOT reliable as an AMO gate because it
	// silently drops any symbol whose shares are locked in an active
	// broker SL order (verified live 2026-07-17: 6 of our 11 positions
	// with intraday SLs placed at 15:12 were completely absent from
	// /v2/holdings, causing Phase A to skip them with "position not in
	// broker holdings" and leaving them unprotected overnight).
	//
	// Two failure modes we're intentionally NOT guarding against here:
	//
	//  1. User manually exited via mobile app AND our WSS+REST both
	//     missed the SELL event — the AMO we place tonight fires at
	//     09:15 tomorrow, broker rejects it with "no position," and the
	//     reconciler picks up the drift on its next sweep. No shares are
	//     mis-sold; no market damage.
	//
	//  2. Shares are pledged to broker for margin — same 09:15 rejection
	//     path. Pledging is a manual user action, rare enough that
	//     gating every position against it (with an unreliable API) is a
	//     bad trade-off vs missing SL protection on 6+ positions
	//     nightly.
	//
	// The holdings map is still populated (fetchEODSellableQtyMap runs
	// upstream) and logged for observability so ops can see divergence
	// between broker's view and ours — but nothing here BLOCKS on it.
	if sellableTomorrow, hasHolding := freeQtyByUserSym[strings.ToUpper(pos.Symbol)]; hasHolding && sellableTomorrow < pos.NetQty {
		// Broker DID return the symbol, but with less sellable qty than
		// we hold — suggests a real pledge/margin situation. LOG loudly
		// but don't block; the 09:15 conversion rejection is the correct
		// backstop and this observability trail lets ops spot the pattern.
		p.logger.Warn("EOD Phase A: broker holdings shows less sellable qty than our net — AMO may reject at 09:15",
			zap.String("user_id", pos.UserID),
			zap.String("symbol", pos.Symbol),
			zap.Int("net_qty_ours", pos.NetQty),
			zap.Int("broker_sellable", sellableTomorrow))
	}

	// Trigger preference, in order:
	//   1. Carried TSL trail from prior SL rows (pos.LatestTrigger)
	//   2. Entry fill price × EODFallbackSLPct
	//
	// We deliberately do NOT use LTP as a fallback — at 16:35 IST the
	// LTP is the day's close, which has nothing to do with our entry
	// basis. A drifted close would either tighten the SL well above
	// entry (cutting position prematurely) or loosen it (giving up the
	// risk-management edge). Entry price is the right anchor.
	intended := pos.LatestTrigger
	if intended <= 0 {
		if pos.EntryFillPrice > 0 {
			intended = pos.EntryFillPrice * EODFallbackSLPct
		} else {
			return 0, 0, info, "no LatestTrigger AND entry_fill_price not recorded — can't compute trigger"
		}
	}

	// Option B: DEFER if the intended 20% stop is below the DPR floor. We do NOT
	// place a premature band-floor stop (which could exit at ~band% on a circuit
	// touch). The position can't reach the intended level in one session, and a
	// later cycle re-attempts once the band re-centers low enough. The caller
	// treats a non-empty reason as a clean skip + ProtectionSkipped event.
	if info.DPRLower > 0 {
		floor := info.DPRLower * DPRSafetyBuffer
		if intended < floor {
			return 0, 0, info, fmt.Sprintf("intended SL %.2f below DPR floor %.2f — deferred (unreachable next session; replay places at 20%% when band re-centers)", intended, floor)
		}
	}
	safeTrigger := p.broker.roundAndClamp(intended, info.TickSize, info.DPRLower, info.DPRUpper)
	safeLimit := p.broker.roundAndClamp(
		safeTrigger-SLLimitGap(safeTrigger, info.TickSize),
		info.TickSize, info.DPRLower, info.DPRUpper)

	return safeTrigger, safeLimit, info, ""
}

// RunEODPhaseANow runs one EOD Phase A cycle synchronously, bypassing the
// 16:35 cron schedule. Used by ArmRetryWorker when a user re-logs in and
// has PENDING retry rows queued: re-running the full cycle is idempotent
// (InsertAMOOrder dedups already-armed positions via the partial UNIQUE
// index on (parent_order_id, trade_date)), so already-protected positions
// for other users skip cleanly and only the retry-target positions get
// fresh AMOs.
func (p *ProtectiveReplay) RunEODPhaseANow(ctx context.Context) {
	p.runEODPhaseA(ctx)
}

// nextTradingDate returns the IST date of the next trading session strictly
// after `now`. Used to stamp the AMO row's trade_date so subsequent cycles
// (and the morning skip-if-protected check) match by exact date.
// nextTradingDate returns the SESSION an AMO submitted at `now` will enter —
// which is what trade_date must record. Indira releases queued AMOs at 09:00
// of the next session to open, so:
//   - after the 15:30 close on a trading day → the next trading day;
//   - BEFORE the close (00:03 arm-retry, 08:50 pre-open, a Saturday) → the
//     first trading day that is today-or-later.
//
// The previous version was calendar-tomorrow unconditionally. The
// ArmRetryWorker re-runs this cycle every 5 min around the clock while a
// user is queued, so its 00:03 IST cycle stamped AMOs that really entered
// TODAY's session with TOMORROW's date. Consequences seen 2026-08-18: the
// same-evening 16:35 cycle then found "already armed" for tomorrow (a dead,
// swept order) and placed nothing; the 09:14 morning cron trusted the same
// row and skipped; and yesterday's correctly-dated attempts had all died on
// the signal_id UNIQUE (see InsertAMOOrder) — the book was protected only
// by accident. Callers that need the label of a *manual* mid-session run
// get today, which is also what the broker does with it.
func (p *ProtectiveReplay) nextTradingDate(now time.Time) time.Time {
	n := now.In(p.ist)
	d := time.Date(n.Year(), n.Month(), n.Day(), 0, 0, 0, 0, p.ist)
	closeAt := d.Add(15*time.Hour + 30*time.Minute)
	if !n.Before(closeAt) {
		d = d.Add(24 * time.Hour)
	}
	for !indiraClient.IsTradingDay(d) {
		d = d.Add(24 * time.Hour)
	}
	return d
}

// ─────────────────────────────────────────────────────────────────────────
// RunOnceForUser — Layer 5 manual-trigger entry point.
// ─────────────────────────────────────────────────────────────────────────

// RunOnceForUser is the operator-override path. Fires the standard 09:14
// hot-SL placement immediately for ONE user's positions only.
//
// Used when:
//   - The 09:14 cron skipped a user because their JWT was expired at the
//     time, but they've since re-logged in.
//   - EOD Phase A failed for a user (audit shows EOD_AMO_FAILED) and the
//     operator wants to re-attempt mid-session after diagnosis.
//   - The same-day TSL trail moved high enough that the operator wants to
//     nudge the SL up manually without waiting for the next trail tick.
//
// Returns nil if at least one position was processed (placed OR skipped
// with a reason), so the HTTP layer can treat "soft" outcomes as 200 OK.
// Returns a non-nil error only on hard infrastructure failures.
func (p *ProtectiveReplay) RunOnceForUser(ctx context.Context, userID string) error {
	if userID == "" {
		return errors.New("user_id is required")
	}
	p.logger.Info("Manual replay requested", zap.String("user_id", userID))

	plans := p.buildPlans(ctx)
	filtered := plans[:0]
	for _, pl := range plans {
		if pl.Pos.UserID == userID {
			filtered = append(filtered, pl)
		}
	}
	if len(filtered) == 0 {
		p.logger.Info("Manual replay: no positions for user — nothing to do",
			zap.String("user_id", userID))
		return nil
	}

	results := p.fireAll(ctx, filtered)
	p.reconcile(ctx, results)

	var ok, skip, fail int
	for _, r := range results {
		switch {
		case r.Err != nil:
			fail++
		case r.Action == FireSkip:
			skip++
		default:
			ok++
		}
	}
	p.logger.Info("Manual replay complete",
		zap.String("user_id", userID),
		zap.Int("placed", ok),
		zap.Int("skipped", skip),
		zap.Int("failed", fail))
	return nil
}
