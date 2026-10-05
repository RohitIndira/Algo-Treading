package manthan

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"time"

	"go.uber.org/zap"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/manthan/types"
)

// Allocator takes eligible ManthanSignals and a user's portfolio, then decides
// which stocks to enter and how much capital to deploy per stock.
//
// Allocation rules (from Manthan spec):
//   - Sector cap: ≤25% of max_positions per industry
//   - MCap bucket cap: ≤50% of max_positions per bucket (LARGE/MID/SMALL)
//   - FCFS order (sheet arrival order), alphabetical tie-break on exit
//   - EMA position sizing: per_call × index EMA allocation %
//   - Max 25 positions (≤25L capital) or 50 (>25L)
//   - Skip stocks already held or in cooldown
//   - Skip stocks with active user_override_until (manual exit cooldown — 3d)
//   - Transaction cost: 0.05% slippage + 0.28% brokerage
type Allocator struct {
	logger *zap.Logger
	// db is optional. When set, Allocate consults manthan_signal_decisions
	// to see if the user manually exited this (strategy, symbol) recently —
	// in which case we skip re-entry until user_override_until expires.
	// Nil-safe: a nil db simply skips the override check.
	db *sql.DB
}

func NewAllocator(logger *zap.Logger) *Allocator {
	return &Allocator{logger: logger}
}

// SetDB attaches the trading_db connection used for the user-override
// cooldown check. Wired during startup; nil-safe to leave unset.
func (a *Allocator) SetDB(db *sql.DB) {
	a.db = db
}

// isUserOverrideActive reports whether the user manually exited this
// (strategy, symbol) within the override window (default 3 days, set by the
// projector on MANUAL_EXIT_DETECTED). Returns the deadline timestamp so the
// caller can include it in the skip reason for clear logs.
//
// Indexed query — partial unique on (strategy_id, symbol, user_override_until)
// where user_override_until IS NOT NULL — sub-millisecond.
func (a *Allocator) isUserOverrideActive(strategyID, symbol string) (bool, time.Time) {
	if a.db == nil {
		return false, time.Time{}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	var until time.Time
	err := a.db.QueryRowContext(ctx, `
		SELECT user_override_until
		FROM manthan_signal_decisions
		WHERE strategy_id = $1
		  AND symbol      = $2
		  AND user_override_until IS NOT NULL
		  AND user_override_until > NOW()
		ORDER BY user_override_until DESC
		LIMIT 1`, strategyID, symbol).Scan(&until)
	if err == sql.ErrNoRows {
		return false, time.Time{}
	}
	if err != nil {
		// On query failure, fail OPEN (allow the buy) rather than blocking
		// the entire pipeline. We log so an operator can investigate.
		a.logger.Warn("user-override check failed — allowing entry",
			zap.String("strategy", strategyID),
			zap.String("symbol", symbol),
			zap.Error(err))
		return false, time.Time{}
	}
	return true, until
}

// AllocateResult holds the full output of one allocation run.
type AllocateResult struct {
	Allocations []types.AllocationResult
	Skipped     []SkipReason

	// Flexi caps (2026-09-30). Both stay zero-valued whenever the feature is
	// off or fx == nil — the golden off-mode identity test pins that.
	//   FlexiEvals      — one FLEXI_EVAL per signal whose bucket was in play
	//                     (dry_run: prediction; on: fact). Consumer persists.
	//   FlexiNotApplied — guard reason when a plan was attempted and not
	//                     built ("" when built, or when flexi was not tried).
	FlexiEvals      []types.FlexiEval
	FlexiNotApplied string
}

// FlexiInput is the per-call flexi context (nil ⇒ feature off). Alias so
// callers in this package don't spell the types path.
type FlexiInput = types.FlexiInput

// portfolioFullReason is the SkipReason text for "no open slot" — emitted
// for every signal when the book is full at call start and for every
// signal left after the mid-loop break (P0.2, 2026-09-30). Before this the
// allocator returned silently and the one regret flexi can create (a late
// LARGE/MID arrival after SMALL borrowed) had no audit row.
//
// GATED on flexi being enabled (fx != nil && fx.Cfg.Enabled()): with the
// feature off the allocator still returns / breaks silently, exactly as
// before, so off-mode writes no new decision rows and no new Kafka events
// (design §7.8 leaves shipping it ungated as an owner decision — until that
// is made, the row only appears in dry_run / on).
func portfolioFullReason(held int, maxPositions int32) string {
	return fmt.Sprintf("portfolio full (%d/%d slots)", held, maxPositions)
}

// holdingReason mirrors the "already holding" guard in the main loop: a
// symbol the strategy tracks in ANY non-exited state. Used by the
// portfolio-full paths so a held symbol never carries a misleading
// "portfolio full" audit row (PublishSignalSkip is first-write-wins per
// (strategy, symbol, run_date)).
func holdingReason(positions map[string]*types.Position, symbol string) (string, bool) {
	pos, ok := positions[symbol]
	if !ok || pos.State == types.StateExited {
		return "", false
	}
	if !pos.Active {
		return "already holding (" + string(pos.State) + ")", true
	}
	return "already holding", true
}

// portfolioFullSkip is the audit SkipReason for one signal that found no
// open slot: "already holding …" when the strategy holds it, else
// "portfolio full (held/N slots)".
func portfolioFullSkip(portfolio *types.Portfolio, sig types.ManthanSignal, held int) SkipReason {
	if reason, ok := holdingReason(portfolio.Positions, sig.Symbol); ok {
		return SkipReason{Symbol: sig.Symbol, Signal: sig, Reason: reason}
	}
	return SkipReason{Symbol: sig.Symbol, Signal: sig, Reason: portfolioFullReason(held, portfolio.MaxPositions)}
}

// SkipReason records why a signal was not allocated. Carries the full
// signal so the consumer can write an auditable decision row + Kafka
// event without re-fetching anything (2026-09-24: skips used to be
// log-only, invisible to /trace and the ops dashboards).
type SkipReason struct {
	Symbol string
	Reason string
	Signal types.ManthanSignal
}

// Allocate processes eligible signals against a user's portfolio and EMA allocations.
// emaByIndex maps index name (e.g., "NIFTY50") → allocation fraction (0.0–1.0).
// Signals are processed in arrival order (FCFS). Alphabetical tie-break is used
// only when a position exits and multiple stocks compete for the freed slot.
func (a *Allocator) Allocate(
	signals []types.ManthanSignal,
	portfolio *types.Portfolio,
	emaByIndex map[string]float64,
) *AllocateResult {
	return a.AllocateWithFlexi(signals, portfolio, emaByIndex, nil)
}

// AllocateWithFlexi is Allocate plus the flexi-caps context. fx == nil (or
// mode off) is the pre-flexi path, byte-for-byte: same AllocateResult, same
// skip strings, no plan, no evals.
//
// Mode on: the plan's ceilings are applied to the live CapCheck; an entry
// above base carries AllocationResult.Flexi.
//
// Mode dry_run: the live decision uses BASE caps exactly as off-mode. The
// plan is computed against a CLONED CapCheck overlaid with the strategy's
// shadow book (earlier WOULD_ALLOCATEs today) and each in-play signal is
// evaluated against that clone, including the real EMA / price / qty tail.
// Real decisions are never influenced by the shadow (design §2.5 — the
// shadow models on-mode; it must not restrict the live book).
func (a *Allocator) AllocateWithFlexi(
	signals []types.ManthanSignal,
	portfolio *types.Portfolio,
	emaByIndex map[string]float64,
	fx *FlexiInput,
) *AllocateResult {
	result := &AllocateResult{}

	// Allocate touches portfolio.Positions + portfolio.Cooldown (delete on
	// re-entry) and is called rarely (once per signal batch — handful of
	// times per day). Holding exclusive Mu for the duration is simpler than
	// snapshotting + rechecking, and contention is negligible.
	portfolio.Mu.Lock()
	defer portfolio.Mu.Unlock()

	// P0.2 audit rows exist only while flexi is enabled (see
	// portfolioFullReason). Off / nil input ⇒ silent return + break, as
	// before — pinned by TestAllocator_OffModeGoldenIdentity.
	auditFull := fx != nil && fx.Cfg.Enabled()

	held := countActive(portfolio.Positions)
	openSlots := int(portfolio.MaxPositions) - held
	if openSlots <= 0 {
		a.logger.Info("Portfolio full, no open slots",
			zap.String("user", portfolio.UserID),
			zap.Int("positions", countActive(portfolio.Positions)))
		if auditFull {
			for _, sig := range signals {
				result.Skipped = append(result.Skipped, portfolioFullSkip(portfolio, sig, held))
			}
		}
		return result
	}

	caps := types.NewCapCheck(portfolio.MaxPositions, portfolio.Positions)
	perCallBase := portfolio.CurrentCapital / float64(portfolio.MaxPositions)

	// ── Flexi plan (one per call; nil on any guard → base caps) ──────────
	//
	// The plan is computed ONCE for the whole call. The live path hands the
	// allocator one signal per call, so there the plan is always fresh. In a
	// multi-signal batch (CatchUpNewStrategy — dead today under the creation
	// gate) the ceiling stays fixed while the batch fills: a receiver signal
	// that lands AT the ceiling inside the batch is blocked with the flexi
	// string, whereas the one-signal-per-call path recomputes Spare 0 → no
	// plan → the legacy string. Cosmetic divergence in the skip text only;
	// the grants themselves are identical (Σ grants ≤ Spare either way).
	var plan *types.FlexiPlan
	var evalCaps *types.CapCheck // caps the FLEXI_EVAL outcomes are judged on
	evalOpenSlots := openSlots
	flexiMode := types.FlexiOff
	if fx != nil && fx.Cfg.Enabled() && len(signals) > 0 {
		flexiMode = fx.Cfg.Mode
		var why string
		switch flexiMode {
		case types.FlexiOn:
			plan, why = types.BuildFlexiPlan(fx, caps, portfolio.MaxPositions, held,
				portfolio.Positions, nil, signals)
			if plan != nil {
				caps.BucketCeiling = plan.Ceiling
				caps.FlexiDonors = plan.Donors
				evalCaps = caps
			}
		case types.FlexiDryRun:
			shadowCaps, shadowN, shadowSyms := types.OverlayShadow(caps, fx.Shadow, portfolio.Positions)
			plan, why = types.BuildFlexiPlan(fx, shadowCaps, portfolio.MaxPositions, held+shadowN,
				portfolio.Positions, shadowSyms, signals)
			if plan != nil {
				shadowCaps.BucketCeiling = plan.Ceiling
				shadowCaps.FlexiDonors = plan.Donors
				evalCaps = shadowCaps
				evalOpenSlots = openSlots - shadowN
			}
		}
		if plan == nil {
			result.FlexiNotApplied = why
		} else {
			a.logger.Info("Flexi plan built",
				zap.String("strategy", portfolio.StrategyID),
				zap.String("mode", string(flexiMode)),
				zap.Any("ceiling", plan.Ceiling),
				zap.Strings("donors", plan.Donors),
				zap.Any("base_claim", plan.BaseClaim),
				zap.Int("spare", plan.SpareBefore),
				zap.Int("free", plan.Free))
		}
	}

	for _, sig := range signals {
		if openSlots <= 0 {
			if !auditFull {
				break // pre-flexi behaviour: the rest of the batch is dropped silently
			}
			// P0.2 (flexi enabled): the rest of the batch is auditable.
			result.Skipped = append(result.Skipped, portfolioFullSkip(portfolio, sig, int(portfolio.MaxPositions)))
			continue
		}

		// Skip if this strategy already tracks the symbol in ANY non-exited
		// state. Requiring pos.Active alone re-bought held symbols every
		// morning: fill confirmations were never wired (2026-08-05), so every
		// position sat PENDING_ENTRY/Active=false forever, the guard never
		// fired, and day-2 signals doubled 7 live positions (~₹44k extra).
		// PENDING_ENTRY / PARTIALLY_FILLED must block re-entry just as hard
		// as ACTIVE — an unconfirmed entry is still deployed capital.
		if pos, ok := portfolio.Positions[sig.Symbol]; ok && pos.State != types.StateExited {
			reason := "already holding"
			if !pos.Active {
				reason = "already holding (" + string(pos.State) + ")"
			}
			result.Skipped = append(result.Skipped, SkipReason{
				Symbol: sig.Symbol, Signal: sig, Reason: reason,
			})
			continue
		}

		// Skip if a user-override cooldown is active. Set by the projector
		// when MANUAL_EXIT_DETECTED fires — user manually exited recently,
		// so respect their intent for the override window (default 3 days).
		// Per (strategy, symbol). Other users / other symbols are unaffected.
		if blocked, until := a.isUserOverrideActive(portfolio.StrategyID, sig.Symbol); blocked {
			result.Skipped = append(result.Skipped, SkipReason{
				Symbol: sig.Symbol, Signal: sig,
				Reason: "user_override_active until " + until.Format(time.RFC3339),
			})
			continue
		}

		// Skip if in cooldown (SL exit, waiting for 20% ATH correction)
		if cd, ok := portfolio.Cooldown[sig.Symbol]; ok {
			if sig.LatestPrice > cd.ReentryBelow {
				result.Skipped = append(result.Skipped, SkipReason{
					Symbol: sig.Symbol, Signal: sig,
					Reason: "in cooldown — price hasn't corrected 20% from ATH yet",
				})
				continue
			}
			// Price corrected enough — remove from cooldown, allow re-entry
			delete(portfolio.Cooldown, sig.Symbol)
		}

		// Sector + MCap cap check
		ok, reason := caps.CanAdd(sig.Industry, sig.MCapBucket)

		// FLEXI_EVAL — only when this signal's bucket is in play under the
		// plan. Dry-run judges the shadow clone (and grows it on a
		// WOULD_ALLOCATE so a multi-signal batch is self-consistent); on-mode
		// judges the live caps, so the eval and the real decision agree.
		if plan.InPlay(sig.MCapBucket) {
			ev := a.evalFlexi(fx, plan, evalCaps, evalOpenSlots, sig, reason, perCallBase, emaByIndex, portfolio.StopLossPct)
			result.FlexiEvals = append(result.FlexiEvals, ev)
			if flexiMode == types.FlexiDryRun && ev.Outcome == types.FlexiWouldAllocate {
				evalCaps.Add(sig.Industry, sig.MCapBucket)
				evalOpenSlots--
			}
		}

		if !ok {
			result.Skipped = append(result.Skipped, SkipReason{
				Symbol: sig.Symbol, Signal: sig, Reason: reason,
			})
			continue
		}

		// EMA allocation for this stock's index
		emaAlloc := emaByIndex[sig.IndexName]
		if emaAlloc <= 0 {
			result.Skipped = append(result.Skipped, SkipReason{
				Symbol: sig.Symbol, Signal: sig,
				Reason: "EMA allocation 0% for index " + sig.IndexName,
			})
			continue
		}

		perCallActual := perCallBase * emaAlloc
		entryPrice := sig.LatestPrice
		if entryPrice <= 0 {
			result.Skipped = append(result.Skipped, SkipReason{
				Symbol: sig.Symbol, Signal: sig, Reason: "latest_price is 0",
			})
			continue
		}

		// Adjust entry for transaction cost
		effectiveEntry := entryPrice * (1 + types.TotalTxnCostPct())
		qty := int32(perCallActual / effectiveEntry)
		if qty <= 0 {
			result.Skipped = append(result.Skipped, SkipReason{
				Symbol: sig.Symbol, Signal: sig,
				Reason: fmt.Sprintf("quantity = 0 (per_call ₹%.0f < effective price ₹%.2f)", perCallActual, effectiveEntry),
			})
			continue
		}

		// Initial stop = strategy's StopLossPct below entry (20% for
		// Manthan). MUST come from config, never a literal: a leftover
		// "TEST MODE" 0.98 here shipped to production and produced every
		// phantom TSL exit of 2026-08-18 (see types.Portfolio.StopLossPct).
		initialSL := entryPrice * (1 - bucketStopLossPct(sig.MCapBucket, portfolio.StopLossPct)/100)

		alloc := types.AllocationResult{
			Symbol:        sig.Symbol,
			Industry:      sig.Industry,
			MCapBucket:    sig.MCapBucket,
			IndexName:     sig.IndexName,
			EMAAllocPct:   emaAlloc,
			PerCallBase:   perCallBase,
			PerCallActual: perCallActual,
			EntryPrice:    entryPrice,
			Quantity:      qty,
			InitialSL:     initialSL,
			ATHClose:      sig.ATHClose,
			Week52High:    sig.Week52High,
			ISIN:          sig.ISIN,
			// Carry the source signal's run_date all the way to OrderGenerator so
			// the deterministic signal_id can be anchored to the SEMANTIC trade day
			// (not wall-clock). See types/allocation.go RunDate doc.
			RunDate: sig.RunDate,
		}

		// On-mode only: an entry that passed CanAdd while its bucket was
		// already AT/ABOVE base took a borrowed slot → record the grant. The
		// stock's MCapBucket / InitialSL above are untouched (rule follows the
		// stock, not the seat). Never set in dry_run or off.
		if plan != nil && flexiMode == types.FlexiOn && caps.BucketCount[sig.MCapBucket] >= caps.MaxPerBucket {
			g := plan.GrantFor(sig.MCapBucket, caps)
			alloc.Flexi = &g
		}

		result.Allocations = append(result.Allocations, alloc)
		caps.Add(sig.Industry, sig.MCapBucket)
		openSlots--

		// Dry-run: a REAL allocation also lands in the shadow world (the
		// shadow clone models on-mode, whose book would hold this entry too),
		// so a later eval in the same multi-signal batch is judged against a
		// clone that already has the seat. In on-mode evalCaps IS caps, so
		// the Add above already covered it. Live path is one signal per call;
		// this matters only for CatchUpNewStrategy-style batches.
		if plan != nil && flexiMode == types.FlexiDryRun && evalCaps != nil {
			evalCaps.Add(sig.Industry, sig.MCapBucket)
			evalOpenSlots--
		}

		a.logger.Debug("Allocated",
			zap.String("symbol", sig.Symbol),
			zap.String("index", sig.IndexName),
			zap.Float64("ema_pct", emaAlloc),
			zap.Float64("per_call", perCallActual),
			zap.Int32("qty", qty),
			zap.Float64("sl", initialSL),
		)
	}

	return result
}

// evalFlexi produces the FLEXI_EVAL record for one signal whose bucket is in
// play. evalCaps is the shadow clone in dry-run (already carrying the plan's
// ceilings) and the live caps in on-mode. The decision ladder mirrors the
// real allocator exactly — sector first, slots, base, ceiling, then the real
// EMA / price / qty tail with the same perCallBase — so a dry-run
// WOULD_ALLOCATE is what on-mode would have bought (first order).
func (a *Allocator) evalFlexi(
	fx *FlexiInput,
	plan *types.FlexiPlan,
	evalCaps *types.CapCheck,
	evalOpenSlots int,
	sig types.ManthanSignal,
	baseReason string,
	perCallBase float64,
	emaByIndex map[string]float64,
	stopLossPct float64,
) types.FlexiEval {
	b := sig.MCapBucket
	ev := types.FlexiEval{
		Mode:        fx.Cfg.Mode,
		Symbol:      sig.Symbol,
		RunDate:     sig.RunDate,
		Bucket:      b,
		Industry:    sig.Industry,
		IndexName:   sig.IndexName,
		ISIN:        sig.ISIN,
		LatestPrice: sig.LatestPrice,
		BaseOutcome: baseReason,
		EvalHeld:    evalCaps.BucketCount[b],
		EvalLimit:   evalCaps.BucketLimit(b),
		Plan:        plan,
		Config:      fx.Cfg.Summary(),
	}
	dry := fx.Cfg.Mode == types.FlexiDryRun

	switch {
	case evalCaps.SectorCount[sig.Industry] >= evalCaps.MaxPerSector:
		ev.Outcome = types.FlexiBlockedSector
	case dry && evalOpenSlots <= 0:
		ev.Outcome = types.FlexiWouldBlockPortfolioFull
	case evalCaps.BucketCount[b] < evalCaps.MaxPerBucket:
		// Fits under its own base cap — flexi makes no difference here.
		ev.Outcome = types.FlexiNoDifference
	case evalCaps.BucketCount[b] >= ev.EvalLimit:
		if dry {
			ev.Outcome = types.FlexiWouldBlockFlexiCeiling
		} else {
			ev.Outcome = types.FlexiBlockedFlexiCeiling
		}
	default:
		// Borrowed slot admitted by the ceiling — simulate the real tail.
		emaAlloc := emaByIndex[sig.IndexName]
		switch {
		case emaAlloc <= 0:
			ev.Outcome = types.FlexiWouldFailTail
			ev.TailReason = "EMA allocation 0% for index " + sig.IndexName
		case sig.LatestPrice <= 0:
			ev.Outcome = types.FlexiWouldFailTail
			ev.TailReason = "latest_price is 0"
		default:
			perCallActual := perCallBase * emaAlloc
			effectiveEntry := sig.LatestPrice * (1 + types.TotalTxnCostPct())
			qty := int32(perCallActual / effectiveEntry)
			if qty <= 0 {
				ev.Outcome = types.FlexiWouldFailTail
				ev.TailReason = fmt.Sprintf("quantity = 0 (per_call ₹%.0f < effective price ₹%.2f)", perCallActual, effectiveEntry)
				break
			}
			ev.Sim = &types.FlexiSim{
				EMAAllocPct: emaAlloc,
				PerCallBase: perCallBase,
				Quantity:    qty,
				Invested:    float64(qty) * sig.LatestPrice,
				InitialSL:   sig.LatestPrice * (1 - bucketStopLossPct(b, stopLossPct)/100),
			}
			if dry {
				ev.Outcome = types.FlexiWouldAllocate
			} else {
				ev.Outcome = types.FlexiGranted
			}
		}
	}
	return ev
}

// SortAlphabetical sorts signals alphabetically by symbol. Used when choosing
// which stock fills a freed slot after an exit (spec: "consider stocks
// alphabetically in case any stock exits").
func SortAlphabetical(signals []types.ManthanSignal) {
	sort.Slice(signals, func(i, j int) bool {
		return signals[i].Symbol < signals[j].Symbol
	})
}

// countActive returns the number of OCCUPIED book slots — fill-confirmed
// positions PLUS dispatched-but-unfilled ones (see types.Position.Occupies).
// The name is historical; the semantics changed 2026-08-18 after a morning
// batch over-allocated because pendings were invisible.
func countActive(positions map[string]*types.Position) int {
	n := 0
	for _, p := range positions {
		if p.Occupies() {
			n++
		}
	}
	return n
}
