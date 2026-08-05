package manthan

import (
	"context"
	"database/sql"
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
}

// SkipReason records why a signal was not allocated.
type SkipReason struct {
	Symbol string
	Reason string
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
	result := &AllocateResult{}

	// Allocate touches portfolio.Positions + portfolio.Cooldown (delete on
	// re-entry) and is called rarely (once per signal batch — handful of
	// times per day). Holding exclusive Mu for the duration is simpler than
	// snapshotting + rechecking, and contention is negligible.
	portfolio.Mu.Lock()
	defer portfolio.Mu.Unlock()

	openSlots := int(portfolio.MaxPositions) - countActive(portfolio.Positions)
	if openSlots <= 0 {
		a.logger.Info("Portfolio full, no open slots",
			zap.String("user", portfolio.UserID),
			zap.Int("positions", countActive(portfolio.Positions)))
		return result
	}

	caps := types.NewCapCheck(portfolio.MaxPositions, portfolio.Positions)
	perCallBase := portfolio.CurrentCapital / float64(portfolio.MaxPositions)

	for _, sig := range signals {
		if openSlots <= 0 {
			break
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
				Symbol: sig.Symbol, Reason: reason,
			})
			continue
		}

		// Skip if a user-override cooldown is active. Set by the projector
		// when MANUAL_EXIT_DETECTED fires — user manually exited recently,
		// so respect their intent for the override window (default 3 days).
		// Per (strategy, symbol). Other users / other symbols are unaffected.
		if blocked, until := a.isUserOverrideActive(portfolio.StrategyID, sig.Symbol); blocked {
			result.Skipped = append(result.Skipped, SkipReason{
				Symbol: sig.Symbol,
				Reason: "user_override_active until " + until.Format(time.RFC3339),
			})
			continue
		}

		// Skip if in cooldown (SL exit, waiting for 20% ATH correction)
		if cd, ok := portfolio.Cooldown[sig.Symbol]; ok {
			if sig.LatestPrice > cd.ReentryBelow {
				result.Skipped = append(result.Skipped, SkipReason{
					Symbol: sig.Symbol,
					Reason: "in cooldown — price hasn't corrected 20% from ATH yet",
				})
				continue
			}
			// Price corrected enough — remove from cooldown, allow re-entry
			delete(portfolio.Cooldown, sig.Symbol)
		}

		// Sector + MCap cap check
		ok, reason := caps.CanAdd(sig.Industry, sig.MCapBucket)
		if !ok {
			result.Skipped = append(result.Skipped, SkipReason{
				Symbol: sig.Symbol, Reason: reason,
			})
			continue
		}

		// EMA allocation for this stock's index
		emaAlloc := emaByIndex[sig.IndexName]
		if emaAlloc <= 0 {
			result.Skipped = append(result.Skipped, SkipReason{
				Symbol: sig.Symbol,
				Reason: "EMA allocation 0% for index " + sig.IndexName,
			})
			continue
		}

		perCallActual := perCallBase * emaAlloc
		entryPrice := sig.LatestPrice
		if entryPrice <= 0 {
			result.Skipped = append(result.Skipped, SkipReason{
				Symbol: sig.Symbol, Reason: "latest_price is 0",
			})
			continue
		}

		// Adjust entry for transaction cost
		effectiveEntry := entryPrice * (1 + types.TotalTxnCostPct())
		qty := int32(perCallActual / effectiveEntry)
		if qty <= 0 {
			result.Skipped = append(result.Skipped, SkipReason{
				Symbol: sig.Symbol,
				Reason: "quantity = 0 (per_call too small for stock price)",
			})
			continue
		}

		// TEST MODE: 2% SL to exercise trailing logic quickly.
		// Original (prod): initialSL := entryPrice * 0.80 // 20% below entry
		initialSL := entryPrice * 0.98 // 2% below entry (TEST)

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
			RunDate:       sig.RunDate,
		}

		result.Allocations = append(result.Allocations, alloc)
		caps.Add(sig.Industry, sig.MCapBucket)
		openSlots--

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

// SortAlphabetical sorts signals alphabetically by symbol. Used when choosing
// which stock fills a freed slot after an exit (spec: "consider stocks
// alphabetically in case any stock exits").
func SortAlphabetical(signals []types.ManthanSignal) {
	sort.Slice(signals, func(i, j int) bool {
		return signals[i].Symbol < signals[j].Symbol
	})
}

func countActive(positions map[string]*types.Position) int {
	n := 0
	for _, p := range positions {
		if p.Active {
			n++
		}
	}
	return n
}
