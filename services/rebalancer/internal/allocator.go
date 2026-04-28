package internal

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

// txnCostFraction is the Manthan transaction-cost guard: 0.05% slippage +
// 0.28% brokerage = 0.33% padding above market price when computing qty
// (so we don't get a partial fill because rounding ate our slack).
const txnCostFraction = 0.0033

// minPositionRupees prevents the allocator from emitting orders too small to
// be useful (qty=0 or 1 share of a ₹50k stock). 100 rupees minimum.
const minPositionRupees = 100.0

// TopUpExistingPositions runs BEFORE Allocate. For each active position whose
// per-stock target (per_call × ema_target_for_index) exceeds its currently-
// invested rupees, generate an additional BUY order to fill the gap.
//
// Manthan rule: EMA going DOWN is NEVER a sell trigger — the only exit path
// is the 20% trailing SL. So gaps are only positive (top up) or skipped.
//
// Top-up entries reuse the MANTHAN_ENTRY pipeline but carry
// TopUpForSignalID = parent's signal_id, which:
//   - tells trade-execution's entry_handler to skip its "already holding"
//     duplicate check
//   - tells rules-engine's projector to ADD qty + invested onto the parent
//     manthan_positions row instead of INSERT-ing a new row
//
// The function MUTATES the working snapshot's deployed[idx], holding map,
// and per-position invested_amt so Allocate (Phase 2) sees the post-topup
// state and won't double-deploy capital.
func TopUpExistingPositions(snap *PortfolioSnapshot, logger *zap.Logger) (topups []PlannedEntry, skipped []SkipReason) {
	if snap.PerCall <= 0 || len(snap.ActivePositions) == 0 {
		return nil, nil
	}

	// Sort positions alphabetically — Manthan FCFS tie-break rule.
	ordered := make([]PositionSummary, len(snap.ActivePositions))
	copy(ordered, snap.ActivePositions)
	sort.SliceStable(ordered, func(i, j int) bool {
		return ordered[i].Symbol < ordered[j].Symbol
	})

	// Track running available cash so we don't generate multiple top-ups
	// that together exceed broker margin. We use Cash (fund-limit) as the
	// budget — Holdings/PositionsValue is already locked to existing shares.
	availableCash := snap.Capital.Cash
	if snap.Capital.Source == "db_fallback" {
		// Without real broker margin we can't meaningfully size top-ups
		// against cash. Use conservatively: assume zero cash → no top-ups.
		// Caller should retry with valid auth.
		logger.Warn("TopUp skipped — capital from db_fallback (no real broker cash)",
			zap.String("user", snap.Cfg.UserID))
		return nil, nil
	}

	for i := range ordered {
		p := &ordered[i]
		if p.SignalID == "" {
			// Pre-CQRS legacy row without signal_id. Can't address parent.
			skipped = append(skipped, SkipReason{
				Symbol: p.Symbol,
				Reason: "topup: position has no signal_id (pre-CQRS legacy)",
			})
			continue
		}
		emaTarget, ok := snap.EMATargets[p.IndexName]
		if !ok || emaTarget <= 0 {
			skipped = append(skipped, SkipReason{
				Symbol: p.Symbol,
				Reason: fmt.Sprintf("topup: EMA target = 0 for %s", p.IndexName),
			})
			continue
		}

		targetPerStock := snap.PerCall * emaTarget
		gap := targetPerStock - p.InvestedAmt
		if gap <= 0 {
			skipped = append(skipped, SkipReason{
				Symbol: p.Symbol,
				Reason: fmt.Sprintf("topup: position already at-or-above target (invested=%.0f, target=%.0f)",
					p.InvestedAmt, targetPerStock),
			})
			continue
		}

		// Use broker-reported LTP if we have it; fall back to entry-based
		// estimate (invested ÷ qty) — least-bad if LTP is missing.
		price, hasLTP := snap.LivePrices[strings.ToUpper(strings.TrimSpace(p.Symbol))]
		if !hasLTP || price <= 0 {
			if p.Quantity > 0 {
				price = p.InvestedAmt / float64(p.Quantity)
			}
		}
		if price <= 0 {
			skipped = append(skipped, SkipReason{
				Symbol: p.Symbol,
				Reason: "topup: no valid price (LTP missing, entry_price=0)",
			})
			continue
		}

		effectivePrice := price * (1 + txnCostFraction)
		addQty := int32(math.Floor(gap / effectivePrice))
		if addQty <= 0 {
			skipped = append(skipped, SkipReason{
				Symbol: p.Symbol,
				Reason: fmt.Sprintf("topup: gap %.0f < 1 share at price %.2f", gap, price),
			})
			continue
		}

		topupCost := float64(addQty) * effectivePrice
		if topupCost > availableCash {
			// Try a smaller qty that fits remaining cash.
			fitQty := int32(math.Floor(availableCash / effectivePrice))
			if fitQty <= 0 {
				skipped = append(skipped, SkipReason{
					Symbol: p.Symbol,
					Reason: fmt.Sprintf("topup: insufficient margin (cash=%.0f required=%.0f for 1 share)",
						availableCash, effectivePrice),
				})
				continue
			}
			addQty = fitQty
			topupCost = float64(addQty) * effectivePrice
		}

		stopLoss := price * (1 - snap.Cfg.StopLossPct/100.0)
		entry := PlannedEntry{
			SignalID:         uuid.New().String(),
			Symbol:           p.Symbol,
			IndexName:        p.IndexName,
			Industry:         p.Industry,
			MCapBucket:       p.MCapBucket,
			EntryPrice:       price,
			Quantity:         addQty,
			InvestedAmt:      float64(addQty) * price,
			StopLoss:         stopLoss,
			EMAFraction:      emaTarget,
			TopUpForSignalID: p.SignalID,
			Reason: fmt.Sprintf("topup: gap=%.0f → +%d shares at ₹%.2f (target=%.0f)",
				gap, addQty, price, targetPerStock),
		}
		topups = append(topups, entry)

		// Update working state so Phase 2 (Allocate) sees post-topup numbers.
		availableCash -= topupCost
		p.InvestedAmt += entry.InvestedAmt
		p.Quantity += int(addQty)
		// Mirror back into the snapshot's slice + maps so Allocate's caps line up.
		for idx := range snap.ActivePositions {
			if snap.ActivePositions[idx].Symbol == p.Symbol {
				snap.ActivePositions[idx].InvestedAmt = p.InvestedAmt
				snap.ActivePositions[idx].Quantity = p.Quantity
				break
			}
		}
		if snap.Capital.Total > 0 {
			snap.IndexDeployedPct[p.IndexName] += entry.InvestedAmt / snap.Capital.Total
		}

		if logger != nil {
			logger.Info("TopUpExistingPositions: planned",
				zap.String("user", snap.Cfg.UserID),
				zap.String("symbol", p.Symbol),
				zap.String("parent_signal_id", p.SignalID),
				zap.Int32("add_qty", addQty),
				zap.Float64("invested", entry.InvestedAmt),
				zap.Float64("target_per_stock", targetPerStock),
				zap.Float64("ema", emaTarget))
		}
	}

	return topups, skipped
}

// Allocate runs the EMA-cycle-aware allocator for ONE strategy snapshot.
//
// Per-stock sizing: position_amt = per_call × ema_target_for_index
//
// Per-INDEX cap: cumulative deployment in that index never exceeds
// `ema_target × current_capital`. As more signals arrive over multiple days
// (each day's run adds a new pass), each index fills up to today's target.
// Tomorrow's larger EMA target opens new room; today's signals beyond the
// cap are skipped with a clear reason ("EMA quota hit").
//
// Order of skip checks (cheap → expensive):
//   1. Already holding (memory)
//   2. User-override active (memory; came from MANUAL_EXIT_DETECTED)
//   3. Re-entry cooldown (memory; LTP must drop below reentry_below)
//   4. Sector cap > 25% of MaxPositions (count-based)
//   5. MCap bucket cap > 50% of MaxPositions (count-based)
//   6. Max positions reached → break, no more candidates considered
//   7. EMA-quota for this index already at/above target → skip
//   8. Quantity floors to 0 or position too small → skip
func Allocate(snap *PortfolioSnapshot, signals []EligibleSignal, logger *zap.Logger) AllocResult {
	res := AllocResult{
		Cfg:     snap.Cfg,
		Capital: snap.Capital,
		PerCall: snap.PerCall,
	}

	if snap.PerCall <= 0 {
		res.Skipped = append(res.Skipped, SkipReason{
			Symbol: "(all)",
			Reason: fmt.Sprintf("per_call=0 (capital=%.2f, max_pos=%d)",
				snap.Capital.Total, snap.Cfg.MaxPositions),
		})
		return res
	}

	// Build the lookup of "what we already hold" by symbol.
	holding := map[string]bool{}
	for _, p := range snap.ActivePositions {
		holding[strings.ToUpper(strings.TrimSpace(p.Symbol))] = true
	}

	// Caps in absolute terms (Manthan: ≤25% sector, ≤50% bucket).
	maxPerSector := int(math.Ceil(float64(snap.Cfg.MaxPositions) * 0.25))
	maxPerBucket := int(math.Ceil(float64(snap.Cfg.MaxPositions) * 0.50))
	if maxPerSector < 1 {
		maxPerSector = 1
	}
	if maxPerBucket < 1 {
		maxPerBucket = 1
	}

	// Working copy of deployed-fraction so we can mutate as we approve.
	deployed := map[string]float64{}
	for k, v := range snap.IndexDeployedPct {
		deployed[k] = v
	}
	sectorCount := map[string]int{}
	for k, v := range snap.SectorCount {
		sectorCount[k] = v
	}
	bucketCount := map[string]int{}
	for k, v := range snap.BucketCount {
		bucketCount[k] = v
	}
	openSlots := snap.Cfg.MaxPositions - len(snap.ActivePositions)
	if openSlots < 0 {
		openSlots = 0
	}

	for _, sig := range signals {
		if openSlots <= 0 {
			res.Skipped = append(res.Skipped, SkipReason{
				Symbol: sig.Symbol, Reason: "max positions reached",
			})
			continue
		}

		symKey := strings.ToUpper(strings.TrimSpace(sig.Symbol))

		// 1. Already holding
		if holding[symKey] {
			res.Skipped = append(res.Skipped, SkipReason{
				Symbol: sig.Symbol, Reason: "already holding",
			})
			continue
		}

		// 2. User override (manual-exit cooldown)
		if until, ok := snap.OverrideSymbols[symKey]; ok {
			res.Skipped = append(res.Skipped, SkipReason{
				Symbol: sig.Symbol,
				Reason: "user_override_active until " + until.Format("2006-01-02 15:04:05"),
			})
			continue
		}

		// 3. Re-entry cooldown — only blocks if price still ABOVE reentry_below
		if reEntryBelow, ok := snap.CooldownSymbols[symKey]; ok {
			if sig.LatestPrice > reEntryBelow {
				res.Skipped = append(res.Skipped, SkipReason{
					Symbol: sig.Symbol,
					Reason: fmt.Sprintf("cooldown: price %.2f > reentry_below %.2f",
						sig.LatestPrice, reEntryBelow),
				})
				continue
			}
		}

		// 4. Sector cap
		if sig.Industry != "" && sectorCount[sig.Industry] >= maxPerSector {
			res.Skipped = append(res.Skipped, SkipReason{
				Symbol: sig.Symbol,
				Reason: fmt.Sprintf("sector cap hit (%s %d/%d)",
					sig.Industry, sectorCount[sig.Industry], maxPerSector),
			})
			continue
		}

		// 5. MCap bucket cap
		if sig.MCapBucket != "" && bucketCount[sig.MCapBucket] >= maxPerBucket {
			res.Skipped = append(res.Skipped, SkipReason{
				Symbol: sig.Symbol,
				Reason: fmt.Sprintf("mcap cap hit (%s %d/%d)",
					sig.MCapBucket, bucketCount[sig.MCapBucket], maxPerBucket),
			})
			continue
		}

		// 6. EMA quota for this index — the heart of the rebalancer's logic.
		emaTarget, ok := snap.EMATargets[sig.IndexName]
		if !ok || emaTarget <= 0 {
			res.Skipped = append(res.Skipped, SkipReason{
				Symbol: sig.Symbol,
				Reason: "EMA target = 0 for index " + sig.IndexName,
			})
			continue
		}
		alreadyDeployed := deployed[sig.IndexName]
		if alreadyDeployed >= emaTarget {
			res.Skipped = append(res.Skipped, SkipReason{
				Symbol: sig.Symbol,
				Reason: fmt.Sprintf("EMA quota hit (%s deployed %.1f%% ≥ target %.1f%%)",
					sig.IndexName, alreadyDeployed*100, emaTarget*100),
			})
			continue
		}

		// 7. Size by EMA target. Cap so a single new position doesn't blow
		// past the index target by more than its own slot.
		positionSize := snap.PerCall * emaTarget
		// Make sure adding this position doesn't push deployed past target.
		// Allowed = (target − already) × current_capital. Take the smaller
		// of (per_call × ema) and the remaining slot.
		allowed := (emaTarget - alreadyDeployed) * snap.Capital.Total
		if positionSize > allowed {
			positionSize = allowed
		}
		if positionSize < minPositionRupees {
			res.Skipped = append(res.Skipped, SkipReason{
				Symbol: sig.Symbol,
				Reason: fmt.Sprintf("size %.0f < min %.0f (per_call=%.0f × ema=%.2f)",
					positionSize, minPositionRupees, snap.PerCall, emaTarget),
			})
			continue
		}

		// 8. Compute qty with txn-cost padding, then check it floors above 0.
		entryPrice := sig.LatestPrice
		if entryPrice <= 0 {
			res.Skipped = append(res.Skipped, SkipReason{
				Symbol: sig.Symbol, Reason: "latest_price = 0",
			})
			continue
		}
		effectivePrice := entryPrice * (1 + txnCostFraction)
		qty := int32(math.Floor(positionSize / effectivePrice))
		if qty <= 0 {
			res.Skipped = append(res.Skipped, SkipReason{
				Symbol: sig.Symbol,
				Reason: fmt.Sprintf("qty rounds to 0 (size %.0f vs price %.2f)",
					positionSize, entryPrice),
			})
			continue
		}

		// Approved — build the planned entry.
		invested := float64(qty) * entryPrice
		stopLoss := entryPrice * (1 - snap.Cfg.StopLossPct/100.0)

		entry := PlannedEntry{
			SignalID:    uuid.New().String(),
			Symbol:      sig.Symbol,
			ISIN:        sig.ISIN,
			IndexName:   sig.IndexName,
			Industry:    sig.Industry,
			MCapBucket:  sig.MCapBucket,
			EntryPrice:  entryPrice,
			Quantity:    qty,
			InvestedAmt: invested,
			StopLoss:    stopLoss,
			EMAFraction: emaTarget,
			Reason: fmt.Sprintf("ema=%.0f%% size=%.0f qty=%d",
				emaTarget*100, positionSize, qty),
		}
		res.Planned = append(res.Planned, entry)

		// Bump the running counters so subsequent signals see the new state.
		holding[symKey] = true
		if sig.Industry != "" {
			sectorCount[sig.Industry]++
		}
		if sig.MCapBucket != "" {
			bucketCount[sig.MCapBucket]++
		}
		if snap.Capital.Total > 0 {
			deployed[sig.IndexName] += invested / snap.Capital.Total
		}
		openSlots--

		if logger != nil {
			logger.Debug("rebalancer.Allocate: approved",
				zap.String("user", snap.Cfg.UserID),
				zap.String("symbol", sig.Symbol),
				zap.String("index", sig.IndexName),
				zap.Float64("ema", emaTarget),
				zap.Float64("invested", invested),
				zap.Int32("qty", qty))
		}
	}

	return res
}
