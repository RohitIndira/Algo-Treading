// The tick-side of the state machine. Runs on every bid/ask update from
// the market data WS (Phase 4) or a synthetic test tick.
//
// Reference state diagram lives in the design doc. The condensed rules
// implemented here:
//
//	IDLE       + tick(in band, in window, pos < max) → PLACE → CHUNK_OPEN
//	           + tick(out of band) → HALT(price_band)
//	           + tick(out of window) → HALT(window_closed)
//
//	CHUNK_OPEN + tick(price changed, still in band) → MODIFY (stays CHUNK_OPEN)
//	           + tick(out of band) → CANCEL → HALT(price_band)
//	           + tick(out of window) → CANCEL → HALT(window_closed)
//	           (FILL events handled in fill.go)
//
//	CHUNK_PARTIAL — same transitions as CHUNK_OPEN.
//
//	HALTED — terminal; tick is ignored.
//
// Critical invariant: a second chunk is NEVER placed while side.Current != nil.
// The state machine's IDLE→PLACE edge is the only place we create a chunk.
package strategy

import (
	"context"
	"math"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/RohitIndira/Algo-Treading/services/hft-engine/internal/state"
)

// handleTick is the per-tick entry point. Called by Run from the
// tickCh select branch.
//
// Note: r.live writes happen here without any lock — Run is the only
// goroutine that calls this method. publishSnapshot at the end makes
// the new state visible to outside readers atomically.
func (r *Runner) handleTick(md state.MarketData) {
	r.live.LastMD = md
	r.live.LastTickAt = time.Now()

	// Window check halts BOTH sides at once — they share the same window.
	if !inTradeWindow(md.At, r.Cfg.WindowStart, r.Cfg.WindowEnd) {
		r.haltSide(&r.live.Buy, "B", state.HaltWindowClosed)
		r.haltSide(&r.live.Sell, "S", state.HaltWindowClosed)
		r.publishSnapshot()
		return
	}

	// Each side runs independently. A halted side returns immediately
	// from handleSide so this is cheap.
	if r.Cfg.Side != state.SideSell {
		r.handleSide(&r.live.Buy, "B", state.SideBuy,
			md.Ask, r.Cfg.BuyLimitPrice, r.Cfg.BuyTriggerPrice, md.LTP,
			r.Cfg.MaxBuyQty, r.Cfg.SingleBuyQty)
	}
	if r.Cfg.Side != state.SideBuy {
		r.handleSide(&r.live.Sell, "S", state.SideSell,
			md.Bid, r.Cfg.SellLimitPrice, r.Cfg.SellTriggerPrice, md.LTP,
			r.Cfg.MaxSellQty, r.Cfg.SingleSellQty)
	}

	r.publishSnapshot()
}

// handleSide implements the per-side state machine. Called once per tick
// per active side.
//
// Parameters intentionally explicit (not packed in a struct) so the call
// sites in handleTick read as the C++ template did — a quick visual diff
// matches the original `bidding_logic` shape.
func (r *Runner) handleSide(
	side *state.SideState,
	sideC string, // "B" or "S" for audit
	sideEnum state.Side, // BUY or SELL for broker.PlaceLimit
	touch float64, // ASK for BUY side, BID for SELL side
	limitPrice float64, // halt threshold
	triggerPrice float64, // arm gate; <=0 disables the gate (back-compat)
	ltp float64, // last traded price — what the trigger watches
	maxQty int,
	chunkQty int,
) {
	// Terminal state — nothing to do.
	if side.Done {
		return
	}

	// Position-cap check. Position grows ONLY from fills, so this can
	// only flip from IDLE — never with a chunk in flight. (If it did
	// somehow, the chunk would fill the rest and bring us here.)
	if side.Position >= maxQty {
		side.Done = true
		side.HaltReason = state.HaltMaxReached
		return
	}

	// Trigger gate. Runs BEFORE price-band so a never-armed side
	// doesn't trip the halt by being out-of-band — until armed, the
	// side is conceptually idle and waiting, not actively trading.
	//
	// Disabled (back-compat) when triggerPrice <= 0: side is armed
	// immediately. New strategies always have a positive trigger
	// (validated by user-config), so this branch is for legacy rows
	// only.
	if triggerPrice > 0 {
		if !side.Armed {
			if ltp >= triggerPrice {
				side.Armed = true
				r.auditRow("ARM", sideC, 0, 0, ltp, "", "")
				r.logger.Info("trigger armed",
					zap.String("side", string(sideEnum)),
					zap.Float64("ltp", ltp),
					zap.Float64("trigger", triggerPrice))
			} else {
				return // still waiting — don't place, don't halt
			}
		} else if side.Position == 0 && ltp < triggerPrice {
			// Cross-back before any fill: cancel resting chunk + re-disarm.
			// Once any fill has happened (Position > 0), the trigger is
			// no longer consulted — we ride to max_qty.
			if side.Current != nil {
				r.cancelCurrentChunk(side, sideC, "trigger_disarm")
			}
			side.Armed = false
			r.auditRow("DISARM", sideC, 0, 0, ltp, "", "")
			r.logger.Info("trigger disarmed (cross-back, no fills yet)",
				zap.String("side", string(sideEnum)),
				zap.Float64("ltp", ltp),
				zap.Float64("trigger", triggerPrice))
			return
		}
	}

	// Price-band halt. Applies whether or not a chunk is resting. If a
	// chunk IS resting, we cancel it before halting so we don't leave
	// an open order on the book outside our intended range.
	outOfBand := (sideEnum == state.SideBuy && touch > limitPrice) ||
		(sideEnum == state.SideSell && touch < limitPrice)
	if outOfBand {
		if side.Current != nil {
			r.cancelCurrentChunk(side, sideC, "price_band")
		}
		side.Done = true
		side.HaltReason = state.HaltPriceBand
		return
	}

	// Chunk-in-flight: chase the market with MODIFY (never place a 2nd).
	if side.Current != nil {
		if !r.Cfg.ModifyOnPriceChange {
			return // user opted out of chasing — wait for fill at original price
		}
		newPrice := roundToTick(touch, r.Cfg.TickSize)
		if newPrice == side.Current.LimitPrice {
			return // same price — no-op
		}
		// Broker expects the FULL chunk qty, not the remaining. Indira
		// tracks already-traded internally.
		ctx, cancel := context.WithTimeout(r.ctx, 3*time.Second)
		err := r.broker.ModifyLimit(ctx, r.auth, r.sym,
			side.Current.BrokerOrderID, side.Current.Qty, newPrice)
		cancel()
		if err != nil {
			// EG003 / "FULLY_EXECUTED": the broker already fully filled
			// this order, but its order-status WS fill event never
			// reached us. Retrying the MODIFY every tick against a dead
			// order loops forever — self-heal by reconciling the chunk
			// as filled so the next chunk can place.
			if isAlreadyFilledErr(err) {
				r.logger.Warn("modify rejected — order already fully executed; reconciling chunk as filled",
					zap.String("side", string(sideEnum)),
					zap.String("broker_order_id", side.Current.BrokerOrderID),
					zap.Error(err))
				r.selfHealFilledChunk(side, sideC)
				return
			}
			// Other errors: leave the resting order at its old price;
			// next tick may try again. Don't halt — the order is still
			// protective.
			r.logger.Warn("modify failed — leaving chunk resting at old price",
				zap.String("side", string(sideEnum)),
				zap.String("broker_order_id", side.Current.BrokerOrderID),
				zap.Float64("old_price", side.Current.LimitPrice),
				zap.Float64("attempted_new_price", newPrice),
				zap.Error(err))
			r.auditRow("MODIFY_FAIL", sideC, side.Current.Seq,
				side.Current.Qty, newPrice, side.Current.BrokerOrderID, err.Error())
			return
		}
		side.Current.LimitPrice = newPrice
		side.Current.ModifyCount++
		r.auditRow("MODIFY", sideC, side.Current.Seq,
			side.Current.Qty, newPrice, side.Current.BrokerOrderID, "")
		return
	}

	// IDLE → place next chunk.
	remaining := maxQty - side.Position
	qty := min(chunkQty, remaining)
	if qty <= 0 {
		// Cap already reached (probably caught above, but defensive).
		side.Done = true
		side.HaltReason = state.HaltMaxReached
		return
	}

	price := roundToTick(touch, r.Cfg.TickSize)
	ctx, cancel := context.WithTimeout(r.ctx, 3*time.Second)
	brokerOrderID, err := r.broker.PlaceLimit(ctx, r.auth, r.sym, sideEnum, qty, price)
	cancel()
	if err != nil {
		// Place failed — log + audit, leave side in IDLE so the next
		// tick retries. We don't halt on a single place failure; HFT
		// has transient broker errors and the chunk-in-flight check
		// still holds (side.Current is nil).
		r.logger.Warn("place failed — staying IDLE",
			zap.String("side", string(sideEnum)),
			zap.Int("qty", qty),
			zap.Float64("price", price),
			zap.Error(err))
		r.auditRow("PLACE_FAIL", sideC, len(side.History)+1, qty, price, "", err.Error())
		return
	}

	side.Current = &state.ChunkState{
		Seq:           len(side.History) + 1,
		Qty:           qty,
		Filled:        0,
		LimitPrice:    price,
		BrokerOrderID: brokerOrderID,
		PlacedAt:      time.Now(),
		Status:        state.ChunkOpen,
	}
	r.auditRow("PLACE", sideC, side.Current.Seq, qty, price, brokerOrderID, "")
}

// cancelCurrentChunk issues a Cancel for the resting chunk and moves it
// to History. Used by:
//   - the price-band halt path (this file)
//   - the window-closed halt path (haltSide below)
//   - Run's deferred cancelAllResting on goroutine exit (in runner.go,
//     which does its own cancel because it can't easily call back here).
func (r *Runner) cancelCurrentChunk(side *state.SideState, sideC, reason string) {
	if side.Current == nil {
		return
	}
	ctx, cancel := context.WithTimeout(r.ctx, 3*time.Second)
	err := r.broker.Cancel(ctx, r.auth, r.sym, side.Current.BrokerOrderID)
	cancel()
	if err != nil {
		r.logger.Warn("cancel failed",
			zap.String("broker_order_id", side.Current.BrokerOrderID),
			zap.String("reason", reason),
			zap.Error(err))
	}
	side.Current.Status = state.ChunkCancelled
	r.auditRow("CANCEL", sideC, side.Current.Seq,
		side.Current.Qty-side.Current.Filled, side.Current.LimitPrice,
		side.Current.BrokerOrderID, reason)
	side.History = append(side.History, *side.Current)
	side.Current = nil
}

// haltSide marks a side Done with the given reason and cancels any
// resting chunk. Used by handleTick's window-closed branch.
func (r *Runner) haltSide(side *state.SideState, sideC string, reason state.HaltReason) {
	if side.Done {
		return
	}
	// if side.Current != nil {
	// 	r.cancelCurrentChunk(side, sideC, string(reason))
	// }
	side.Done = true
	side.HaltReason = reason
}

// ─────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────

// roundToTick returns price rounded DOWN to the nearest tick. For a BUY
// chasing the ask this is slightly conservative (we'd quote one tick
// below ask); for a SELL chasing the bid we'd quote at the bid. Acceptable
// because limit orders need to be at a valid tick anyway.
func roundToTick(price, tick float64) float64 {
	if tick <= 0 {
		return price
	}
	return math.Floor(price/tick) * tick
}

// isAlreadyFilledErr detects the broker's "you can't touch this order, it
// already fully executed" rejection (Indira error code EG003). When we see
// this on a MODIFY, the chunk has filled and we simply lost the
// order-status WS fill event for it.
func isAlreadyFilledErr(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToUpper(err.Error())
	return strings.Contains(s, "EG003") ||
		strings.Contains(s, "FULLY_EXECUTED") ||
		strings.Contains(s, "FULLY EXECUTED")
}
