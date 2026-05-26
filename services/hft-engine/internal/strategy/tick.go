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

	// Trigger gate — continuous, re-evaluated on EVERY tick.
	//
	// Direction is side-dependent:
	//   BUY  is "in zone" when LTP >= trigger (breakout buy)
	//   SELL is "in zone" when LTP <= trigger (breakdown sell)
	//
	// State machine (Position is preserved across all transitions —
	// only fills change it; trigger only gates whether we place):
	//
	//   !Armed && !inZone → no-op (waiting / paused; return early)
	//   !Armed &&  inZone → ARM (first entry, Position==0)
	//                       or RESUME (re-entry after prior fills);
	//                       fall through to normal place/modify flow
	//    Armed &&  inZone → normal place/modify flow (no transition)
	//    Armed && !inZone → PAUSE: cancel any resting chunk, set
	//                       Armed=false, KEEP Position; return.
	//
	// Done is never set by this gate — only price-band halt or
	// max_reached can flip the side terminal.
	//
	// Disabled (back-compat) when triggerPrice <= 0: side is treated as
	// permanently armed. New strategies always have a positive trigger
	// (user-config validates this); this branch is for legacy rows only.
	if triggerPrice > 0 {
		var inZone bool
		if sideEnum == state.SideBuy {
			inZone = ltp >= triggerPrice
		} else {
			inZone = ltp <= triggerPrice
		}

		switch {
		case !side.Armed && !inZone:
			// Waiting (or paused) for LTP to come back into the zone.
			return
		case side.Armed && !inZone:
			// PAUSE — disarm but LEAVE any resting chunk on the book.
			// The chunk's limit was already within the user's authorised
			// band; a sub-tick LTP flicker out of the trigger zone does
			// not invalidate it. We do NOT cancel because the cancel
			// frequently races a sub-second fill — the broker replies
			// EG003 FULLY_EXECUTED and we mis-record a real fill as a
			// cancellation. Fill handling is event-driven and continues
			// to work during pause. When LTP re-enters the zone, the
			// !Armed && inZone branch RESUMEs and the normal modify-chase
			// loop continues until the chunk fills.
			side.Armed = false
			r.auditRow("PAUSE", sideC, 0, 0, ltp, "", "")
			r.logger.Info("trigger left zone — paused (chunk left resting)",
				zap.String("side", string(sideEnum)),
				zap.Float64("ltp", ltp),
				zap.Float64("trigger", triggerPrice),
				zap.Int("position", side.Position),
				zap.Bool("chunk_resting", side.Current != nil))
			return
		case !side.Armed && inZone:
			// Entering zone — ARM (first time, Position==0) or RESUME
			// (re-entry after we already filled some). Same downstream
			// effect; distinguish only in audit/log for clarity.
			side.Armed = true
			action := "ARM"
			if side.Position > 0 {
				action = "RESUME"
			}
			r.auditRow(action, sideC, 0, 0, ltp, "", "")
			r.logger.Info("trigger entered zone",
				zap.String("side", string(sideEnum)),
				zap.String("action", action),
				zap.Float64("ltp", ltp),
				zap.Float64("trigger", triggerPrice),
				zap.Int("position", side.Position))
			// Fall through to normal place/modify flow.
		}
		// Armed && inZone: normal flow, no transition; fall through.
	}

	// Unified band check — the trigger and limit configured by the user
	// describe a CLOSED price range in which all placements must sit:
	//
	//   BUY:  [buy_trigger_price, buy_limit_price]    (trigger=floor,  limit=ceiling)
	//   SELL: [sell_limit_price,  sell_trigger_price] (limit=floor,    trigger=ceiling)
	//
	// When the proposed limit price drifts OUTSIDE the band, the engine
	// WAITS — it does not halt, and it does not cancel any resting
	// chunk. Symmetric pause behaviour on both edges so the strategy
	// resumes naturally when the market returns to the band: the next
	// tick that brings touch back into [trigger, limit] re-enters the
	// place/modify flow and the engine fires the next pending chunk
	// toward max_qty without operator intervention.
	//
	// Replaces the legacy "HaltPriceBand TERMINAL" behaviour that used
	// to set side.Done=true and exit the runner the moment touch poked
	// above the ceiling — even a one-tick spike was unrecoverable
	// because the runner goroutine had already exited. With pause-on-
	// both-edges the runner stays alive and the strategy completes
	// naturally when the market lets it.
	//
	// LimitPrice is always enforced. TriggerPrice is enforced only when
	// configured (>0) so legacy strategies with no trigger keep their
	// original "respect the ceiling only" semantics.
	//
	// We compare the proposed limit price (touch rounded to tick) rather
	// than raw touch — a touch of 423.03 with tick 0.05 rounds down to
	// 423.00, which IS exactly at a [422.95, 423.00] ceiling. Aligns the
	// check with what we would actually quote to the broker.
	proposed := roundToTick(touch, r.Cfg.TickSize)
	var outOfBand bool
	switch sideEnum {
	case state.SideBuy:
		outOfBand = (proposed > limitPrice) ||
			(triggerPrice > 0 && proposed < triggerPrice)
	case state.SideSell:
		outOfBand = (proposed < limitPrice) ||
			(triggerPrice > 0 && proposed > triggerPrice)
	}
	if outOfBand {
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

// haltSide marks a side terminal under the given reason. Used by
// handleTick's window-closed branch. Resting chunks are NOT cancelled
// here — the runner-exit goroutine (cancelAllResting in runner.go)
// handles that defensively when the strategy unwinds, with the EG003
// self-heal in place to handle the case where the chunk filled in the
// race between our cancel request and the broker's response.
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
