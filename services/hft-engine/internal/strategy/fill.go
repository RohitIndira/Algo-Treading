// The fill side of the state machine. Applies broker FillEvents to the
// in-flight chunk, transitions CHUNK_OPEN → CHUNK_PARTIAL → IDLE, and
// promotes the side to HALT(max_reached) when the cap is hit.
//
// All event types arrive on r.fillCh:
//
//   FILL   — broker reports partial or complete trade against our order.
//            We add to side.Position and chunk.Filled.
//   CANCEL — broker confirmed cancel (we asked, or external cancel).
//            We move the chunk to History; side returns to IDLE unless
//            we'd already halted.
//   REJECT — broker rejected our place/modify (rare for fills, but
//            included for completeness).
//
// Concurrency: Run is the only goroutine that calls onFill. Outside
// readers see updates through the atomic snapshot.
package strategy

import (
	"strings"

	"go.uber.org/zap"

	"github.com/RohitIndira/Algo-Treading/services/hft-engine/internal/state"
)

// maxConsecutiveRejects bounds how many broker REJECTs in a row before
// applyReject halts the side. Structural rejects (margin / risk / DPR)
// trip the halt instantly via isStructuralReject; this catches every
// other case so we don't spam broker rate limits indefinitely.
const maxConsecutiveRejects = 5

// isStructuralReject returns true when a broker reject reason indicates
// the order can never succeed without external action (more margin,
// smaller qty, lifted risk limit, fresh login). Placing again would just
// keep getting rejected — caller should halt the side. Patterns are
// observed live from Indira's order-status WS payloads (e.g.
// "M.Lmt excd.S/OptCFS/=...SFall=...").
func isStructuralReject(reason string) bool {
	r := strings.ToLower(reason)
	return strings.Contains(r, "m.lmt") ||
		strings.Contains(r, "margin") ||
		strings.Contains(r, "sfall") ||
		strings.Contains(r, "shortfall") ||
		strings.Contains(r, "insufficient") ||
		strings.Contains(r, "exposure") ||
		strings.Contains(r, "risk lmt") ||
		strings.Contains(r, "risk limit") ||
		strings.Contains(r, "dpr")
}

// onFill applies one event to the matching side. If the event's
// broker_order_id doesn't match any current chunk, it's logged and
// ignored — late fills after we've already moved the chunk to History
// are common during the race between our cancel and a broker fill.
func (r *Runner) onFill(f state.FillEvent) {
	side, sideC := r.findChunkOwner(f.BrokerOrderID)
	if side == nil {
		r.logger.Debug("fill event for unknown broker_order_id — ignored",
			zap.String("broker_order_id", f.BrokerOrderID),
			zap.String("event_type", f.EventType))
		return
	}

	switch f.EventType {
	case "FILL":
		r.applyFill(side, sideC, f)
	case "CANCEL":
		r.applyCancelAck(side, sideC, f)
	case "REJECT":
		r.applyReject(side, sideC, f)
	default:
		r.logger.Warn("unknown fill event type — ignored",
			zap.String("event_type", f.EventType),
			zap.String("broker_order_id", f.BrokerOrderID))
		return
	}

	r.publishSnapshot()
}

// applyFill increments chunk.Filled + side.Position. Promotes the chunk
// to FILLED status when it reaches Qty, then clears side.Current so the
// next tick can place chunk #2.
func (r *Runner) applyFill(side *state.SideState, sideC string, f state.FillEvent) {
	if side.Current == nil {
		// Defensive — should be impossible because we matched by broker_order_id.
		return
	}
	chunk := side.Current
	chunk.Filled += f.FillQty
	side.Position += f.FillQty
	// Any successful fill clears the consecutive-reject counter — whatever
	// transient condition caused recent rejects is no longer in effect.
	side.ConsecutiveRejects = 0

	r.auditRow("FILL", sideC, chunk.Seq, f.FillQty, f.FillPrice, chunk.BrokerOrderID, "")

	r.logger.Info("fill",
		zap.String("side", sideC),
		zap.Int("chunk_seq", chunk.Seq),
		zap.Int("fill_qty", f.FillQty),
		zap.Float64("fill_price", f.FillPrice),
		zap.Int("chunk_filled", chunk.Filled),
		zap.Int("chunk_qty", chunk.Qty),
		zap.Int("side_position", side.Position))

	if chunk.Filled >= chunk.Qty {
		// Chunk complete — move to history, free side.Current for next chunk.
		chunk.Status = state.ChunkFilled
		side.History = append(side.History, *chunk)
		side.Current = nil

		// Cap-reached check — if this fill pushed us to the cap, HALT.
		maxQty := r.maxQtyFor(sideC)
		if side.Position >= maxQty {
			side.Done = true
			side.HaltReason = state.HaltMaxReached
		}
	} else {
		// Partial fill — chunk stays in flight, will be MODIFY'd on
		// subsequent ticks until the remainder fills.
		chunk.Status = state.ChunkPartial
	}
}

// selfHealFilledChunk reconciles a chunk the broker says is already fully
// executed but for which we never received an order-status WS fill event.
// It is the EG003 ("FULLY_EXECUTED order not allowed to MODIFY") recovery
// path: without it, the tick loop retries the MODIFY against a dead order
// forever and the next chunk never places.
//
// We reconcile optimistically — the whole remaining qty is treated as
// filled at the chunk's last known LimitPrice. The exact average fill
// price is corrected later by the trade-book reconciler; what matters
// here is unblocking the state machine. Mirrors applyFill's completion
// branch.
func (r *Runner) selfHealFilledChunk(side *state.SideState, sideC string) {
	if side.Current == nil {
		return
	}
	chunk := side.Current
	remaining := chunk.Qty - chunk.Filled
	if remaining > 0 {
		side.Position += remaining
		chunk.Filled = chunk.Qty
	}
	chunk.Status = state.ChunkFilled

	r.auditRow("FILL_RECONCILED", sideC, chunk.Seq,
		remaining, chunk.LimitPrice, chunk.BrokerOrderID,
		"broker reports order fully executed (modify rejected EG003)")

	r.logger.Warn("self-healed chunk: broker says fully executed but no WS fill event arrived",
		zap.String("side", sideC),
		zap.Int("chunk_seq", chunk.Seq),
		zap.String("broker_order_id", chunk.BrokerOrderID),
		zap.Int("reconciled_qty", remaining),
		zap.Int("side_position", side.Position))

	side.History = append(side.History, *chunk)
	side.Current = nil

	maxQty := r.maxQtyFor(sideC)
	if side.Position >= maxQty {
		side.Done = true
		side.HaltReason = state.HaltMaxReached
	}
}

// applyCancelAck handles a broker confirmation that our cancel went
// through (or that the order was already gone). Moves the chunk to
// History as CANCELLED and clears side.Current.
//
// We don't transition Done — that's the caller's job (price_band /
// window_closed / EXIT). A bare CANCEL with no halt reason means
// "external cancel" (someone cancelled from the broker app), which
// we treat as IDLE: the next tick will place chunk #2 unless other
// conditions halt us.
func (r *Runner) applyCancelAck(side *state.SideState, sideC string, f state.FillEvent) {
	if side.Current == nil {
		return
	}
	chunk := side.Current
	chunk.Status = state.ChunkCancelled
	side.History = append(side.History, *chunk)
	side.Current = nil

	r.auditRow("CANCEL_ACK", sideC, chunk.Seq,
		chunk.Qty-chunk.Filled, chunk.LimitPrice, chunk.BrokerOrderID, f.Reason)

	r.logger.Info("chunk cancelled by broker",
		zap.String("side", sideC),
		zap.Int("chunk_seq", chunk.Seq),
		zap.String("reason", f.Reason))
}

// applyReject handles broker rejections. Moves the chunk to History as
// CANCELLED, clears side.Current, and halts the side under either of
// two conditions:
//   - structural reject (margin/risk/DPR) — re-placing would just keep
//     getting rejected, so halt immediately.
//   - maxConsecutiveRejects transient rejects in a row — protects broker
//     rate limits if we somehow keep hitting non-structural rejects
//     (e.g. price slipped through every retry).
//
// Anything else falls through and the next tick re-attempts — a single
// transient reject is recoverable.
func (r *Runner) applyReject(side *state.SideState, sideC string, f state.FillEvent) {
	if side.Current == nil {
		return
	}
	chunk := side.Current
	chunk.Status = state.ChunkCancelled

	r.auditRow("REJECT", sideC, chunk.Seq,
		chunk.Qty-chunk.Filled, chunk.LimitPrice, chunk.BrokerOrderID, f.Reason)

	r.logger.Warn("chunk rejected by broker",
		zap.String("side", sideC),
		zap.Int("chunk_seq", chunk.Seq),
		zap.String("reason", f.Reason))

	side.History = append(side.History, *chunk)
	side.Current = nil
	side.ConsecutiveRejects++

	// Structural rejects (margin / risk / DPR) cannot self-heal — placing
	// again would just keep getting rejected. Halt the side immediately.
	if isStructuralReject(f.Reason) {
		r.logger.Warn("structural broker reject — halting side",
			zap.String("side", sideC),
			zap.String("reason", f.Reason),
			zap.Int("consecutive_rejects", side.ConsecutiveRejects))
		side.Done = true
		side.HaltReason = state.HaltBrokerHardFail
		return
	}

	// Halt after too many transient rejects in a row — last-resort guard
	// against spamming broker rate limits on a non-recovering condition.
	if side.ConsecutiveRejects >= maxConsecutiveRejects {
		r.logger.Warn("too many consecutive rejects — halting side",
			zap.String("side", sideC),
			zap.Int("consecutive_rejects", side.ConsecutiveRejects))
		side.Done = true
		side.HaltReason = state.HaltBrokerHardFail
		return
	}
}

// findChunkOwner returns the side + sideChar whose Current chunk has
// the given broker_order_id, or (nil, "") if none match. History
// chunks aren't searched because they're terminal.
func (r *Runner) findChunkOwner(brokerOrderID string) (*state.SideState, string) {
	if r.live.Buy.Current != nil && r.live.Buy.Current.BrokerOrderID == brokerOrderID {
		return &r.live.Buy, "B"
	}
	if r.live.Sell.Current != nil && r.live.Sell.Current.BrokerOrderID == brokerOrderID {
		return &r.live.Sell, "S"
	}
	return nil, ""
}

// maxQtyFor returns the side's configured cap given the side char.
func (r *Runner) maxQtyFor(sideC string) int {
	if sideC == "B" {
		return r.Cfg.MaxBuyQty
	}
	return r.Cfg.MaxSellQty
}
