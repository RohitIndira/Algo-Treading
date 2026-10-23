package manthan

import (
	"context"
	"fmt"

	"go.uber.org/zap"
)

// exitSuperseded answers "did the position's own stop sell it before this exit
// could?" for an exit the RMS rejected for its quantity. If so the exit row is
// closed as EXIT_SUPERSEDED — the strategy got what it wanted (the position is
// gone at the stop), so nobody is paged.
//
// Order of evidence:
//  1. the ledger: net ≤ 0 means a SELL (stop or exit) is already booked FILLED;
//  2. the broker: a standing stop of ours that shows (partly) executed but was
//     never booked, because the monitor skips positions with an open exit row
//     (FILATEX 2026-10-23: the stop filled at 10:14:50, the exit was rejected
//     at 10:14:50, and the fill was booked only after the exit gave up).
func (m *SafetyMonitor) exitSuperseded(ctx context.Context, auth *BrokerAuth, row *ManthanOrder, reason string) bool {
	net, _, has, nerr := m.repo.NetHeldQty(ctx, row.StrategyID, row.Symbol)
	if nerr == nil && (!has || net <= 0) {
		m.supersedeExit(ctx, row, fmt.Sprintf("position already sold in the ledger (net %d) — exit not needed (%s)", net, reason))
		return true
	}

	standing, serr := m.repo.ListStandingSLs(ctx, row.StrategyID, row.Symbol)
	if serr != nil {
		m.logger.Warn("Safety monitor: standing-stop lookup failed while judging a rejected exit",
			zap.String("symbol", row.Symbol), zap.Error(serr))
		return false
	}
	booked := 0
	for _, sl := range standing {
		if sl.BrokerOrderID == "" {
			continue
		}
		st, fq, ap, gerr := m.broker.GetOrderStatus(ctx, *auth, sl.BrokerOrderID)
		if gerr != nil || fq <= 0 || !(isFilledStatus(st) || isFilledOrPartialBrokerStatus(st)) {
			continue
		}
		if uerr := m.repo.UpdateOrderFilled(ctx, sl.ID, fq, ap); uerr != nil {
			m.logger.Error("Safety monitor: stop fill found but could not be booked",
				zap.Int64("row", sl.ID), zap.String("symbol", sl.Symbol), zap.Error(uerr))
			continue
		}
		_ = m.repo.InsertEvent(ctx, sl.ID, "SL_FILLED", string(sl.Status), "SL_FILLED", st, ap, fq,
			"stop filled at the exchange — found while its exit was rejected for the quantity")
		if m.eventPub != nil {
			if entrySignalID, e := m.repo.GetEntrySignalIDByOrderID(ctx, sl.ID); e == nil && entrySignalID != "" {
				m.eventPub.PublishSLFilled(ctx, entrySignalID, sl.StrategyID, sl.UserID,
					sl.Symbol, sl.BrokerOrderID, ap, int32(fq), 0)
			}
		}
		m.logger.Warn("Safety monitor: standing stop had filled — booked while judging the rejected exit",
			zap.String("user_id", sl.UserID), zap.String("symbol", sl.Symbol),
			zap.String("broker_order_id", sl.BrokerOrderID), zap.Int("filled", fq), zap.Float64("avg_price", ap))
		booked += fq
	}
	if booked == 0 {
		return false
	}
	net, _, has, nerr = m.repo.NetHeldQty(ctx, row.StrategyID, row.Symbol)
	if nerr == nil && (!has || net <= 0) {
		m.supersedeExit(ctx, row, fmt.Sprintf("stop filled %d at the exchange first — exit not needed (%s)", booked, reason))
		return true
	}
	return false
}

// supersedeExit closes an exit row whose position was sold by its own stop.
// Not an alert condition: the outcome is exactly the strategy's intent.
func (m *SafetyMonitor) supersedeExit(ctx context.Context, row *ManthanOrder, why string) {
	if uerr := m.repo.UpdateOrderCancelled(ctx, row.ID); uerr != nil {
		m.logger.Error("Safety monitor: supersede write failed — will retry next cycle", zap.Int64("row", row.ID), zap.Error(uerr))
		return
	}
	_ = m.repo.UpdateOrderStatus(ctx, row.ID, StatusCancelled, "SUPERSEDED", row.RetryCount, "superseded: "+why)
	_ = m.repo.InsertEvent(ctx, row.ID, "EXIT_SUPERSEDED", "PLACED", "CANCELLED", "", row.LimitPrice, row.Qty, why)
	m.logger.Info("Safety monitor: exit superseded by the position's own stop fill — no action, no alert",
		zap.String("user_id", row.UserID), zap.String("symbol", row.Symbol), zap.Int("qty", row.Qty), zap.String("why", why))
}

// giveUpGhost closes an exit the RMS rejected with "Set= 0": the broker holds
// nothing of the scrip, so the position in our book is a ghost — nothing can
// be sold. The position's latest stop row is parked as GHOST_SUSPECT so the
// naked scan does not place a fresh stop for it every cycle, and the operator
// is paged once to heal the ledger through the admin ghost endpoint.
func (m *SafetyMonitor) giveUpGhost(ctx context.Context, row *ManthanOrder, reason string) {
	why := fmt.Sprintf("broker holds 0 of %s (%s) — the position does not exist at the broker; nothing to sell. Heal the ledger via the admin ghost endpoint", row.Symbol, reason)
	if uerr := m.repo.UpdateOrderCancelled(ctx, row.ID); uerr != nil {
		m.logger.Error("Safety monitor: ghost give-up write failed — will retry next cycle", zap.Int64("row", row.ID), zap.Error(uerr))
		return
	}
	_ = m.repo.UpdateOrderStatus(ctx, row.ID, StatusCancelled, "GHOST_SUSPECT", row.RetryCount, exitChaseExhaustedMark+": "+why)
	_ = m.repo.InsertEvent(ctx, row.ID, "EXIT_GHOST_SUSPECT", "PLACED", "CANCELLED", "", row.LimitPrice, row.Qty, why)
	if parked, perr := m.repo.ParkLatestStopRowForPosition(ctx, row.StrategyID, row.Symbol, "GHOST_SUSPECT",
		"GHOST_SUSPECT: "+why); perr != nil {
		m.logger.Warn("Safety monitor: could not park the ghost's stop row — the naked scan may place one stop before CHECK 1 parks it",
			zap.String("symbol", row.Symbol), zap.Error(perr))
	} else if parked > 0 {
		_ = m.repo.InsertEvent(ctx, parked, "SL_GHOST_SUSPECT", "CANCELLED", "SL_DEFERRED_BAND", "GHOST_SUSPECT", 0, row.Qty,
			"parked as cover: broker holds 0 of the scrip (exit rejected Set= 0)")
	}
	m.logger.Error("Safety monitor: broker holds NOTHING of this position — exit closed as ghost suspect; ledger needs healing",
		zap.String("user_id", row.UserID), zap.String("symbol", row.Symbol), zap.Int("qty", row.Qty), zap.String("rms", reason))
	notifyOps(m.authNotif, ctx, row.UserID, row.Symbol, "GHOST_SUSPECT",
		fmt.Sprintf("broker holds 0 of %s but our ledger holds %d (RMS: %s) — nothing to sell; verify at the broker and heal via the admin ghost endpoint", row.Symbol, row.Qty, reason))
}
