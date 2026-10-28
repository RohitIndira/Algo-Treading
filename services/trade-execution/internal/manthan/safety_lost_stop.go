package manthan

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"
)

// sessionLookback bounds "today" for per-row counters without timezone
// arithmetic: 20 h back from now always covers the current session and never
// reaches the previous one (09:15 → 05:15 next day).
const sessionLookback = 20 * time.Hour

// handleLostStop is CHECK 1's single entry point for a stop whose broker
// order is gone (absent from the book, Cancelled, Rejected). Before anything
// is placed it asks three cheap questions (2026-10-23 incident):
//
//  1. Does OUR ledger still hold the position? net ≤ 0 → the row is a stale
//     leftover of a position already sold; close it, no exit, no alert.
//  2. Did the broker say it holds NOTHING ("Limit exceeded Set= 0")? → ghost:
//     park the row as cover, alert once, never re-place.
//  3. Has this row already been re-placed reprotectBudgetPerDay times today?
//     → park + alert once instead of fighting the broker every 15 s.
//
// Only a live position with budget left goes to reprotectLostSL.
func (m *SafetyMonitor) handleLostStop(ctx context.Context, auth *BrokerAuth, sl *ManthanOrder, brokerStatus string) {
	reason := ""
	if strings.EqualFold(strings.TrimSpace(brokerStatus), "REJECTED") && sl.BrokerOrderID != "" {
		if r, err := m.broker.GetOrderRejectReason(ctx, *auth, sl.BrokerOrderID); err == nil {
			reason = r
		} else {
			m.logger.Warn("Safety monitor: could not read the rejection reason — deciding without it",
				zap.String("symbol", sl.Symbol), zap.String("broker_order_id", sl.BrokerOrderID), zap.Error(err))
		}
	}
	net, _, hasEntry, nerr := m.repo.NetHeldQty(ctx, sl.StrategyID, sl.Symbol)
	if nerr != nil {
		m.logger.Warn("Safety monitor: ledger net lookup failed — treating the position as live",
			zap.String("symbol", sl.Symbol), zap.Error(nerr))
	}
	replaced, cerr := m.repo.CountOrderEventsSince(ctx, sl.ID, "SL_REPLACED", time.Now().Add(-sessionLookback))
	if cerr != nil {
		m.logger.Warn("Safety monitor: re-protect counter lookup failed — assuming 0",
			zap.String("symbol", sl.Symbol), zap.Error(cerr))
		replaced = 0
	}

	// A buy within settlement makes the RMS say "Set= 0" for a position the
	// broker holds; the ghost verdict waits until the newest buy has settled.
	// A failed lookup withholds it too (the budget cap bounds re-placements).
	settled := false
	if lastBuy, lerr := m.repo.LastBuyFilledAt(ctx, sl.StrategyID, sl.Symbol); lerr == nil {
		settled = settledForRMS(lastBuy, time.Now())
	} else {
		m.logger.Warn("Safety monitor: last-buy lookup failed — withholding any ghost verdict",
			zap.String("symbol", sl.Symbol), zap.Error(lerr))
	}

	switch classifyLostStop(net, hasEntry, nerr == nil, reason, replaced, settled) {
	case lostStopOrphan:
		m.closeOrphanStop(ctx, sl, net, brokerStatus)
	case lostStopGhost:
		m.parkGhostSuspect(ctx, sl, brokerStatus, reason, net)
	case lostStopBudget:
		m.parkBudgetSpent(ctx, sl, brokerStatus, replaced)
	default:
		m.reprotectLostSL(ctx, auth, sl, brokerStatus)
	}
}

// closeOrphanStop closes a stop row whose position our ledger shows fully
// sold. Nothing is sent to the broker and nobody is paged: the position was
// sold by a stop (or an exit) that is already booked FILLED.
func (m *SafetyMonitor) closeOrphanStop(ctx context.Context, sl *ManthanOrder, net int, brokerStatus string) {
	note := fmt.Sprintf("orphan stop row: ledger net %d for the position — nothing to protect; closed by the safety monitor (broker status %s)", net, brokerStatus)
	if err := m.repo.CloseOrphanStopRow(ctx, sl.ID, note); err != nil {
		m.logger.Error("Safety monitor: orphan stop row could not be closed — will retry next cycle",
			zap.Int64("row", sl.ID), zap.String("symbol", sl.Symbol), zap.Error(err))
		return
	}
	_ = m.repo.InsertEvent(ctx, sl.ID, "SL_ORPHAN_CLOSED", string(sl.Status), "CANCELLED", brokerStatus, sl.TriggerPrice, sl.Qty, note)
	m.logger.Warn("Safety monitor: stale stop row of an already-sold position closed — no exit, no re-protect",
		zap.String("user_id", sl.UserID), zap.String("symbol", sl.Symbol), zap.Int64("row", sl.ID),
		zap.Int("ledger_net", net), zap.String("broker_status", brokerStatus))
}

// parkGhostSuspect parks a stop the broker rejected with "Set= 0": it holds
// nothing of the scrip while our ledger still carries the lot. The parked row
// keeps the naked scan from placing a fresh stop every cycle; the operator
// heals the ledger through the admin ghost endpoint (broker-verified).
func (m *SafetyMonitor) parkGhostSuspect(ctx context.Context, sl *ManthanOrder, brokerStatus, reason string, net int) {
	parent := int64(0)
	if sl.ParentOrderID != nil {
		parent = *sl.ParentOrderID
	}
	note := fmt.Sprintf("GHOST_SUSPECT: 0 sellable at the broker for %s (%s) while the ledger holds %d — sold, transferred or pledged; not re-placing; the book reconciler decides (or heal via the admin ghost endpoint)", sl.Symbol, reason, net)
	if err := m.repo.ParkStopRow(ctx, sl.ID, parent, "GHOST_SUSPECT", note); err != nil {
		m.logger.Error("Safety monitor: ghost-suspect park failed — will retry next cycle",
			zap.Int64("row", sl.ID), zap.String("symbol", sl.Symbol), zap.Error(err))
		return
	}
	_ = m.repo.InsertEvent(ctx, sl.ID, "SL_GHOST_SUSPECT", string(sl.Status), "SL_DEFERRED_BAND", brokerStatus, sl.TriggerPrice, sl.Qty, note)
	m.logger.Error("Safety monitor: 0 sellable at the broker for this position (sold, transferred or pledged) — ghost suspect parked, not re-placing",
		zap.String("user_id", sl.UserID), zap.String("symbol", sl.Symbol), zap.Int64("row", sl.ID),
		zap.Int("ledger_net", net), zap.String("rms", reason))
	notifyOps(m.authNotif, ctx, sl.UserID, sl.Symbol, "GHOST_SUSPECT",
		fmt.Sprintf("0 sellable at the broker for %s but our ledger holds %d (RMS: %s) — sold, transferred or pledged; stop parked; the book reconciler decides", sl.Symbol, net, reason))
}

// parkBudgetSpent parks a stop row that keeps vanishing after the day's
// re-protect budget: something outside this service removes it every time.
func (m *SafetyMonitor) parkBudgetSpent(ctx context.Context, sl *ManthanOrder, brokerStatus string, replaced int) {
	parent := int64(0)
	if sl.ParentOrderID != nil {
		parent = *sl.ParentOrderID
	}
	note := fmt.Sprintf("re-protect budget spent: stop re-placed %d times today and lost again (%s) — parked; software stop still active", replaced, brokerStatus)
	if err := m.repo.ParkStopRow(ctx, sl.ID, parent, "REPROTECT_BUDGET", note); err != nil {
		m.logger.Error("Safety monitor: budget park failed — will retry next cycle",
			zap.Int64("row", sl.ID), zap.String("symbol", sl.Symbol), zap.Error(err))
		return
	}
	_ = m.repo.InsertEvent(ctx, sl.ID, "SL_LOST_PARKED", string(sl.Status), "SL_DEFERRED_BAND", brokerStatus, sl.TriggerPrice, sl.Qty, note)
	m.logger.Error("Safety monitor: stop keeps vanishing — re-protect budget spent, parked; position has NO broker stop (software stop still active)",
		zap.String("user_id", sl.UserID), zap.String("symbol", sl.Symbol), zap.Int64("row", sl.ID), zap.Int("replaced_today", replaced))
	notifyOps(m.authNotif, ctx, sl.UserID, sl.Symbol, "SL_LOST",
		fmt.Sprintf("stop for %s was re-placed %d times today and vanished again (%s) — parked without a broker stop; check the broker", sl.Symbol, replaced, brokerStatus))
}
