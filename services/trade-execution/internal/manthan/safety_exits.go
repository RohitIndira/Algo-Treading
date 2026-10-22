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

const (
	// exitChaseAfter — how long a marketable LIMIT exit may rest unfilled
	// before it is re-priced (and how long a rejected one waits before the
	// re-place, so a deterministic rejection cannot storm the broker).
	exitChaseAfter = 45 * time.Second
	// exitChaseMax — re-pricings / re-placements per exit lineage before we
	// stop and alert a human.
	exitChaseMax = 12
	// exitLostAfter — a DAY exit absent from the broker's book this long after
	// placement is treated as lost (order-book lag is seconds, not a minute).
	exitLostAfter = 60 * time.Second
	// exitAMOAdoptDeadlineMin — an AMO exit queued in an earlier session must
	// have found its converted order by this IST minute of the next session.
	exitAMOAdoptDeadlineMin = 10 * 60
	// orphanPendingAfter — a PENDING exit row with no broker id this old was
	// never placed (process died between INSERT and the broker call).
	orphanPendingAfter = 10 * time.Minute
	// exitChaseExhaustedMark is stamped on last_error once a lineage gave up,
	// so the row is left alone (fill/cancel still confirmed by the reconciler).
	exitChaseExhaustedMark = "chase exhausted"
)

// exitCycle caches the broker order book per user for one monitor cycle so
// several unmatched AMO rows cost one fetch, not one each.
type exitCycle struct {
	books map[string][]indiraClient.OrderBook
}

func (c *exitCycle) book(ctx context.Context, m *SafetyMonitor, auth *BrokerAuth) []indiraClient.OrderBook {
	if b, ok := c.books[auth.UserID]; ok {
		return b
	}
	b, err := m.broker.GetOrderBook(ctx, *auth)
	if err != nil {
		b = nil
	}
	c.books[auth.UserID] = b
	return b
}

// checkPendingExits confirms or chases every exit sell still PLACED at the
// broker. An exit is a LIMIT (Market is rejected for algo orders), so it can
// rest; this loop is what turns "placed" into "sold":
//   - broker says executed        → row FILLED, EXIT_FILLED published
//   - cancelled / rejected / swept → partial fill booked, remainder re-placed
//     from live prices (the exit is the strategy's decision), spaced ≥45 s
//     and capped at exitChaseMax per lineage
//   - vanished from the book       → EXIT_LOST, re-placed (same cap)
//   - AMO converted (new id)       → adopted; unmatched past 10:00 → alerted
//   - resting below the live LTP ≥45 s → cancel + re-place at LTP − slippage
func (m *SafetyMonitor) checkPendingExits(ctx context.Context) {
	if n, err := m.repo.ExpireOrphanPendingExits(ctx, orphanPendingAfter); err == nil && n > 0 {
		m.logger.Warn("Safety monitor: expired orphan PENDING exit rows (never reached the broker)", zap.Int64("count", n))
	}
	rows, err := m.repo.ListOpenExitSells(ctx)
	if err != nil {
		m.logger.Warn("Safety monitor: open-exit query failed", zap.Error(err))
		return
	}
	cyc := &exitCycle{books: map[string][]indiraClient.OrderBook{}}
	for _, row := range rows {
		m.checkPendingExit(ctx, cyc, row)
	}
}

func (m *SafetyMonitor) checkPendingExit(ctx context.Context, cyc *exitCycle, row *ManthanOrder) {
	if authGated(m.authNotif, row.UserID) {
		return
	}
	auth := m.getAuth(row.UserID)
	if auth == nil {
		return
	}
	if strings.Contains(row.LastError, exitChaseExhaustedMark) {
		return // gave up and alerted; the reconciler still confirms fill/cancel
	}
	info := &SymbolInfo{
		Symbol:        row.Symbol,
		ISIN:          row.ISIN,
		IndiraSymbol:  row.IndiraSymbol,
		ExchangeToken: row.ExchangeToken,
		Exchange:      "NSE",
		TickSize:      0.05,
	}
	var age time.Duration
	if row.PlacedAt != nil {
		age = time.Since(*row.PlacedAt)
	}

	status, filledQty, avgPrice, err := m.broker.GetOrderStatus(ctx, *auth, row.BrokerOrderID)
	if err != nil {
		if errors.Is(err, indiraClient.ErrAuthExpired) {
			notifyAuthExpired(m.authNotif, ctx, row.UserID, "safety-monitor-exit")
			return
		}
		if !strings.Contains(err.Error(), "not found") {
			return
		}
		switch {
		case row.OrderType == OrderTypeAMOSell:
			// An AMO exit gets a NEW broker id at the 08:50 conversion; find
			// the converted order in the book and adopt it (next cycle
			// confirms/chases). Unmatched past the deadline → a human must look:
			// the converted order may rest under an id we do not know.
			if conv := matchConvertedExit(row, cyc.book(ctx, m, auth)); conv != nil {
				if aerr := m.repo.AdoptConvertedExit(ctx, row.ID, conv.OrdId); aerr != nil {
					m.logger.Warn("Safety monitor: AMO adoption write failed", zap.Int64("row", row.ID), zap.Error(aerr))
					return
				}
				_ = m.repo.InsertEvent(ctx, row.ID, "EXIT_AMO_CONVERTED", "PLACED", "PLACED", conv.Status, row.LimitPrice, row.Qty,
					"AMO exit converted: "+row.BrokerOrderID+" → "+conv.OrdId)
				m.logger.Info("Safety monitor: AMO exit converted — adopted new broker id",
					zap.String("symbol", row.Symbol), zap.String("old", row.BrokerOrderID), zap.String("new", conv.OrdId))
				return
			}
			if m.amoAdoptionOverdue(row) {
				m.giveUp(ctx, row, true, "AMO exit not found in the book and no converted order matched by the adoption deadline — its converted order (if any) is untracked and may still lock the quantity")
			}
		case age >= exitLostAfter:
			// A DAY exit that never reached the broker's book (BODALCHEM
			// 2026-10-22: "Requested" at placement, then absent). The strategy
			// still wants out: book the loss and place a fresh exit from LIVE
			// prices — bounded like every other re-place.
			if row.RetryCount >= exitChaseMax {
				m.giveUp(ctx, row, true, "exit vanished from the broker book after the maximum number of re-placements")
				return
			}
			if uerr := m.repo.UpdateOrderCancelled(ctx, row.ID); uerr != nil {
				m.logger.Error("Safety monitor: could not mark the lost exit — not re-placing this cycle", zap.Int64("row", row.ID), zap.Error(uerr))
				return
			}
			_ = m.repo.InsertEvent(ctx, row.ID, "EXIT_LOST", "PLACED", "CANCELLED", "", row.LimitPrice, row.Qty,
				"exit order not in the broker book — re-placing from live prices")
			m.logger.Error("Safety monitor: exit sell vanished from the broker book — re-placing from live prices",
				zap.String("user_id", row.UserID), zap.String("symbol", row.Symbol),
				zap.String("broker_order_id", row.BrokerOrderID), zap.Duration("age", age), zap.Int("repricing", row.RetryCount+1))
			m.replaceExit(ctx, auth, row, info, row.Qty, "exit vanished from the broker book", 0)
		}
		return
	}

	st := strings.ToUpper(strings.TrimSpace(status)) // REST book is mixed case ("Cancelled")
	if isFilledStatus(st) {
		m.confirmExitFilled(ctx, row, st, filledQty, avgPrice, "confirmed by safety monitor")
		return
	}
	if isCancelledBrokerStatus(st) {
		if filledQty >= row.Qty && filledQty > 0 {
			m.confirmExitFilled(ctx, row, st, filledQty, avgPrice, "filled before the terminal status")
			return
		}
		// Spacing: a rejection is re-placed no sooner than exitChaseAfter after
		// the placement (the row stays PLACED until then), so a deterministic
		// rejection cannot storm the broker every 15 s.
		if age < exitChaseAfter {
			return
		}
		if row.RetryCount >= exitChaseMax {
			m.giveUp(ctx, row, true, "exit "+st+" at broker after the maximum number of re-placements")
			return
		}
		remaining := row.Qty
		if filledQty > 0 {
			// Partial fill then cancel/sweep: the filled part is real money —
			// book it, re-place only the remainder (2026-10-22 review).
			_ = m.repo.UpdateOrderFilled(ctx, row.ID, filledQty, avgPrice)
			_ = m.repo.InsertEvent(ctx, row.ID, "EXIT_PARTIAL", "PLACED", "FILLED", st, avgPrice, filledQty,
				"partial fill before the exit was "+st+"; remainder re-placed on a new row")
			m.publishExitFilled(ctx, row, avgPrice, filledQty)
			remaining = row.Qty - filledQty
		} else {
			if uerr := m.repo.UpdateOrderCancelled(ctx, row.ID); uerr != nil {
				m.logger.Error("Safety monitor: could not mark the cancelled exit — not re-placing this cycle", zap.Int64("row", row.ID), zap.Error(uerr))
				return
			}
			_ = m.repo.InsertEvent(ctx, row.ID, "EXIT_LOST", "PLACED", "CANCELLED", st, row.LimitPrice, row.Qty,
				"exit sell "+st+" at broker — re-placing from live prices")
		}
		// The exit is the strategy's decision: whether the exchange swept it or
		// a person cancelled it, the stop is still crossed, so it is re-placed
		// (bounded). Operators who want a position kept pause the strategy.
		m.logger.Error("Safety monitor: exit sell "+st+" at broker — position still held, re-placing from live prices",
			zap.String("user_id", row.UserID), zap.String("symbol", row.Symbol),
			zap.String("broker_order_id", row.BrokerOrderID), zap.Int("remaining", remaining), zap.Int("repricing", row.RetryCount+1))
		m.replaceExit(ctx, auth, row, info, remaining, "exit "+st+" at broker — re-placed", 0)
		return
	}

	if row.OrderType != OrderTypeMarketSell {
		return // AMO exits wait for the next session — nothing to chase today
	}
	if age < exitChaseAfter {
		return
	}
	if info.ExchangeToken == "" && !m.broker.refreshToken(ctx, info) {
		m.giveUp(ctx, row, false, "exit row has no exchange token and the ISIN key cannot resolve one — cannot price a re-place")
		return
	}
	ltp, stale, asOf, lerr := m.broker.FreshLTP(ctx, info)
	switch {
	case lerr != nil:
		return
	case stale:
		// No live row under any token: an exit resting this long on a dead
		// feed is re-priced from whatever price source ExitLimitPrice finds.
		m.logger.Warn("Safety monitor: exit resting on a STALE feed row — re-pricing",
			zap.String("symbol", row.Symbol), zap.Time("feed_as_of", asOf), zap.Float64("limit", row.LimitPrice))
	case ltp <= 0 || ltp >= row.LimitPrice:
		return // still marketable at our price — give the exchange time
	}
	if row.RetryCount >= exitChaseMax {
		m.giveUp(ctx, row, false, fmt.Sprintf("unfilled after %d re-pricings (limit %.2f, ltp %.2f)", row.RetryCount, row.LimitPrice, ltp))
		return
	}

	m.logger.Warn("Safety monitor: exit LIMIT resting below market — re-pricing",
		zap.String("user_id", row.UserID), zap.String("symbol", row.Symbol),
		zap.Float64("limit", row.LimitPrice), zap.Float64("ltp", ltp), zap.Int("repricing", row.RetryCount+1))
	if cerr := m.broker.CancelOrder(ctx, *auth, info, row.BrokerOrderID); cerr != nil && !IsOrderNotFoundError(cerr) {
		m.logger.Warn("Safety monitor: exit re-price cancel failed — will retry next cycle",
			zap.String("symbol", row.Symbol), zap.Error(cerr))
		return
	}
	// The cancel may have raced a fill — re-read before re-placing.
	status2, filled2, avg2, serr := m.broker.GetOrderStatus(ctx, *auth, row.BrokerOrderID)
	st2 := strings.ToUpper(strings.TrimSpace(status2))
	if serr == nil && isFilledStatus(st2) {
		m.confirmExitFilled(ctx, row, st2, filled2, avg2, "filled during re-price cancel")
		return
	}
	remaining := row.Qty
	if serr == nil && filled2 > 0 && filled2 < row.Qty {
		_ = m.repo.UpdateOrderFilled(ctx, row.ID, filled2, avg2)
		_ = m.repo.InsertEvent(ctx, row.ID, "EXIT_PARTIAL", "PLACED", "FILLED", st2, avg2, filled2,
			"partial fill before re-price; remainder re-placed on a new row")
		m.publishExitFilled(ctx, row, avg2, filled2)
		remaining = row.Qty - filled2
	} else {
		if uerr := m.repo.UpdateOrderCancelled(ctx, row.ID); uerr != nil {
			m.logger.Error("Safety monitor: could not mark the cancelled exit — the order is cancelled at the broker; retrying the re-place next cycle",
				zap.Int64("row", row.ID), zap.Error(uerr))
			return
		}
		_ = m.repo.InsertEvent(ctx, row.ID, "EXIT_REPRICE_CANCEL", "PLACED", "CANCELLED", st2, row.LimitPrice, row.Qty,
			"resting exit cancelled for re-price")
	}
	m.replaceExit(ctx, auth, row, info, remaining,
		fmt.Sprintf("re-priced exit #%d (was %.2f, ltp %.2f)", row.RetryCount+1, row.LimitPrice, ltp), row.LimitPrice)
}

// amoAdoptionOverdue: the AMO row was queued in an earlier session and the
// next session's adoption deadline (10:00 IST) has passed.
func (m *SafetyMonitor) amoAdoptionOverdue(row *ManthanOrder) bool {
	ist := istLocation()
	n := time.Now().In(ist)
	if !indiraClient.IsTradingDay(n) || n.Hour()*60+n.Minute() < exitAMOAdoptDeadlineMin {
		return false
	}
	placed := row.CreatedAt
	if row.PlacedAt != nil {
		placed = *row.PlacedAt
	}
	sessionStart := time.Date(n.Year(), n.Month(), n.Day(), 8, 0, 0, 0, ist)
	return placed.Before(sessionStart)
}

// giveUp records, once, that a lineage will not be re-placed again and alerts
// a human. lost=true: nothing rests at the broker → the row is CANCELLED so it
// stops counting as cover; lost=false: the order still rests → the row stays
// PLACED (fill/cancel confirmed by the reconciler) but is no longer chased.
func (m *SafetyMonitor) giveUp(ctx context.Context, row *ManthanOrder, lost bool, why string) {
	if lost {
		if uerr := m.repo.UpdateOrderCancelled(ctx, row.ID); uerr != nil {
			m.logger.Error("Safety monitor: give-up write failed", zap.Int64("row", row.ID), zap.Error(uerr))
			return
		}
		_ = m.repo.UpdateOrderStatus(ctx, row.ID, StatusCancelled, "", row.RetryCount, exitChaseExhaustedMark+": "+why)
		_ = m.repo.InsertEvent(ctx, row.ID, "EXIT_CHASE_EXHAUSTED", "PLACED", "CANCELLED", "", row.LimitPrice, row.Qty, why)
	} else {
		_ = m.repo.UpdateOrderStatus(ctx, row.ID, StatusPlaced, "", row.RetryCount, exitChaseExhaustedMark+": "+why)
		_ = m.repo.InsertEvent(ctx, row.ID, "EXIT_CHASE_EXHAUSTED", "PLACED", "PLACED", "", row.LimitPrice, row.Qty, why)
	}
	m.logger.Error("Safety monitor: exit lineage exhausted — manual intervention required",
		zap.String("user_id", row.UserID), zap.String("symbol", row.Symbol), zap.Int("qty", row.Qty),
		zap.Bool("nothing_resting_at_broker", lost), zap.String("why", why))
	notifyOps(m.authNotif, ctx, row.UserID, row.Symbol, "EXIT_CHASE_EXHAUSTED",
		fmt.Sprintf("exit for %s (qty %d) gave up: %s", row.Symbol, row.Qty, why))
}

// replaceExit places a fresh marketable LIMIT for the remainder of a lost /
// re-priced exit and books it as a new PLACED row carrying the lineage's
// re-price count. The quantity is capped at the net holding minus whatever
// other exit rows already have resting. restoreLimit > 0: if no price can be
// found (dead feed AND no broker price) the previous order is put back at
// that limit so the exit stays in flight and tracked; 0 means there is
// nothing to restore.
func (m *SafetyMonitor) replaceExit(ctx context.Context, auth *BrokerAuth, row *ManthanOrder, info *SymbolInfo, qty int, reason string, restoreLimit float64) {
	if net, _, has, nerr := m.repo.NetHeldQty(ctx, row.StrategyID, row.Symbol); nerr == nil && has {
		other, _ := m.repo.SumOpenExitQty(ctx, row.StrategyID, row.Symbol, row.ID)
		if cap := net - other; cap < qty {
			m.logger.Warn("Safety monitor: re-place qty capped at the uncovered holding",
				zap.String("symbol", row.Symbol), zap.Int("requested", qty), zap.Int("net", net), zap.Int("other_open_exits", other))
			qty = cap
		}
	}
	if qty <= 0 {
		m.logger.Info("Safety monitor: nothing left to re-place for this exit", zap.String("symbol", row.Symbol))
		return
	}
	newID, newLimit, perr := m.broker.PlaceMarketableLimitSell(ctx, *auth, info, qty)
	if perr != nil {
		m.logger.Error("Safety monitor: exit re-place FAILED — manual intervention required",
			zap.String("user_id", row.UserID), zap.String("symbol", row.Symbol), zap.Int("qty", qty),
			zap.Float64("restore_limit", restoreLimit), zap.Error(perr))
		_ = m.repo.InsertEvent(ctx, row.ID, "EXIT_REPRICE_FAILED", "CANCELLED", "CANCELLED", "", restoreLimit, qty, perr.Error())
		notifyOps(m.authNotif, ctx, row.UserID, row.Symbol, "EXIT_REPRICE_FAILED",
			fmt.Sprintf("exit for %s (qty %d) could not be re-placed: %v", row.Symbol, qty, perr))
		if restoreLimit > 0 {
			if restoreID, rerr := m.broker.PlaceLimitSell(ctx, *auth, info, qty, restoreLimit); rerr == nil {
				newID, newLimit, perr = restoreID, restoreLimit, nil
			}
		}
		if perr != nil {
			return
		}
	}
	parent := int64(0)
	if row.ParentOrderID != nil {
		parent = *row.ParentOrderID
	}
	newRowID, ierr := m.repo.InsertExitSellPlaced(ctx, ExitSellRow{
		StrategyID: row.StrategyID, UserID: row.UserID, Symbol: row.Symbol, ISIN: row.ISIN,
		Info: info, OrderType: OrderTypeMarketSell, Qty: qty, LimitPrice: newLimit,
		BrokerOrderID: newID, ParentOrderID: parent, Reason: reason,
	})
	if ierr != nil {
		m.logger.Error("Safety monitor: replacement exit placed but NOT recorded",
			zap.String("symbol", row.Symbol), zap.String("broker_order_id", newID), zap.Error(ierr))
		return
	}
	if newRowID > 0 {
		_ = m.repo.UpdateOrderStatus(ctx, newRowID, StatusPlaced, "", row.RetryCount+1, "")
	}
}

func (m *SafetyMonitor) confirmExitFilled(ctx context.Context, row *ManthanOrder, status string, filledQty int, avgPrice float64, note string) {
	if filledQty <= 0 {
		filledQty = row.Qty
	}
	_ = m.repo.UpdateOrderFilled(ctx, row.ID, filledQty, avgPrice)
	_ = m.repo.InsertEvent(ctx, row.ID, "EXIT_FILLED", "PLACED", "FILLED", status, avgPrice, filledQty, note)
	m.logger.Info("Safety monitor: exit sell FILLED",
		zap.String("user_id", row.UserID), zap.String("symbol", row.Symbol),
		zap.Int("filled_qty", filledQty), zap.Float64("avg_price", avgPrice),
		zap.String("broker_order_id", row.BrokerOrderID))
	m.publishExitFilled(ctx, row, avgPrice, filledQty)
}

func (m *SafetyMonitor) publishExitFilled(ctx context.Context, row *ManthanOrder, avgPrice float64, qty int) {
	if m.eventPub == nil {
		return
	}
	entrySignalID, err := m.repo.GetEntrySignalIDByOrderID(ctx, row.ID)
	if err != nil || entrySignalID == "" {
		return
	}
	m.eventPub.PublishExitFilled(ctx, entrySignalID, row.StrategyID, row.UserID, row.Symbol,
		row.BrokerOrderID, "SAFETY_MONITOR", avgPrice, int32(qty), 0)
}

// recordExit books an exit sell the monitor just placed (escalation ladder,
// lost-stop paths) as a PLACED row for checkPendingExits to confirm.
func (m *SafetyMonitor) recordExit(ctx context.Context, sl *ManthanOrder, ot OrderType, info *SymbolInfo, qty int, limit float64, brokerID, reason string) {
	parent := int64(0)
	if sl.ParentOrderID != nil {
		parent = *sl.ParentOrderID
	}
	if _, err := m.repo.InsertExitSellPlaced(ctx, ExitSellRow{
		StrategyID: sl.StrategyID, UserID: sl.UserID, Symbol: sl.Symbol, ISIN: sl.ISIN,
		Info: info, OrderType: ot, Qty: qty, LimitPrice: limit, BrokerOrderID: brokerID,
		ParentOrderID: parent, Reason: reason,
	}); err != nil {
		m.logger.Error("Safety monitor: exit sell placed at broker but NOT recorded",
			zap.String("symbol", sl.Symbol), zap.String("broker_order_id", brokerID), zap.Error(err))
	}
	m.logger.Warn("Safety monitor: exit sell placed — awaiting fill confirmation",
		zap.String("user_id", sl.UserID), zap.String("symbol", sl.Symbol),
		zap.String("type", string(ot)), zap.Int("qty", qty), zap.Float64("limit", limit),
		zap.String("broker_order_id", brokerID), zap.String("reason", reason))
}
