package manthan

import (
	"context"
	"errors"
	"fmt"
	"time"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
	"go.uber.org/zap"
)

const (
	// exitChaseAfter — how long a marketable LIMIT exit may rest unfilled
	// below the market before it is cancelled and re-priced from the live LTP.
	exitChaseAfter = 45 * time.Second
	// exitChaseMax — re-pricings per exit lineage before we stop and alert.
	exitChaseMax = 12
	// exitLookback — how far back open exit rows are considered (an AMO exit
	// queued yesterday evening converts this morning).
	exitLookback = 48 * time.Hour
)

// checkPendingExits confirms or chases every exit sell still PLACED at the
// broker. An exit is a LIMIT (Market is rejected for algo orders), so it can
// rest; this loop is what turns "placed" into "sold":
//   - broker says executed       → row FILLED, EXIT_FILLED published
//   - broker says cancelled/rej  → row CANCELLED (position shows as held
//     again; the protection paths take over)
//   - open and LTP below our limit for ≥ exitChaseAfter → cancel + re-place
//     at a fresh marketable limit (a partial fill is closed out on the old
//     row; the remainder gets its own row so net-qty arithmetic stays exact)
func (m *SafetyMonitor) checkPendingExits(ctx context.Context) {
	rows, err := m.repo.ListOpenExitSells(ctx, time.Now().Add(-exitLookback))
	if err != nil {
		m.logger.Warn("Safety monitor: open-exit query failed", zap.Error(err))
		return
	}
	for _, row := range rows {
		m.checkPendingExit(ctx, row)
	}
}

func (m *SafetyMonitor) checkPendingExit(ctx context.Context, row *ManthanOrder) {
	if authGated(m.authNotif, row.UserID) {
		return
	}
	auth := m.getAuth(row.UserID)
	if auth == nil {
		return
	}
	status, filledQty, avgPrice, err := m.broker.GetOrderStatus(ctx, *auth, row.BrokerOrderID)
	if err != nil {
		if errors.Is(err, indiraClient.ErrAuthExpired) {
			notifyAuthExpired(m.authNotif, ctx, row.UserID, "safety-monitor-exit")
		}
		return
	}
	if isFilledStatus(status) {
		m.confirmExitFilled(ctx, row, status, filledQty, avgPrice, "confirmed by safety monitor")
		return
	}
	if status == "CANCELLED" || IsRejectedWSStatus(status) {
		_ = m.repo.UpdateOrderCancelled(ctx, row.ID)
		_ = m.repo.InsertEvent(ctx, row.ID, "EXIT_LOST", "PLACED", "CANCELLED", status, row.LimitPrice, row.Qty,
			"exit sell cancelled/rejected at broker — position is held again; protection paths will re-arm")
		m.logger.Error("Safety monitor: exit sell cancelled/rejected at broker — position still held",
			zap.String("user_id", row.UserID), zap.String("symbol", row.Symbol),
			zap.String("broker_order_id", row.BrokerOrderID), zap.String("broker_status", status))
		return
	}
	if row.OrderType != OrderTypeMarketSell {
		return // AMO exits wait for the next session — nothing to chase today
	}
	if row.PlacedAt == nil || time.Since(*row.PlacedAt) < exitChaseAfter || row.ExchangeToken == "" {
		return
	}
	ltp, lerr := m.broker.FetchLTP(ctx, row.ExchangeToken)
	if lerr != nil || ltp <= 0 || ltp >= row.LimitPrice {
		return // still marketable at our price — give the exchange time
	}
	if row.RetryCount >= exitChaseMax {
		m.logger.Error("Safety monitor: exit sell unfilled after max re-pricings — manual intervention required",
			zap.String("user_id", row.UserID), zap.String("symbol", row.Symbol),
			zap.Float64("limit", row.LimitPrice), zap.Float64("ltp", ltp), zap.Int("repricings", row.RetryCount))
		return
	}

	info := &SymbolInfo{
		Symbol:        row.Symbol,
		IndiraSymbol:  row.IndiraSymbol,
		ExchangeToken: row.ExchangeToken,
		Exchange:      "NSE",
		TickSize:      0.05,
	}
	m.broker.EnrichMarketInfo(ctx, info)

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
	if serr == nil && isFilledStatus(status2) {
		m.confirmExitFilled(ctx, row, status2, filled2, avg2, "filled during re-price cancel")
		return
	}
	remaining := row.Qty
	if serr == nil && filled2 > 0 && filled2 < row.Qty {
		_ = m.repo.UpdateOrderFilled(ctx, row.ID, filled2, avg2)
		_ = m.repo.InsertEvent(ctx, row.ID, "EXIT_PARTIAL", "PLACED", "FILLED", status2, avg2, filled2,
			"partial fill before re-price; remainder re-placed on a new row")
		m.publishExitFilled(ctx, row, avg2, filled2)
		remaining = row.Qty - filled2
	} else {
		_ = m.repo.UpdateOrderCancelled(ctx, row.ID)
		_ = m.repo.InsertEvent(ctx, row.ID, "EXIT_REPRICE_CANCEL", "PLACED", "CANCELLED", status2, row.LimitPrice, row.Qty,
			"resting exit cancelled for re-price")
	}

	newID, newLimit, perr := m.broker.PlaceMarketableLimitSell(ctx, *auth, info, remaining)
	if perr != nil {
		m.logger.Error("Safety monitor: exit re-place FAILED after cancel — position held, protection paths will re-arm",
			zap.String("user_id", row.UserID), zap.String("symbol", row.Symbol), zap.Int("qty", remaining), zap.Error(perr))
		_ = m.repo.InsertEvent(ctx, row.ID, "EXIT_REPRICE_FAILED", "CANCELLED", "CANCELLED", "", 0, remaining, perr.Error())
		return
	}
	parent := int64(0)
	if row.ParentOrderID != nil {
		parent = *row.ParentOrderID
	}
	newRowID, ierr := m.repo.InsertExitSellPlaced(ctx, ExitSellRow{
		StrategyID: row.StrategyID, UserID: row.UserID, Symbol: row.Symbol, ISIN: row.ISIN,
		Info: info, OrderType: OrderTypeMarketSell, Qty: remaining, LimitPrice: newLimit,
		BrokerOrderID: newID, ParentOrderID: parent,
		Reason: fmt.Sprintf("re-priced exit #%d (was %.2f, ltp %.2f)", row.RetryCount+1, row.LimitPrice, ltp),
	})
	if ierr != nil {
		m.logger.Error("Safety monitor: re-priced exit placed but NOT recorded",
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
// lost-SL fallbacks) as a PLACED row for checkPendingExits to confirm.
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
