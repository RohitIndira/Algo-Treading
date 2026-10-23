package manthan

import (
	"context"
	"fmt"
	"math"
	"time"

	"go.uber.org/zap"
)

const (
	// avgVerifyEvery paces the verifier; avgVerifyWindow bounds which fills
	// are re-read (a day covers any trade-book lag many times over).
	avgVerifyEvery  = time.Minute
	avgVerifyWindow = 24 * time.Hour
	avgVerifyEps    = 0.005
)

// verifyFillAverages re-reads the broker's trade book for every SELL booked
// FILLED in the last day and replaces the ledger's avg_fill_price when it
// differs.
//
// Why: GetOrderStatus books a fill the moment the ORDER book shows it
// executed, and falls back to the order's LIMIT price when the TRADE book has
// no rows yet (it lags by seconds). That provisional number then sat in
// manthan_orders for good — and from 2026-10-23 the positions service treats
// avg_fill_price as the truth it re-books client P&L from, so a stale limit
// here would have propagated. Each row is verified once (AVG_FILL_VERIFIED);
// a row whose trades are still not visible is retried next minute.
func (m *SafetyMonitor) verifyFillAverages(ctx context.Context) {
	if time.Since(m.lastAvgVerify) < avgVerifyEvery {
		return
	}
	m.lastAvgVerify = time.Now()
	rows, err := m.repo.ListFilledSellsForAvgCheck(ctx, time.Now().Add(-avgVerifyWindow))
	if err != nil {
		m.logger.Warn("avg-fill verify: list failed", zap.Error(err))
		return
	}
	failed := 0
	for _, o := range rows {
		if ctx.Err() != nil {
			return
		}
		if authGated(m.authNotif, o.UserID) {
			continue
		}
		auth := m.getAuth(o.UserID)
		if auth == nil {
			continue
		}
		avg, terr := m.broker.TradeBookAvg(ctx, *auth, o.BrokerOrderID)
		if terr != nil {
			failed++
			continue
		}
		if avg <= 0 {
			continue // trade book not showing the trades yet — next minute
		}
		if math.Abs(avg-o.AvgFillPrice) >= avgVerifyEps {
			if uerr := m.repo.UpdateAvgFillPrice(ctx, o.ID, avg); uerr != nil {
				m.logger.Warn("avg-fill verify: update failed", zap.Int64("row", o.ID), zap.Error(uerr))
				continue
			}
			_ = m.repo.InsertEvent(ctx, o.ID, "AVG_FILL_CORRECTED", "FILLED", "FILLED", "", avg, o.FilledQty,
				fmt.Sprintf("avg fill %.4f → %.4f from the trade book (order-book read had carried the limit leg %.2f)", o.AvgFillPrice, avg, o.LimitPrice))
			m.logger.Warn("avg-fill verify: ledger average corrected from the trade book",
				zap.String("user_id", o.UserID), zap.String("symbol", o.Symbol), zap.String("broker_order_id", o.BrokerOrderID),
				zap.Float64("was", o.AvgFillPrice), zap.Float64("now", avg), zap.Float64("limit_leg", o.LimitPrice))
		}
		_ = m.repo.InsertEvent(ctx, o.ID, "AVG_FILL_VERIFIED", "FILLED", "FILLED", "", avg, o.FilledQty,
			"avg fill verified against the trade book")
	}
	if failed > 0 {
		m.logger.Warn("avg-fill verify: trade-book reads failed this pass", zap.Int("failed", failed), zap.Int("rows", len(rows)))
	}
}
