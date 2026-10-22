package manthan

import (
	"context"
	"fmt"
	"strings"

	"go.uber.org/zap"
)

// sweepUnfilledExits runs at 16:35 before EOD Phase A. Every exit sell we
// place intraday is a DAY LIMIT; one that did not fill was swept by the
// exchange at 15:30 but its row is still PLACED, and a PLACED exit row counts
// as cover everywhere (HasOpenExitSell) — so without this sweep the position
// would sit all night with no stop and nothing resting at the broker
// (2026-10-22 review). For each PLACED MARKET_SELL row:
//   - executed at the broker        → FILLED (and published)
//   - partially filled then swept   → FILLED for the part, remainder re-queued
//   - swept / cancelled / not found → CANCELLED (EXIT_EXPIRED), and a fresh
//     AMO LIMIT exit is queued for the next session (the stop was crossed; the
//     strategy still wants out). The morning adoption/chase takes it from there.
func (p *ProtectiveReplay) sweepUnfilledExits(ctx context.Context) {
	if marketOpenNowIST() {
		p.logger.Info("EOD exit sweep: market still open — skipping (runs after 15:30 only)")
		return
	}
	rows, err := p.repo.ListOpenExitSells(ctx)
	if err != nil {
		p.logger.Warn("EOD exit sweep: list failed", zap.Error(err))
		return
	}
	swept, filled := 0, 0
	for _, row := range rows {
		if row.OrderType != OrderTypeMarketSell {
			continue // AMO exits are for the next session already
		}
		if authGated(p.authNotif, row.UserID) {
			continue
		}
		auth := p.getAuth(row.UserID)
		if auth == nil {
			continue
		}
		status, fq, ap, serr := p.broker.GetOrderStatus(ctx, *auth, row.BrokerOrderID)
		st := strings.ToUpper(strings.TrimSpace(status))
		if serr == nil && (isFilledStatus(st) || (fq > 0 && fq >= row.Qty)) {
			if fq <= 0 {
				fq = row.Qty
			}
			if uerr := p.repo.UpdateOrderFilled(ctx, row.ID, fq, ap); uerr != nil {
				p.logger.Error("EOD exit sweep: fill write failed", zap.Int64("row", row.ID), zap.Error(uerr))
				continue
			}
			_ = p.repo.InsertEvent(ctx, row.ID, "EXIT_FILLED", "PLACED", "FILLED", st, ap, fq, "confirmed by EOD exit sweep")
			filled++
			continue
		}
		if serr != nil && !IsOrderNotInBook(serr) {
			continue // auth / transport trouble: leave the row for the morning
		}
		if serr == nil && !isCancelledBrokerStatus(st) {
			// Still live at the broker after the close (odd, but never assume):
			// cancel it and re-read; anything but a terminal state is left alone.
			if cerr := p.broker.CancelOrder(ctx, *auth, &SymbolInfo{Symbol: row.Symbol, IndiraSymbol: row.IndiraSymbol, ExchangeToken: row.ExchangeToken, Exchange: "NSE"}, row.BrokerOrderID); cerr != nil && !IsOrderNotFoundError(cerr) {
				p.logger.Warn("EOD exit sweep: leftover exit still live and cancel failed — leaving it",
					zap.String("symbol", row.Symbol), zap.String("broker_order_id", row.BrokerOrderID), zap.Error(cerr))
				continue
			}
			status2, fq2, ap2, serr2 := p.broker.GetOrderStatus(ctx, *auth, row.BrokerOrderID)
			st2 := strings.ToUpper(strings.TrimSpace(status2))
			if serr2 == nil && isFilledStatus(st2) {
				if fq2 <= 0 {
					fq2 = row.Qty
				}
				_ = p.repo.UpdateOrderFilled(ctx, row.ID, fq2, ap2)
				_ = p.repo.InsertEvent(ctx, row.ID, "EXIT_FILLED", "PLACED", "FILLED", st2, ap2, fq2, "filled during EOD sweep cancel")
				filled++
				continue
			}
			if serr2 == nil && !isCancelledBrokerStatus(st2) {
				p.logger.Warn("EOD exit sweep: leftover exit not terminal after cancel — leaving it",
					zap.String("symbol", row.Symbol), zap.String("broker_order_id", row.BrokerOrderID), zap.String("status", status2))
				continue
			}
			st, fq, ap = st2, fq2, ap2
		}
		remaining := row.Qty
		if fq > 0 && fq < row.Qty {
			if uerr := p.repo.UpdateOrderFilled(ctx, row.ID, fq, ap); uerr != nil {
				p.logger.Error("EOD exit sweep: partial-fill write failed — not re-queuing", zap.Int64("row", row.ID), zap.Error(uerr))
				continue
			}
			_ = p.repo.InsertEvent(ctx, row.ID, "EXIT_PARTIAL", "PLACED", "FILLED", st, ap, fq,
				"partial fill; remainder re-queued as AMO LIMIT for the next session")
			remaining = row.Qty - fq
		} else {
			if uerr := p.repo.UpdateOrderCancelled(ctx, row.ID); uerr != nil {
				p.logger.Error("EOD exit sweep: could not mark the expired exit", zap.Int64("row", row.ID), zap.Error(uerr))
				continue
			}
			_ = p.repo.InsertEvent(ctx, row.ID, "EXIT_EXPIRED", "PLACED", "CANCELLED", st, row.LimitPrice, row.Qty,
				"DAY exit unfilled at close — re-queued as AMO LIMIT for the next session")
		}
		// Never queue more than the uncovered holding.
		if net, _, has, nerr := p.repo.NetHeldQty(ctx, row.StrategyID, row.Symbol); nerr == nil && has {
			other, _ := p.repo.SumOpenExitQty(ctx, row.StrategyID, row.Symbol, row.ID)
			if lim := net - other; lim < remaining {
				remaining = lim
			}
		}
		if remaining <= 0 {
			continue
		}

		info := &SymbolInfo{
			Symbol:        row.Symbol,
			ISIN:          row.ISIN,
			IndiraSymbol:  row.IndiraSymbol,
			ExchangeToken: row.ExchangeToken,
			Exchange:      "NSE",
			TickSize:      0.05,
		}
		amoID, limit, aerr := p.broker.PlaceAMOSellPriced(ctx, *auth, info, remaining)
		if aerr != nil {
			p.logger.Error("EOD exit sweep: AMO exit for the next session FAILED — position has no exit and no stop; manual intervention required",
				zap.String("user_id", row.UserID), zap.String("symbol", row.Symbol), zap.Int("qty", remaining), zap.Error(aerr))
			_ = p.repo.InsertEvent(ctx, row.ID, "EXIT_AMO_FAILED", "CANCELLED", "CANCELLED", "", 0, remaining, aerr.Error())
			notifyOps(p.authNotif, ctx, row.UserID, row.Symbol, "EXIT_AMO_FAILED",
				fmt.Sprintf("unfilled exit for %s could not be re-queued for tomorrow: %v", row.Symbol, aerr))
			continue
		}
		parent := int64(0)
		if row.ParentOrderID != nil {
			parent = *row.ParentOrderID
		}
		if _, ierr := p.repo.InsertExitSellPlaced(ctx, ExitSellRow{
			StrategyID: row.StrategyID, UserID: row.UserID, Symbol: row.Symbol, ISIN: row.ISIN,
			Info: info, OrderType: OrderTypeAMOSell, Qty: remaining, LimitPrice: limit,
			BrokerOrderID: amoID, ParentOrderID: parent, RetryCount: row.RetryCount + 1,
			Reason: "EOD: unfilled DAY exit re-queued as AMO LIMIT for the next session",
		}); ierr != nil {
			p.logger.Error("EOD exit sweep: AMO exit placed but NOT recorded",
				zap.String("symbol", row.Symbol), zap.String("broker_order_id", amoID), zap.Error(ierr))
		}
		p.logger.Warn("EOD exit sweep: unfilled exit re-queued as AMO LIMIT",
			zap.String("user_id", row.UserID), zap.String("symbol", row.Symbol),
			zap.Int("qty", remaining), zap.Float64("limit", limit), zap.String("broker_order_id", amoID))
		swept++
	}
	if swept > 0 || filled > 0 {
		p.logger.Info("EOD exit sweep complete", zap.Int("re_queued_as_amo", swept), zap.Int("confirmed_filled", filled))
	}
}
