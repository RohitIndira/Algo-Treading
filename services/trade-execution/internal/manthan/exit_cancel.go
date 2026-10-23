package manthan

import (
	"context"
	"math"
	"strings"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
	"go.uber.org/zap"
)

// matchConvertedExit finds, in the broker order book, the order that one of
// our AMO exit rows became at conversion (AMOs get a NEW broker id at 08:50):
// same base symbol, SELL, a plain Limit, same qty, same limit price (2dp),
// placed today. Any status (open or executed) — the caller decides. Returns
// nil unless the match is unambiguous.
func matchConvertedExit(row *ManthanOrder, book []indiraClient.OrderBook) *indiraClient.OrderBook {
	if row == nil {
		return nil
	}
	want := strings.ToUpper(strings.TrimSpace(row.Symbol))
	var found *indiraClient.OrderBook
	for i := range book {
		o := &book[i]
		if o.OrdId == "" || o.OrdId == row.BrokerOrderID {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(o.OrdAction), "SELL") {
			continue
		}
		ot := strings.ToUpper(o.OrdType)
		if strings.Contains(ot, "SL") || strings.Contains(ot, "STOP") || strings.Contains(ot, "MARKET") {
			continue
		}
		base := strings.ToUpper(strings.TrimSpace(o.Symbol.BaseSym))
		if base == "" {
			base = strings.ToUpper(strings.TrimSpace(o.Symbol.DispSym))
			if i := strings.Index(base, "-"); i > 0 {
				base = base[:i]
			}
		}
		if base != want {
			continue
		}
		if o.Qty != row.Qty || math.Abs(o.Price-row.LimitPrice) > 0.011 {
			continue
		}
		if !placedTodayIST(o.OrdDate) || !placedAfterIST(o.OrdDate, row.CreatedAt) {
			continue // the converted order is born at the 08:50 release, after the AMO was queued
		}
		if found != nil {
			return nil // ambiguous — leave it for a human
		}
		found = o
	}
	return found
}

// cancelOpenExitSells clears every exit sell of ours still resting for the
// position before a fresh exit is placed — a DAY LIMIT from earlier today, or
// last evening's AMO LIMIT (matched back by symbol/side/qty/price when its id
// changed at conversion). A resting sell locks the quantity at the broker, so
// without this the new sell is rejected "Free Qty=0". An order found already
// executed is confirmed FILLED instead of cancelled. Returns the number of
// fills confirmed so the caller can re-check what is still held.
func (h *SLHandler) cancelOpenExitSells(ctx context.Context, auth BrokerAuth, info *SymbolInfo, strategyID, reason string) int {
	if h.repo == nil || strategyID == "" {
		return 0
	}
	rows, err := h.repo.ListOpenExitSellsForPosition(ctx, strategyID, info.Symbol)
	if err != nil {
		h.logger.Warn("exit: open-exit lookup failed — selling anyway (broker rejects if the qty is locked)",
			zap.String("symbol", info.Symbol), zap.Error(err))
		return 0
	}
	if len(rows) == 0 {
		return 0
	}
	var book []indiraClient.OrderBook
	bookLoaded := false
	loadBook := func() []indiraClient.OrderBook {
		if !bookLoaded {
			bookLoaded = true
			if b, berr := h.broker.GetOrderBook(ctx, auth); berr == nil {
				book = b
			}
		}
		return book
	}

	confirmed := 0
	for _, row := range rows {
		id := row.BrokerOrderID
		status, filled, avg, serr := h.broker.GetOrderStatus(ctx, auth, id)
		if serr != nil && row.OrderType == OrderTypeAMOSell {
			if m := matchConvertedExit(row, loadBook()); m != nil {
				h.logger.Info("exit: AMO exit converted — adopting its new broker id",
					zap.String("symbol", info.Symbol), zap.String("old", id), zap.String("new", m.OrdId))
				_ = h.repo.AdoptConvertedExit(ctx, row.ID, m.OrdId)
				id = m.OrdId
				status, filled, avg, serr = m.Status, m.TradedQty, m.Price, nil
			}
		}
		book := func(st string, fq int, ap float64, note string) bool {
			if fq <= 0 {
				fq = row.Qty
			}
			if uerr := h.repo.UpdateOrderFilled(ctx, row.ID, fq, ap); uerr != nil {
				h.logger.Error("exit: earlier exit filled at broker but the fill could not be booked",
					zap.String("symbol", info.Symbol), zap.String("broker_order_id", id), zap.Error(uerr))
				return false
			}
			_ = h.repo.InsertEvent(ctx, row.ID, "EXIT_FILLED", "PLACED", "FILLED", st, ap, fq, note)
			h.logger.Info("exit: earlier exit sell filled — booked",
				zap.String("symbol", info.Symbol), zap.String("broker_order_id", id), zap.Int("filled", fq))
			confirmed++
			return true
		}
		if serr == nil && isFilledStatus(strings.ToUpper(strings.TrimSpace(status))) {
			book(status, filled, avg, "confirmed while preparing a fresh exit: "+reason)
			continue
		}
		if cerr := h.broker.CancelOrder(ctx, auth, info, id); cerr != nil && !IsOrderNotFoundError(cerr) {
			h.logger.Warn("exit: resting exit cancel failed — selling anyway",
				zap.String("symbol", info.Symbol), zap.String("broker_order_id", id), zap.Error(cerr))
			continue
		}
		// Re-read AFTER the cancel: a fill (full or partial) in the window
		// between the first read and the cancel must be booked, never dropped.
		if st2, fq2, ap2, serr2 := h.broker.GetOrderStatus(ctx, auth, id); serr2 == nil && (isFilledStatus(strings.ToUpper(strings.TrimSpace(st2))) || fq2 > 0) {
			book(st2, fq2, ap2, "filled before/while being cancelled ahead of a fresh exit: "+reason)
			continue
		}
		_ = h.repo.UpdateOrderCancelled(ctx, row.ID)
		_ = h.repo.InsertEvent(ctx, row.ID, "EXIT_SUPERSEDED", "PLACED", "CANCELLED", status, row.LimitPrice, row.Qty,
			"resting exit cancelled ahead of a fresh exit: "+reason)
		h.logger.Info("exit: resting exit sell cancelled ahead of a fresh exit",
			zap.String("symbol", info.Symbol), zap.String("broker_order_id", id), zap.Int("qty", row.Qty))
	}
	return confirmed
}
