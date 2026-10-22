package manthan

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"
)

// reprotectLostSL restores protection for a position whose standing stop
// vanished at the broker (cancelled by a person, rejected, swept).
//
// 2026-10-22 review: the old path re-placed the stop with the stored token
// and NO band, and on ANY failure sold the whole position — a lost stop on a
// healthy winner (whose exact 20% stop legitimately sits below today's band)
// became a liquidation. Order of business now:
//  1. live token + fresh band (ISIN key first; stale rows ignored);
//  2. stop already crossed (fresh LTP ≤ intended trigger) → exit NOW with a
//     marketable LIMIT, tracked as an exit row — the ONLY case that sells;
//  3. intended trigger inside today's band → re-place the stop (wide leg);
//  4. intended trigger below the band → DEFER (SL_DEFERRED_BAND), never clamp;
//  5. placement rejected / no fresh price → park as deferred and alert. The
//     naked scan, the 09:35 / 11:00 / 09:14 passes and the rules-engine
//     software stop stay in force. Nothing here sells on a failure.
func (m *SafetyMonitor) reprotectLostSL(ctx context.Context, auth *BrokerAuth, sl *ManthanOrder, brokerStatus string) {
	info := &SymbolInfo{
		Symbol:        sl.Symbol,
		ISIN:          sl.ISIN,
		IndiraSymbol:  sl.IndiraSymbol,
		ExchangeToken: sl.ExchangeToken,
		Exchange:      "NSE",
		TickSize:      0.05,
	}
	ltp, stale, asOf, lerr := m.broker.FreshLTP(ctx, info)
	if info.ExchangeToken != "" && info.ExchangeToken != sl.ExchangeToken {
		_, _ = m.repo.UpdateSymbolToken(ctx, sl.StrategyID, sl.Symbol, info.ExchangeToken, info.IndiraSymbol)
	}
	m.broker.EnrichMarketInfo(ctx, info)
	parent := int64(0)
	if sl.ParentOrderID != nil {
		parent = *sl.ParentOrderID
	}
	park := func(reason string) {
		_ = m.repo.MarkSLDeferredBand(ctx, sl.ID, parent)
		_ = m.repo.InsertEvent(ctx, sl.ID, "SL_LOST_PARKED", "SL_PLACED", "SL_DEFERRED_BAND",
			brokerStatus, sl.TriggerPrice, sl.Qty, reason)
		m.logger.Error("Safety monitor: lost stop could not be re-placed — parked as deferred; position has NO broker stop (software stop still active)",
			zap.String("user_id", sl.UserID), zap.String("symbol", sl.Symbol),
			zap.Float64("intended_trigger", sl.TriggerPrice), zap.String("reason", reason))
		notifyOps(m.authNotif, ctx, sl.UserID, sl.Symbol, "SL_LOST",
			fmt.Sprintf("stop for %s lost at broker (%s) and not re-placed: %s", sl.Symbol, brokerStatus, reason))
	}

	if lerr != nil || stale || ltp <= 0 {
		park(fmt.Sprintf("no fresh price (stale=%v as_of=%s err=%v) — cannot judge band or stop", stale, asOf.Format(time.RFC3339), lerr))
		return
	}

	// 2. The stop was crossed: this is exactly what the stop was for.
	if ltp <= sl.TriggerPrice {
		m.logger.Warn("Safety monitor: lost stop and LTP already below the trigger — exiting with a marketable LIMIT",
			zap.String("user_id", sl.UserID), zap.String("symbol", sl.Symbol),
			zap.Float64("ltp", ltp), zap.Float64("trigger", sl.TriggerPrice))
		if uerr := m.repo.UpdateOrderCancelled(ctx, sl.ID); uerr != nil {
			m.logger.Error("Safety monitor: could not mark the lost stop — not selling this cycle",
				zap.String("symbol", sl.Symbol), zap.Error(uerr))
			return
		}
		brokerID, limit, err := m.broker.PlaceMarketableLimitSell(ctx, *auth, info, sl.Qty)
		if err != nil {
			_ = m.repo.InsertEvent(ctx, sl.ID, "EMERGENCY_SELL_FAILED", "CANCELLED", "CANCELLED",
				"", ltp, sl.Qty, "stop lost and crossed — LIMIT exit failed: "+err.Error())
			m.logger.Error("Safety monitor: exit after lost stop FAILED — manual intervention required",
				zap.String("user_id", sl.UserID), zap.String("symbol", sl.Symbol), zap.Error(err))
			notifyOps(m.authNotif, ctx, sl.UserID, sl.Symbol, "EXIT_FAILED",
				fmt.Sprintf("stop for %s lost and crossed (ltp %.2f ≤ %.2f); exit rejected: %v", sl.Symbol, ltp, sl.TriggerPrice, err))
			return
		}
		m.recordExit(ctx, sl, OrderTypeMarketSell, info, sl.Qty, limit, brokerID, "stop lost at broker and already crossed — exit")
		return
	}

	// 3/4. Band gate on the EXACT intended trigger — never clamp.
	if _, _, deferWhy := resolveBandFloor(sl.TriggerPrice, info.DPRLower); deferWhy != "" {
		park("intended stop outside today's band — deferred: " + deferWhy)
		return
	}
	limit := sl.TriggerPrice - SLLimitGap(sl.TriggerPrice, info.TickSize)
	newBrokerID, bt, bl, err := m.broker.PlaceSLSell(ctx, *auth, info, sl.Qty, sl.TriggerPrice, limit)
	if err != nil {
		if IsAuthError(err) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			m.logger.Warn("Safety monitor: lost-stop re-place hit auth/network — retrying next cycle",
				zap.String("symbol", sl.Symbol), zap.Error(err))
			return // row stays SL_PLACED → CHECK 1 runs again next cycle
		}
		park("re-place rejected by broker: " + err.Error())
		return
	}
	_ = m.repo.UpdateSLBrokerID(ctx, sl.ID, newBrokerID, parent)
	_ = m.repo.UpdateBrokerTrigger(ctx, sl.ID, bt, bl)
	_ = m.repo.InsertEvent(ctx, sl.ID, "SL_REPLACED", "CANCELLED", "SL_PLACED",
		"", sl.TriggerPrice, sl.Qty, "re-placed by safety monitor: "+newBrokerID)
	m.logger.Info("Safety monitor: lost stop re-placed",
		zap.String("user_id", sl.UserID), zap.String("symbol", sl.Symbol),
		zap.Float64("trigger", bt), zap.Float64("limit", bl), zap.String("broker_order_id", newBrokerID))
	if m.eventPub != nil {
		if entrySignalID, e := m.repo.GetEntrySignalIDByOrderID(ctx, sl.ID); e == nil && entrySignalID != "" {
			m.eventPub.PublishSLPlaced(ctx,
				ManthanSignal{OrderID: entrySignalID, UserID: sl.UserID, StrategyID: sl.StrategyID, Symbol: sl.Symbol, TradingMode: "LIVE"},
				newBrokerID, sl.TriggerPrice, limit)
		}
	}
}
