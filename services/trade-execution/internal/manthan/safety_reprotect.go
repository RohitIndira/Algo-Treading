package manthan

import (
	"context"
	"errors"
	"fmt"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
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
		// No fresh price: nothing can be judged this cycle. Leave the row as it
		// is (CHECK 1 re-enters next cycle) and alert — parking it would hide
		// it from every re-protection path for the rest of the day.
		m.logger.Warn("Safety monitor: lost stop but no fresh price yet — retrying next cycle",
			zap.String("user_id", sl.UserID), zap.String("symbol", sl.Symbol),
			zap.Bool("stale", stale), zap.Time("feed_as_of", asOf), zap.NamedError("feed_err", lerr))
		notifyOps(m.authNotif, ctx, sl.UserID, sl.Symbol, "SL_LOST",
			fmt.Sprintf("stop for %s lost at broker (%s); no fresh price to re-protect yet", sl.Symbol, brokerStatus))
		return
	}

	// 2. The stop was crossed: this is exactly what the stop was for. Place the
	// exit FIRST; the stop row is closed only once the exit is accepted (its
	// exit row then makes checkPosition skip this position), so a rejected
	// exit leaves the row for the next cycle to retry.
	if ltp <= sl.TriggerPrice {
		m.logger.Warn("Safety monitor: lost stop and LTP already below the trigger — exiting with a marketable LIMIT",
			zap.String("user_id", sl.UserID), zap.String("symbol", sl.Symbol),
			zap.Float64("ltp", ltp), zap.Float64("trigger", sl.TriggerPrice))
		var brokerID string
		var limit float64
		var err error
		ot := OrderTypeMarketSell
		if _, atLower, cerr := m.broker.CheckCircuit(ctx, info.ExchangeToken); cerr == nil && atLower {
			ot = OrderTypeAMOSell
			brokerID, limit, err = m.broker.PlaceAMOSellPriced(ctx, *auth, info, sl.Qty)
		} else {
			brokerID, limit, err = m.broker.PlaceMarketableLimitSell(ctx, *auth, info, sl.Qty)
		}
		if err != nil {
			_ = m.repo.InsertEvent(ctx, sl.ID, "EMERGENCY_SELL_FAILED", "SL_PLACED", "SL_PLACED",
				"", ltp, sl.Qty, "stop lost and crossed — exit rejected (will retry): "+err.Error())
			m.logger.Error("Safety monitor: exit after lost stop FAILED — retrying next cycle; manual check advised",
				zap.String("user_id", sl.UserID), zap.String("symbol", sl.Symbol), zap.Error(err))
			notifyOps(m.authNotif, ctx, sl.UserID, sl.Symbol, "EXIT_FAILED",
				fmt.Sprintf("stop for %s lost and crossed (ltp %.2f ≤ %.2f); exit rejected: %v", sl.Symbol, ltp, sl.TriggerPrice, err))
			return
		}
		m.recordExit(ctx, sl, ot, info, sl.Qty, limit, brokerID, "stop lost at broker and already crossed — exit")
		if uerr := m.repo.UpdateOrderCancelled(ctx, sl.ID); uerr != nil {
			m.logger.Error("Safety monitor: exit placed but the lost stop row could not be closed (exit row protects against a re-sell)",
				zap.String("symbol", sl.Symbol), zap.Error(uerr))
		}
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
		var rej *BrokerRejection
		var biz *indiraClient.BrokerBusinessError
		if errors.As(err, &rej) || errors.As(err, &biz) {
			park("re-place rejected by broker: " + err.Error())
			return
		}
		// Auth, context, transport, anything unclassified: retry next cycle
		// (the row stays SL_PLACED → CHECK 1 runs again) — never park on a
		// transient (2026-10-22 review).
		m.logger.Warn("Safety monitor: lost-stop re-place hit a transient error — retrying next cycle",
			zap.String("symbol", sl.Symbol), zap.Error(err))
		return
	}
	if uerr := m.repo.UpdateSLBrokerID(ctx, sl.ID, newBrokerID, parent); uerr != nil {
		m.logger.Error("Safety monitor: stop re-placed at broker but the row could not be updated — reconciler must re-point it",
			zap.String("symbol", sl.Symbol), zap.String("new_broker_order_id", newBrokerID), zap.Error(uerr))
	}
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
