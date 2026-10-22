package manthan

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
	"go.uber.org/zap"
)

// Recovery handles startup reconciliation between our DB state and the broker.
//
// On service restart:
//  1. Load active SL orders from manthan_orders
//  2. For each: check broker orderbook
//  3. SL still active → resume monitoring (register in WSS bridge)
//  4. SL filled (while we were down) → mark closed, update P&L
//  5. SL missing/cancelled → re-place immediately or MARKET SELL
type Recovery struct {
	broker    *BrokerAdapter
	repo      *Repository
	wssBridge *WSSBridge
	slHandler *SLHandler
	logger    *zap.Logger
	getAuth   func(userID string) *BrokerAuth
}

func NewRecovery(
	broker *BrokerAdapter,
	repo *Repository,
	wssBridge *WSSBridge,
	slHandler *SLHandler,
	getAuth func(userID string) *BrokerAuth,
	logger *zap.Logger,
) *Recovery {
	return &Recovery{
		broker:    broker,
		repo:      repo,
		wssBridge: wssBridge,
		slHandler: slHandler,
		getAuth:   getAuth,
		logger:    logger,
	}
}

// Run performs startup reconciliation. Must complete before consuming new signals.
func (r *Recovery) Run(ctx context.Context) error {
	r.logger.Info("Manthan recovery: starting reconciliation")

	// 1. Load active SL orders
	slOrders, err := r.repo.GetActiveSLOrders(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			r.logger.Info("Manthan recovery: no active positions — clean start")
			return nil
		}
		return err
	}

	if len(slOrders) == 0 {
		r.logger.Info("Manthan recovery: no active SL orders")
		return nil
	}

	r.logger.Info("Manthan recovery: reconciling positions",
		zap.Int("active_sl_orders", len(slOrders)))

	recovered := 0
	filled := 0
	replaced := 0
	emergencySold := 0

	for _, sl := range slOrders {
		auth := r.getAuth(sl.UserID)
		if auth == nil {
			r.logger.Warn("Recovery: no auth for user — skipping",
				zap.String("user", sl.UserID),
				zap.String("symbol", sl.Symbol))
			continue
		}

		// PAPER mode — just register in WSS bridge (no broker check)
		if sl.BrokerOrderID != "" && len(sl.BrokerOrderID) > 5 && sl.BrokerOrderID[:5] == "PAPER" {
			r.logger.Info("Recovery: paper position resumed",
				zap.String("symbol", sl.Symbol))
			recovered++
			continue
		}

		// LIVE mode — check broker
		if sl.BrokerOrderID == "" {
			r.logger.Warn("Recovery: SL order has no broker ID — re-placing",
				zap.String("symbol", sl.Symbol))
			r.replaceSL(ctx, sl, auth)
			replaced++
			continue
		}

		// Check broker orderbook
		status, filledQty, avgPrice, err := r.broker.GetOrderStatus(ctx, *auth, sl.BrokerOrderID)
		if err != nil {
			// AU004 on startup means the cached JWT is dead — every SL we
			// check this loop will return the same error. Surface a single
			// info line per SL (instead of warn-spam) and lean on the safety
			// monitor + reconciler to pick up state once the user re-logs in.
			if errors.Is(err, indiraClient.ErrAuthExpired) {
				r.logger.Info("Recovery: SL status check skipped — broker session expired",
					zap.String("symbol", sl.Symbol))
			} else {
				r.logger.Warn("Recovery: can't check SL status — will be caught by safety monitor",
					zap.String("symbol", sl.Symbol), zap.Error(err))
			}
			// Register in WSS bridge so future updates are caught
			if r.wssBridge != nil {
				r.wssBridge.Register(sl.BrokerOrderID)
			}
			recovered++
			continue
		}

		// SL was FILLED while we were down
		if isFilledStatus(status) {
			r.logger.Info("Recovery: SL was filled while down",
				zap.String("symbol", sl.Symbol),
				zap.Int("filled_qty", filledQty),
				zap.Float64("avg_price", avgPrice))

			_ = r.repo.UpdateOrderFilled(ctx, sl.ID, filledQty, avgPrice)
			_ = r.repo.InsertEvent(ctx, sl.ID, "SL_FILLED", "SL_PLACED", "SL_FILLED",
				status, avgPrice, filledQty, "filled during downtime — recovered on startup")
			filled++
			continue
		}

		// SL was CANCELLED or REJECTED (broker cleared it)
		if isCancelledBrokerStatus(strings.ToUpper(strings.TrimSpace(status))) {
			r.logger.Warn("Recovery: SL was cancelled/rejected — re-placing!",
				zap.String("symbol", sl.Symbol),
				zap.String("broker_status", status))
			r.replaceSL(ctx, sl, auth)
			replaced++
			continue
		}

		// SL still active on broker — register in WSS bridge for updates
		if r.wssBridge != nil {
			r.wssBridge.Register(sl.BrokerOrderID)
		}
		recovered++
	}

	r.logger.Info("Manthan recovery complete",
		zap.Int("recovered", recovered),
		zap.Int("filled_during_down", filled),
		zap.Int("sl_replaced", replaced),
		zap.Int("emergency_sold", emergencySold))

	return nil
}

func (r *Recovery) replaceSL(ctx context.Context, sl *ManthanOrder, auth *BrokerAuth) {
	// 2026-10-22 review: this path used to re-place the stop with the stored
	// token and no band, and on ANY failure sold the whole position. It now
	// mirrors SafetyMonitor.reprotectLostSL: live token, fresh band, exit
	// only when the stop is actually crossed, defer when outside the band,
	// park + alert on a rejection — never a sell on a failure, and nothing
	// at all outside the regular session.
	if !marketOpenNowIST() {
		r.logger.Warn("Recovery: SL missing at broker but the market is closed — leaving it for the 09:14 cycle / naked scan",
			zap.String("symbol", sl.Symbol), zap.String("user", sl.UserID))
		return
	}
	info := &SymbolInfo{
		Symbol:        sl.Symbol,
		ISIN:          sl.ISIN,
		IndiraSymbol:  sl.IndiraSymbol,
		ExchangeToken: sl.ExchangeToken,
		Exchange:      "NSE",
		TickSize:      0.05,
	}
	ltp, stale, asOf, lerr := r.broker.FreshLTP(ctx, info)
	if info.ExchangeToken != "" && info.ExchangeToken != sl.ExchangeToken {
		_, _ = r.repo.UpdateSymbolToken(ctx, sl.StrategyID, sl.Symbol, info.ExchangeToken, info.IndiraSymbol)
	}
	r.broker.EnrichMarketInfo(ctx, info)
	parent := int64(0)
	if sl.ParentOrderID != nil {
		parent = *sl.ParentOrderID
	}
	park := func(reason string) {
		_ = r.repo.MarkSLDeferredBand(ctx, sl.ID, parent)
		_ = r.repo.InsertEvent(ctx, sl.ID, "SL_LOST_PARKED", "SL_PLACED", "SL_DEFERRED_BAND", "", sl.TriggerPrice, sl.Qty, reason)
		r.logger.Error("Recovery: lost stop could not be re-placed — parked as deferred; NO broker stop (software stop still active)",
			zap.String("symbol", sl.Symbol), zap.String("user", sl.UserID), zap.String("reason", reason))
	}
	if lerr != nil || stale || ltp <= 0 {
		park(fmt.Sprintf("no fresh price (stale=%v as_of=%s err=%v)", stale, asOf.Format(time.RFC3339), lerr))
		return
	}
	if ltp <= sl.TriggerPrice {
		r.logger.Warn("Recovery: stop lost and LTP already below the trigger — exiting with a marketable LIMIT",
			zap.String("symbol", sl.Symbol), zap.Float64("ltp", ltp), zap.Float64("trigger", sl.TriggerPrice))
		if uerr := r.repo.UpdateOrderCancelled(ctx, sl.ID); uerr != nil {
			r.logger.Error("Recovery: could not mark the lost stop — not selling", zap.Error(uerr))
			return
		}
		brokerID, limit, serr := r.broker.PlaceMarketableLimitSell(ctx, *auth, info, sl.Qty)
		if serr != nil {
			r.logger.Error("Recovery: LIMIT exit FAILED — position has no stop; manual intervention required",
				zap.String("symbol", sl.Symbol), zap.Error(serr))
			_ = r.repo.InsertEvent(ctx, sl.ID, "EMERGENCY_SELL_FAILED", "CANCELLED", "CANCELLED",
				"", ltp, sl.Qty, "recovery: stop crossed, LIMIT exit failed: "+serr.Error())
			return
		}
		_ = r.repo.InsertEvent(ctx, sl.ID, "EMERGENCY_SELL", "SL_PLACED", "CANCELLED",
			"", limit, sl.Qty, "recovery: stop lost and crossed — LIMIT exit broker_id="+brokerID)
		if _, ierr := r.repo.InsertExitSellPlaced(ctx, ExitSellRow{
			StrategyID: sl.StrategyID, UserID: sl.UserID, Symbol: sl.Symbol, ISIN: sl.ISIN,
			Info: info, OrderType: OrderTypeMarketSell, Qty: sl.Qty, LimitPrice: limit,
			BrokerOrderID: brokerID, ParentOrderID: parent, Reason: "recovery: stop lost and crossed — exit",
		}); ierr != nil {
			r.logger.Error("Recovery: exit placed but NOT recorded",
				zap.String("symbol", sl.Symbol), zap.String("broker_order_id", brokerID), zap.Error(ierr))
		}
		return
	}
	if _, _, deferWhy := resolveBandFloor(sl.TriggerPrice, info.DPRLower); deferWhy != "" {
		park("intended stop outside today's band — deferred: " + deferWhy)
		return
	}
	limit := sl.TriggerPrice - SLLimitGap(sl.TriggerPrice, info.TickSize)
	newBrokerID, bt, bl, err := r.broker.PlaceSLSell(ctx, *auth, info, sl.Qty, sl.TriggerPrice, limit)
	if err != nil {
		if IsAuthError(err) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			r.logger.Warn("Recovery: SL re-place hit auth/network — safety monitor retries", zap.String("symbol", sl.Symbol), zap.Error(err))
			return
		}
		park("re-place rejected: " + err.Error())
		return
	}
	_ = r.repo.UpdateSLBrokerID(ctx, sl.ID, newBrokerID, parent)
	_ = r.repo.UpdateBrokerTrigger(ctx, sl.ID, bt, bl)
	_ = r.repo.InsertEvent(ctx, sl.ID, "SL_REPLACED", "CANCELLED", "SL_PLACED",
		"", sl.TriggerPrice, sl.Qty, "recovery: re-placed SL "+newBrokerID)

	if r.wssBridge != nil {
		r.wssBridge.Register(newBrokerID)
	}
}
