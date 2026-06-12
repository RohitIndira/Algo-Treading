package manthan

import (
	"context"
	"database/sql"
	"errors"

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
		if status == "CANCELLED" || IsRejectedWSStatus(status) {
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
	info := &SymbolInfo{
		Symbol:        sl.Symbol,
		IndiraSymbol:  sl.IndiraSymbol,
		ExchangeToken: sl.ExchangeToken,
		Exchange:      "NSE",
		TickSize:      0.05,
	}

	newBrokerID, _, _, err := r.broker.PlaceSLSell(ctx, *auth, info, sl.Qty, sl.TriggerPrice, sl.LimitPrice)
	if err != nil {
		r.logger.Error("Recovery: SL re-place FAILED — MARKET SELL",
			zap.String("symbol", sl.Symbol), zap.Error(err))
		_, _ = r.broker.PlaceMarketSell(ctx, *auth, info, sl.Qty)
		_ = r.repo.InsertEvent(ctx, sl.ID, "EMERGENCY_SELL", "SL_PLACED", "EMERGENCY_SELL",
			"", 0, sl.Qty, "recovery: SL re-place failed, emergency sell")
		return
	}

	_ = r.repo.UpdateOrderPlaced(ctx, sl.ID, newBrokerID)
	_ = r.repo.InsertEvent(ctx, sl.ID, "SL_REPLACED", "CANCELLED", "SL_PLACED",
		"", sl.TriggerPrice, sl.Qty, "recovery: re-placed SL "+newBrokerID)

	if r.wssBridge != nil {
		r.wssBridge.Register(newBrokerID)
	}
}
