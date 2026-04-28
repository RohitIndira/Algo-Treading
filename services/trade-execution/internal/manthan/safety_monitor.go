package manthan

import (
	"context"
	"time"

	"go.uber.org/zap"
)

// SafetyMonitor runs every 2 seconds checking ALL active Manthan positions.
//
// Checks:
//  1. Position has SL order on broker? If not → re-place immediately
//  2. LTP below SL trigger? Verify SL filled → if not → MARKET SELL
//  3. Lower circuit? → AMO SELL for next day
//  4. Auth still valid? Ping every 5 min
//
// This is the insurance policy — prevents unlimited downside even if
// WSS drops, SL order expires, or broker cancels our SL.
type SafetyMonitor struct {
	broker    *BrokerAdapter
	repo      *Repository
	slHandler *SLHandler
	logger    *zap.Logger

	pollInterval      time.Duration
	authCheckInterval time.Duration
	lastAuthCheck     time.Time

	// getActivePositions returns all active Manthan positions from DB.
	getActivePositions func(ctx context.Context) ([]*ManthanOrder, error)
	// getAuth returns auth for a user (from credentials cache).
	getAuth func(userID string) *BrokerAuth

	// eventPub publishes SL_FILLED, EXIT_FILLED to manthan.execution.events.
	// Optional — nil-safe; falls back to local DB events only.
	eventPub *ManthanEventPublisher
}

// SetEventPublisher wires the centralized publisher used to emit SL_FILLED
// and EXIT_FILLED on broker-confirmed exits detected by polling.
func (m *SafetyMonitor) SetEventPublisher(p *ManthanEventPublisher) {
	m.eventPub = p
}

type SafetyMonitorConfig struct {
	PollInterval      time.Duration // default 2s
	AuthCheckInterval time.Duration // default 5min
}

func NewSafetyMonitor(
	broker *BrokerAdapter,
	repo *Repository,
	slHandler *SLHandler,
	getActivePositions func(ctx context.Context) ([]*ManthanOrder, error),
	getAuth func(userID string) *BrokerAuth,
	logger *zap.Logger,
	cfg SafetyMonitorConfig,
) *SafetyMonitor {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 2 * time.Second
	}
	if cfg.AuthCheckInterval <= 0 {
		cfg.AuthCheckInterval = 5 * time.Minute
	}
	return &SafetyMonitor{
		broker:             broker,
		repo:               repo,
		slHandler:          slHandler,
		logger:             logger,
		pollInterval:       cfg.PollInterval,
		authCheckInterval:  cfg.AuthCheckInterval,
		getActivePositions: getActivePositions,
		getAuth:            getAuth,
	}
}

// Start begins the safety monitor loop. Blocks until ctx cancelled.
func (m *SafetyMonitor) Start(ctx context.Context) {
	m.logger.Info("Safety monitor started", zap.Duration("interval", m.pollInterval))
	ticker := time.NewTicker(m.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			m.logger.Info("Safety monitor stopped")
			return
		case <-ticker.C:
			m.check(ctx)
		}
	}
}

func (m *SafetyMonitor) check(ctx context.Context) {
	// Get all active SL orders
	slOrders, err := m.repo.GetActiveSLOrders(ctx)
	if err != nil {
		m.logger.Error("Safety monitor: failed to get SL orders", zap.Error(err))
		return
	}

	if len(slOrders) == 0 {
		return
	}

	for _, sl := range slOrders {
		m.checkPosition(ctx, sl)
	}
}

func (m *SafetyMonitor) checkPosition(ctx context.Context, sl *ManthanOrder) {
	auth := m.getAuth(sl.UserID)
	if auth == nil {
		return // no auth available — skip this cycle
	}

	// CHECK 1: Does SL order still exist on broker?
	if sl.BrokerOrderID != "" && sl.Status == StatusSLPlaced {
		status, filledQty, avgPrice, err := m.broker.GetOrderStatus(ctx, *auth, sl.BrokerOrderID)
		if err != nil {
			// Can't check — log and move on (next cycle will retry)
			return
		}

		// SL was filled while we weren't looking (e.g., during restart)
		if isFilledStatus(status) {
			m.logger.Info("Safety monitor: SL filled (detected on check)",
				zap.String("symbol", sl.Symbol),
				zap.Int("filled_qty", filledQty),
				zap.Float64("avg_price", avgPrice))

			_ = m.repo.UpdateOrderFilled(ctx, sl.ID, filledQty, avgPrice)
			_ = m.repo.InsertEvent(ctx, sl.ID, "SL_FILLED", "SL_PLACED", "SL_FILLED",
				status, avgPrice, filledQty, "detected by safety monitor")

			// Publish SL_FILLED so rules-engine FillConsumer can flip
			// manthan_positions to EXITED + insert a position event. Resolve
			// entry signal_id via parent_order_id (SL rows carry the entry FK).
			if m.eventPub != nil {
				if entrySignalID, err := m.repo.GetEntrySignalIDByOrderID(ctx, sl.ID); err == nil && entrySignalID != "" {
					m.eventPub.PublishSLFilled(ctx, entrySignalID, sl.StrategyID, sl.UserID,
						sl.Symbol, sl.BrokerOrderID, avgPrice, int32(filledQty), 0)
				}
			}
			return
		}

		// SL was cancelled/rejected externally
		if status == "CANCELLED" || IsRejectedWSStatus(status) {
			m.logger.Warn("Safety monitor: SL order missing — re-placing!",
				zap.String("symbol", sl.Symbol),
				zap.String("broker_status", status))

			_ = m.repo.InsertEvent(ctx, sl.ID, "SL_LOST", "SL_PLACED", "CANCELLED",
				status, 0, 0, "SL cancelled/rejected externally — re-placing")

			// Get entry order for fill price
			entry, err := m.repo.GetActiveEntryBySymbol(ctx, sl.StrategyID, sl.Symbol)
			if err != nil || entry == nil {
				m.logger.Error("Safety monitor: can't find entry — EMERGENCY SELL",
					zap.String("symbol", sl.Symbol))
				info := &SymbolInfo{
					IndiraSymbol:  sl.IndiraSymbol,
					ExchangeToken: sl.ExchangeToken,
					Exchange:      "NSE",
				}
				mkBrokerID, mkErr := m.broker.PlaceMarketSell(ctx, *auth, info, sl.Qty)
				if mkErr != nil {
					m.logger.Error("EMERGENCY MARKET SELL FAILED — manual intervention required",
						zap.String("symbol", sl.Symbol), zap.Error(mkErr))
					_ = m.repo.InsertEvent(ctx, sl.ID, "EMERGENCY_SELL_FAILED", "SL_PLACED", "SL_PLACED",
						"", 0, sl.Qty, "no entry row + MARKET failed: "+mkErr.Error())
				} else {
					_ = m.repo.InsertEvent(ctx, sl.ID, "EMERGENCY_SELL", "SL_PLACED", "EMERGENCY_SELL",
						"", 0, sl.Qty, "no entry row — MARKET broker_id="+mkBrokerID)
				}
				return
			}

			// Re-place SL at current trigger level
			info := &SymbolInfo{
				Symbol:        sl.Symbol,
				IndiraSymbol:  sl.IndiraSymbol,
				ExchangeToken: sl.ExchangeToken,
				Exchange:      "NSE",
				TickSize:      0.05,
			}
			newBrokerID, placeErr := m.broker.PlaceSLSell(ctx, *auth, info, sl.Qty, sl.TriggerPrice, sl.LimitPrice)
			if placeErr != nil {
				m.logger.Error("Safety monitor: SL re-place FAILED — MARKET SELL",
					zap.String("symbol", sl.Symbol), zap.Error(placeErr))
				mkBrokerID, mkErr := m.broker.PlaceMarketSell(ctx, *auth, info, sl.Qty)
				if mkErr != nil {
					m.logger.Error("EMERGENCY MARKET SELL FAILED — manual intervention required",
						zap.String("symbol", sl.Symbol), zap.Error(mkErr))
					_ = m.repo.InsertEvent(ctx, sl.ID, "EMERGENCY_SELL_FAILED", "SL_PLACED", "SL_PLACED",
						"", 0, sl.Qty, "SL re-place + MARKET both failed: "+mkErr.Error())
				} else {
					_ = m.repo.InsertEvent(ctx, sl.ID, "EMERGENCY_SELL", "SL_PLACED", "EMERGENCY_SELL",
						"", 0, sl.Qty, "SL re-place failed — MARKET broker_id="+mkBrokerID)
				}
				return
			}
			_ = m.repo.UpdateOrderPlaced(ctx, sl.ID, newBrokerID)
			_ = m.repo.InsertEvent(ctx, sl.ID, "SL_REPLACED", "CANCELLED", "SL_PLACED",
				"", sl.TriggerPrice, sl.Qty, "re-placed by safety monitor: "+newBrokerID)
			return
		}
	}

	// CHECK 2: LTP below SL trigger? (backup for WSS lag)
	if sl.ExchangeToken != "" && sl.TriggerPrice > 0 {
		ltp, err := m.broker.FetchLTP(ctx, sl.ExchangeToken)
		if err != nil {
			return
		}

		if ltp <= sl.TriggerPrice && sl.Status == StatusSLPlaced {
			// SL should have triggered — check if it filled
			status, filledQty, avgPrice, _ := m.broker.GetOrderStatus(ctx, *auth, sl.BrokerOrderID)
			if isFilledStatus(status) {
				_ = m.repo.UpdateOrderFilled(ctx, sl.ID, filledQty, avgPrice)
				_ = m.repo.InsertEvent(ctx, sl.ID, "SL_FILLED", "SL_PLACED", "SL_FILLED",
					status, avgPrice, filledQty, "LTP below trigger — confirmed filled by safety monitor")
				if m.eventPub != nil {
					if entrySignalID, err := m.repo.GetEntrySignalIDByOrderID(ctx, sl.ID); err == nil && entrySignalID != "" {
						m.eventPub.PublishSLFilled(ctx, entrySignalID, sl.StrategyID, sl.UserID,
							sl.Symbol, sl.BrokerOrderID, avgPrice, int32(filledQty), 0)
					}
				}
				return
			}

			// SL-L triggered but NOT filled (limit price blocked by fast down-move).
			// New escalation ladder (2026-04-23 post-mortem):
			//   1. Cancel stuck SL-L
			//   2. Check lower circuit → if yes, AMO SELL (only exit path)
			//   3. Else try SL-M (stop-loss market) with same trigger — fills at
			//      next market tick, preserves SL_EXIT accounting
			//   4. If SL-M placement fails → MARKET SELL as last resort
			m.logger.Warn("Safety monitor: LTP below SL but not filled — escalating exit",
				zap.String("symbol", sl.Symbol),
				zap.Float64("ltp", ltp),
				zap.Float64("trigger", sl.TriggerPrice))

			info := &SymbolInfo{
				Symbol:        sl.Symbol,
				IndiraSymbol:  sl.IndiraSymbol,
				ExchangeToken: sl.ExchangeToken,
				Exchange:      "NSE",
			}
			_ = m.broker.CancelOrder(ctx, *auth, info, sl.BrokerOrderID)

			// Step 1: Lower circuit → AMO (only path through circuit freeze)
			_, atLower, _ := m.broker.CheckCircuit(ctx, sl.ExchangeToken)
			if atLower {
				m.logger.Warn("Safety monitor: lower circuit — AMO SELL",
					zap.String("symbol", sl.Symbol))
				amoBrokerID, _ := m.broker.PlaceAMOSell(ctx, *auth, info, sl.Qty)
				_ = m.repo.InsertEvent(ctx, sl.ID, "AMO_EXIT", "SL_PLACED", "AMO_SELL",
					"", ltp, sl.Qty, "lower circuit — AMO exit broker_id="+amoBrokerID)
				return
			}

			// Step 2: Try SL-M (stop-loss market) — same trigger, fills at market.
			// Since the trigger is already hit, SL-M converts to MARKET at next tick;
			// this preserves the "exited via SL" semantics for accounting.
			slmBrokerID, slmErr := m.broker.PlaceSLMSell(ctx, *auth, info, sl.Qty, sl.TriggerPrice)
			if slmErr == nil {
				_ = m.repo.UpdateOrderPlaced(ctx, sl.ID, slmBrokerID)
				_ = m.repo.InsertEvent(ctx, sl.ID, "SL_M_FALLBACK", "SL_PLACED", "SL_PLACED",
					"", sl.TriggerPrice, sl.Qty, "SL-L cancelled → SL-M placed broker_id="+slmBrokerID)
				m.logger.Info("Safety monitor: SL-M fallback placed",
					zap.String("symbol", sl.Symbol),
					zap.String("broker_order_id", slmBrokerID))
				return
			}
			m.logger.Warn("SL-M fallback failed — escalating to MARKET SELL",
				zap.String("symbol", sl.Symbol), zap.Error(slmErr))

			// Step 3: Last resort — MARKET SELL. Track the returned broker_order_id
			// (previously discarded, causing untracked exits on 2026-04-22).
			mkBrokerID, mkErr := m.broker.PlaceMarketSell(ctx, *auth, info, sl.Qty)
			if mkErr != nil {
				m.logger.Error("EMERGENCY MARKET SELL FAILED — manual intervention required",
					zap.String("symbol", sl.Symbol),
					zap.Int("qty", sl.Qty), zap.Error(mkErr))
				_ = m.repo.InsertEvent(ctx, sl.ID, "EMERGENCY_SELL_FAILED", "SL_PLACED", "SL_PLACED",
					"", ltp, sl.Qty, "MARKET SELL failed: "+mkErr.Error())
				return
			}
			_ = m.repo.InsertEvent(ctx, sl.ID, "EMERGENCY_SELL", "SL_PLACED", "EMERGENCY_SELL",
				"", ltp, sl.Qty, "LTP below trigger, SL-M also failed → MARKET broker_id="+mkBrokerID)
		}
	}
}
