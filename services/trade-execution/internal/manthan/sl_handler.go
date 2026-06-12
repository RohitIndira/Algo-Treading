package manthan

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

// SLHandler manages stop-loss lifecycle:
//   - Initial SL-L SELL after entry fill
//   - Trail modify on +2% trigger (with 30s cooldown)
//   - Emergency MARKET SELL when SL-L fails
//   - AMO SELL for lower circuit
type SLHandler struct {
	broker       *BrokerAdapter
	repo         *Repository
	authProvider AuthProvider
	// refreshAuth: invalidate cache + reload from DB on AU004/401 — see
	// EntryHandler.refreshAuth for rationale.
	refreshAuth AuthProvider
	// eventPub publishes SL_PLACED / SL_MODIFIED / SL_REJECTED to
	// manthan.execution.events. Optional — nil-safe.
	eventPub *ManthanEventPublisher
	logger   *zap.Logger

	// Cooldown: min 30s between SL modifications per symbol
	cooldownMu sync.Mutex
	lastModify map[string]time.Time // symbol → last modify time
	cooldown   time.Duration
}

func NewSLHandler(broker *BrokerAdapter, repo *Repository, logger *zap.Logger) *SLHandler {
	return &SLHandler{
		broker:     broker,
		repo:       repo,
		logger:     logger,
		lastModify: make(map[string]time.Time),
		cooldown:   30 * time.Second,
	}
}

// PlaceInitialSL places the first SL-L SELL after entry fill.
// Called by EntryHandler after BUY is confirmed.
// SetAuthProvider sets the auth provider for fetching fresh credentials.
func (h *SLHandler) SetAuthProvider(ap AuthProvider) {
	h.authProvider = ap
}

// SetRefreshAuth sets the callback invoked on broker auth errors to pull
// fresh creds from DB (bypassing the in-memory cache).
func (h *SLHandler) SetRefreshAuth(rp AuthProvider) {
	h.refreshAuth = rp
}

// SetEventPublisher wires the centralized publisher used to emit SL_PLACED,
// SL_MODIFIED, SL_REJECTED to manthan.execution.events.
func (h *SLHandler) SetEventPublisher(p *ManthanEventPublisher) {
	h.eventPub = p
}

func (h *SLHandler) PlaceInitialSL(ctx context.Context, entryOrderID int64, signal ManthanSignal, info *SymbolInfo, qty int, triggerPrice, limitPrice float64) {
	// Use fresh credentials from cache (signal token may be stale)
	auth := BrokerAuth{
		UserID:      signal.UserID,
		BearerToken: signal.BearerToken,
		AppID:       signal.AppId,
		Source:      signal.Source,
	}
	if h.authProvider != nil {
		if freshAuth := h.authProvider(signal.UserID); freshAuth != nil {
			auth = *freshAuth
		}
	}

	// NOTE: the DPR clamp deliberately does NOT happen here. The incoming
	// triggerPrice is the INTENDED stop (entry/high * 0.80) and is stored as-is
	// in trigger_price so the trail ratchet can keep modifying on 2% moves and
	// relax back to the true 20%. The broker adapter (PlaceSLSell) applies the
	// DPR/tick clamp at placement time only and returns the broker-real value,
	// which we record separately in broker_trigger_price. Clamping-then-storing
	// here is what froze SANDHAR's SL too tight (see migration 015).

	// Create SL order record (trigger_price = intended, un-clamped)
	slOrder := &ManthanOrder{
		SignalID:      fmt.Sprintf("sl-%s", signal.OrderID),
		StrategyID:    signal.StrategyID,
		UserID:        signal.UserID,
		Symbol:        signal.Symbol,
		ISIN:          signal.ISIN,
		Exchange:      "NSE",
		OrderType:     OrderTypeSLSell,
		OrderSide:     "SELL",
		ProductType:   "CNC",
		Qty:           qty,
		TriggerPrice:  triggerPrice,
		LimitPrice:    limitPrice,
		IndiraSymbol:  info.IndiraSymbol,
		ExchangeToken: info.ExchangeToken,
		Status:        StatusPending,
		MaxRetries:    3,
	}
	slOrderID, err := h.repo.InsertOrder(ctx, slOrder)
	if err != nil {
		h.logger.Error("Failed to insert SL order record", zap.Error(err))
		return
	}

	// PAPER mode — just record it, no broker call
	if signal.TradingMode == "PAPER" {
		paperBrokerID := "PAPER-SL-" + signal.OrderID
		_ = h.repo.UpdateSLBrokerID(ctx, slOrderID, paperBrokerID, entryOrderID)
		_ = h.repo.InsertEvent(ctx, slOrderID, "SL_PLACED", "PENDING", "SL_PLACED", "PAPER",
			triggerPrice, qty, fmt.Sprintf("trigger=%.2f limit=%.2f", triggerPrice, limitPrice))

		h.logger.Info("PAPER SL placed",
			zap.String("symbol", signal.Symbol),
			zap.Float64("trigger", triggerPrice),
			zap.Float64("limit", limitPrice))
		if h.eventPub != nil {
			h.eventPub.PublishSLPlaced(ctx, signal, paperBrokerID, triggerPrice, limitPrice)
		}
		return
	}

	// LIVE mode — place with broker, retry on failure (includes one auth-refresh
	// retry on AU004/401 before falling back to exponential backoff).
	var brokerID string
	var brokerTrig, brokerLim float64
	err = RetryWithBackoff(ctx, 3, func() error {
		brokerID2, bt, bl, placeErr := h.broker.PlaceSLSell(ctx, auth, info, qty, triggerPrice, limitPrice)
		if IsAuthError(placeErr) && h.refreshAuth != nil {
			h.logger.Warn("SL placement hit auth error — refreshing credentials",
				zap.String("user", signal.UserID), zap.Error(placeErr))
			if freshAuth := h.refreshAuth(signal.UserID); freshAuth != nil {
				auth = *freshAuth
				brokerID2, bt, bl, placeErr = h.broker.PlaceSLSell(ctx, auth, info, qty, triggerPrice, limitPrice)
			}
		}
		brokerID, brokerTrig, brokerLim = brokerID2, bt, bl
		return placeErr
	})

	if err != nil {
		errMsg := err.Error()
		_ = h.repo.UpdateOrderRejected(ctx, slOrderID, "SL placement failed: "+errMsg)
		_ = h.repo.InsertEvent(ctx, slOrderID, "SL_FAILED", "PENDING", "REJECTED", "", 0, 0, errMsg)

		// Only emergency sell for real order failures — NOT for auth/session issues
		// Auth failures will be retried on next safety monitor cycle with fresh token
		if strings.Contains(errMsg, "Session expired") || strings.Contains(errMsg, "AU004") || strings.Contains(errMsg, "401") {
			h.logger.Warn("SL placement failed due to auth — will retry via safety monitor",
				zap.String("symbol", signal.Symbol), zap.Error(err))
		} else {
			h.logger.Error("SL placement FAILED — EMERGENCY MARKET SELL",
				zap.String("symbol", signal.Symbol), zap.Error(err))
			if h.eventPub != nil {
				h.eventPub.PublishSLRejected(ctx, signal, "", errMsg)
			}
			h.emergencySell(ctx, signal, info, auth, qty, "SL placement failed")
		}
		return
	}

	// Store intended trigger (trigger_price, set at InsertOrder) + broker-real
	// trigger/limit the exchange accepted (broker_trigger_price). SSOT split.
	_ = h.repo.UpdateSLBrokerExec(ctx, slOrderID, brokerID, entryOrderID, brokerTrig, brokerLim)
	_ = h.repo.InsertEvent(ctx, slOrderID, "SL_PLACED", "PENDING", "SL_PLACED", "",
		triggerPrice, qty, fmt.Sprintf("broker_id=%s intended=%.2f broker_trigger=%.2f broker_limit=%.2f", brokerID, triggerPrice, brokerTrig, brokerLim))

	h.logger.Info("SL-L SELL placed",
		zap.String("symbol", signal.Symbol),
		zap.String("broker_id", brokerID),
		zap.Float64("intended_trigger", triggerPrice),
		zap.Float64("broker_trigger", brokerTrig),
		zap.Float64("broker_limit", brokerLim))

	// Publish SL_PLACED so rules-engine knows the SL is at broker. Used by
	// FillConsumer to update manthan_positions.current_sl atomically.
	if h.eventPub != nil {
		h.eventPub.PublishSLPlaced(ctx, signal, brokerID, triggerPrice, limitPrice)
	}
}

// MergeTopupSL extends the PARENT SL to cover the combined position after a
// top-up fill, instead of placing a separate SL for the top-up qty alone.
//
// Why: without merging, every MANTHAN_TOPUP fill creates a new SL_SELL on
// the broker. Over 8 top-ups a single symbol ends up with 8 broker SL rows,
// only one of which the trail-tick will see (GetActiveSLOrders returns one
// row per (strategy, symbol) match). The rest stay at their original
// triggers forever, exposing the position to staggered partial exits — a
// price dip can fire the lowest SL while the highest stays armed.
//
// Approach: ModifyOrder on the parent SL with combined_qty. Indira's
// modify-order accepts a new qty as long as it's >= already-traded. The
// new trigger is the higher of (existing_trigger, topup_avg × 0.80) so the
// SL is never lowered (any prior trailing is preserved).
//
// Fallback: if the parent SL can't be located (DB out of sync, or AMO not
// yet promoted) we fall back to PlaceInitialSL with the top-up qty alone.
// That recreates the old behaviour for that one top-up — operationally
// safe (position still SL-protected), just sub-optimal until the operator
// cleans up the duplicate.
func (h *SLHandler) MergeTopupSL(
	ctx context.Context,
	topupEntryOrderID int64,
	signal ManthanSignal,
	info *SymbolInfo,
	topupQty int,
	topupAvgPrice float64,
) {
	parentSL, err := h.repo.GetActiveSLByEntrySignalID(ctx, signal.TopUpForSignalID)
	if err != nil || parentSL == nil || parentSL.BrokerOrderID == "" {
		h.logger.Warn("Top-up SL merge: parent SL not found — placing per-tranche SL as fallback",
			zap.String("symbol", signal.Symbol),
			zap.String("parent_signal_id", signal.TopUpForSignalID),
			zap.Error(err))
		triggerPrice := topupAvgPrice * 0.80
		limitPrice := triggerPrice - SLLimitGap(triggerPrice, info.TickSize)
		h.PlaceInitialSL(ctx, topupEntryOrderID, signal, info, topupQty, triggerPrice, limitPrice)
		return
	}

	auth := BrokerAuth{
		UserID:      signal.UserID,
		BearerToken: signal.BearerToken,
		AppID:       signal.AppId,
		Source:      signal.Source,
	}
	if h.authProvider != nil {
		if fresh := h.authProvider(signal.UserID); fresh != nil {
			auth = *fresh
		}
	}

	combinedQty := parentSL.Qty + topupQty
	// Ratchet on the INTENDED stop: never lower below parentSL.TriggerPrice
	// (which is now the intended, un-clamped value). DPR clamping is left to the
	// broker adapter at placement time; we store the intended here.
	newTrigger := parentSL.TriggerPrice
	if topupBased := topupAvgPrice * 0.80; topupBased > newTrigger {
		newTrigger = topupBased
	}
	newLimit := newTrigger - SLLimitGap(newTrigger, info.TickSize)

	h.logger.Info("Top-up SL merge: extending parent SL",
		zap.String("symbol", signal.Symbol),
		zap.String("parent_broker_id", parentSL.BrokerOrderID),
		zap.Int("old_qty", parentSL.Qty),
		zap.Int("topup_qty", topupQty),
		zap.Int("combined_qty", combinedQty),
		zap.Float64("old_trigger", parentSL.TriggerPrice),
		zap.Float64("new_trigger", newTrigger))

	brokerTrig, brokerLim, modErr := h.broker.ModifySLOrder(ctx, auth, info, parentSL.BrokerOrderID,
		combinedQty, newTrigger, newLimit)
	if IsAuthError(modErr) && h.refreshAuth != nil {
		if fresh := h.refreshAuth(signal.UserID); fresh != nil {
			auth = *fresh
			brokerTrig, brokerLim, modErr = h.broker.ModifySLOrder(ctx, auth, info, parentSL.BrokerOrderID,
				combinedQty, newTrigger, newLimit)
		}
	}
	if modErr != nil {
		h.logger.Error("Top-up SL merge: modify failed — falling back to per-tranche SL",
			zap.String("symbol", signal.Symbol),
			zap.String("parent_broker_id", parentSL.BrokerOrderID),
			zap.Error(modErr))
		triggerPrice := topupAvgPrice * 0.80
		limitPrice := triggerPrice - SLLimitGap(triggerPrice, info.TickSize)
		h.PlaceInitialSL(ctx, topupEntryOrderID, signal, info, topupQty, triggerPrice, limitPrice)
		return
	}

	// Persist new qty + intended trigger onto the parent SL row, plus the
	// broker-real trigger/limit. Without this the next TSL trail-modify reads
	// the stale parent.qty and would shrink the broker stop back down.
	if err := h.repo.UpdateSLAfterTopupMerge(ctx, parentSL.ID, combinedQty, newTrigger, newLimit, brokerTrig, brokerLim); err != nil {
		h.logger.Error("Top-up SL merge: DB update failed — broker is correct but DB drifted",
			zap.Int64("sl_order_id", parentSL.ID), zap.Error(err))
	}
	_ = h.repo.InsertEvent(ctx, parentSL.ID, "SL_MERGED_TOPUP", "SL_PLACED", "SL_PLACED", "",
		newTrigger, combinedQty,
		fmt.Sprintf("topup signal=%s: qty %d→%d, intended %.2f→%.2f broker_trigger=%.2f",
			signal.OrderID, parentSL.Qty, combinedQty, parentSL.TriggerPrice, newTrigger, brokerTrig))

	// Publish SL_MODIFIED so the rules-engine projector syncs
	// manthan_positions.current_sl with the new broker-side trigger.
	if h.eventPub != nil {
		h.eventPub.PublishSLModified(ctx, signal, parentSL.BrokerOrderID, newTrigger, newLimit, 0)
	}
}

// ModifyTrail modifies an existing SL order for trailing SL.
// Called when rules-engine detects +2% move.
func (h *SLHandler) ModifyTrail(ctx context.Context, signal SLModifySignal) error {
	// Cooldown check
	if !h.checkCooldown(signal.Symbol) {
		h.logger.Debug("SL modify skipped — cooldown",
			zap.String("symbol", signal.Symbol))
		return nil
	}

	// Find the active SL order BEFORE resolving the symbol. The rules-engine
	// SL_MODIFY signal omits ISIN (see rules-engine/internal/manthan/order.go
	// GenerateSLModify — only ENTRY orders carry it), so ResolveSymbol's ISIN
	// → token Redis lookup ("isin:<ISIN>") returns redis:nil and every modify
	// retries TRANSIENT forever. The parent SL order row in manthan_orders
	// already has the resolved ISIN + exchange_token from when we placed it,
	// so we use that as the authoritative source.
	slOrders, err := h.repo.GetActiveSLOrders(ctx)
	if err != nil {
		return fmt.Errorf("get active SL orders: %w", err)
	}

	var targetSL *ManthanOrder
	for _, o := range slOrders {
		if o.Symbol == signal.Symbol && o.StrategyID == signal.StrategyID {
			targetSL = o
			break
		}
	}

	if targetSL == nil || targetSL.BrokerOrderID == "" {
		return fmt.Errorf("no active SL order found for %s", signal.Symbol)
	}

	// Backfill ISIN from the SL order if the signal didn't carry one — the
	// rules-engine producer doesn't include it on SL_MODIFY (only on entry).
	isin := signal.ISIN
	if isin == "" {
		isin = targetSL.ISIN
	}

	info, err := h.broker.ResolveSymbol(ctx, signal.Symbol, isin)
	if err != nil {
		return fmt.Errorf("resolve symbol: %w", err)
	}

	// Pre-check
	check := h.preCheck(ctx, signal, info)
	if !check.CanProceed {
		return fmt.Errorf("SL modify pre-check: %s", check.Reason)
	}

	// Minimum change check (> 0.1%)
	if signal.OldSL > 0 {
		changePct := math.Abs(signal.NewSL-signal.OldSL) / signal.OldSL
		if changePct < 0.001 {
			return nil // not worth an API call
		}
	}

	newLimit := signal.NewSL - SLLimitGap(signal.NewSL, info.TickSize)

	// PAPER mode
	if signal.TradingMode == "PAPER" {
		h.logger.Info("PAPER SL modified",
			zap.String("symbol", signal.Symbol),
			zap.Float64("old_sl", signal.OldSL),
			zap.Float64("new_sl", signal.NewSL))
		h.recordCooldown(signal.Symbol)
		return nil
	}

	// RATCHET GUARD — for a long position (SL is SELL-side), the SL trigger
	// can only move UP. Refuse any modify that would LOWER the broker's
	// current trigger.
	//
	// This is the last line of defence against rules-engine state divergence.
	// If somehow a stale SL_MODIFY signal slips through (e.g. rules-engine
	// computed off an internal value that didn't reflect broker DPR-clamping),
	// applying it at the broker would WIDEN the stop and increase downside
	// risk on a long position. We refuse and return nil so the inbox marks
	// the signal DONE (not an error worth retrying — the signal is stale,
	// the next legitimate trail will land normally).
	//
	// Direction-aware: only applies when the SL order is a SELL (i.e. the
	// position is long). A SELL SL going DOWN means stop loosening. For
	// short positions (BUY-side SL, future feature) the inverse would apply.
	if isLongPositionSL(targetSL) && signal.NewSL < targetSL.TriggerPrice {
		h.logger.Warn("SL modify refused — would LOWER broker stop on a long position",
			zap.String("symbol", signal.Symbol),
			zap.Float64("broker_trigger", targetSL.TriggerPrice),
			zap.Float64("requested_new_sl", signal.NewSL),
			zap.Float64("delta", targetSL.TriggerPrice-signal.NewSL),
			zap.String("broker_order_id", targetSL.BrokerOrderID),
			zap.String("reason", "ratchet violation — rules-engine state likely diverged from broker reality"))
		// Record this as a position event for forensics — operator can grep
		// for SL_MODIFY_REJECTED_RATCHET in the audit trail.
		_ = h.repo.InsertEvent(ctx, targetSL.ID, "SL_MODIFY_REJECTED_RATCHET",
			"SL_PLACED", "SL_PLACED", "",
			signal.NewSL, 0,
			fmt.Sprintf("requested %.2f < broker %.2f (high=%.2f, old_sl=%.2f)",
				signal.NewSL, targetSL.TriggerPrice, signal.NewHigh, signal.OldSL))

		// Tell rules-engine "broker is at this trigger, sync your state".
		// We emit SL_PLACED (not SL_MODIFIED) because we don't want the
		// projector to advance trail state — we're reconciling, not trailing.
		// Without this the rules-engine's internal pos.CurrentSL stays stale
		// and the very next tick computes the same wrong new_sl all over again.
		if h.eventPub != nil {
			entrySignalID, _ := h.repo.GetEntrySignalIDByOrderID(ctx, targetSL.ID)
			if entrySignalID != "" {
				h.eventPub.PublishSLPlaced(ctx,
					ManthanSignal{
						OrderID: entrySignalID, UserID: signal.UserID,
						StrategyID: signal.StrategyID, Symbol: signal.Symbol,
						TradingMode: signal.TradingMode,
					},
					targetSL.BrokerOrderID, targetSL.TriggerPrice, targetSL.LimitPrice)
			}
		}

		h.recordCooldown(signal.Symbol)
		return nil
	}

	auth := BrokerAuth{
		UserID:      signal.UserID,
		BearerToken: signal.BearerToken,
		AppID:       signal.AppId,
		Source:      signal.Source,
	}

	// Atomic modify-in-place. On AU004/401, refresh credentials then retry.
	// signal.NewSL is the INTENDED stop (high*0.80); the adapter clamps it to
	// DPR for the broker and returns the broker-real trigger/limit we record.
	var brokerTrig, brokerLim float64
	brokerTrig, brokerLim, err = h.broker.ModifySLOrder(ctx, auth, info, targetSL.BrokerOrderID, targetSL.Qty, signal.NewSL, newLimit)
	if IsAuthError(err) && h.refreshAuth != nil {
		h.logger.Warn("SL modify hit auth error — refreshing credentials",
			zap.String("user", signal.UserID), zap.Error(err))
		if freshAuth := h.refreshAuth(signal.UserID); freshAuth != nil {
			auth = *freshAuth
			brokerTrig, brokerLim, err = h.broker.ModifySLOrder(ctx, auth, info, targetSL.BrokerOrderID, targetSL.Qty, signal.NewSL, newLimit)
		}
	}
	if err != nil {
		h.logger.Error("SL modify FAILED — will retry or emergency sell",
			zap.String("symbol", signal.Symbol),
			zap.Error(err))

		// Retry once more
		time.Sleep(2 * time.Second)
		brokerTrig, brokerLim, err = h.broker.ModifySLOrder(ctx, auth, info, targetSL.BrokerOrderID, targetSL.Qty, signal.NewSL, newLimit)
		if err != nil {
			h.logger.Error("SL modify retry FAILED — EMERGENCY MARKET SELL",
				zap.String("symbol", signal.Symbol))
			// Resolve the entry signal_id from this SL order's parent so the
			// SL_REJECTED event lands on the right manthan_signal_decisions row.
			entrySignalID, _ := h.repo.GetEntrySignalIDByOrderID(ctx, targetSL.ID)
			if h.eventPub != nil && entrySignalID != "" {
				h.eventPub.PublishSLRejected(ctx,
					ManthanSignal{
						OrderID: entrySignalID, UserID: signal.UserID,
						StrategyID: signal.StrategyID, Symbol: signal.Symbol,
						TradingMode: signal.TradingMode,
					},
					targetSL.BrokerOrderID,
					fmt.Sprintf("modify retry failed: %v", err))
			}
			h.emergencySell(ctx, ManthanSignal{
				OrderID: entrySignalID, // carry entry signal_id into emergency-sell publishing
				UserID:  signal.UserID, StrategyID: signal.StrategyID,
				Symbol: signal.Symbol, BearerToken: signal.BearerToken,
				AppId: signal.AppId, Source: signal.Source,
				Quantity: int32(targetSL.Qty), TradingMode: signal.TradingMode,
			}, info, auth, targetSL.Qty, "SL modify failed after retry")
			return err
		}
	}

	// Persist the new INTENDED stop (trigger_price) + the broker-real trigger/limit
	// the exchange accepted. Previously the trail updated the broker but NEVER the
	// DB row, so trigger_price went stale on every trail — fixed here.
	if uerr := h.repo.UpdateSLAfterModify(ctx, targetSL.ID, signal.NewSL, newLimit, brokerTrig, brokerLim); uerr != nil {
		h.logger.Error("SL trail: DB update failed — broker modified but DB drifted",
			zap.Int64("sl_order_id", targetSL.ID), zap.Error(uerr))
	}
	_ = h.repo.InsertEvent(ctx, targetSL.ID, "MODIFIED", "SL_PLACED", "SL_PLACED", "",
		signal.NewSL, 0, fmt.Sprintf("trail: old=%.2f new=%.2f high=%.2f broker_trigger=%.2f", signal.OldSL, signal.NewSL, signal.NewHigh, brokerTrig))

	// Publish SL_MODIFIED keyed on the entry signal_id (so rules-engine can
	// project the new current_sl onto the right position row).
	entrySignalID, sidErr := h.repo.GetEntrySignalIDByOrderID(ctx, targetSL.ID)
	if h.eventPub != nil && sidErr == nil && entrySignalID != "" {
		h.eventPub.PublishSLModified(ctx,
			ManthanSignal{
				OrderID: entrySignalID, UserID: signal.UserID,
				StrategyID: signal.StrategyID, Symbol: signal.Symbol,
				TradingMode: signal.TradingMode,
			},
			targetSL.BrokerOrderID, signal.NewSL, newLimit, 0)
	}

	h.recordCooldown(signal.Symbol)

	h.logger.Info("SL modified",
		zap.String("symbol", signal.Symbol),
		zap.Float64("old_sl", signal.OldSL),
		zap.Float64("new_sl", signal.NewSL),
		zap.Float64("new_high", signal.NewHigh))

	return nil
}

// EmergencySell places a MARKET SELL as safety net when SL fails.
func (h *SLHandler) EmergencySell(ctx context.Context, signal SLExitSignal) error {
	info, err := h.broker.ResolveSymbol(ctx, signal.Symbol, signal.ISIN)
	if err != nil {
		return fmt.Errorf("resolve symbol: %w", err)
	}

	auth := BrokerAuth{
		UserID:      signal.UserID,
		BearerToken: signal.BearerToken,
		AppID:       signal.AppId,
		Source:      signal.Source,
	}

	// PAPER mode
	if signal.TradingMode == "PAPER" {
		h.logger.Info("PAPER emergency SELL",
			zap.String("symbol", signal.Symbol),
			zap.Int32("qty", signal.Quantity),
			zap.Float64("exit_price", signal.ExitPrice),
			zap.Float64("pnl", signal.PnL))
		return nil
	}

	return h.emergencySellInternal(ctx, info, auth, int(signal.Quantity), "SL triggered — emergency exit")
}

func (h *SLHandler) emergencySell(ctx context.Context, signal ManthanSignal, info *SymbolInfo, auth BrokerAuth, qty int, reason string) {
	if signal.TradingMode == "PAPER" {
		h.logger.Warn("PAPER emergency SELL",
			zap.String("symbol", signal.Symbol),
			zap.Int("qty", qty),
			zap.String("reason", reason))
		return
	}
	_ = h.emergencySellInternal(ctx, info, auth, qty, reason)
}

func (h *SLHandler) emergencySellInternal(ctx context.Context, info *SymbolInfo, auth BrokerAuth, qty int, reason string) error {
	// Check if lower circuit — use AMO if so
	_, atLower, _ := h.broker.CheckCircuit(ctx, info.ExchangeToken)
	if atLower {
		h.logger.Warn("Lower circuit — placing AMO SELL",
			zap.String("symbol", info.Symbol))
		_, err := h.broker.PlaceAMOSell(ctx, auth, info, qty)
		return err
	}

	// Normal MARKET SELL
	_, err := h.broker.PlaceMarketSell(ctx, auth, info, qty)
	if err != nil {
		h.logger.Error("EMERGENCY MARKET SELL FAILED",
			zap.String("symbol", info.Symbol),
			zap.Error(err))
	}
	return err
}

func (h *SLHandler) preCheck(ctx context.Context, signal SLModifySignal, info *SymbolInfo) PreCheckResult {
	if signal.TradingMode == "PAPER" {
		return pass()
	}
	_, atLower, _ := h.broker.CheckCircuit(ctx, info.ExchangeToken)
	if atLower {
		return fail("lower circuit")
	}
	return pass()
}

func (h *SLHandler) checkCooldown(symbol string) bool {
	h.cooldownMu.Lock()
	defer h.cooldownMu.Unlock()
	last, ok := h.lastModify[symbol]
	if !ok {
		return true
	}
	return time.Since(last) >= h.cooldown
}

func (h *SLHandler) recordCooldown(symbol string) {
	h.cooldownMu.Lock()
	defer h.cooldownMu.Unlock()
	h.lastModify[symbol] = time.Now()
}

// isLongPositionSL reports whether targetSL belongs to a long position, i.e.
// the SL order is a SELL (entry was BUY → exit is SELL). Used by the ratchet
// guard in ModifyTrail to decide whether "new_sl < broker_trigger" means
// "loosening the stop" (only true for long-position SELL-side SLs).
//
// Short positions (not yet supported by Manthan but on the roadmap) would
// have BUY-side SLs where the inverse direction is the loosening direction.
// Defensive case-insensitive compare; empty OrderSide on legacy rows is
// treated as long, matching today's BUY-only Manthan strategy.
func isLongPositionSL(o *ManthanOrder) bool {
	if o == nil {
		return false
	}
	side := strings.ToUpper(strings.TrimSpace(o.OrderSide))
	return side == "" || side == "SELL"
}
