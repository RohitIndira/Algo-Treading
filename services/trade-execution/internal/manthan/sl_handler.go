package manthan

import (
	"context"
	"errors"
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

	// Emergency-sell latch: one emergency sell per symbol per window.
	// 2026-08-26 IOLCP incident: the modify-failure path fired an EMERGENCY
	// MARKET SELL on every trail tick (~every 10s) until the position book
	// caught up — only the broker's holdings limit stopped duplicates from
	// executing. A repeat inside the window is a bug upstream, never a
	// legitimate second exit of the same position.
	emergencyMu   sync.Mutex
	lastEmergency map[string]time.Time // symbol → last emergency sell time
}

// emergencyLatchWindow: refuse a second emergency sell for the same symbol
// within this window. Long enough for fills + reconciler to update the book,
// short enough that a genuinely new position later in the day can still be
// emergency-exited.
const emergencyLatchWindow = 10 * time.Minute

func NewSLHandler(broker *BrokerAdapter, repo *Repository, logger *zap.Logger) *SLHandler {
	return &SLHandler{
		broker:     broker,
		repo:       repo,
		logger:     logger,
		lastModify: make(map[string]time.Time),
		cooldown:   30 * time.Second,

		lastEmergency: make(map[string]time.Time),
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

// resolveAuth fetches broker creds at-edge via authProvider (user-config gRPC
// with DB fallback). Returns ok=false and logs if creds can't be obtained;
// callers should return early in that case rather than attempting the broker
// call (which would just 401). Replaces the legacy "build from signal then
// override" pattern from before 2026-06-25 — see internal/manthan/models.go.
func (h *SLHandler) resolveAuth(userID, symbol, op string) (BrokerAuth, bool) {
	if h.authProvider == nil {
		h.logger.Error("SL: authProvider not wired",
			zap.String("user_id", userID),
			zap.String("symbol", symbol),
			zap.String("op", op))
		return BrokerAuth{}, false
	}
	a := h.authProvider(userID)
	if a == nil || a.BearerToken == "" {
		h.logger.Error("SL: no broker credentials available",
			zap.String("user_id", userID),
			zap.String("symbol", symbol),
			zap.String("op", op))
		return BrokerAuth{}, false
	}
	return *a, true
}

// PlaceInitialSL places the initial 20%-below-fill SL for an entry. Returns an
// error on any failure so the caller (handleFill) can surface a naked position
// loudly; the safety-monitor's naked-position scan (FIX 3) is the actual
// recovery — it re-drives this every 2s until an active SL exists. A nil return
// means the SL is placed OR deliberately deferred (SL_DEFERRED_BAND) — both are
// "protected" states, not naked.
func (h *SLHandler) PlaceInitialSL(ctx context.Context, entryOrderID int64, signal ManthanSignal, info *SymbolInfo, qty int, triggerPrice, limitPrice float64) error {
	// Resolve auth at-edge via authProvider — token no longer rides the Kafka
	// signal. If creds are unavailable we'd 401 against the broker anyway, so
	// fail fast with a clear log instead.
	auth, ok := h.resolveAuth(signal.UserID, signal.Symbol, "PlaceInitialSL")
	if !ok {
		return fmt.Errorf("PlaceInitialSL: auth unavailable for user %s symbol %s", signal.UserID, signal.Symbol)
	}

	// NOTE: the DPR clamp deliberately does NOT happen here. The incoming
	// triggerPrice is the INTENDED stop (entry/high x slFactor(pct)) and is stored as-is
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
		// FIX 3 idempotency: a prior attempt may have left a terminal
		// (REJECTED/CANCELLED/EXPIRED) SL row with this deterministic signal_id,
		// so UNIQUE(signal_id) blocks a fresh insert. Reset that row to PENDING
		// and reuse it — this lets the safety-monitor naked scan re-drive
		// placement WITHOUT creating a duplicate broker SL (enforces "exactly
		// one SL per entry"). If no terminal row matched, it's a real failure.
		resetID, found, resetErr := h.repo.ResetTerminalSLToPending(ctx, slOrder.SignalID, triggerPrice, limitPrice)
		if resetErr != nil || !found {
			h.logger.Error("Failed to insert SL order record", zap.Error(err))
			return fmt.Errorf("PlaceInitialSL: insert SL order: %w", err)
		}
		slOrderID = resetID
		h.logger.Info("PlaceInitialSL: reusing prior terminal SL row (idempotent re-drive)",
			zap.String("symbol", signal.Symbol), zap.Int64("sl_order_id", slOrderID))
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
		return nil
	}

	// Option B — DEFER instead of placing a premature band-floor stop. If the
	// intended 20% trigger is below today's DPR lower band, the exchange would
	// reject a resting SL there, and clamping up to the floor would create a
	// ~band% stop that can exit on a single-day circuit touch (NOT the 20% the
	// strategy intends). The stock physically cannot reach the intended level in
	// one session (circuit caps the move), so we hold: mark the row
	// SL_DEFERRED_BAND (no broker order) and let the daily protective-replay
	// place the real 20% SL once the band re-centers low enough.
	if resolved, clamped, deferWhy := resolveBandFloor(triggerPrice, info.DPRLower); clamped {
		// Policy (2026-08-19): floor within 5% of the intended stop → place AT
		// the floor now (broker-resident, gap-protected). trigger_price stays
		// the intended level (set at InsertOrder); the broker holds the clamp.
		h.logger.Info("Initial SL below band — placing at the band floor (within policy)",
			zap.String("symbol", signal.Symbol),
			zap.Float64("intended_trigger", triggerPrice), zap.Float64("floor", resolved))
		triggerPrice = resolved
		limitPrice = h.broker.roundAndClamp(triggerPrice-SLLimitGap(triggerPrice, info.TickSize), info.TickSize, info.DPRLower, info.DPRUpper)
	} else if deferWhy != "" {
		// Park it: link to the parent entry, keep trigger_price = intended (set at
		// InsertOrder). The trail keeps this intended current via the deferred
		// branch in ModifyTrail; the daily replay places it once the band allows.
		_ = h.repo.MarkSLDeferredBand(ctx, slOrderID, entryOrderID)
		_ = h.repo.InsertEvent(ctx, slOrderID, "SL_DEFERRED_BAND", "PENDING", "SL_DEFERRED_BAND", "",
			triggerPrice, qty, fmt.Sprintf("intended=%.2f < dpr_lower=%.2f — deferred (unreachable this session; replay places at 20%% when band allows)", triggerPrice, info.DPRLower))
		h.logger.Info("SL deferred — intended 20% below DPR band; will place when band re-centers",
			zap.String("symbol", signal.Symbol),
			zap.Float64("intended_trigger", triggerPrice),
			zap.Float64("dpr_lower", info.DPRLower))
		return nil // deferred is a deliberate protected state, not naked
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
		// A CANCELLED CONTEXT (service shutdown / restart, caller deadline) or
		// a network-level failure is NOT a broker refusal: the stop can be
		// placed on the next cycle. Escalating those to a MARKET SELL would let
		// a routine pm2 restart during the session liquidate a position.
		// 2026-08-19: a restart cancelled SHANTIGOLD's placement mid-flight and
		// the fallback fired (it only failed because the same context was
		// already cancelled). The safety monitor's naked-position check retries
		// placement every cycle, so "retry" is always the safe default here.
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
			strings.Contains(errMsg, "context canceled") || strings.Contains(errMsg, "context deadline exceeded") ||
			strings.Contains(errMsg, "connection refused") || strings.Contains(errMsg, "no such host") ||
			strings.Contains(errMsg, "i/o timeout") || strings.Contains(errMsg, "EOF") {
			h.logger.Warn("SL placement interrupted (shutdown/network) — will retry via safety monitor, NOT escalating",
				zap.String("symbol", signal.Symbol), zap.Error(err))
		} else if strings.Contains(errMsg, "Session expired") || strings.Contains(errMsg, "AU004") || strings.Contains(errMsg, "401") {
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
		return fmt.Errorf("PlaceInitialSL: broker place SL for %s: %w", signal.Symbol, err)
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
	return nil
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
// new trigger is the higher of (existing_trigger, topup_avg x slFactor(pct)) so the
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
		triggerPrice := topupAvgPrice * slFactor(signal.StopLossPct)
		limitPrice := triggerPrice - SLLimitGap(triggerPrice, info.TickSize)
		h.PlaceInitialSL(ctx, topupEntryOrderID, signal, info, topupQty, triggerPrice, limitPrice)
		return
	}

	auth, ok := h.resolveAuth(signal.UserID, signal.Symbol, "TopUpSL")
	if !ok {
		return
	}

	combinedQty := parentSL.Qty + topupQty
	// Ratchet on the INTENDED stop: never lower below parentSL.TriggerPrice
	// (which is now the intended, un-clamped value). DPR clamping is left to the
	// broker adapter at placement time; we store the intended here.
	newTrigger := parentSL.TriggerPrice
	if topupBased := topupAvgPrice * slFactor(signal.StopLossPct); topupBased > newTrigger {
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
		triggerPrice := topupAvgPrice * slFactor(signal.StopLossPct)
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
		// Option B: the position may be DEFERRED (intended 20% below the DPR band,
		// no broker order). The trail must still advance the recorded intended so
		// the eventual placement is at the trailed 20% — and place it the moment
		// the intended rises into the band. Returns nil (handled) so the inbox
		// doesn't retry this signal forever.
		if deferred, derr := h.repo.GetDeferredSLBySymbol(ctx, signal.StrategyID, signal.Symbol); derr == nil && deferred != nil {
			return h.trailDeferredSL(ctx, signal, deferred)
		}
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

	auth, ok := h.resolveAuth(signal.UserID, signal.Symbol, "ModifyTrail")
	if !ok {
		return fmt.Errorf("no broker creds for SL modify")
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
	if IsOrderNotFoundError(err) {
		// The SL order is GONE from the broker book (e.g. Indira silently
		// downgraded the GTC stop to DAY and it expired at close). The
		// POSITION is untouched — mark our row CANCELLED so the reconciler /
		// protective replay places a FRESH stop at the intended level. Never
		// market-sell a healthy position over a failed modify (2026-08-26
		// IOLCP incident: repeated EMERGENCY MARKET SELLs from this path).
		h.logger.Warn("SL modify target vanished at broker — marking row CANCELLED; fresh stop will be re-placed",
			zap.String("symbol", signal.Symbol),
			zap.String("broker_order_id", targetSL.BrokerOrderID))
		if uerr := h.repo.UpdateOrderStatus(ctx, targetSL.ID, StatusCancelled, "NOT_IN_BROKER_BOOK", 0,
			"modify target not found at broker — GTC likely expired; fresh SL to be placed"); uerr != nil {
			h.logger.Error("failed to mark vanished SL row CANCELLED",
				zap.String("symbol", signal.Symbol), zap.Error(uerr))
		}
		return fmt.Errorf("SL modify target vanished at broker: %w", err)
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
				Symbol:   signal.Symbol,
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

// trailDeferredSL handles an SL_MODIFY for a DEFERRED position (intended 20%
// below the DPR band, so no broker order exists yet). There is nothing to modify
// at the broker — instead we advance the RECORDED intended (trigger_price) as the
// high trails up, so the eventual placement is at the trailed 20% and not the
// stale entry level. Placement itself stays the daily replay's job: it re-checks
// the band every cycle and places the real SL the moment the intended fits inside
// it (reading this trigger_price). Keeping a single placer avoids any trail↔replay
// double-placement race. Returns nil so the inbox marks the signal done (no retry
// spam) rather than failing forever on "no active SL order".
func (h *SLHandler) trailDeferredSL(ctx context.Context, signal SLModifySignal, deferred *ManthanOrder) error {
	// Approximate limit (~0.3% below trigger); the replay recomputes the precise,
	// tick-aligned limit at placement time. Avoids a broker ResolveSymbol call on
	// every trail tick.
	newLimit := signal.NewSL - SLLimitGap(signal.NewSL, 0.05)
	if err := h.repo.UpdateDeferredIntended(ctx, deferred.ID, signal.NewSL, newLimit); err != nil {
		return fmt.Errorf("update deferred intended: %w", err)
	}
	_ = h.repo.InsertEvent(ctx, deferred.ID, "SL_DEFERRED_TRAIL", "SL_DEFERRED_BAND", "SL_DEFERRED_BAND", "",
		signal.NewSL, deferred.Qty,
		fmt.Sprintf("deferred trail: intended %.2f→%.2f (high=%.2f); placement awaits band via daily replay",
			signal.OldSL, signal.NewSL, signal.NewHigh))
	h.logger.Info("deferred SL trailed — recorded intended advanced; placement awaits band",
		zap.String("symbol", signal.Symbol),
		zap.Float64("new_intended", signal.NewSL),
		zap.Float64("new_high", signal.NewHigh))
	h.recordCooldown(signal.Symbol)
	return nil
}

// EmergencySell executes a software-stop exit (rules-engine SL_EXIT signal).
// 2026-10-22 hardening — every step below failed or was missing that morning:
//   - symbol resolution falls back to the filled entry row when the signal
//     has no ISIN (the payload carried none for weeks → "redis: nil" exits);
//   - a lot the book already shows sold (broker SL filled first) is skipped;
//   - never sells more than the order-derived net holding;
//   - the sell itself is a marketable LIMIT with standing stops cancelled
//     first, recorded as a PLACED exit row (see emergencySellInternal).
func (h *SLHandler) EmergencySell(ctx context.Context, signal SLExitSignal) error {
	info, isin, err := h.resolveExitSymbol(ctx, signal)
	if err != nil {
		return err
	}

	auth, ok := h.resolveAuth(signal.UserID, signal.Symbol, "EmergencySell")
	if !ok {
		return fmt.Errorf("no broker creds for emergency sell")
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

	qty := int(signal.Quantity)
	ec := exitContext{StrategyID: signal.StrategyID, SignalID: signal.OrderID, ISIN: isin}
	if h.repo != nil {
		net, entryID, hasEntry, perr := h.repo.NetHeldQty(ctx, signal.StrategyID, signal.Symbol)
		switch {
		case perr != nil:
			h.logger.Warn("EmergencySell: net-holding lookup failed — selling the signalled qty",
				zap.String("symbol", signal.Symbol), zap.Error(perr))
		case hasEntry && net <= 0:
			h.logger.Warn("EmergencySell: book shows the lot already sold (broker SL filled / manual exit) — nothing to sell",
				zap.String("user_id", signal.UserID), zap.String("symbol", signal.Symbol),
				zap.Int32("signalled_qty", signal.Quantity), zap.Int("net", net))
			return nil
		case hasEntry:
			ec.EntryOrderID = entryID
			if net < qty {
				h.logger.Warn("EmergencySell: signalled qty exceeds held qty — capping to the holding",
					zap.String("symbol", signal.Symbol), zap.Int("signalled", qty), zap.Int("held", net))
				qty = net
			}
		}
	}
	if qty <= 0 {
		return fmt.Errorf("emergency sell %s: non-positive qty %d", signal.Symbol, qty)
	}

	return h.emergencySellInternal(ctx, ec, info, auth, qty, "SL triggered — emergency exit")
}

// resolveExitSymbol resolves the broker symbol for an exit. Order: the signal
// ISIN → the position's filled entry row (stored token / indira symbol, else
// its ISIN). Also returns the ISIN that worked, for bookkeeping.
func (h *SLHandler) resolveExitSymbol(ctx context.Context, signal SLExitSignal) (*SymbolInfo, string, error) {
	var firstErr error
	if signal.ISIN != "" {
		info, err := h.broker.ResolveSymbol(ctx, signal.Symbol, signal.ISIN)
		if err == nil {
			return info, signal.ISIN, nil
		}
		firstErr = err
		h.logger.Warn("EmergencySell: ISIN resolve failed — falling back to the entry row",
			zap.String("symbol", signal.Symbol), zap.String("isin", signal.ISIN), zap.Error(err))
	}
	if h.repo != nil {
		entry, err := h.repo.GetActiveEntryBySymbol(ctx, signal.StrategyID, signal.Symbol)
		switch {
		case err == nil && entry != nil && entry.ExchangeToken != "":
			info := &SymbolInfo{
				Symbol:        signal.Symbol,
				ExchangeToken: entry.ExchangeToken,
				IndiraSymbol:  strDefault(entry.IndiraSymbol, fmt.Sprintf("STK_%s_EQ_NSE_%s", signal.Symbol, entry.ExchangeToken)),
				Exchange:      "NSE",
				TickSize:      0.05,
			}
			h.broker.EnrichMarketInfo(ctx, info)
			h.logger.Info("EmergencySell: symbol resolved from the entry row",
				zap.String("symbol", signal.Symbol), zap.String("token", entry.ExchangeToken), zap.String("isin", entry.ISIN))
			return info, entry.ISIN, nil
		case err == nil && entry != nil && entry.ISIN != "" && entry.ISIN != signal.ISIN:
			info, rerr := h.broker.ResolveSymbol(ctx, signal.Symbol, entry.ISIN)
			if rerr == nil {
				return info, entry.ISIN, nil
			}
			if firstErr == nil {
				firstErr = rerr
			}
		case err != nil && firstErr == nil:
			firstErr = err
		}
	}
	if firstErr == nil {
		firstErr = fmt.Errorf("no isin on signal and no filled entry row with a token for %s/%s", signal.StrategyID, signal.Symbol)
	}
	return nil, "", fmt.Errorf("resolve symbol: %w", firstErr)
}

func (h *SLHandler) emergencySell(ctx context.Context, signal ManthanSignal, info *SymbolInfo, auth BrokerAuth, qty int, reason string) {
	if signal.TradingMode == "PAPER" {
		h.logger.Warn("PAPER emergency SELL",
			zap.String("symbol", signal.Symbol),
			zap.Int("qty", qty),
			zap.String("reason", reason))
		return
	}
	_ = h.emergencySellInternal(ctx, exitContext{StrategyID: signal.StrategyID, SignalID: signal.OrderID, ISIN: signal.ISIN}, info, auth, qty, reason)
}

// exitContext carries the position identity an exit needs for bookkeeping.
type exitContext struct {
	StrategyID   string
	SignalID     string // originating signal id (audit only)
	EntryOrderID int64  // parent manthan_orders.id when known (0 → resolved from the book)
	ISIN         string
}

// emergencySellInternal is the one place an immediate exit is executed:
//  1. per-(user,symbol) latch — one exit per 10-minute window (loop guard);
//  2. cancel OUR standing stop orders for the position first: a resting SL
//     locks the quantity at the broker ("Free Qty=0") and the sell would be
//     rejected — the 2026-10-22 AMO rejections were exactly this;
//  3. lower circuit → AMO LIMIT at the band floor, else a marketable LIMIT
//     (Market / SL-M are rejected for algo-tagged orders);
//  4. record the sell as a PLACED exit row — confirmed FILLED from broker
//     truth by the safety monitor / reconciler, re-priced if it rests.
func (h *SLHandler) emergencySellInternal(ctx context.Context, ec exitContext, info *SymbolInfo, auth BrokerAuth, qty int, reason string) error {
	latchKey := auth.UserID + ":" + info.Symbol
	h.emergencyMu.Lock()
	if last, ok := h.lastEmergency[latchKey]; ok && time.Since(last) < emergencyLatchWindow {
		h.emergencyMu.Unlock()
		h.logger.Error("EMERGENCY SELL SUPPRESSED — already fired for user/symbol inside latch window; upstream loop must be investigated",
			zap.String("user_id", auth.UserID),
			zap.String("symbol", info.Symbol),
			zap.Int("qty", qty),
			zap.String("reason", reason),
			zap.Time("last_fired", last))
		return fmt.Errorf("emergency sell for %s suppressed: already fired at %s", info.Symbol, last.Format(time.RFC3339))
	}
	h.lastEmergency[latchKey] = time.Now()
	h.emergencyMu.Unlock()
	// A definite broker rejection means nothing rests at the exchange, so the
	// next retry must not be blocked for 10 minutes. Ambiguous failures
	// (timeouts) keep the latch — the order may have gone through.
	releaseLatchIfRejected := func(err error) {
		var rej *BrokerRejection
		if errors.As(err, &rej) || IsAuthError(err) {
			h.emergencyMu.Lock()
			delete(h.lastEmergency, latchKey)
			h.emergencyMu.Unlock()
		}
	}

	// Step 2 — free the quantity: cancel our resting stops for this position.
	if ec.StrategyID != "" && h.repo != nil {
		standing, err := h.repo.ListStandingSLs(ctx, ec.StrategyID, info.Symbol)
		if err != nil {
			h.logger.Warn("EmergencySell: standing-SL lookup failed — selling anyway (broker rejects if the qty is locked)",
				zap.String("symbol", info.Symbol), zap.Error(err))
		}
		for _, sl := range standing {
			if sl.BrokerOrderID != "" {
				if cerr := h.broker.CancelOrder(ctx, auth, info, sl.BrokerOrderID); cerr != nil && !IsOrderNotFoundError(cerr) {
					h.logger.Warn("EmergencySell: standing SL cancel failed — selling anyway",
						zap.String("symbol", info.Symbol), zap.String("broker_order_id", sl.BrokerOrderID), zap.Error(cerr))
					continue
				}
			}
			_ = h.repo.UpdateOrderCancelled(ctx, sl.ID)
			_ = h.repo.InsertEvent(ctx, sl.ID, "SL_CANCELLED_FOR_EXIT", "SL_PLACED", "CANCELLED",
				"", 0, sl.Qty, "stop removed ahead of the exit sell: "+reason)
			h.logger.Info("EmergencySell: standing stop cancelled ahead of exit",
				zap.String("symbol", info.Symbol), zap.String("broker_order_id", sl.BrokerOrderID), zap.Int("qty", sl.Qty))
		}
	}

	// Step 2b — an exit of ours still resting (today's DAY LIMIT, or last
	// evening's AMO LIMIT under its post-conversion id) also locks the qty.
	if confirmed := h.cancelOpenExitSells(ctx, auth, info, ec.StrategyID, reason); confirmed > 0 && h.repo != nil {
		if net, _, has, nerr := h.repo.NetHeldQty(ctx, ec.StrategyID, info.Symbol); nerr == nil && has {
			if net <= 0 {
				h.logger.Warn("EmergencySell: the earlier exit had already filled — nothing left to sell",
					zap.String("user_id", auth.UserID), zap.String("symbol", info.Symbol))
				h.emergencyMu.Lock()
				delete(h.lastEmergency, latchKey)
				h.emergencyMu.Unlock()
				return nil
			}
			if net < qty {
				qty = net
			}
		}
	}

	// Step 3a — lower circuit: nothing trades today; queue an AMO LIMIT at the floor.
	_, atLower, _ := h.broker.CheckCircuit(ctx, info.ExchangeToken)
	if atLower {
		h.logger.Warn("Lower circuit — queueing AMO LIMIT SELL for the next session",
			zap.String("symbol", info.Symbol), zap.Int("qty", qty))
		brokerID, err := h.broker.PlaceAMOSell(ctx, auth, info, qty)
		if err != nil {
			releaseLatchIfRejected(err)
			h.logger.Error("EMERGENCY AMO SELL FAILED",
				zap.String("symbol", info.Symbol), zap.Error(err))
			return err
		}
		h.recordExitSell(ctx, ec, info, auth.UserID, OrderTypeAMOSell, qty, info.DPRLower, brokerID, reason)
		return nil
	}

	// Step 3b — marketable LIMIT: fills now, bounded slippage, algo-compliant.
	brokerID, limit, err := h.broker.PlaceMarketableLimitSell(ctx, auth, info, qty)
	if err != nil {
		releaseLatchIfRejected(err)
		h.logger.Error("EMERGENCY LIMIT SELL FAILED",
			zap.String("symbol", info.Symbol),
			zap.Int("qty", qty),
			zap.Error(err))
		return err
	}
	h.recordExitSell(ctx, ec, info, auth.UserID, OrderTypeMarketSell, qty, limit, brokerID, reason)
	return nil
}

// recordExitSell books the exit as a PLACED row (see exit_repo.go).
func (h *SLHandler) recordExitSell(ctx context.Context, ec exitContext, info *SymbolInfo, userID string, ot OrderType, qty int, limit float64, brokerID, reason string) {
	if h.repo == nil {
		return
	}
	if _, err := h.repo.InsertExitSellPlaced(ctx, ExitSellRow{
		StrategyID: ec.StrategyID, UserID: userID, Symbol: info.Symbol, ISIN: ec.ISIN,
		Info: info, OrderType: ot, Qty: qty, LimitPrice: limit, BrokerOrderID: brokerID,
		ParentOrderID: ec.EntryOrderID, Reason: reason,
	}); err != nil {
		h.logger.Error("exit sell placed at broker but NOT recorded — reconciler cannot track it",
			zap.String("symbol", info.Symbol), zap.String("broker_order_id", brokerID), zap.Error(err))
		return
	}
	h.logger.Warn("EXIT SELL placed — awaiting fill confirmation",
		zap.String("user_id", userID), zap.String("symbol", info.Symbol),
		zap.String("type", string(ot)), zap.Int("qty", qty), zap.Float64("limit", limit),
		zap.String("broker_order_id", brokerID), zap.String("reason", reason))
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
