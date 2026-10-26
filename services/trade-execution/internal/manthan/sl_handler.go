package manthan

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
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

	// opsNotif (optional) carries operator alerts (OPS_ALERT) for conditions
	// that need a human — a stop that could not be placed, an exit that failed.
	opsNotif AuthExpiryNotifier
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

// SetAuthExpiryNotifier wires the notifier used for operator alerts.
func (h *SLHandler) SetAuthExpiryNotifier(n AuthExpiryNotifier) {
	h.opsNotif = n
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
			// A broker/exchange rejection of the stop is NOT a reason to sell a
			// position bought seconds ago (2026-10-22 review; the old path
			// market-sold it). The lot stays held: the safety monitor's naked
			// scan re-drives PlaceInitialSL every cycle (band permitting) and
			// the rules-engine software stop remains in force. Alert a human.
			h.logger.Error("SL placement REJECTED — position is NAKED; naked scan keeps re-trying (no exit sell)",
				zap.String("symbol", signal.Symbol), zap.Error(err))
			if h.eventPub != nil {
				h.eventPub.PublishSLRejected(ctx, signal, "", errMsg)
			}
			notifyOps(h.opsNotif, ctx, signal.UserID, signal.Symbol, "SL_REJECTED",
				fmt.Sprintf("initial stop for %s rejected by broker: %s — position held without a broker stop", signal.Symbol, errMsg))
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

	// HARD RULE: the trigger is never clamped to the band floor. If the merged
	// intended stop is below today's band, keep the broker's resting trigger
	// (already inside the band) and extend the quantity only; the intended
	// level is recorded below and the trail / daily replay places it when the
	// band allows (2026-10-22 review — ModifySLOrder's clamp would otherwise
	// park the whole combined lot at today's lower circuit).
	modTrigger, modLimit := newTrigger, newLimit
	if _, _, deferWhy := resolveBandFloor(newTrigger, info.DPRLower); deferWhy != "" && parentSL.BrokerTriggerPrice > 0 {
		modTrigger, modLimit = parentSL.BrokerTriggerPrice, parentSL.BrokerLimitPrice
		if modLimit <= 0 {
			modLimit = modTrigger - SLLimitGap(modTrigger, info.TickSize)
		}
		h.logger.Warn("Top-up SL merge: merged stop is below today's band — extending qty only, resting trigger kept",
			zap.String("symbol", signal.Symbol), zap.Float64("intended", newTrigger),
			zap.Float64("resting_trigger", modTrigger), zap.String("why", deferWhy))
	}
	brokerTrig, brokerLim, modErr := h.broker.ModifySLOrder(ctx, auth, info, parentSL.BrokerOrderID,
		combinedQty, modTrigger, modLimit)
	if IsAuthError(modErr) && h.refreshAuth != nil {
		if fresh := h.refreshAuth(signal.UserID); fresh != nil {
			auth = *fresh
			brokerTrig, brokerLim, modErr = h.broker.ModifySLOrder(ctx, auth, info, parentSL.BrokerOrderID,
				combinedQty, modTrigger, modLimit)
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
		// The broker said it holds nothing of this scrip ("Set= 0", any stop
		// type parked GHOST_SUSPECT), or holds it PLEDGED (parked by the book
		// reconciler): trailing or promoting the parked row would only place a
		// stop the RMS rejects again. Handled (not retried); the operator heals
		// the ledger via the admin ghost endpoint / the client unpledges.
		if tag, terr := h.repo.ParkedStopTag(ctx, signal.StrategyID, signal.Symbol); terr == nil && tag != "" {
			h.logger.Warn("SL_MODIFY ignored — "+parkedStopRefusal(tag, signal.Symbol),
				zap.String("user_id", signal.UserID), zap.String("symbol", signal.Symbol), zap.String("park_tag", tag))
			return nil
		}
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
	if targetSL.ExchangeToken != "" && targetSL.ExchangeToken != info.ExchangeToken {
		h.logger.Warn("ModifyTrail: stored token differs from the live ISIN mapping — re-pointing rows",
			zap.String("symbol", signal.Symbol), zap.String("stored", targetSL.ExchangeToken), zap.String("live", info.ExchangeToken))
		_, _ = h.repo.UpdateSymbolToken(ctx, signal.StrategyID, signal.Symbol, info.ExchangeToken, info.IndiraSymbol)
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

	// HARD RULE: the trigger is never clamped to the band floor. If the trailed
	// stop is below today's band, ModifySLOrder's clamp would park the broker
	// trigger at the lower circuit — so leave the resting trigger untouched (it
	// is inside the band and still protects), record the intended level, and
	// let the next trail / daily replay place it when the band allows
	// (2026-10-22 review; MergeTopupSL got the same guard).
	if _, _, deferWhy := resolveBandFloor(signal.NewSL, info.DPRLower); deferWhy != "" {
		if uerr := h.repo.UpdateSLIntendedOnly(ctx, targetSL.ID, signal.NewSL, newLimit); uerr != nil {
			h.logger.Warn("SL trail below band: intended-only update failed", zap.Int64("sl_order_id", targetSL.ID), zap.Error(uerr))
		}
		_ = h.repo.InsertEvent(ctx, targetSL.ID, "SL_TRAIL_BELOW_BAND", "SL_PLACED", "SL_PLACED", "",
			signal.NewSL, targetSL.Qty, "trailed stop below today's band — broker trigger left in place: "+deferWhy)
		h.logger.Warn("SL trail below today's band — broker trigger left in place, intended recorded",
			zap.String("symbol", signal.Symbol), zap.Float64("intended", signal.NewSL),
			zap.Float64("dpr_lower", info.DPRLower), zap.String("why", deferWhy))
		h.recordCooldown(signal.Symbol)
		return nil
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
			// A failed trail-up modify is NOT a reason to sell: the stop is still
			// resting at the broker at its previous trigger and the position is,
			// by construction of a trail, at a new high. The old path liquidated
			// here (2026-10-22 review). Alert, publish, and let the inbox retry.
			h.logger.Error("SL modify retry FAILED — stop left at its previous trigger; no exit sell",
				zap.String("symbol", signal.Symbol), zap.Error(err))
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
			_ = h.repo.InsertEvent(ctx, targetSL.ID, "SL_MODIFY_FAILED", "SL_PLACED", "SL_PLACED", "",
				signal.NewSL, targetSL.Qty, "trail modify failed twice — stop left at previous trigger: "+err.Error())
			notifyOps(h.opsNotif, ctx, signal.UserID, signal.Symbol, "SL_MODIFY_FAILED",
				fmt.Sprintf("trail of %s to %.2f rejected twice (%v); stop still resting at the previous trigger", signal.Symbol, signal.NewSL, err))
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
	// A LIVE exit is a DAY LIMIT and cancels the standing stop first; outside
	// the regular session it cannot execute and would strip tonight's AMO
	// stop (2026-10-22 review). Hold it: the inbox wakes it at 09:15:10.
	if signal.TradingMode != "PAPER" && !marketOpenNowIST() {
		return fmt.Errorf("%w: exit sell for %s outside the regular session — held until 09:15:10 IST", ErrPreOpenHold, signal.Symbol)
	}
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
		// The broker's RMS already said it holds nothing of this scrip for the
		// position ("Set= 0" → GHOST_SUSPECT parked stop), or the shares are
		// PLEDGED at the broker (parked by the book reconciler): another sell
		// can only be rejected and page the operator again. POISON → the inbox
		// parks the signal; the admin ghost heal / the client's unpledge is the
		// way out (2026-10-23 review; PLEDGED 2026-10-26).
		if tag, terr := h.repo.ParkedStopTag(ctx, signal.StrategyID, signal.Symbol); terr == nil && tag != "" {
			h.logger.Error("EmergencySell refused — "+parkedStopRefusal(tag, signal.Symbol),
				zap.String("user_id", signal.UserID), zap.String("symbol", signal.Symbol), zap.Int32("qty", signal.Quantity), zap.String("park_tag", tag))
			return poisonErr("pre-check: %s", parkedStopRefusal(tag, signal.Symbol))
		}
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

// resolveExitSymbol resolves the broker symbol for an exit. The ISIN key in
// Redis is the LIVE token mapping and is always preferred (signal ISIN, else
// the entry row's ISIN); the token stored on the entry row is only a fallback
// — it can be weeks stale (BODALCHEM 25017→25021, 2026-10-22). When the two
// disagree the position's rows are re-pointed at the live token. Also returns
// the ISIN used, for bookkeeping.
func (h *SLHandler) resolveExitSymbol(ctx context.Context, signal SLExitSignal) (*SymbolInfo, string, error) {
	var entry *ManthanOrder
	if h.repo != nil {
		e, err := h.repo.GetActiveEntryBySymbol(ctx, signal.StrategyID, signal.Symbol)
		if err != nil {
			h.logger.Warn("EmergencySell: entry row lookup failed", zap.String("symbol", signal.Symbol), zap.Error(err))
		} else {
			entry = e
		}
	}
	isin := signal.ISIN
	if isin == "" && entry != nil {
		isin = entry.ISIN
	}

	var firstErr error
	if isin != "" {
		info, err := h.broker.ResolveSymbol(ctx, signal.Symbol, isin)
		if err == nil {
			if entry != nil && entry.ExchangeToken != "" && entry.ExchangeToken != info.ExchangeToken {
				h.logger.Warn("EmergencySell: stored token differs from the live ISIN mapping — using live, re-pointing rows",
					zap.String("symbol", signal.Symbol), zap.String("stored", entry.ExchangeToken), zap.String("live", info.ExchangeToken))
				if h.repo != nil {
					if n, uerr := h.repo.UpdateSymbolToken(ctx, signal.StrategyID, signal.Symbol, info.ExchangeToken, info.IndiraSymbol); uerr != nil {
						h.logger.Warn("EmergencySell: token re-point failed", zap.String("symbol", signal.Symbol), zap.Error(uerr))
					} else {
						h.logger.Info("EmergencySell: position rows re-pointed at the live token", zap.String("symbol", signal.Symbol), zap.Int64("rows", n))
					}
				}
			}
			h.logger.Info("EmergencySell: symbol resolved from the live ISIN key",
				zap.String("symbol", signal.Symbol), zap.String("isin", isin), zap.String("token", info.ExchangeToken))
			return info, isin, nil
		}
		firstErr = err
		h.logger.Warn("EmergencySell: ISIN resolve failed — falling back to the entry row's stored token",
			zap.String("symbol", signal.Symbol), zap.String("isin", isin), zap.Error(err))
	}
	if entry != nil && entry.ExchangeToken != "" {
		info := &SymbolInfo{
			Symbol:        signal.Symbol,
			ISIN:          isin,
			ExchangeToken: entry.ExchangeToken,
			IndiraSymbol:  strDefault(entry.IndiraSymbol, fmt.Sprintf("STK_%s_EQ_NSE_%s", signal.Symbol, entry.ExchangeToken)),
			Exchange:      "NSE",
			TickSize:      0.05,
		}
		h.broker.EnrichMarketInfo(ctx, info)
		h.logger.Info("EmergencySell: symbol resolved from the entry row's stored token",
			zap.String("symbol", signal.Symbol), zap.String("token", entry.ExchangeToken), zap.String("isin", isin))
		return info, isin, nil
	}
	if firstErr == nil {
		firstErr = fmt.Errorf("no isin on signal and no filled entry row with a token for %s/%s", signal.StrategyID, signal.Symbol)
	}
	return nil, "", fmt.Errorf("resolve symbol: %w", firstErr)
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
	// Keyed like the position lock (strategy + symbol, plus the user): two
	// strategies of one user holding the same stock exit independently.
	latchKey := auth.UserID + ":" + ec.StrategyID + ":" + info.Symbol
	h.emergencyMu.Lock()
	if last, ok := h.lastEmergency[latchKey]; ok && time.Since(last) < emergencyLatchWindow {
		h.emergencyMu.Unlock()
		h.logger.Error("EMERGENCY SELL SUPPRESSED — already fired for user/strategy/symbol inside latch window; upstream loop must be investigated",
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
		var biz *indiraClient.BrokerBusinessError
		if errors.As(err, &rej) || errors.As(err, &biz) || IsAuthError(err) || errors.Is(err, ErrUnpriced) {
			h.emergencyMu.Lock()
			delete(h.lastEmergency, latchKey)
			h.emergencyMu.Unlock()
		}
	}

	// Step 2 — free the quantity: cancel our resting stops for this position.
	confirmed := 0
	if ec.StrategyID != "" && h.repo != nil {
		standing, err := h.repo.ListStandingSLs(ctx, ec.StrategyID, info.Symbol)
		if err != nil {
			h.logger.Warn("EmergencySell: standing-SL lookup failed — selling anyway (broker rejects if the qty is locked)",
				zap.String("symbol", info.Symbol), zap.Error(err))
		}
		var book []indiraClient.OrderBook
		bookLoaded := false
		for _, sl := range standing {
			if sl.BrokerOrderID != "" {
				id := sl.BrokerOrderID
				cerr := h.broker.CancelOrder(ctx, auth, info, id)
				if cerr != nil && IsOrderNotFoundError(cerr) && sl.OrderType == OrderTypeSLSellAMO {
					// Last night's AMO stop converted at 08:50 to a NEW broker id the
					// reconciler may not have promoted yet: the resting stop is the
					// converted order. Find it in the book and cancel THAT, or the
					// quantity stays locked and the row below is mislabelled.
					if !bookLoaded {
						bookLoaded = true
						if b, berr := h.broker.GetOrderBook(ctx, auth); berr == nil {
							book = b
						}
					}
					if conv := matchConvertedAMO(sl, book, map[string]bool{}); conv != nil {
						h.logger.Info("EmergencySell: AMO stop converted — cancelling its live id",
							zap.String("symbol", info.Symbol), zap.String("old", id), zap.String("new", conv.OrdId))
						id = conv.OrdId
						cerr = h.broker.CancelOrder(ctx, auth, info, id)
					}
				}
				// The broker stop fires on the same LTP cross as the software
				// stop: if it already EXECUTED (fully or partly), the fill must be
				// booked — marking it CANCELLED would drop it from the ledger and
				// leave a ghost lot (2026-10-22 review). A cancel can FAIL for
				// exactly that reason ("FULLY_EXECUTED order not allowed to
				// CANCEL" — FILATEX 2026-10-23, which then placed a second sell
				// the RMS rejected and paged the operator for nothing). So the
				// order is read whatever the cancel said, and the read decides.
				st, fq, ap, serr := h.broker.GetOrderStatus(ctx, auth, id)
				if serr != nil && looksExecutedCancelError(cerr) {
					// The book lags the cancel reply by a moment. Bounded, ctx-aware
					// (this runs under the position lock).
					select {
					case <-time.After(time.Second):
					case <-ctx.Done():
					}
					st, fq, ap, serr = h.broker.GetOrderStatus(ctx, auth, id)
				}
				if serr == nil && (isFilledStatus(st) || fq > 0) {
					if berr := h.bookStandingStopFill(ctx, ec, auth, info, sl, id, st, fq, ap); berr != nil {
						return berr
					}
					confirmed++
					continue
				}
				if serr != nil && (cerr == nil || IsOrderNotFoundError(cerr)) {
					// Cancel went through but the order could not be read: its fill
					// (if any) is unverified. Leave the row SL_PLACED so CHECK 1
					// re-reads and books it; marking it CANCELLED here would drop
					// a real fill from the ledger. The exit proceeds — the RMS
					// refuses it if the quantity is in fact sold.
					h.logger.Warn("EmergencySell: stop cancelled but its status could not be read — row left for the monitor to book",
						zap.String("symbol", info.Symbol), zap.String("broker_order_id", id), zap.Error(serr))
					continue
				}
				if cerr != nil && !IsOrderNotFoundError(cerr) {
					if looksExecutedCancelError(cerr) {
						// The broker says the stop executed but the fill could not be
						// read: nothing is left to sell and nothing can be booked here.
						// The safety monitor books the fill on its next pass; a second
						// sell would only be rejected ("Set= N, Prev= N").
						h.emergencyMu.Lock()
						delete(h.lastEmergency, latchKey)
						h.emergencyMu.Unlock()
						return fmt.Errorf("%w: stop %s for %s reported executed by the broker (%v) but its fill could not be read (%v) — not selling; the safety monitor books the fill",
							ErrStopAlreadyFilled, id, info.Symbol, cerr, serr)
					}
					h.logger.Warn("EmergencySell: standing SL cancel failed — selling anyway (the RMS rejects the sell if the qty is still locked)",
						zap.String("symbol", info.Symbol), zap.String("broker_order_id", id), zap.Error(cerr))
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
	confirmed += h.cancelOpenExitSells(ctx, auth, info, ec.StrategyID, reason)
	if confirmed > 0 && h.repo != nil {
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
		brokerID, amoLimit, err := h.broker.PlaceAMOSellPriced(ctx, auth, info, qty)
		if err != nil {
			releaseLatchIfRejected(err)
			h.logger.Error("EMERGENCY AMO SELL FAILED",
				zap.String("symbol", info.Symbol), zap.Error(err))
			return err
		}
		h.recordExitSell(ctx, ec, info, auth.UserID, OrderTypeAMOSell, qty, amoLimit, brokerID, reason)
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

// ErrStopAlreadyFilled: the standing stop executed at the exchange before the
// exit could cancel it and its fill could not be read in time — the position
// is sold, nothing may be placed; the safety monitor books the fill.
var ErrStopAlreadyFilled = errors.New("standing stop already executed at the broker")

// bookStandingStopFill books a standing stop that (partly) executed at the
// exchange before the exit sell could cancel it: ledger FILLED, SL_FILLED
// event, SL_FILLED published. A booking failure is returned so the caller
// never sells blind on top of an unbooked fill.
func (h *SLHandler) bookStandingStopFill(ctx context.Context, ec exitContext, auth BrokerAuth, info *SymbolInfo, sl *ManthanOrder, id, st string, fq int, ap float64) error {
	if fq <= 0 {
		fq = sl.Qty
	}
	if uerr := h.repo.UpdateOrderFilled(ctx, sl.ID, fq, ap); uerr != nil {
		return fmt.Errorf("standing stop %s filled %d at broker but the fill could not be booked (%v) — not selling blind", id, fq, uerr)
	}
	_ = h.repo.InsertEvent(ctx, sl.ID, "SL_FILLED", "SL_PLACED", "SL_FILLED", st, ap, fq,
		"stop (partly) filled at the exchange before the exit sell could cancel it")
	if h.eventPub != nil {
		if esid, e := h.repo.GetEntrySignalIDByOrderID(ctx, sl.ID); e == nil && esid != "" {
			h.eventPub.PublishSLFilled(ctx, esid, ec.StrategyID, auth.UserID, info.Symbol, id, ap, int32(fq), 0)
		}
	}
	h.logger.Warn("EmergencySell: standing stop had already filled — booked, not cancelled, not re-sold",
		zap.String("user_id", auth.UserID), zap.String("symbol", info.Symbol), zap.String("broker_order_id", id),
		zap.Int("filled", fq), zap.Int("qty", sl.Qty), zap.Float64("avg_price", ap))
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
		notifyOps(h.opsNotif, ctx, userID, info.Symbol, "EXIT_UNRECORDED",
			fmt.Sprintf("exit for %s placed as %s but could not be recorded: %v", info.Symbol, brokerID, err))
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
