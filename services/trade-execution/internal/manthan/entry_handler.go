package manthan

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
	"go.uber.org/zap"
)

// EntryHandler manages LIMIT BUY order placement with WSS-based fill detection.
//
// LIVE mode flow:
//  1. Pre-checks (market hours, circuit, duplicate, margin)
//  2. Place LIMIT BUY at LTP + 2 ticks
//  3. Register WSS callback for broker_order_id
//  4. Wait on channel for fill/reject (real-time from broker WSS)
//  5. If timeout (30s) → smart retry: modify if price near, cancel if drifted
//  6. Handle partial fills: accept what we got, cancel remainder
//  7. After fill: trigger SL placement immediately
//
// PAPER mode:
//  - Instant fill at live LTP (no broker call, no WSS)
// AuthProvider fetches fresh broker auth credentials for a user.
type AuthProvider func(userID string) *BrokerAuth

// WSSSubscriber starts the broker WSS subscription for a user (idempotent).
type WSSSubscriber func(userID, bearerToken, appID, source string) error

type EntryHandler struct {
	broker        *BrokerAdapter
	repo          *Repository
	preCheck      *PreChecker
	slHandler     *SLHandler
	wssBridge     *WSSBridge
	authProvider  AuthProvider
	// refreshAuth invalidates the credentials cache for a user and returns
	// fresh credentials from DB. Called when a broker call returns
	// AU004/401 — the cached bearer may be stale (browser re-login kills
	// server-side session); refetching from DB picks up any newer token the
	// user-config service wrote.
	refreshAuth   AuthProvider
	wssSubscriber WSSSubscriber
	// eventPub publishes broker activity to manthan.execution.events with
	// idempotent (signal_id, event_seq) keys. Same publisher emits the legacy
	// FILL_CONFIRMED/FILL_UPDATE types alongside the canonical ENTRY_FILLED/
	// ENTRY_PARTIAL_FILL — so existing rules-engine consumers keep working
	// during the cutover.
	eventPub      *ManthanEventPublisher
	logger        *zap.Logger

	// Optional SESSION_EXPIRED publisher — nil-safe.
	authNotif AuthExpiryNotifier

	maxRetries     int
	waitTimeout    time.Duration // how long to wait for WSS fill before retry
	priceThreshold float64       // max % drift before cancel (0.003 = 0.3%)
}

// SetAuthExpiryNotifier wires the SESSION_EXPIRED publisher fired when a
// broker poll returns AU004 mid-entry.
func (h *EntryHandler) SetAuthExpiryNotifier(n AuthExpiryNotifier) {
	h.authNotif = n
}

func NewEntryHandler(broker *BrokerAdapter, repo *Repository, preCheck *PreChecker, slHandler *SLHandler, logger *zap.Logger) *EntryHandler {
	return &EntryHandler{
		broker:         broker,
		repo:           repo,
		preCheck:       preCheck,
		slHandler:      slHandler,
		logger:         logger,
		maxRetries:     3,
		waitTimeout:    10 * time.Second,
		priceThreshold: 0.01, // 1% drift threshold (was 0.3% — too tight for live)
	}
}

// SetWSSBridge sets the WSS bridge for real-time fill detection.
func (h *EntryHandler) SetWSSBridge(bridge *WSSBridge) {
	h.wssBridge = bridge
}

// SetAuthProvider sets the auth provider for fetching fresh credentials.
func (h *EntryHandler) SetAuthProvider(ap AuthProvider) {
	h.authProvider = ap
}

// SetRefreshAuth sets the callback invoked on broker auth errors (AU004/401).
// It should invalidate any cached credentials for the user and re-fetch from
// the authoritative store (DB). Returning nil means no fresh auth is available;
// the original error is then propagated to the caller.
func (h *EntryHandler) SetRefreshAuth(rp AuthProvider) {
	h.refreshAuth = rp
}

// SetWSSSubscriber sets the function that starts broker WSS for a user.
func (h *EntryHandler) SetWSSSubscriber(fn WSSSubscriber) {
	h.wssSubscriber = fn
}

// SetEventPublisher wires the centralized publisher used to emit broker
// activity to manthan.execution.events. Replaces the older single-purpose
// FillPublisher callback — the new publisher emits ENTRY_FILLED,
// ENTRY_PARTIAL_FILL, ENTRY_REJECTED (and the legacy FILL_CONFIRMED /
// FILL_UPDATE for backwards compatibility).
func (h *EntryHandler) SetEventPublisher(p *ManthanEventPublisher) {
	h.eventPub = p
}

// ExecuteEntry processes a MANTHAN_ENTRY signal end-to-end.
func (h *EntryHandler) ExecuteEntry(ctx context.Context, signal ManthanSignal) (int64, error) {
	// 1. Resolve symbol
	info, err := h.broker.ResolveSymbol(ctx, signal.Symbol, signal.ISIN)
	if err != nil {
		// Symbol not found in our universe (missing ISIN→token mapping etc).
		// This is the NATIONALUM/LLOYDSME ghost-position root cause — without
		// this event the rules-engine would mark the decision DISPATCHED
		// forever and never create a corresponding position; the reaper would
		// later mark it TIMED_OUT. Publishing REJECTED is faster + more
		// accurate.
		if h.eventPub != nil {
			h.eventPub.PublishEntryRejected(ctx, signal, "", fmt.Sprintf("resolve_symbol: %v", err), "TRADE_EXEC")
		}
		return 0, fmt.Errorf("resolve symbol: %w", err)
	}

	// 2. Check for duplicate — skip if we already have a FILLED/PLACED BUY
	// for same symbol+strategy. We do NOT publish REJECTED here: the existing
	// decision is already in a terminal-or-progressing state; this signal is
	// a benign duplicate (e.g. catch-up + live consumer racing).
	//
	// EXCEPTION: a top-up signal (TopUpForSignalID set) intentionally targets
	// an already-held position to fill the gap to today's EMA target. Skip
	// the dup check for top-ups; rules-engine projector recognises the
	// parent linkage on the fill event and adds qty + invested onto the
	// parent manthan_positions row.
	if signal.TopUpForSignalID == "" {
		existingEntry, _ := h.repo.GetActiveEntryBySymbol(ctx, signal.StrategyID, signal.Symbol)
		if existingEntry != nil {
			h.logger.Info("Skipping duplicate entry — already have position",
				zap.String("symbol", signal.Symbol),
				zap.String("existing_status", string(existingEntry.Status)))
			return 0, fmt.Errorf("duplicate: already have %s position for %s", existingEntry.Status, signal.Symbol)
		}
	} else {
		h.logger.Info("Top-up entry — duplicate check bypassed",
			zap.String("symbol", signal.Symbol),
			zap.String("parent_signal_id", signal.TopUpForSignalID))
	}

	// 3. Pre-checks
	check := h.preCheck.CheckEntry(ctx, signal, info)
	if !check.CanProceed {
		h.logger.Info("Entry pre-check failed",
			zap.String("symbol", signal.Symbol),
			zap.String("reason", check.Reason))
		if h.eventPub != nil {
			h.eventPub.PublishEntryRejected(ctx, signal, "", fmt.Sprintf("pre_check: %s", check.Reason), "TRADE_EXEC")
		}
		return 0, fmt.Errorf("pre-check: %s", check.Reason)
	}

	// 3. Fetch live LTP
	ltp, err := h.broker.FetchLTP(ctx, info.ExchangeToken)
	if err != nil {
		if h.eventPub != nil {
			h.eventPub.PublishEntryRejected(ctx, signal, "", fmt.Sprintf("fetch_ltp: %v", err), "TRADE_EXEC")
		}
		return 0, fmt.Errorf("fetch LTP: %w", err)
	}

	// 4. Calculate entry price = LTP + 2 ticks
	entryPrice := ltp + (info.TickSize * 2)

	// 5. Override qty if test mode (MANTHAN_TEST_QTY env var)
	qty := int(signal.Quantity)
	if testQty := os.Getenv("MANTHAN_TEST_QTY"); testQty != "" {
		if tq, err := strconv.Atoi(testQty); err == nil && tq > 0 {
			h.logger.Warn("TEST MODE: overriding qty",
				zap.String("symbol", signal.Symbol),
				zap.Int("original_qty", qty),
				zap.Int("test_qty", tq))
			qty = tq
		}
	}

	// 5b. Freeze-qty cap. Exchange rejects single orders exceeding the
	// stock's freeze-qty limit ("Freeze Qty Exceeded"). For Manthan sizing
	// this is rarely hit, but when it is we CAP rather than split — taking
	// a smaller position is safer than orchestrating multi-chunk fills where
	// partial success on one chunk leaves us over/under-margin on the rest.
	// If splitting is ever needed, the allocator should divide upstream.
	if info.FreezeQty > 0 && qty > info.FreezeQty {
		h.logger.Warn("Qty exceeds exchange freeze limit — capping",
			zap.String("symbol", signal.Symbol),
			zap.Int("requested_qty", qty),
			zap.Int("freeze_qty", info.FreezeQty))
		qty = info.FreezeQty
	}

	// Create DB record
	order := &ManthanOrder{
		SignalID:      signal.OrderID,
		StrategyID:    signal.StrategyID,
		UserID:        signal.UserID,
		Symbol:        signal.Symbol,
		ISIN:          signal.ISIN,
		Exchange:      "NSE",
		OrderType:     OrderTypeLimitBuy,
		OrderSide:     "BUY",
		ProductType:   "CNC",
		Qty:           qty,
		LimitPrice:    entryPrice,
		IndiraSymbol:  info.IndiraSymbol,
		ExchangeToken: info.ExchangeToken,
		Status:        StatusPending,
		MaxRetries:    h.maxRetries,
	}
	orderID, err := h.repo.InsertOrder(ctx, order)
	if err != nil {
		// Race defense: another concurrent worker already has an active
		// entry order for this (strategy, symbol, order_type). Migration
		// 016's partial UNIQUE index just prevented us from placing a
		// duplicate at the broker. The sibling worker's order IS being
		// processed correctly; we just lost the race. Log + return
		// gracefully — inbox_worker classifies this as DONE (not retry,
		// not DLQ) via ErrDuplicateActiveEntry.
		if errors.Is(err, ErrDuplicateActiveEntry) {
			h.logger.Info("Skipping duplicate entry — sibling worker won race (DB-level dedup)",
				zap.String("symbol", signal.Symbol),
				zap.String("strategy", signal.StrategyID),
				zap.String("signal_id", signal.OrderID))
			return 0, fmt.Errorf("%w: sibling worker holds the active entry", ErrDuplicateActiveEntry)
		}
		return 0, fmt.Errorf("insert order: %w", err)
	}

	// Resolve auth at-edge via authProvider (user-config gRPC + DB fallback).
	// Auth no longer rides the Kafka signal — see internal/manthan/models.go
	// and internal/repository/grpc_credentials_repository.go. If authProvider
	// can't resolve creds (no SSO yet, both gRPC + DB unreachable), we fail
	// the entry — placing a broker order without creds would 401 anyway.
	if h.authProvider == nil {
		h.logger.Error("Entry rejected — authProvider not wired",
			zap.String("user_id", signal.UserID),
			zap.String("symbol", signal.Symbol))
		return 0, fmt.Errorf("authProvider not configured")
	}
	freshAuth := h.authProvider(signal.UserID)
	if freshAuth == nil || freshAuth.BearerToken == "" {
		h.logger.Error("Entry rejected — no broker credentials available",
			zap.String("user_id", signal.UserID),
			zap.String("symbol", signal.Symbol))
		return 0, fmt.Errorf("no broker credentials for user %s", signal.UserID)
	}
	auth := *freshAuth

	// Margin pre-check (LIVE only) — skip the broker call if we're short on
	// margin. Saves a round-trip + avoids noisy "Margin Limit exceeded"
	// rejections in the order book.
	if marginCheck := h.preCheck.CheckMargin(ctx, auth, signal.TradingMode, int(order.Qty), order.LimitPrice); !marginCheck.CanProceed {
		h.logger.Warn("Entry skipped — margin pre-check failed",
			zap.String("symbol", signal.Symbol),
			zap.String("reason", marginCheck.Reason))
		_ = h.repo.UpdateOrderCancelled(ctx, orderID)
		_ = h.repo.InsertEvent(ctx, orderID, "MARGIN_INSUFFICIENT", "PENDING", "CANCELLED",
			"", order.LimitPrice, int(order.Qty), marginCheck.Reason)
		if h.eventPub != nil {
			h.eventPub.PublishEntryRejected(ctx, signal, "", fmt.Sprintf("margin: %s", marginCheck.Reason), "TRADE_EXEC")
		}
		return orderID, fmt.Errorf("margin pre-check: %s", marginCheck.Reason)
	}

	// 6. Route by trading mode
	if signal.TradingMode == "PAPER" {
		return h.executePaper(ctx, orderID, order, info, ltp, signal)
	}
	return h.executeLive(ctx, orderID, order, info, auth, ltp, signal)
}

// executePaper simulates an instant fill.
func (h *EntryHandler) executePaper(ctx context.Context, orderID int64, order *ManthanOrder, info *SymbolInfo, ltp float64, signal ManthanSignal) (int64, error) {
	_ = h.repo.UpdateOrderPlaced(ctx, orderID, "PAPER-"+signal.OrderID)
	_ = h.repo.UpdateOrderFilled(ctx, orderID, order.Qty, ltp)
	_ = h.repo.InsertEvent(ctx, orderID, "FILL", "PLACED", "FILLED", "PAPER", ltp, order.Qty, "paper mode instant fill")

	h.logger.Info("PAPER BUY filled",
		zap.String("symbol", signal.Symbol),
		zap.Int("qty", order.Qty),
		zap.Float64("price", ltp))

	// Manthan production SL: 20% below entry. Positional strategy — wide SL lets
	// normal market noise pass while capping catastrophic loss.
	slTrigger := ltp * 0.80
	slLimit := slTrigger - SLLimitGap(slTrigger, info.TickSize)
	h.slHandler.PlaceInitialSL(ctx, orderID, signal, info, order.Qty, slTrigger, slLimit)

	// Publish fill for rules-engine sync (paper uses LTP as fill price)
	if h.eventPub != nil {
		h.eventPub.PublishEntryFilled(ctx, signal, "", ltp, int32(order.Qty), 0)
	}

	return orderID, nil
}

// executeLive places with broker and waits for WSS fill callback.
func (h *EntryHandler) executeLive(ctx context.Context, orderID int64, order *ManthanOrder, info *SymbolInfo, auth BrokerAuth, originalLTP float64, signal ManthanSignal) (int64, error) {

	// Place initial LIMIT BUY with one auth-refresh retry on AU004/401.
	// Broker silently downgrades JWT server-side session on re-login (per
	// Indira behaviour); our cached creds may be stale even though the DB
	// has a fresher token from a recent strategy update.
	brokerID, err := h.broker.PlaceLimitBuy(ctx, auth, info, order.Qty, order.LimitPrice)
	if IsAuthError(err) && h.refreshAuth != nil {
		h.logger.Warn("Entry hit auth error — refreshing credentials and retrying once",
			zap.String("user", signal.UserID), zap.Error(err))
		if freshAuth := h.refreshAuth(signal.UserID); freshAuth != nil {
			auth = *freshAuth
			brokerID, err = h.broker.PlaceLimitBuy(ctx, auth, info, order.Qty, order.LimitPrice)
			if err == nil {
				_ = h.repo.InsertEvent(ctx, orderID, "AUTH_REFRESHED", "PENDING", "PENDING",
					"", 0, 0, "bearer token refreshed from DB after AU004; retry succeeded")
			}
		}
	}
	if err != nil {
		_ = h.repo.UpdateOrderRejected(ctx, orderID, err.Error())
		_ = h.repo.InsertEvent(ctx, orderID, "REJECTED", "PENDING", "REJECTED", "", 0, 0, err.Error())
		if h.eventPub != nil {
			h.eventPub.PublishEntryRejected(ctx, signal, "", fmt.Sprintf("place_limit_buy: %v", err), "TRADE_EXEC")
		}
		return 0, fmt.Errorf("place limit buy: %w", err)
	}
	_ = h.repo.UpdateOrderPlaced(ctx, orderID, brokerID)
	_ = h.repo.InsertEvent(ctx, orderID, "PLACED", "PENDING", "PLACED", "", order.LimitPrice, order.Qty, "")

	// Start broker WSS subscription for this user (idempotent — safe to call multiple times)
	if h.wssSubscriber != nil {
		go func() {
			if err := h.wssSubscriber(auth.UserID, auth.BearerToken, auth.AppID, auth.Source); err != nil {
				h.logger.Warn("WSS subscription failed (will fall back to polling)",
					zap.String("user", auth.UserID), zap.Error(err))
			}
		}()
	}

	// Register WSS callback — real-time fill detection
	var wssCh <-chan *OrderUpdate
	if h.wssBridge != nil {
		wssCh = h.wssBridge.Register(brokerID)
		defer h.wssBridge.Unregister(brokerID)
	}

	// Wait for fill via WSS with smart retry on timeout
	for retry := 0; retry <= h.maxRetries; retry++ {
		result := h.waitForFill(ctx, wssCh, brokerID, auth, signal, order, info, orderID, originalLTP, retry)
		switch result.action {
		case actionFilled:
			return h.handleFill(ctx, orderID, result, signal, info)
		case actionPartialFilled:
			return h.handlePartialFill(ctx, orderID, result, signal, info, auth, brokerID)
		case actionRejected:
			// (waitForFill already published ENTRY_REJECTED via WSS path above.)
			return orderID, fmt.Errorf("order rejected: %s", result.reason)
		case actionCancelled:
			// If the cancellation is specifically because LIMIT retries were
			// exhausted (not a price-drift abort), attempt a MARKET fallback
			// to guarantee fill on illiquid stocks before giving up.
			if result.reason == reasonLimitRetriesExhausted {
				return h.marketFallback(ctx, auth, signal, order, info, orderID, brokerID)
			}
			// Price-drift cancel — opportunity missed. Tell rules-engine so it
			// stops waiting on a position that will never come.
			if h.eventPub != nil {
				h.eventPub.PublishEntryRejected(ctx, signal, brokerID, fmt.Sprintf("cancelled: %s", result.reason), "TRADE_EXEC")
			}
			return orderID, fmt.Errorf("order cancelled: %s", result.reason)
		case actionRetry:
			continue // next retry iteration
		}
	}

	// Loop exited without actionCancelled — treat as retries exhausted + try MARKET.
	return h.marketFallback(ctx, auth, signal, order, info, orderID, brokerID)
}

// marketFallback cancels the stuck LIMIT and places a MARKET BUY to guarantee
// fill on an illiquid stock where LIMIT retries couldn't cross the ASK.
// On KINGFA 2026-04-23 the LIMIT sat ₹0.30 below ASK for 30s and never crossed;
// a MARKET would have filled instantly at the current ASK.
//
// SAFETY (added after 2026-07-15 double-fill incident): before cancelling +
// placing the MARKET on top, we do a FINAL authoritative poll to confirm the
// LIMIT is truly unfilled. Why: the retry loop in waitForFill exits with
// reasonLimitRetriesExhausted whenever EVERY poll during the wait window
// failed to reach terminal state — INCLUDING when every poll returned
// AU004 "Session expired" (auth-fail counts as "not yet filled" from the
// outer loop's point of view). If the LIMIT had actually filled but we
// couldn't confirm it because auth was down, blindly firing the MARKET
// would leave us with 2× fills — exactly what happened to CUB / IPCALAB /
// AADHARHFC on 2026-07-15 (broker returned EG003 "FULLY_EXECUTED order
// not allowed to CANCEL" on the pre-MARKET cancel, and MARKET still fired).
//
// Rule: NEVER place a second order when the first order's state is UNKNOWN.
// Only when we've CONFIRMED it's unfilled.
func (h *EntryHandler) marketFallback(ctx context.Context, auth BrokerAuth, signal ManthanSignal, order *ManthanOrder, info *SymbolInfo, orderID int64, limitBrokerID string) (int64, error) {
	h.logger.Warn("LIMIT retries exhausted — verifying state before MARKET fallback",
		zap.String("symbol", order.Symbol),
		zap.String("stuck_broker_id", limitBrokerID))

	// Refresh auth in case a re-login landed between our last poll and now
	// (the mobile app may have posted a fresh JWT via /auth/credentials).
	if h.refreshAuth != nil {
		if fresh := h.refreshAuth(signal.UserID); fresh != nil {
			auth = *fresh
		}
	}

	// SAFETY POLL: get authoritative fill state before touching the LIMIT.
	// This is the single defense against the "unknown-state = place MARKET"
	// class of bug. Three outcomes:
	//   (a) Poll succeeds + FILLED   → skip MARKET entirely, treat as filled
	//   (b) Poll succeeds + PARTIAL  → hand off to partial-fill handler
	//   (c) Poll succeeds + UNFILLED → continue with existing cancel+MARKET
	//   (d) Poll FAILS (auth/network) → ABORT the fallback. Do not place a
	//       second order when we can't confirm the first is done. Return an
	//       error that classifyHandlerErr will tag as AUTH_EXPIRED (via
	//       errors.Is wrap of ErrAuthExpired) so inbox_worker re-queues
	//       with 30s backoff — by then auth is refreshed and the retry
	//       will succeed.
	status, filledQty, avgPrice, pollErr := h.broker.GetOrderStatus(ctx, auth, limitBrokerID)
	if pollErr != nil {
		reason := "poll failed"
		if errors.Is(pollErr, indiraClient.ErrAuthExpired) {
			reason = "auth expired"
			notifyAuthExpired(h.authNotif, ctx, signal.UserID, "market-fallback-safety-poll")
		}
		h.logger.Warn("MARKET fallback ABORTED — LIMIT state unknown, refusing to double-place",
			zap.String("symbol", order.Symbol),
			zap.String("stuck_broker_id", limitBrokerID),
			zap.String("reason", reason),
			zap.Error(pollErr))
		_ = h.repo.InsertEvent(ctx, orderID, "MARKET_FALLBACK_ABORTED", "PLACED", "PLACED",
			"", 0, 0, "safety poll failed ("+reason+") — cannot confirm LIMIT fill state; re-queueing without placing MARKET")
		// Wrap ErrAuthExpired if applicable so inbox_worker classifies as
		// AUTH_EXPIRED (30s backoff, waits for re-login).
		if errors.Is(pollErr, indiraClient.ErrAuthExpired) {
			return orderID, fmt.Errorf("market fallback aborted (auth expired — cannot verify LIMIT fill state): %w", pollErr)
		}
		return orderID, fmt.Errorf("market fallback aborted (safety poll failed): %w", pollErr)
	}

	// (a) LIMIT is already FILLED — no MARKET needed. Route through the
	// standard fill handler (records fill, places trailing SL).
	if isFilledStatus(status) && filledQty >= order.Qty {
		h.logger.Info("MARKET fallback SKIPPED — LIMIT already filled at broker",
			zap.String("symbol", order.Symbol),
			zap.String("broker_id", limitBrokerID),
			zap.Int("filled_qty", filledQty),
			zap.Float64("avg_price", avgPrice))
		_ = h.repo.InsertEvent(ctx, orderID, "SAFETY_POLL_FOUND_FILL", "PLACED", "FILLED",
			status, avgPrice, filledQty, "LIMIT filled during retry window — MARKET fallback skipped")
		return h.handleFill(ctx, orderID, fillResult{
			action:    actionFilled,
			filledQty: filledQty,
			avgPrice:  avgPrice,
			brokerID:  limitBrokerID,
		}, signal, info)
	}

	// (b) Partial fill during the retry window. Accept the partial via the
	// standard partial handler (which only tops up the missing qty, not the
	// whole order — so no double-buy on the filled slice).
	if filledQty > 0 && filledQty < order.Qty {
		h.logger.Info("MARKET fallback → partial-fill handler (LIMIT partially filled during retry)",
			zap.String("symbol", order.Symbol),
			zap.Int("filled_qty", filledQty),
			zap.Int("order_qty", order.Qty))
		return h.handlePartialFill(ctx, orderID, fillResult{
			action:    actionPartialFilled,
			filledQty: filledQty,
			avgPrice:  avgPrice,
			brokerID:  limitBrokerID,
		}, signal, info, auth, limitBrokerID)
	}

	// (c) Confirmed unfilled — safe to cancel + place MARKET.
	h.logger.Warn("MARKET fallback proceeding — LIMIT confirmed unfilled by safety poll",
		zap.String("symbol", order.Symbol),
		zap.String("stuck_broker_id", limitBrokerID),
		zap.String("status", status))

	// Cancel the stuck LIMIT first (ignore error — may already be cancelled/rejected)
	_ = h.broker.CancelOrder(ctx, auth, info, limitBrokerID)
	_ = h.repo.InsertEvent(ctx, orderID, "LIMIT_CANCELLED", "PLACED", "CANCELLED", "",
		0, 0, "max LIMIT retries — switching to MARKET fallback (safety-poll confirmed unfilled)")

	// Place MARKET BUY — guaranteed fill at current ASK
	marketBrokerID, mkErr := h.broker.PlaceMarketBuy(ctx, auth, info, order.Qty)
	if mkErr != nil {
		h.logger.Error("MARKET fallback failed — abandoning entry",
			zap.String("symbol", order.Symbol), zap.Error(mkErr))
		_ = h.repo.UpdateOrderCancelled(ctx, orderID)
		_ = h.repo.InsertEvent(ctx, orderID, "MARKET_FALLBACK_FAILED", "CANCELLED", "CANCELLED", "",
			0, 0, mkErr.Error())
		if h.eventPub != nil {
			h.eventPub.PublishEntryRejected(ctx, signal, limitBrokerID, fmt.Sprintf("market_fallback_failed: %v", mkErr), "TRADE_EXEC")
		}
		return orderID, fmt.Errorf("LIMIT retries + MARKET fallback both failed: %w", mkErr)
	}

	// Swap broker_order_id to MARKET order in DB
	_ = h.repo.UpdateOrderPlaced(ctx, orderID, marketBrokerID)
	_ = h.repo.InsertEvent(ctx, orderID, "MARKET_FALLBACK_PLACED", "CANCELLED", "PLACED",
		"", 0, order.Qty, "broker_id="+marketBrokerID)

	// Poll the MARKET order — it fills instantly (or rejects instantly).
	// One poll 2s after placement is sufficient for a MARKET order.
	time.Sleep(2 * time.Second)
	status, filledQty, avgPrice, err := h.broker.GetOrderStatus(ctx, auth, marketBrokerID)
	if err != nil {
		h.logger.Warn("MARKET order poll failed — will retry via safety monitor", zap.Error(err))
		return orderID, fmt.Errorf("market fallback placed but fill-status unknown: %w", err)
	}

	h.logger.Info("MARKET fallback poll result",
		zap.String("symbol", order.Symbol),
		zap.String("broker_id", marketBrokerID),
		zap.String("status", status),
		zap.Int("filled_qty", filledQty),
		zap.Float64("avg_price", avgPrice))

	if !isFilledStatus(status) || filledQty < order.Qty {
		return orderID, fmt.Errorf("market fallback not filled: status=%s filled=%d/%d",
			status, filledQty, order.Qty)
	}

	// Proceed with the standard fill handler (places SL, publishes FILL_CONFIRMED)
	return h.handleFill(ctx, orderID, fillResult{
		action:    actionFilled,
		filledQty: filledQty,
		avgPrice:  avgPrice,
		brokerID:  marketBrokerID,
	}, signal, info)
}

type fillAction int

const (
	actionRetry         fillAction = iota
	actionFilled
	actionPartialFilled
	actionRejected
	actionCancelled
)

type fillResult struct {
	action    fillAction
	filledQty int
	avgPrice  float64
	reason    string
	brokerID  string // broker order id that produced this fill (for event_publisher)
}

// Sentinel reason used to signal the outer entry loop that LIMIT retries are
// exhausted and it should attempt a MARKET fallback before giving up.
const reasonLimitRetriesExhausted = "limit_retries_exhausted"

// waitForFill waits for a WSS update or timeout, then decides next action.
func (h *EntryHandler) waitForFill(ctx context.Context, wssCh <-chan *OrderUpdate, brokerID string, auth BrokerAuth, signal ManthanSignal, order *ManthanOrder, info *SymbolInfo, orderID int64, originalLTP float64, retry int) fillResult {

	timeout := h.waitTimeout
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return fillResult{action: actionCancelled, reason: "context cancelled"}

		case update, ok := <-wssCh:
			if !ok {
				return fillResult{action: actionCancelled, reason: "WSS channel closed"}
			}

			h.logger.Info("WSS update received",
				zap.String("symbol", signal.Symbol),
				zap.String("broker_id", brokerID),
				zap.String("status", update.Status),
				zap.Int("filled_qty", update.FilledQty),
				zap.Float64("avg_price", update.AvgFillPrice))

			// Full fill
			if IsFilledWSStatus(update.Status) && update.FilledQty >= order.Qty {
				return fillResult{
					action:    actionFilled,
					filledQty: update.FilledQty,
					avgPrice:  update.AvgFillPrice,
					brokerID:  brokerID,
				}
			}

			// Partial fill
			if IsPartialWSStatus(update.Status) && update.FilledQty > 0 {
				return fillResult{
					action:    actionPartialFilled,
					filledQty: update.FilledQty,
					avgPrice:  update.AvgFillPrice,
					brokerID:  brokerID,
				}
			}

			// Rejected — broker actively refused after PLACED. Publish so
			// rules-engine can flip the decision to REJECTED and avoid
			// writing manthan_positions for this signal.
			if IsRejectedWSStatus(update.Status) {
				_ = h.repo.UpdateOrderRejected(ctx, orderID, update.Reason)
				_ = h.repo.InsertEvent(ctx, orderID, "REJECTED", "PLACED", "REJECTED", update.Status, 0, 0, update.Reason)
				if h.eventPub != nil {
					h.eventPub.PublishEntryRejected(ctx, signal, brokerID, update.Reason, "WSS")
				}
				return fillResult{action: actionRejected, reason: update.Reason, brokerID: brokerID}
			}

			// Cancelled (externally)
			if update.Status == "CANCELLED" {
				_ = h.repo.UpdateOrderCancelled(ctx, orderID)
				_ = h.repo.InsertEvent(ctx, orderID, "CANCELLED", "PLACED", "CANCELLED", update.Status, 0, 0, update.Reason)
				return fillResult{action: actionCancelled, reason: update.Reason}
			}

			// Non-terminal (PENDING, OPEN, TRIGGER PENDING) — keep waiting
			continue

		case <-timer.C:
			// Timeout — smart retry
			return h.handleTimeout(ctx, brokerID, auth, order, info, orderID, originalLTP, retry)
		}
	}
}

// handleTimeout decides whether to modify, cancel, or give up after wait timeout.
func (h *EntryHandler) handleTimeout(ctx context.Context, brokerID string, auth BrokerAuth, order *ManthanOrder, info *SymbolInfo, orderID int64, originalLTP float64, retry int) fillResult {

	// First check if order got filled while we were timing out (poll as safety)
	status, filledQty, avgPrice, err := h.broker.GetOrderStatus(ctx, auth, brokerID)
	if err == nil {
		h.logger.Info("Poll orderbook result",
			zap.String("symbol", order.Symbol),
			zap.String("broker_id", brokerID),
			zap.String("status", status),
			zap.Bool("is_filled", isFilledStatus(status)),
			zap.Int("filled_qty", filledQty),
			zap.Int("order_qty", order.Qty),
			zap.Float64("avg_price", avgPrice))

		if isFilledStatus(status) && filledQty >= order.Qty {
			return fillResult{action: actionFilled, filledQty: filledQty, avgPrice: avgPrice}
		}
		if filledQty > 0 && filledQty < order.Qty {
			return fillResult{action: actionPartialFilled, filledQty: filledQty, avgPrice: avgPrice}
		}
	} else if errors.Is(err, indiraClient.ErrAuthExpired) {
		// JWT dead at broker — every subsequent broker call this tick will
		// fail too (modify, cancel, FetchLTP). Skip silently and let the
		// outer retry loop pick up after re-login refreshes the cache.
		h.logger.Info("Poll orderbook skipped — broker session expired",
			zap.String("symbol", order.Symbol))
		notifyAuthExpired(h.authNotif, ctx, order.UserID, "entry-poll")
		return fillResult{action: actionRetry}
	} else {
		h.logger.Warn("Poll orderbook failed", zap.Error(err))
	}

	// Fetch current LTP first — needed both for escalation math and MARKET fallback sizing check.
	currentLTP, ltpErr := h.broker.FetchLTP(ctx, info.ExchangeToken)
	if ltpErr != nil {
		return fillResult{action: actionRetry}
	}

	drift := math.Abs(currentLTP-originalLTP) / originalLTP

	// Max LIMIT retries exhausted — handleTimeout returns actionCancelled with a
	// sentinel reason. The outer entry loop catches this and attempts a MARKET
	// fallback (see waitForFill caller). This keeps handleTimeout focused on
	// LIMIT retry decisions; MARKET is a distinct phase.
	if retry >= h.maxRetries {
		if drift > h.priceThreshold {
			// Price drifted too far — abort entirely, MARKET would overpay
			h.logger.Info("Price drifted past threshold — abandoning entry",
				zap.String("symbol", order.Symbol),
				zap.Float64("drift_pct", drift*100))
			_ = h.broker.CancelOrder(ctx, auth, info, brokerID)
			_ = h.repo.UpdateOrderCancelled(ctx, orderID)
			_ = h.repo.InsertEvent(ctx, orderID, "CANCELLED", "PLACED", "CANCELLED", "",
				0, 0, fmt.Sprintf("drift %.2f%% > threshold %.2f%%", drift*100, h.priceThreshold*100))
			return fillResult{action: actionCancelled, reason: fmt.Sprintf("price drifted %.2f%%", drift*100)}
		}
		// Signal the outer loop to attempt MARKET fallback.
		return fillResult{action: actionCancelled, reason: reasonLimitRetriesExhausted}
	}

	if drift <= h.priceThreshold {
		// Percent-based escalation — 0.2%, 0.4%, 0.6% above LTP per retry.
		// Tick-only bumps were too small on illiquid stocks (KINGFA thin book
		// needed ~0.2% lift to cross ASK; we were bumping ₹0.10 = 0.002%).
		// escalationPct grows so each retry crosses wider spreads.
		escalationPct := 0.002 * float64(retry+1)
		minTickBump := info.TickSize * float64(2+retry)
		priceBump := currentLTP * escalationPct
		if priceBump < minTickBump {
			priceBump = minTickBump
		}
		newPrice := currentLTP + priceBump

		// Skip no-op modify — if new price rounds to same value as current limit,
		// the broker returns EG003 "Duplicate Modification" and wastes a retry.
		roundedNew := roundToTick(newPrice, info.TickSize)
		if roundedNew == order.LimitPrice {
			roundedNew += info.TickSize // bump one extra tick to force a real change
		}

		h.logger.Info("Entry timeout — modifying price",
			zap.String("symbol", order.Symbol),
			zap.Float64("original_ltp", originalLTP),
			zap.Float64("current_ltp", currentLTP),
			zap.Float64("new_price", roundedNew),
			zap.Float64("escalation_pct", escalationPct*100),
			zap.Int("retry", retry+1))

		// Use ModifySLOrder for limit price modification (same API)
		modReq := &ModifyLimitRequest{
			BrokerOrderID: brokerID,
			Info:          info,
			Auth:          auth,
			Qty:           order.Qty,
			NewPrice:      roundedNew,
		}
		if modErr := h.modifyLimitBuy(ctx, modReq); modErr != nil {
			h.logger.Warn("Entry modify failed", zap.Error(modErr))
		}

		_ = h.repo.UpdateOrderStatus(ctx, orderID, StatusPlaced, "", retry+1, "")
		_ = h.repo.InsertEvent(ctx, orderID, "MODIFIED", "PLACED", "PLACED", "",
			roundedNew, 0, fmt.Sprintf("retry %d, drift %.2f%%, escalation %.2f%%", retry+1, drift*100, escalationPct*100))

		return fillResult{action: actionRetry}
	}

	// Price moved too far — cancel
	h.logger.Info("Price drifted — cancelling entry",
		zap.String("symbol", order.Symbol),
		zap.Float64("drift_pct", drift*100))

	_ = h.broker.CancelOrder(ctx, auth, info, brokerID)
	_ = h.repo.UpdateOrderCancelled(ctx, orderID)
	_ = h.repo.InsertEvent(ctx, orderID, "CANCELLED", "PLACED", "CANCELLED", "",
		0, 0, fmt.Sprintf("opportunity missed — price drifted %.2f%%", drift*100))

	return fillResult{action: actionCancelled, reason: fmt.Sprintf("price drifted %.2f%%", drift*100)}
}

func (h *EntryHandler) handleFill(ctx context.Context, orderID int64, result fillResult, signal ManthanSignal, info *SymbolInfo) (int64, error) {
	_ = h.repo.UpdateOrderFilled(ctx, orderID, result.filledQty, result.avgPrice)
	_ = h.repo.InsertEvent(ctx, orderID, "FILL", "PLACED", "FILLED", "",
		result.avgPrice, result.filledQty, "WSS fill confirmed")

	h.logger.Info("LIVE BUY filled",
		zap.String("symbol", signal.Symbol),
		zap.Int("qty", result.filledQty),
		zap.Float64("avg_price", result.avgPrice))

	// Top-up fill: extend the parent SL to cover combined qty instead of
	// placing a brand-new SL_SELL. Without this, every top-up accumulates
	// another broker SL row that the trail-tick can't see.
	if signal.TopUpForSignalID != "" {
		h.slHandler.MergeTopupSL(ctx, orderID, signal, info, result.filledQty, result.avgPrice)
	} else {
		// First-time entry — fresh SL at 20% below actual fill price.
		slTrigger := result.avgPrice * 0.80
		slLimit := slTrigger - SLLimitGap(slTrigger, info.TickSize)
		h.slHandler.PlaceInitialSL(ctx, orderID, signal, info, result.filledQty, slTrigger, slLimit)
	}

	// Publish FILL_CONFIRMED to Kafka — rules-engine overwrites entry with real price
	if h.eventPub != nil {
		h.eventPub.PublishEntryFilled(ctx, signal, result.brokerID, result.avgPrice, int32(result.filledQty), 0)
	}

	return orderID, nil
}

func (h *EntryHandler) handlePartialFill(ctx context.Context, orderID int64, result fillResult, signal ManthanSignal, info *SymbolInfo, auth BrokerAuth, brokerID string) (int64, error) {
	h.logger.Info("Partial fill — cancelling remainder",
		zap.String("symbol", signal.Symbol),
		zap.Int("filled", result.filledQty),
		zap.Int("total", int(signal.Quantity)))

	_ = h.broker.CancelOrder(ctx, auth, info, brokerID)
	_ = h.repo.UpdateOrderFilled(ctx, orderID, result.filledQty, result.avgPrice)
	_ = h.repo.InsertEvent(ctx, orderID, "PARTIAL_FILL", "PLACED", "FILLED", "",
		result.avgPrice, result.filledQty,
		fmt.Sprintf("accepted %d of %d", result.filledQty, signal.Quantity))

	// Top-up partial fills also merge into parent SL — only the actually-filled
	// qty is covered (the rest got cancelled with the remainder).
	if signal.TopUpForSignalID != "" {
		h.slHandler.MergeTopupSL(ctx, orderID, signal, info, result.filledQty, result.avgPrice)
	} else {
		slTrigger := result.avgPrice * 0.80
		slLimit := slTrigger - SLLimitGap(slTrigger, info.TickSize)
		h.slHandler.PlaceInitialSL(ctx, orderID, signal, info, result.filledQty, slTrigger, slLimit)
	}
	return orderID, nil
}

// ModifyLimitRequest holds params for modifying a LIMIT BUY price.
type ModifyLimitRequest struct {
	BrokerOrderID string
	Info          *SymbolInfo
	Auth          BrokerAuth
	Qty           int
	NewPrice      float64
}

func (h *EntryHandler) modifyLimitBuy(ctx context.Context, req *ModifyLimitRequest) error {
	rounded := h.broker.roundAndClamp(req.NewPrice, req.Info.TickSize, req.Info.DPRLower, req.Info.DPRUpper)

	modReq := &modifyBuyRequest{
		ordId:         req.BrokerOrderID,
		symbol:        req.Info.IndiraSymbol,
		exchangeToken: req.Info.ExchangeToken,
		qty:           req.Qty,
		limitPrice:    rounded,
	}
	return h.broker.modifyLimitBuyOrder(ctx, req.Auth, modReq)
}

// modifyBuyRequest is the internal struct for modify.
type modifyBuyRequest struct {
	ordId         string
	symbol        string
	exchangeToken string
	qty           int
	limitPrice    float64
}

func isFilledStatus(status string) bool {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "EXECUTED", "TRADED", "COMPLETE", "FILLED":
		return true
	}
	return false
}
