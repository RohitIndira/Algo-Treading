package manthan

// TODO(orderstatus): moves to orderstatus svc when extracted.
// See docs/orderstatus_service_design.md — this file's WSS-driven / poll-driven
// concern belongs to the status-observation layer, not the placement layer.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
)

// ExternalActivityDetector watches the broker's position-book + holdings
// every N minutes and compares them against our manthan_orders view of
// "what we believe we hold". Any divergence is published to
// manthan.execution.events as a MANUAL_*_DETECTED / EXTERNAL_QTY_MISMATCH
// event so the rules-engine projector can flip our state to match reality.
//
// Detection categories:
//   - broker_qty == 0           → MANUAL_EXIT_DETECTED
//   - 0 < broker_qty < db_qty   → MANUAL_PARTIAL_EXIT_DETECTED
//   - broker_qty > db_qty       → MANUAL_BUY_DETECTED  (rare but possible)
//   - broker_qty == db_qty      → no event
//
// In-memory dedup map keeps us from re-publishing the same divergence on
// every tick. Map is cleared on restart — the projector's WHERE-clause
// idempotency takes care of duplicates if we re-detect after restart.
//
// Off by default. Enable via env var MANTHAN_EXTERNAL_DETECTOR_ENABLED=true.
type ExternalActivityDetector struct {
	broker   *BrokerAdapter
	repo     *Repository
	eventPub *ManthanEventPublisher
	logger   *zap.Logger

	getActiveUserIDs func() []string
	getAuth          func(userID string) *BrokerAuth

	pollInterval time.Duration

	// dedup is keyed by signal_id — once we've published a MANUAL_* event
	// for a given signal we don't re-publish until our internal state moves
	// off this signal (i.e., the projector caught up). Avoids spamming the
	// Kafka topic when broker continues to show the divergence.
	dedupMu sync.Mutex
	dedup   map[string]string // signal_id → last published event_type

	// Optional SESSION_EXPIRED publisher — nil-safe.
	authNotif AuthExpiryNotifier
}

// SetAuthExpiryNotifier wires the SESSION_EXPIRED publisher.
func (d *ExternalActivityDetector) SetAuthExpiryNotifier(n AuthExpiryNotifier) {
	d.authNotif = n
}

type ExternalActivityDetectorConfig struct {
	PollInterval time.Duration // default 30 minutes
}

func NewExternalActivityDetector(
	broker *BrokerAdapter,
	repo *Repository,
	eventPub *ManthanEventPublisher,
	getActiveUserIDs func() []string,
	getAuth func(userID string) *BrokerAuth,
	logger *zap.Logger,
	cfg ExternalActivityDetectorConfig,
) *ExternalActivityDetector {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 30 * time.Minute
	}
	return &ExternalActivityDetector{
		broker:           broker,
		repo:             repo,
		eventPub:         eventPub,
		logger:           logger,
		getActiveUserIDs: getActiveUserIDs,
		getAuth:          getAuth,
		pollInterval:     cfg.PollInterval,
		dedup:            make(map[string]string),
	}
}

// Start runs the detector loop. Blocks until ctx is cancelled.
func (d *ExternalActivityDetector) Start(ctx context.Context) {
	d.logger.Info("ExternalActivityDetector started — watching for manual user activity",
		zap.Duration("interval", d.pollInterval))

	// Initial run shortly after startup so we don't have to wait the full
	// poll interval to catch any drift accumulated during downtime.
	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return
	case <-timer.C:
	}
	d.detectAll(ctx)

	ticker := time.NewTicker(d.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			d.logger.Info("ExternalActivityDetector stopped")
			return
		case <-ticker.C:
			d.detectAll(ctx)
		}
	}
}

func (d *ExternalActivityDetector) detectAll(ctx context.Context) {
	users := d.getActiveUserIDs()
	if len(users) == 0 {
		return
	}
	for _, uid := range users {
		auth := d.getAuth(uid)
		if auth == nil {
			d.logger.Debug("ExternalActivityDetector: skipping user without auth",
				zap.String("user", uid))
			continue
		}
		d.detectUser(ctx, uid, *auth)
	}
}

// detectUser is the production-grade replacement for the previous naive
// position-book qty comparison. The naive approach was a known false-
// positive generator because broker.position-book.netQty conflates user's
// separate-book activity (e.g. selling shares from yesterday's holdings)
// with actual exits of our algo's shares.
//
// The new approach uses TWO sources, with broker-side time-ordering as
// the disambiguator:
//
//  1. broker order-book — find SELL orders for the symbol that are
//     EXECUTED, occurred AFTER our entry's fill timestamp, and whose
//     broker_order_id is NOT in our manthan_orders set. Any such order
//     is a third-party (manual) sell that targeted our position.
//
//  2. effective-qty cross-check — if no third-party sell is found, the
//     algo position is intact regardless of broker.netQty being negative
//     (user trading separate book) or lower than db_qty (settled holdings
//     not yet visible).
//
// Edge cases handled correctly:
//   - User had 4 KINGFA in holdings, sold 4 today, algo bought 1 today
//     → broker netQty=-3 → naive flagged false-positive
//     → new: SELL is timestamped BEFORE algo BUY → SKIP correctly
//   - User actually exits algo's share by selling
//     → SELL is AFTER algo BUY, broker_order_id we don't own
//     → new: FLAG correctly
//   - User adds extra shares of same symbol manually (third-party BUY)
//     → SELL absent → SKIP. The MANUAL_BUY case is no longer flagged as
//     a position issue (algo book is unaffected).
func (d *ExternalActivityDetector) detectUser(ctx context.Context, userID string, auth BrokerAuth) {
	// 1. What we believe we hold for this user.
	entries, err := d.repo.GetLiveEntriesByUser(ctx, userID)
	if err != nil {
		d.logger.Warn("ExternalActivityDetector: GetLiveEntriesByUser failed",
			zap.String("user", userID), zap.Error(err))
		return
	}
	if len(entries) == 0 {
		return
	}

	// 2. Broker order-book: the source of truth for "what trades happened".
	brokerOrders, err := d.broker.client.GetOrderBook(ctx, d.broker.toIndiraAuth(auth))
	if err != nil {
		// Broker session expired — wait for the user to re-login. Quiet log
		// since the reconciler already surfaces the same condition.
		if errors.Is(err, indiraClient.ErrAuthExpired) {
			d.logger.Info("ExternalActivityDetector: skipping user — broker session expired",
				zap.String("user", userID))
			notifyAuthExpired(d.authNotif, ctx, userID, "external-activity")
			return
		}
		d.logger.Warn("ExternalActivityDetector: order-book fetch failed",
			zap.String("user", userID), zap.Error(err))
		return
	}

	// 3. Set of broker_order_ids WE placed — anything else is third-party.
	ourBrokerIDs, err := d.repo.ListOurBrokerOrderIDsForUser(ctx, userID)
	if err != nil {
		d.logger.Warn("ExternalActivityDetector: ListOurBrokerOrderIDsForUser failed",
			zap.String("user", userID), zap.Error(err))
		return
	}

	// 4. Per-entry: scan order-book for third-party SELLs after our buy.
	mismatches := 0
	for _, e := range entries {
		entryFilledAt, err := d.repo.GetEntryFilledAt(ctx, e.SignalID)
		if err != nil {
			d.logger.Debug("ExternalActivityDetector: GetEntryFilledAt failed — skipping entry",
				zap.String("signal_id", e.SignalID), zap.Error(err))
			continue
		}
		if entryFilledAt.IsZero() {
			// Pre-CQRS legacy entry without a recorded fill_at; can't
			// time-order. Skip — reconciler will catch any drift slowly.
			continue
		}

		thirdPartySell, soldQty := findThirdPartySellAfter(
			brokerOrders, e.Symbol, entryFilledAt, ourBrokerIDs)
		if thirdPartySell == "" {
			// No external SELL after our buy → algo position intact.
			d.clearDedup(e.SignalID)
			continue
		}

		// We found a SELL for our symbol, executed after we bought, that we
		// didn't place. That's a genuine third-party exit.
		if soldQty >= e.FilledQty {
			d.publishOnce(ctx, e, "MANUAL_EXIT_DETECTED",
				fmt.Sprintf("third-party SELL %s for %d shares of %s executed after our buy — user exited via broker app/web",
					thirdPartySell, soldQty, e.Symbol))
		} else {
			d.publishOnce(ctx, e, "MANUAL_PARTIAL_EXIT_DETECTED",
				fmt.Sprintf("third-party SELL %s for %d shares of %s (we hold %d) — partial exit",
					thirdPartySell, soldQty, e.Symbol, e.FilledQty))
		}
		mismatches++
	}

	if mismatches > 0 {
		d.logger.Info("ExternalActivityDetector pass complete",
			zap.String("user", userID),
			zap.Int("entries_checked", len(entries)),
			zap.Int("mismatches_found", mismatches))
	}
}

// findThirdPartySellAfter scans the broker order-book for a SELL on
// `symbol` that:
//   - is EXECUTED (not Pending / Cancelled / Rejected)
//   - was traded AFTER `afterTime` (entry fill timestamp)
//   - has a broker_order_id we did NOT place (not in `ourIDs`)
//
// Returns the broker_order_id of the matching SELL and its tradedQty, or
// ("", 0) if no such order exists. If multiple match, returns the largest
// tradedQty so the caller can decide full vs partial exit.
func findThirdPartySellAfter(
	orders []indiraClient.OrderBook,
	symbol string,
	afterTime time.Time,
	ourIDs map[string]bool,
) (string, int) {
	target := strings.ToUpper(strings.TrimSpace(symbol))
	bestID := ""
	bestQty := 0
	for _, o := range orders {
		oSym := strings.ToUpper(strings.TrimSpace(o.Symbol.DispSym))
		if oSym != target {
			continue
		}
		if !strings.EqualFold(o.OrdAction, "SELL") {
			continue
		}
		if !isExecutedStatus(o.Status) {
			continue
		}
		if ourIDs[o.OrdId] {
			// We placed it (entry SL fire / manual algo exit / etc.) —
			// not a third-party action.
			continue
		}
		// Time check: the broker SELL must have happened AFTER our buy.
		// excOrdTime is the exchange-confirmed time on the broker side.
		t := parseBrokerExchTime(o.ExcOrdTime)
		if t.IsZero() {
			t = parseBrokerExchTime(o.OrdDate) // fallback
		}
		if !t.IsZero() && !t.After(afterTime) {
			// Pre-existing SELL (e.g. user sold from holdings earlier today
			// before algo bought) — not against our algo's share.
			continue
		}
		if o.TradedQty > bestQty {
			bestID = o.OrdId
			bestQty = o.TradedQty
		}
	}
	return bestID, bestQty
}

func isExecutedStatus(s string) bool {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "EXECUTED", "TRADED", "COMPLETE", "FILLED":
		return true
	}
	return false
}

// parseBrokerExchTime parses Indira's "2026-04-28 12:18:00" timestamps
// (IST). Returns zero time if the input is empty or unparseable, leaving
// the caller to handle it conservatively.
func parseBrokerExchTime(s string) time.Time {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}
	}
	ist, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		ist = time.FixedZone("IST", 5*3600+30*60)
	}
	layouts := []string{
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
		"02-Jan-2006 15:04:05",
	}
	for _, l := range layouts {
		if t, err := time.ParseInLocation(l, s, ist); err == nil {
			return t
		}
	}
	return time.Time{}
}

// publishOnce emits the event only if we haven't already published the same
// event_type for this signal_id since startup (or since the last time the
// broker matched our state).
func (d *ExternalActivityDetector) publishOnce(ctx context.Context, e LiveEntry, eventType, reason string) {
	if e.SignalID == "" {
		// Pre-CQRS rows without signal_id can't be addressed by the projector.
		// Log for visibility but skip publish — the reconciler will handle them
		// via its own slower path.
		d.logger.Warn("ExternalActivityDetector: skipping mismatch with no signal_id",
			zap.String("symbol", e.Symbol),
			zap.String("user", e.UserID),
			zap.String("reason", reason))
		return
	}

	d.dedupMu.Lock()
	last, seen := d.dedup[e.SignalID]
	d.dedupMu.Unlock()
	if seen && last == eventType {
		return // already published this divergence flavour
	}

	if d.eventPub == nil {
		return
	}

	d.logger.Warn("ExternalActivityDetector: divergence detected — publishing event",
		zap.String("type", eventType),
		zap.String("user", e.UserID),
		zap.String("strategy", e.StrategyID),
		zap.String("symbol", e.Symbol),
		zap.String("signal_id", e.SignalID),
		zap.String("reason", reason))

	// We use the existing PublishReconcilerDriftFix shape but override the
	// event type to MANUAL_*. The publisher's generic envelope carries the
	// fields we need (signal_id, strategy/user/symbol, rejection_reason).
	d.eventPub.publish(ctx, EventEnvelope{
		Type:            eventType,
		SignalID:        e.SignalID,
		EventSeq:        time.Now().UnixMicro(),
		Source:          "API_POLLER",
		StrategyID:      e.StrategyID,
		UserID:          e.UserID,
		Symbol:          e.Symbol,
		ExpectedQty:     int32(e.FilledQty),
		RejectionReason: reason,
	})

	d.dedupMu.Lock()
	d.dedup[e.SignalID] = eventType
	d.dedupMu.Unlock()

	// Side effect: when the user has fully exited, our SL order at broker is
	// now an orphan — selling shares we no longer own. Indira will eventually
	// auto-cancel it (because the underlying is gone) but proactively
	// cancelling avoids a confusing dangling row in the broker order-book.
	//
	// We do this only on FULL exit. On partial exit, broker auto-shrinks our
	// SL qty to match remaining shares, so no action needed.
	if eventType == "MANUAL_EXIT_DETECTED" {
		d.cancelOrphanSL(ctx, e)
	}
}

// cancelOrphanSL finds the active SL order tied to this entry and cancels
// it at the broker. Best-effort: if the SL is already gone (auto-cancelled
// by broker on user's manual sell) we just log and move on.
func (d *ExternalActivityDetector) cancelOrphanSL(ctx context.Context, e LiveEntry) {
	sl, err := d.repo.GetActiveSLByEntrySignalID(ctx, e.SignalID)
	if err != nil {
		d.logger.Warn("cancelOrphanSL: SL lookup failed",
			zap.String("signal_id", e.SignalID), zap.Error(err))
		return
	}
	if sl == nil || sl.BrokerOrderID == "" {
		// No active SL — broker likely auto-cancelled it already when user
		// sold the underlying shares. Nothing to do.
		return
	}

	auth := d.getAuth(e.UserID)
	if auth == nil {
		d.logger.Warn("cancelOrphanSL: no auth available for user",
			zap.String("user", e.UserID))
		return
	}

	info := &SymbolInfo{
		Symbol:        sl.Symbol,
		IndiraSymbol:  sl.IndiraSymbol,
		ExchangeToken: sl.ExchangeToken,
		Exchange:      "NSE",
	}

	d.logger.Info("cancelOrphanSL: cancelling dangling SL after manual exit",
		zap.String("symbol", sl.Symbol),
		zap.String("sl_broker_id", sl.BrokerOrderID),
		zap.String("entry_signal_id", e.SignalID))

	cancelErr := d.broker.CancelOrder(ctx, *auth, info, sl.BrokerOrderID)
	if cancelErr != nil {
		// Treat "already cancelled / not found" as success — broker beat us
		// to it. Anything else worth flagging.
		errStr := strings.ToLower(cancelErr.Error())
		if strings.Contains(errStr, "already") ||
			strings.Contains(errStr, "not found") ||
			strings.Contains(errStr, "cancelled") {
			d.logger.Info("cancelOrphanSL: broker had already cancelled the SL",
				zap.String("sl_broker_id", sl.BrokerOrderID))
		} else {
			d.logger.Warn("cancelOrphanSL: broker CancelOrder failed",
				zap.String("sl_broker_id", sl.BrokerOrderID), zap.Error(cancelErr))
			return
		}
	}

	// Update our DB to reflect the cancel + log an event for the audit trail.
	if err := d.repo.UpdateOrderCancelled(ctx, sl.ID); err != nil {
		d.logger.Warn("cancelOrphanSL: DB cancel update failed",
			zap.Int64("sl_order_id", sl.ID), zap.Error(err))
	}
	_ = d.repo.InsertEvent(ctx, sl.ID, "ORPHAN_SL_CANCELLED", "SL_PLACED", "CANCELLED",
		"", 0, sl.Qty,
		"orphan SL cancelled after detected manual exit on entry "+e.SignalID)
}

// clearDedup is called when broker state matches our DB — it lets a future
// divergence on the same signal trigger a fresh publish.
func (d *ExternalActivityDetector) clearDedup(signalID string) {
	if signalID == "" {
		return
	}
	d.dedupMu.Lock()
	delete(d.dedup, signalID)
	d.dedupMu.Unlock()
}
