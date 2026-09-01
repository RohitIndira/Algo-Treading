package manthan

// TODO(orderstatus): moves to orderstatus svc when extracted.
// See docs/orderstatus_service_design.md — this file's WSS-driven / poll-driven
// concern belongs to the status-observation layer, not the placement layer.

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
	"go.uber.org/zap"
)

// Reconciler enforces "Postgres = source of truth" for order state by
// periodically fetching each active user's broker order-book and fixing any
// drift between DB beliefs and broker reality.
//
// Runs every 5 minutes (configurable). For each user's live (non-terminal)
// orders it:
//
//  1. Fetches the broker order-book (reality)
//  2. For each of our DB orders with a broker_order_id:
//     - If broker says Executed/Traded  → UpdateOrderFilled + RECONCILER_FIXED event
//     - If broker says Cancelled/Rejected → UpdateOrderCancelled + RECONCILER_FIXED event
//     - If broker has no such ordId     → log warning (not auto-cancelled:
//     broker may have purged the order at EOD rollover; too risky to flip)
//
// Pairs with SafetyMonitor (2s interval, SL-focused) — together they provide
// fast-reaction SL insurance + slow-reaction state-consistency enforcement.
//
// Real case this would have auto-fixed (2026-04-23 KINGFA):
//
//   - Broker: NZWLI00001G4 Executed tradedQty=2 @ ₹4940.40
//   - DB:     NZWLI00001G4 PLACED filled_qty=0  (handler gave up before fill)
//   - Reconciler: detect drift → UpdateOrderFilled(2, 4940.40) + event
type Reconciler struct {
	broker *BrokerAdapter
	repo   *Repository
	logger *zap.Logger

	pollInterval time.Duration

	// activeUserIDs returns all userIDs that currently have a live strategy.
	activeUserIDs func() []string
	getAuth       func(userID string) *BrokerAuth

	// eventPub publishes RECONCILER_DRIFT_FIX to manthan.execution.events.
	// Optional — nil-safe.
	eventPub *ManthanEventPublisher

	// authNotif publishes SESSION_EXPIRED to manthan.notifications when this
	// loop sees AU004. Optional — nil-safe.
	authNotif AuthExpiryNotifier
}

// SetEventPublisher wires the centralized publisher used to emit
// RECONCILER_DRIFT_FIX whenever the reconciler corrects a DB ↔ broker drift.
func (r *Reconciler) SetEventPublisher(p *ManthanEventPublisher) {
	r.eventPub = p
}

// SetAuthExpiryNotifier wires the SESSION_EXPIRED publisher.
func (r *Reconciler) SetAuthExpiryNotifier(n AuthExpiryNotifier) {
	r.authNotif = n
}

type ReconcilerConfig struct {
	PollInterval time.Duration // default 5min
}

func NewReconciler(
	broker *BrokerAdapter,
	repo *Repository,
	activeUserIDs func() []string,
	getAuth func(userID string) *BrokerAuth,
	logger *zap.Logger,
	cfg ReconcilerConfig,
) *Reconciler {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 5 * time.Minute
	}
	return &Reconciler{
		broker:        broker,
		repo:          repo,
		logger:        logger,
		pollInterval:  cfg.PollInterval,
		activeUserIDs: activeUserIDs,
		getAuth:       getAuth,
	}
}

// Start begins the reconciliation loop. Blocks until ctx is cancelled.
func (r *Reconciler) Start(ctx context.Context) {
	r.logger.Info("Reconciler started (broker ↔ DB truth sync)",
		zap.Duration("interval", r.pollInterval))

	// Run once on startup — catches drift accumulated during any downtime.
	r.reconcileAll(ctx)

	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			r.logger.Info("Reconciler stopped")
			return
		case <-ticker.C:
			r.reconcileAll(ctx)
		}
	}
}

func (r *Reconciler) reconcileAll(ctx context.Context) {
	users := r.activeUserIDs()
	if len(users) == 0 {
		return
	}
	for _, uid := range users {
		auth := r.getAuth(uid)
		if auth == nil {
			continue
		}
		r.reconcileUser(ctx, uid, *auth)
	}
}

func (r *Reconciler) reconcileUser(ctx context.Context, userID string, auth BrokerAuth) {
	// Gate: skip if a recent AU004 marked this user expired (resets when the
	// credentials cache invalidator clears the gate on re-login).
	if authGated(r.authNotif, userID) {
		return
	}
	brokerOrders, err := r.broker.GetOrderBook(ctx, auth)
	if err != nil {
		// AU004 / session-expired: the cached JWT is dead at the broker.
		// Nothing to reconcile until the user re-logs in (the credentials
		// cache invalidator will refresh the auth on the next /auth/credentials
		// POST). Log at info — warn-spamming every 5 min adds no value.
		if errors.Is(err, indiraClient.ErrAuthExpired) {
			r.logger.Info("Reconciler: skipping user — broker session expired (re-login required)",
				zap.String("user", userID))
			notifyAuthExpired(r.authNotif, ctx, userID, "reconciler")
			return
		}
		r.logger.Warn("Reconciler: order-book fetch failed",
			zap.String("user", userID), zap.Error(err))
		return
	}
	brokerByID := make(map[string]*indiraClient.OrderBook, len(brokerOrders))
	for i := range brokerOrders {
		brokerByID[brokerOrders[i].OrdId] = &brokerOrders[i]
	}

	dbOrders, err := r.repo.GetLiveOrdersByUser(ctx, userID)
	if err != nil {
		r.logger.Error("Reconciler: DB live-orders query failed",
			zap.String("user", userID), zap.Error(err))
		return
	}
	if len(dbOrders) == 0 {
		return
	}

	// Broker ids already referenced by some DB row — a converted AMO must
	// never be matched to an order that another row legitimately owns.
	claimed := make(map[string]bool, len(dbOrders))
	for _, o := range dbOrders {
		if o.BrokerOrderID != "" {
			claimed[o.BrokerOrderID] = true
		}
	}

	// AMO pre-pass, NEWEST FIRST: several SL_PLACED AMO rows for one symbol
	// can share qty+trigger across days (yesterday's stale rows plus last
	// night's real one). The row that converted today is the newest, so it
	// must get first claim on the broker order; rows from past sessions are
	// retired instead of being allowed to steal the match.
	handled := make(map[int64]bool)
	fixed := 0
	if len(dbOrders) > 0 {
		sorted := append([]*ManthanOrder(nil), dbOrders...)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i].CreatedAt.After(sorted[j].CreatedAt) })
		ist, _ := time.LoadLocation("Asia/Kolkata")
		if ist == nil {
			ist = time.FixedZone("IST", 5*3600+1800)
		}
		windowStart := armedWindowStart(time.Now().In(ist))
		for _, dbOrd := range sorted {
			if dbOrd.OrderType != OrderTypeSLSellAMO || dbOrd.Status != StatusSLPlaced {
				continue
			}
			if _, ok := brokerByID[dbOrd.BrokerOrderID]; ok {
				continue // still visible under its own id (queued, not yet released)
			}
			if !dbOrd.CreatedAt.IsZero() && dbOrd.CreatedAt.Before(windowStart) {
				reason := "AMO session passed without conversion sync — retired by reconciler"
				if err := r.repo.ExpireStaleAMORow(ctx, dbOrd.ID, reason); err == nil {
					handled[dbOrd.ID] = true
					fixed++
					r.logger.Info("Reconciler: stale AMO row from a past session retired (EXPIRED)",
						zap.String("user", userID), zap.String("symbol", dbOrd.Symbol),
						zap.String("amo_id", dbOrd.BrokerOrderID), zap.Time("created_at", dbOrd.CreatedAt))
				}
				continue
			}
			// (matching happens in the main loop below, in this newest-first
			// order, so the freshest row claims the broker order)
		}
	}

	notFound := 0
	ordered := append([]*ManthanOrder(nil), dbOrders...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].CreatedAt.After(ordered[j].CreatedAt) })
	for _, dbOrd := range ordered {
		if handled[dbOrd.ID] {
			continue
		}
		bOrd, ok := brokerByID[dbOrd.BrokerOrderID]
		if !ok && dbOrd.OrderType == OrderTypeSLSellAMO && dbOrd.Status == StatusSLPlaced {
			// AMO CONVERSION SYNC (the "Phase C" the models always promised):
			// Indira gives a queued AMO a NEW ordId when it releases it at
			// 08:50 IST, so the id we stored can never appear in the book.
			// Match by content instead — same symbol, SELL, stop order, same
			// qty and trigger — and either promote the row to the live SL_SELL
			// under the new id, or record the exchange's REJECTION so the
			// 09:14 cron stops trusting a stop that does not exist
			// (2026-08-19: six S4450 AMOs rejected "invalid data" at
			// conversion; DB said SL_PLACED; cron skipped them all).
			if conv := matchConvertedAMO(dbOrd, brokerOrders, claimed); conv != nil {
				br := strings.ToUpper(strings.TrimSpace(conv.Status))
				claimed[conv.OrdId] = true
				if br == "REJECTED" {
					reason := "AMO rejected at conversion: " + conv.RejReason
					if err := r.repo.MarkAMOConversionRejected(ctx, dbOrd.ID, conv.OrdId, reason); err != nil {
						r.logger.Warn("Reconciler: AMO rejection sync failed", zap.Int64("order_id", dbOrd.ID), zap.Error(err))
					} else {
						_ = r.repo.InsertEvent(ctx, dbOrd.ID, "AMO_CONVERSION_REJECTED", string(dbOrd.Status), "REJECTED",
							conv.Status, conv.TriggerPrice, conv.Qty, reason)
						r.logger.Warn("Reconciler: AMO REJECTED at conversion — row marked REJECTED (position needs a fresh stop)",
							zap.String("user", userID), zap.String("symbol", dbOrd.Symbol),
							zap.String("amo_id", dbOrd.BrokerOrderID), zap.String("broker_id", conv.OrdId),
							zap.Float64("trigger", conv.TriggerPrice), zap.String("reason", conv.RejReason))
						fixed++
					}
					continue
				}
				if err := r.repo.PromoteAMOToLiveSL(ctx, dbOrd.ID, conv.OrdId, conv.Status); err != nil {
					r.logger.Warn("Reconciler: AMO promotion failed", zap.Int64("order_id", dbOrd.ID), zap.Error(err))
					continue
				}
				_ = r.repo.InsertEvent(ctx, dbOrd.ID, "AMO_CONVERTED", string(dbOrd.Status), string(StatusSLPlaced),
					conv.Status, conv.TriggerPrice, conv.Qty, "AMO released as live SL under new broker id "+conv.OrdId)
				r.logger.Info("Reconciler: AMO converted — promoted to live SL_SELL under new broker id",
					zap.String("user", userID), zap.String("symbol", dbOrd.Symbol),
					zap.String("amo_id", dbOrd.BrokerOrderID), zap.String("broker_id", conv.OrdId),
					zap.String("broker_status", conv.Status))
				dbOrd.BrokerOrderID = conv.OrdId
				dbOrd.OrderType = OrderTypeSLSell
				bOrd, ok = conv, true
				fixed++
			}
		}
		if !ok {
			notFound++
			r.logger.Warn("Reconciler: DB order not in broker order-book",
				zap.String("user", userID),
				zap.String("symbol", dbOrd.Symbol),
				zap.String("broker_order_id", dbOrd.BrokerOrderID),
				zap.String("db_status", string(dbOrd.Status)))
			continue
		}
		if r.applyDrift(ctx, dbOrd, bOrd) {
			fixed++
		}
	}

	if fixed > 0 || notFound > 0 {
		r.logger.Info("Reconciler pass complete",
			zap.String("user", userID),
			zap.Int("db_orders_checked", len(dbOrders)),
			zap.Int("drifts_fixed", fixed),
			zap.Int("not_in_broker_book", notFound))
	}
}

// applyDrift compares one DB order to its broker counterpart and fixes any
// observed mismatch. Returns true if a DB write was performed.
func (r *Reconciler) applyDrift(ctx context.Context, db *ManthanOrder, bOrd *indiraClient.OrderBook) bool {
	br := strings.ToUpper(strings.TrimSpace(bOrd.Status))

	// SSOT sync: mirror the broker's actual resting SL trigger/limit into the DB
	// (broker_trigger_price) so manthan_orders always reflects exchange reality,
	// independent of the intended trigger_price. Idempotent UPDATE; not a "fix".
	if db.Status == StatusSLPlaced && bOrd.TriggerPrice > 0 {
		if err := r.repo.UpdateBrokerTrigger(ctx, db.ID, bOrd.TriggerPrice, bOrd.Price); err != nil {
			r.logger.Warn("Reconciler: broker_trigger_price sync failed",
				zap.Int64("order_id", db.ID), zap.Error(err))
		}
	}

	// Drift A: broker says Executed but DB says still-placed
	if isExecutedBrokerStatus(br) && db.Status == StatusPlaced && db.FilledQty < db.Qty {
		tradedQty := bOrd.TradedQty
		if tradedQty <= 0 {
			tradedQty = db.Qty // some broker rows carry 0 tradedQty after EOD consolidation
		}
		// Broker's OrderBook response doesn't expose avg-fill-price; `price`
		// (the submitted limit price, per Indira API doc page 14) is the
		// best approximation for marketable-limit fills. Exchange fills never
		// cross the limit on a BUY, so this is an upper bound.
		avgPrice := bOrd.Price
		_ = r.repo.UpdateOrderFilled(ctx, db.ID, tradedQty, avgPrice)
		_ = r.repo.InsertEvent(ctx, db.ID, "RECONCILER_FIXED", string(db.Status), "FILLED",
			bOrd.Status, avgPrice, tradedQty,
			fmt.Sprintf("reconciler: broker=%s but DB=PLACED → synced (filled=%d @ %.2f)",
				bOrd.Status, tradedQty, avgPrice))
		r.logger.Info("Reconciler fixed order → FILLED",
			zap.Int64("order_id", db.ID),
			zap.String("symbol", db.Symbol),
			zap.String("broker_order_id", db.BrokerOrderID),
			zap.Int("traded_qty", tradedQty),
			zap.Float64("price", avgPrice))
		if r.eventPub != nil {
			if entrySignalID, err := r.repo.GetEntrySignalIDByOrderID(ctx, db.ID); err == nil && entrySignalID != "" {
				r.eventPub.PublishReconcilerDriftFix(ctx, entrySignalID, db.StrategyID, db.UserID,
					db.Symbol, db.BrokerOrderID, bOrd.Status,
					fmt.Sprintf("synced FILLED qty=%d @ %.2f", tradedQty, avgPrice))
			}
		}
		return true
	}

	// Drift B: broker says Cancelled/Rejected but DB says still-placed or sl-placed
	if isCancelledBrokerStatus(br) && (db.Status == StatusPlaced || db.Status == StatusSLPlaced) {
		_ = r.repo.UpdateOrderCancelled(ctx, db.ID)
		rej := strings.TrimSpace(bOrd.RejReason)
		if rej != "" {
			if len(rej) > 200 {
				rej = rej[:200]
			}
			_ = r.repo.AnnotateOrderError(ctx, db.ID, "broker: "+rej)
		}
		_ = r.repo.InsertEvent(ctx, db.ID, "RECONCILER_FIXED", string(db.Status), "CANCELLED",
			bOrd.Status, 0, 0,
			fmt.Sprintf("reconciler: broker=%s but DB=%s → synced%s", bOrd.Status, db.Status,
				func() string { if rej != "" { return " — " + rej }; return "" }()))
		r.logger.Warn("Reconciler fixed order → CANCELLED",
			zap.Int64("order_id", db.ID),
			zap.String("symbol", db.Symbol),
			zap.String("broker_order_id", db.BrokerOrderID),
			zap.String("broker_status", bOrd.Status))
		if r.eventPub != nil {
			if entrySignalID, err := r.repo.GetEntrySignalIDByOrderID(ctx, db.ID); err == nil && entrySignalID != "" {
				r.eventPub.PublishReconcilerDriftFix(ctx, entrySignalID, db.StrategyID, db.UserID,
					db.Symbol, db.BrokerOrderID, bOrd.Status, "synced CANCELLED")
			}
		}
		return true
	}

	return false
}

// matchConvertedAMO finds the broker order that a queued SL_SELL_AMO row
// became at conversion: same base symbol, SELL, a stop-type order, same qty,
// same trigger (2dp), placed TODAY, and not already claimed by another DB
// row. Returns nil when there is no unambiguous match.
func matchConvertedAMO(db *ManthanOrder, book []indiraClient.OrderBook, claimed map[string]bool) *indiraClient.OrderBook {
	want := strings.ToUpper(strings.TrimSpace(db.Symbol))
	var found *indiraClient.OrderBook
	for i := range book {
		o := &book[i]
		if o.OrdId == "" || claimed[o.OrdId] {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(o.OrdAction), "SELL") {
			continue
		}
		ot := strings.ToUpper(o.OrdType)
		if !strings.Contains(ot, "SL") && !strings.Contains(ot, "STOP") {
			continue
		}
		base := strings.ToUpper(strings.TrimSpace(o.Symbol.BaseSym))
		if base == "" {
			base = strings.ToUpper(strings.TrimSpace(o.Symbol.DispSym))
			if i := strings.Index(base, "-"); i > 0 {
				base = base[:i]
			}
		}
		if base != want {
			continue
		}
		if o.Qty != db.Qty || math.Abs(o.TriggerPrice-db.TriggerPrice) > 0.011 {
			continue
		}
		if !placedTodayIST(o.OrdDate) {
			continue
		}
		if found != nil {
			return nil // ambiguous — leave it for a human
		}
		found = o
	}
	return found
}

// placedTodayIST reports whether an order-book date string ("2026-08-19
// 09:15:14" or "19-Aug-2026 09:15:14") falls on today's IST calendar day.
// Unparseable → false (never match on a guess).
func placedTodayIST(ordDate string) bool {
	ist, _ := time.LoadLocation("Asia/Kolkata")
	if ist == nil {
		ist = time.FixedZone("IST", 5*3600+1800)
	}
	s := strings.TrimSpace(ordDate)
	for _, layout := range []string{"2006-01-02 15:04:05", "02-Jan-2006 15:04:05", "2006-01-02", "02-Jan-2006"} {
		if t, err := time.ParseInLocation(layout, s, ist); err == nil {
			n := time.Now().In(ist)
			return t.Year() == n.Year() && t.YearDay() == n.YearDay()
		}
	}
	return false
}

func isExecutedBrokerStatus(s string) bool {
	switch s {
	case "EXECUTED", "TRADED", "COMPLETE", "FILLED":
		return true
	}
	return false
}

func isCancelledBrokerStatus(s string) bool {
	switch s {
	case "CANCELLED", "CANCELED", "REJECTED", "EXPIRED":
		return true
	}
	return false
}
