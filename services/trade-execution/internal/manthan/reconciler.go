package manthan

import (
	"context"
	"errors"
	"fmt"
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
//       broker may have purged the order at EOD rollover; too risky to flip)
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

	fixed, notFound := 0, 0
	for _, dbOrd := range dbOrders {
		bOrd, ok := brokerByID[dbOrd.BrokerOrderID]
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
		_ = r.repo.InsertEvent(ctx, db.ID, "RECONCILER_FIXED", string(db.Status), "CANCELLED",
			bOrd.Status, 0, 0,
			fmt.Sprintf("reconciler: broker=%s but DB=%s → synced", bOrd.Status, db.Status))
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
