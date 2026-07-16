// Package reconciler is Layers 2 + 3 of the truth hierarchy for orderstatus
// svc (see docs/orderstatus_service_design.md and the "truth hierarchy"
// pattern captured with the user 2026-07-10).
//
//	Layer 1 (WSS, real-time)       → internal/wss/listener.go
//	Layer 2 (REST orderbook, 15s)  → this file: catches WSS misses
//	Layer 3 (REST full sweep, 5min)→ this file: startup catch-up + drift
//
// Every observation lands in broker_events via the same store.Writer used
// by Layer 1. Idempotency (UNIQUE(broker_order_id, event_seq)) makes the
// WSS+REST race safe — first writer wins, second no-ops. Publisher fans
// out only NEW rows to order.events.
//
// The business logic that used to live in trade-execution's
// safety_monitor.go (market-sell escalation, DPR-clamp AMO retries, etc.)
// STAYS in trade-execution and consumes order.events. Orderstatus svc is
// pure observation.
package reconciler

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	indira "github.com/RohitIndira/Algo-Treading/pkg/indira"

	"github.com/RohitIndira/Algo-Treading/services/orderstatus/internal/publisher"
	"github.com/RohitIndira/Algo-Treading/services/orderstatus/internal/store"
)

// UserAuthProvider returns the current auth for a userID. Backed by the
// user-config gRPC client in main.go so tokens refreshed on the frontend
// take effect on the next poll (no long-lived AuthContext caches).
type UserAuthProvider func(ctx context.Context, userID string) (*indira.AuthContext, error)

// UserLister returns the current set of userIDs orderstatus svc should
// reconcile. Same source as boot subscription — user-config.
type UserLister func(ctx context.Context) ([]string, error)

// Config holds the two poll intervals.
type Config struct {
	// Layer 2: quick drift check against REST orderbook.
	FastPollInterval time.Duration // default 15s
	// Layer 3: full sweep — orderbook + tradebook + holdings.
	SlowPollInterval time.Duration // default 5m
}

// Reconciler polls broker REST endpoints and appends any observed events
// to broker_events. Same normalisation + fan-out as the WSS listener.
type Reconciler struct {
	broker     *indira.Client
	writer     *store.Writer
	pub        *publisher.Publisher
	authFor    UserAuthProvider
	listUsers  UserLister
	fastPoll   time.Duration
	slowPoll   time.Duration
	logger     *zap.Logger
}

func New(
	broker *indira.Client,
	writer *store.Writer,
	pub *publisher.Publisher,
	authFor UserAuthProvider,
	listUsers UserLister,
	cfg Config,
	logger *zap.Logger,
) *Reconciler {
	if cfg.FastPollInterval <= 0 {
		cfg.FastPollInterval = 15 * time.Second
	}
	if cfg.SlowPollInterval <= 0 {
		cfg.SlowPollInterval = 5 * time.Minute
	}
	return &Reconciler{
		broker:    broker,
		writer:    writer,
		pub:       pub,
		authFor:   authFor,
		listUsers: listUsers,
		fastPoll:  cfg.FastPollInterval,
		slowPoll:  cfg.SlowPollInterval,
		logger:    logger,
	}
}

// Start launches two goroutines — one for Layer 2, one for Layer 3 — and
// runs Layer 3 once immediately to catch up any events missed while the
// service was down.
func (r *Reconciler) Start(ctx context.Context) {
	// Startup catch-up: run a full sweep now, before either loop's first tick.
	// This is the "on startup" path from the truth hierarchy — after a
	// deploy or crash, any events missed while we were down get pulled in
	// via GetOrderBook and INSERT'd (dedupes with any WSS events that later
	// arrive for the same broker_order_id + event_seq).
	go func() {
		bootCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		defer cancel()
		r.logger.Info("reconciler startup catch-up beginning")
		r.pollAll(bootCtx, store.SourceRESTReconciler)
		r.logger.Info("reconciler startup catch-up complete")
	}()

	go r.loop(ctx, r.fastPoll, "layer2-fast", store.SourceRESTOrderbook)
	go r.loop(ctx, r.slowPoll, "layer3-slow", store.SourceRESTReconciler)
}

// loop runs a poll every `interval`. Skips a tick if the previous poll is
// still in flight (safer than piling parallel scans on the broker).
func (r *Reconciler) loop(ctx context.Context, interval time.Duration, name string, source store.Source) {
	r.logger.Info("reconciler loop started",
		zap.String("name", name),
		zap.Duration("interval", interval),
		zap.String("source", string(source)))
	defer r.logger.Info("reconciler loop stopped", zap.String("name", name))

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var busy sync.Mutex
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !busy.TryLock() {
				// Previous poll still running — skip this tick, don't backlog.
				r.logger.Debug("reconciler poll skipped — previous still running",
					zap.String("name", name))
				continue
			}
			go func() {
				defer busy.Unlock()
				pollCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
				defer cancel()
				r.pollAll(pollCtx, source)
			}()
		}
	}
}

// pollAll runs one poll across every active user. Errors on any single user
// are logged but never abort the sweep — one user's stale JWT shouldn't
// starve everyone else's reconciliation.
func (r *Reconciler) pollAll(ctx context.Context, source store.Source) {
	users, err := r.listUsers(ctx)
	if err != nil {
		r.logger.Warn("reconciler user list failed", zap.Error(err))
		return
	}

	var observed, inserted, published, skipped, failed int
	for _, userID := range users {
		auth, err := r.authFor(ctx, userID)
		if err != nil {
			r.logger.Debug("reconciler skip: auth fetch failed",
				zap.String("user_id", userID), zap.Error(err))
			skipped++
			continue
		}
		if auth == nil {
			// NOT_FOUND — user has no cached JWT (never SSO'd or wiped). Silent skip.
			skipped++
			continue
		}
		o, i, p, f := r.pollUser(ctx, userID, auth, source)
		observed += o
		inserted += i
		published += p
		failed += f
	}

	r.logger.Info("reconciler poll pass complete",
		zap.String("source", string(source)),
		zap.Int("users", len(users)),
		zap.Int("orders_observed", observed),
		zap.Int("events_inserted", inserted),
		zap.Int("events_published", published),
		zap.Int("users_skipped", skipped),
		zap.Int("users_failed", failed))
}

// pollUser fetches one user's REST orderbook and INSERTs any new observation.
// Returns (observed, inserted, published, failedUsers).
func (r *Reconciler) pollUser(ctx context.Context, userID string, auth *indira.AuthContext, source store.Source) (int, int, int, int) {
	orders, err := r.broker.GetOrderBook(ctx, auth)
	if err != nil {
		r.logger.Warn("reconciler GetOrderBook failed",
			zap.String("user_id", userID), zap.Error(err))
		return 0, 0, 0, 1
	}
	observed := len(orders)
	if observed == 0 {
		return 0, 0, 0, 0
	}

	var inserted, published int
	for i := range orders {
		ev := r.eventFromOrderBookRow(userID, &orders[i], source)
		if ev == nil {
			continue
		}
		wasInserted, err := r.writer.Insert(ctx, ev)
		if err != nil {
			r.logger.Warn("reconciler insert failed",
				zap.String("user_id", userID),
				zap.String("broker_order_id", ev.BrokerOrderID),
				zap.Error(err))
			continue
		}
		if !wasInserted {
			// Dedup — WSS already got this event. Silent.
			continue
		}
		inserted++
		if r.pub != nil {
			r.pub.Publish(ctx, ev)
			published++
		}
	}
	return observed, inserted, published, 0
}

// eventFromOrderBookRow maps one indira.OrderBook row into a store.Event.
//
// event_seq derivation MUST match the WSS-path derivation so idempotency
// works — same broker event via WSS + REST → same event_seq → ON CONFLICT
// DO NOTHING dedupes.
//
// The REST orderbook doesn't include OMSOrderStatus, MessageSequenceNumber,
// or exchange timestamps in the same shape as WSS. We use OrderNumber
// (exchange order id) as the deterministic seq — it's monotonic per order
// and identical across WSS + REST paths for the same lifecycle event.
func (r *Reconciler) eventFromOrderBookRow(userID string, o *indira.OrderBook, source store.Source) *store.Event {
	brokerOrderID := strings.TrimSpace(o.OrdId)
	if brokerOrderID == "" || brokerOrderID == "0" {
		return nil
	}

	raw, err := json.Marshal(o)
	if err != nil {
		// Should never happen — OrderBook is well-formed JSON in-memory.
		r.logger.Error("reconciler marshal orderbook row failed", zap.Error(err))
		return nil
	}

	// exchOrdId arrives as either a JSON number or string — the raw bytes
	// give us the string form we need. Empty when order hasn't reached the
	// exchange yet (still at broker's OMS).
	exchOrderID := strings.Trim(string(o.ExchOrdId), `"`)
	if exchOrderID == "" || exchOrderID == "null" {
		exchOrderID = ""
	}

	status := strings.ToUpper(strings.TrimSpace(o.Status))
	ordTypeUpper := strings.ToUpper(strings.TrimSpace(o.OrdType))

	// TradedPrice sourcing from the OrderBook API:
	//   - Market / SL-M ordTypes: broker has no user-specified LIMIT to
	//     store in `price`, so it puts the actual traded price there once
	//     the order fills. Confirmed live 2026-07-16 on BECTORFOOD @
	//     ordType=Market, status=Executed, price=188.59 (avg trade).
	//   - Limit / SL / SL-L ordTypes: `price` is the user's LIMIT — the
	//     fill lives in TradeBook (Layer 3) only. Leave TradedPrice=0
	//     so positions svc defers to a later WSS event that carries the
	//     real fill (see services/positions/statemachine/handler.go
	//     handleBuyFill precedence — meta.AvgFillPrice > ev.TradedPrice
	//     > skip).
	var tradedPrice float64
	if isMarketOrdType(ordTypeUpper) && o.TradedQty > 0 && o.Price > 0 {
		tradedPrice = o.Price
	}

	return &store.Event{
		BrokerOrderID:   brokerOrderID,
		ExchangeOrderID: exchOrderID,
		EventSeq:        deriveEventSeq(o, exchOrderID),
		Source:          source,
		EventType:       eventTypeFromStatus(status),
		Status:          status,
		OMSStatusCode:   0, // orderbook doesn't expose OMS code
		UserID:          userID,
		Symbol:          strings.TrimSpace(o.Symbol.Symbol),
		Exchange:        strings.TrimSpace(o.Symbol.Exc),
		BuySell:         strings.TrimSpace(o.OrdAction),
		OrderType:       strings.TrimSpace(o.OrdType),
		Product:         strings.TrimSpace(o.PrdType),
		OrderPrice:      o.Price,
		TriggerPrice:    o.TriggerPrice,
		Quantity:        o.Qty,
		FilledQty:       o.TradedQty,
		TradedPrice:     tradedPrice,
		PendingQty:      o.RemainQty,
		Reason:          strings.TrimSpace(o.RejReason),
		RawPayload:      raw,
		BrokerTsMs:      0, // set when we plumb exch time through
	}
}

// isMarketOrdType returns true for Indira ordType values whose `price`
// field on the OrderBook API carries the AVG TRADED price rather than a
// user LIMIT. Used by the REST reconciler to opportunistically populate
// TradedPrice for Market fills (Limit / SL / SL-L stay 0 — those need
// TradeBook / WSS for the fill price).
//
// Kept as a small case-fold list rather than a regex — Indira sometimes
// returns "Market", sometimes "MARKET", and different SDK versions have
// used "SL-M" vs "SL_MARKET".
func isMarketOrdType(ordType string) bool {
	switch ordType {
	case "MARKET", "MKT", "SL-M", "SL_M", "SL_MARKET":
		return true
	}
	return false
}

// deriveEventSeq matches the WSS-side derivation used in
// internal/wss/listener.go — same lifecycle event via WSS + REST paths
// produces the SAME sequence key → idempotency guarantee holds.
//
// Preference:
//  1. Exchange order id (monotonic per order, present in both paths)
//  2. Fall back: length-of-id + filled_qty (better than 0 collision)
func deriveEventSeq(o *indira.OrderBook, exchOrderID string) int64 {
	if exchOrderID != "" && exchOrderID != "0" {
		// Exchange order numbers are digit-strings — parse as int64.
		// The exchange assigns them monotonically per order, so different
		// events for the same order share the same value. Combine with
		// filled_qty so a fill and a partial-fill get different seqs.
		var n int64
		for _, c := range exchOrderID {
			if c < '0' || c > '9' {
				break
			}
			n = n*10 + int64(c-'0')
		}
		return n*1000 + int64(o.TradedQty)
	}
	return int64(len(o.OrdId))*1000 + int64(o.TradedQty)
}

// eventTypeFromStatus maps REST orderbook Status → store.EventType, matching
// the same 6-value enum the WSS path uses.
func eventTypeFromStatus(status string) store.EventType {
	switch strings.TrimSpace(strings.ToUpper(status)) {
	case "EXECUTED", "TRADED", "FILLED", "COMPLETE":
		return store.EventFilled
	case "CANCELLED":
		return store.EventCancelled
	case "REJECTED", "A.REJECTED", "ORDER ERROR":
		return store.EventRejected
	case "PARTIALLY TRADED", "PARTIALLY EXECUTED":
		return store.EventPartiallyFilled
	case "PENDING", "ADMIN PENDING", "OPEN":
		return store.EventStatusChanged
	default:
		return store.EventStatusChanged
	}
}
