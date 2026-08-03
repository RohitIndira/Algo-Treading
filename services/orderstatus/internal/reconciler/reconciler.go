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
	"errors"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	indira "github.com/RohitIndira/Algo-Treading/pkg/indira"

	"github.com/RohitIndira/Algo-Treading/services/orderstatus/internal/publisher"
	"github.com/RohitIndira/Algo-Treading/services/orderstatus/internal/store"
)

// Tradebook sweep fires once per trading day at this IST wall-clock time
// (Layer 3b — post-market fill-price truth). 15:31 IST is just after the 15:30
// close so the exchange tradebook is settled for the day.
const (
	tradebookHour   = 15
	tradebookMinute = 31
	istTimezone     = "Asia/Kolkata"
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

	// cutoff — Indira orders with ordDate STRICTLY BEFORE this timestamp are
	// skipped by eventFromOrderBookRow. Set once in Start() from the greater
	// of (MAX(observed_at) in broker_events, time.Now() at boot). Prevents
	// the reconciler's startup sweep from re-emitting the entire Indira
	// orderbook after a DB flush — the 2026-07-17 postmortem.
	cutoff time.Time
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
//
// Before launching, computes r.cutoff so eventFromOrderBookRow can skip
// pre-boot Indira orders. Resume-friendly semantics:
//
//   - broker_events has rows (normal restart / crash recovery)  →
//     cutoff = MAX(observed_at). Orders placed during a crash window are
//     STILL re-observed; only orders older than our last recorded
//     observation get skipped.
//   - broker_events is EMPTY (fresh install / test-data flush)  →
//     cutoff = time.Now(). Historical Indira orderbook stays out of Kafka.
//
// See docs/orderstatus_service_design.md § "boot cutoff" for the sequence
// diagram + the 2026-07-17 incident that drove this.
func (r *Reconciler) Start(ctx context.Context) {
	r.cutoff = r.computeCutoff(ctx)
	r.logger.Info("reconciler boot cutoff computed",
		zap.Time("cutoff", r.cutoff),
		zap.String("note", "Indira orders with ordDate < cutoff will NOT be re-emitted"))

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

	// Layer 3b — post-market tradebook sweep at 15:31 IST on trading days.
	go r.tradebookLoop(ctx)
}

// computeCutoff picks the boot cutoff — see Start() docstring.
// Query-error fallback is time.Now(): if we can't read broker_events we
// treat the world as fresh, which is the SAFER behavior (no re-emission).
func (r *Reconciler) computeCutoff(ctx context.Context) time.Time {
	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	latest, err := r.writer.LatestObservedAt(queryCtx)
	if err != nil {
		r.logger.Warn("reconciler cutoff query failed — defaulting to time.Now (no historical re-emit)",
			zap.Error(err))
		return time.Now()
	}
	if !latest.Valid {
		r.logger.Info("reconciler cutoff: broker_events is empty — fresh install; cutoff = time.Now",
			zap.Time("cutoff", time.Now()))
		return time.Now()
	}
	r.logger.Info("reconciler cutoff: resuming from last observed event",
		zap.Time("cutoff", latest.Time))
	return latest.Time
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
		// Distinguish AU004 (surfaced as ErrAuthExpired by pkg/indira's
		// doRequest, both 200-body and 401) from other failures, matching the
		// tradebook path so a log grep for "auth expired" catches both REST
		// poll paths.
		if errors.Is(err, indira.ErrAuthExpired) {
			r.logger.Warn("orderbook poll: broker auth expired — skipping user",
				zap.String("user_id", userID))
		} else {
			r.logger.Warn("reconciler GetOrderBook failed",
				zap.String("user_id", userID), zap.Error(err))
		}
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
		id, wasInserted, err := r.writer.Insert(ctx, ev)
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
		// Stamp published_at only on a confirmed publish; a failed inline
		// publish leaves the row for the durability worker to retry.
		if r.pub != nil && r.pub.Publish(ctx, ev) {
			if err := r.writer.MarkPublished(ctx, id); err != nil {
				r.logger.Warn("reconciler mark published failed — durability worker will retry",
					zap.String("broker_order_id", ev.BrokerOrderID),
					zap.Int64("id", id), zap.Error(err))
			}
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

	// Boot-cutoff filter — skip Indira orders older than r.cutoff so a fresh
	// install (empty broker_events) doesn't re-publish the entire day's
	// history to Kafka. See Start()'s docstring for the resume semantics.
	// Parse errors → include the row (safer to observe than to drop).
	if !r.cutoff.IsZero() && o.OrdDate != "" {
		if ordTime, err := parseIndiraOrdDate(o.OrdDate); err == nil {
			if ordTime.Before(r.cutoff) {
				return nil
			}
		}
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

// parseIndiraOrdDate parses Indira's ordDate string into a time.Time.
// Format observed live: "2026-07-17 12:07:56" — no timezone info, but
// Indira runs India-only so we treat the wall clock as Asia/Kolkata.
//
// Used by the boot-cutoff filter in eventFromOrderBookRow. Falls back
// to Asia/Kolkata via time.LoadLocation; if tzdata isn't linked into
// the binary the parse still succeeds via time.UTC and the cutoff
// comparison stays correct within a 5.5-hour ambiguity — sufficient
// for a day-boundary filter (worst case: a few overnight orders
// re-observed once — the broker_events UNIQUE dedup handles it).
func parseIndiraOrdDate(s string) (time.Time, error) {
	const layout = "2006-01-02 15:04:05"
	loc, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		loc = time.UTC
	}
	return time.ParseInLocation(layout, strings.TrimSpace(s), loc)
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

// ── Layer 3b: post-market tradebook sweep ──────────────────────────────────
//
// The tradebook is the ONLY source that carries the authoritative fill price
// for every trade — the orderbook reports TradedPrice=0 for LIMIT/SL fills and
// WSS drops AMO/manual fills entirely. Each tradebook row becomes a FILLED
// broker_event keyed on ExchTrdId (the exchange trade id).
//
// event_seq = ExchTrdId (per-trade, distinct). This deliberately does NOT
// dedup with the WSS/orderbook FILLED for the same fill — cross-source seq
// unification is a follow-up PR. The accepted consequence (multi-fill manual
// sells double-counting) is documented in services/orderstatus/docs/known_issues.md.

// tradebookLoop fires the sweep once per trading day at 15:31 IST. It uses a
// 1-minute ticker + a lastFiredDate guard so it runs exactly once even though
// the target minute is hit by multiple ticks.
func (r *Reconciler) tradebookLoop(ctx context.Context) {
	loc, err := time.LoadLocation(istTimezone)
	if err != nil {
		r.logger.Warn("tradebook loop: could not load IST — using fixed +5:30 offset", zap.Error(err))
		loc = time.FixedZone("IST", 5*60*60+30*60)
	}

	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	var lastFiredDate string
	r.logger.Info("tradebook loop started",
		zap.Int("hour", tradebookHour), zap.Int("minute", tradebookMinute))
	defer r.logger.Info("tradebook loop stopped")

	for {
		select {
		case <-ctx.Done():
			return
		case t := <-ticker.C:
			now := t.In(loc)
			if now.Hour() != tradebookHour || now.Minute() != tradebookMinute {
				continue
			}
			today := now.Format("2006-01-02")
			if lastFiredDate == today {
				continue
			}
			lastFiredDate = today

			if !indira.IsTradingDay(now) {
				r.logger.Info("tradebook sweep skipped — non-trading day", zap.String("date", today))
				continue
			}

			sweepCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
			r.tradebookSweep(sweepCtx)
			cancel()
		}
	}
}

// RunTradebookSweepOnce computes the boot cutoff and runs a single tradebook
// sweep immediately, bypassing the 15:31 IST schedule. For ops/verification
// (trigger a sweep on demand) — the scheduled path uses tradebookLoop.
func (r *Reconciler) RunTradebookSweepOnce(ctx context.Context) {
	if r.cutoff.IsZero() {
		r.cutoff = r.computeCutoff(ctx)
	}
	r.tradebookSweep(ctx)
}

// tradebookSweep pulls each active user's tradebook and appends any new FILLED
// events. One user's auth failure never aborts the sweep.
func (r *Reconciler) tradebookSweep(ctx context.Context) {
	users, err := r.listUsers(ctx)
	if err != nil {
		r.logger.Warn("tradebook sweep: user list failed", zap.Error(err))
		return
	}

	var observed, inserted, published, collided, preCutoff, skipped, usersFailed int
	for _, userID := range users {
		auth, err := r.authFor(ctx, userID)
		if err != nil || auth == nil {
			continue
		}
		trades, err := r.broker.GetTradeBook(ctx, auth)
		if err != nil {
			// Fix 4 (AU004 read-path visibility) will make this louder + add
			// refresh; for now, distinguish auth-expiry and skip the user
			// without killing the sweep.
			if errors.Is(err, indira.ErrAuthExpired) {
				r.logger.Warn("tradebook sweep: broker auth expired — skipping user (Fix 4 will add refresh)",
					zap.String("user_id", userID))
			} else {
				r.logger.Warn("tradebook GetTradeBook failed",
					zap.String("user_id", userID), zap.Error(err))
			}
			usersFailed++
			continue
		}
		o, i, p, c, pc, s := r.tradebookSweepForTrades(ctx, userID, trades)
		observed += o
		inserted += i
		published += p
		collided += c
		preCutoff += pc
		skipped += s
	}

	r.logger.Info("tradebook sweep complete",
		zap.Int("users", len(users)),
		zap.Int("trades_observed", observed),
		zap.Int("events_inserted", inserted),
		zap.Int("events_published", published),
		zap.Int("cross_source_collisions", collided),
		zap.Int("pre_cutoff_filtered", preCutoff),
		zap.Int("other_skipped", skipped),
		zap.Int("users_failed", usersFailed))
}

// tradebookSweepForTrades processes one user's tradebook rows. Split out from
// the broker call so it's unit-testable with a fabricated []TradeBook.
// Returns (observed, inserted, published, collided, preCutoff, otherSkipped).
func (r *Reconciler) tradebookSweepForTrades(ctx context.Context, userID string, trades []indira.TradeBook) (int, int, int, int, int, int) {
	var observed, inserted, published, collided, preCutoff, skipped int
	for i := range trades {
		observed++
		ev, seq, reason := r.eventFromTradeBookRow(userID, &trades[i])
		if ev == nil {
			switch reason {
			case "pre_cutoff":
				preCutoff++
			default:
				skipped++
			}
			continue
		}
		ins, coll, pub := r.observeTradeEvent(ctx, ev, seq)
		if coll {
			collided++
		}
		if ins {
			inserted++
		}
		if pub {
			published++
		}
	}
	if preCutoff > 0 {
		r.logger.Debug("tradebook: pre-cutoff trades filtered",
			zap.String("user_id", userID), zap.Int("count", preCutoff))
	}
	return observed, inserted, published, collided, preCutoff, skipped
}

// eventFromTradeBookRow maps one tradebook row into a store.Event. Returns
// (nil, seq, reason) to skip: reason is "empty_order_id", "bad_exch_trd_id",
// "pre_cutoff", or "marshal_failed".
func (r *Reconciler) eventFromTradeBookRow(userID string, t *indira.TradeBook) (*store.Event, int64, string) {
	brokerOrderID := strings.TrimSpace(t.OrdId)
	if brokerOrderID == "" || brokerOrderID == "0" {
		return nil, 0, "empty_order_id"
	}

	// event_seq = ExchTrdId, parsed exactly (fails loudly on overflow/garbage
	// rather than truncating — see pkg/indira TradeBook.ExchTradeID).
	seq, err := t.ExchTradeID()
	if err != nil {
		r.logger.Warn("tradebook row has unusable exchTrdId — skipping trade",
			zap.String("user_id", userID),
			zap.String("broker_order_id", brokerOrderID),
			zap.Error(err))
		return nil, 0, "bad_exch_trd_id"
	}

	// Boot-cutoff guard — reuse the SAME r.cutoff computed in Start() so a
	// fresh install / DB flush never re-emits the whole day's tradebook. Parse
	// TradeTime as IST (verified: broker portfolio-services naive timestamps
	// are IST, matching observed_at within poll latency). BrokerTsMs is stamped
	// from the same value — the tradebook is the first path to populate it.
	var brokerTsMs int64
	if t.TradeTime != "" {
		if tt, perr := parseIndiraOrdDate(t.TradeTime); perr == nil {
			brokerTsMs = tt.UnixMilli()
			if !r.cutoff.IsZero() && tt.Before(r.cutoff) {
				return nil, seq, "pre_cutoff"
			}
		}
	}

	raw, err := json.Marshal(t)
	if err != nil {
		r.logger.Error("tradebook marshal row failed", zap.Error(err))
		return nil, seq, "marshal_failed"
	}

	return &store.Event{
		BrokerOrderID:   brokerOrderID,
		ExchangeOrderID: strings.TrimSpace(t.ExchOrdId),
		EventSeq:        seq,
		Source:          store.SourceRESTTradebook,
		EventType:       store.EventFilled,
		Status:          "EXECUTED",
		UserID:          userID,
		// Symbol/Exchange intentionally left empty: the tradebook Symbol is a
		// complex broker object with no confirmed shape, and inventing a parse
		// is disallowed. Manthan dedup is by broker_order_id / signal_id, which
		// don't need symbol; only manual-sell FIFO does — the documented
		// known-issue path. Symbol enrichment is deferred until we capture a
		// real tradebook Symbol sample.
		BuySell:     strings.TrimSpace(t.OrdAction),
		OrderType:   strings.TrimSpace(t.OrdType),
		Product:     strings.TrimSpace(t.PrdType),
		Quantity:    t.Qty,
		FilledQty:   t.TradedQty,
		TradedPrice: t.TradedPrice,
		PendingQty:  t.RemainQty,
		RawPayload:  raw,
		BrokerTsMs:  brokerTsMs,
	}, seq, ""
}

// observeTradeEvent inserts one tradebook event, publishes inline on a fresh
// insert (durability worker retries failures), and emits the loud
// cross-source-collision WARN when the ExchTrdId seq already exists.
// Returns (inserted, collided, published).
func (r *Reconciler) observeTradeEvent(ctx context.Context, ev *store.Event, proposedSeq int64) (bool, bool, bool) {
	id, inserted, err := r.writer.Insert(ctx, ev)
	if err != nil {
		r.logger.Warn("tradebook insert failed",
			zap.String("broker_order_id", ev.BrokerOrderID),
			zap.Int64("event_seq", proposedSeq), zap.Error(err))
		return false, false, false
	}

	if !inserted {
		// Cross-source collision: ExchTrdId equalled an existing WSS
		// (MessageSequenceNumber) or orderbook (exchOrdId*1000+qty) event_seq
		// at this broker_order_id — the "negligible coincidence" we accepted in
		// the design. NEVER silent: a silent no-op is what corrupted data in the
		// 2026-07-17 incident. Surface the existing row so ops can tell a
		// harmless coincidence from a real seq-scheme bug.
		fields := []zap.Field{
			zap.String("source", string(store.SourceRESTTradebook)),
			zap.String("broker_order_id", ev.BrokerOrderID),
			zap.Int64("tradebook_event_seq", proposedSeq),
		}
		if existing, lerr := r.writer.LookupConflictSource(ctx, ev.BrokerOrderID, proposedSeq); lerr == nil && existing != nil {
			fields = append(fields,
				zap.String("existing_source", existing.Source),
				zap.Int64("existing_event_seq", existing.EventSeq),
				zap.String("existing_event_type", existing.EventTyp))
		}
		r.logger.Warn("tradebook event_seq collided with an existing broker_events row — dropped as duplicate (expected-rare cross-source coincidence; investigate if frequent — see docs/known_issues.md)", fields...)
		return false, true, false
	}

	published := false
	if r.pub != nil && r.pub.Publish(ctx, ev) {
		if err := r.writer.MarkPublished(ctx, id); err != nil {
			r.logger.Warn("tradebook mark published failed — durability worker will retry",
				zap.String("broker_order_id", ev.BrokerOrderID),
				zap.Int64("id", id), zap.Error(err))
		}
		published = true
	}
	return true, false, published
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
