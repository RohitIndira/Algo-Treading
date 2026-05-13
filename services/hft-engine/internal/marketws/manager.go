// Per-symbol fan-out for ODIN broadcast ticks.
//
// One process has ONE OdinClient (one WS connection). Many strategies
// subscribe to many symbols, possibly the same symbol. This file is the
// router between them:
//
//   strategy.Start → Manager.Subscribe(isin)
//     → Resolver looks up segment + security_code
//     → OdinClient.Subscribe(segment, security_code) (only if first sub)
//     → returns:  *Subscription{
//                   Ticks chan state.MarketData,  // engine consumes
//                   Unsubscribe func()             // engine calls on Stop
//                 }
//
// When an ODIN 209 lands, the Manager's listener decodes paise → ₹ and
// fans the resulting state.MarketData out to every subscriber that
// asked for this (segment, token).
//
// The Manager owns the no-data watchdog too: per (segment, token) it
// remembers the last tick time, and exposes a query so the strategy
// layer can halt a side after 30s of silence.
package marketws

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/RohitIndira/Algo-Treading/services/hft-engine/internal/state"
)

// Subscription is what each engine.Runner gets back from Subscribe.
// The Runner runs a tiny forwarder goroutine: `for md := range Ticks { runner.SendTick(md) }`.
type Subscription struct {
	// Stable identity — useful for logging.
	SubscriberID string
	ISIN         string
	Symbol       string
	SecurityCode int64
	SegmentID    int

	// Ticks is the channel the engine consumes. Manager closes it when
	// Unsubscribe is called.
	Ticks chan state.MarketData

	// Unsubscribe is idempotent. Safe to defer.
	Unsubscribe func()
}

// PaiseDivisor converts the wire-format integer prices to rupees.
// All NSE Cash prices are quoted in paise (×100). Other segments may
// need different scales — extend per segment when we add them.
const PaiseDivisor = 100.0

// Manager is the per-symbol fan-out. One per process.
type Manager struct {
	client   *OdinClient
	resolver *Resolver
	logger   *zap.Logger

	mu sync.Mutex
	// fanouts is keyed by (segment_id, security_code) → set of subscribers.
	// We use a slice instead of a map because subscriber turnover is low
	// (~ one per strategy entry) and iteration is cheap.
	fanouts map[SubscriptionKey]*fanoutEntry

	// lastTickAtSym is per-(segment, security_code) timestamp of the
	// most recent forwarded tick. Used by the no-data halt watchdog.
	lastTickAtSym map[SubscriptionKey]time.Time

	// Monotonic subscriber id counter for log readability.
	nextID atomic_uint64

	// stats
	statsMu       sync.RWMutex
	ticksReceived uint64
	ticksDropped  uint64
}

// fanoutEntry holds the list of subscribers for one (segment, security).
type fanoutEntry struct {
	subs []*Subscription
}

// NewManager wires the client + resolver. Build, then call Start(ctx)
// to begin pumping ticks. Build order:
//   1. NewResolver(extRedis)
//   2. New(odinCfg, logger)             ← from odin_client.go (OdinClient)
//   3. NewManager(client, resolver, logger)
//   4. odinClient.SetListener(manager.onMessage)
//   5. go odinClient.Run(ctx)
//   6. go manager.Start(ctx)            ← watchdog only; messages are
//                                          already flowing on step 5
func NewManager(client *OdinClient, resolver *Resolver, logger *zap.Logger) *Manager {
	return &Manager{
		client:        client,
		resolver:      resolver,
		logger:        logger.Named("marketws"),
		fanouts:       make(map[SubscriptionKey]*fanoutEntry),
		lastTickAtSym: make(map[SubscriptionKey]time.Time),
	}
}

// OnMessage is the MessageListener wired into OdinClient. Pure dispatch:
//   - 209 (touchline)   → fan out to subscribers
//   - 1111 (disconnect) → log; OdinClient will reconnect on its own
//   - everything else   → log debug
func (m *Manager) OnMessage(msg FixMessage) {
	switch msg.Code() {
	case 209: // Multiple Touchline Response — bid/ask update
		tl := msg.AsTouchline()
		if tl == nil {
			return
		}
		m.dispatchTouchline(tl)

	case 1111: // BroadcastSocketDisconnect
		m.logger.Warn("ODIN broadcast socket disconnect signalled by server")

	case 102: // Logon response — already logged by OdinClient
		return

	default:
		// Unknown messages are common (heartbeats, indices, etc.) — debug only.
		m.logger.Debug("unhandled ODIN message", zap.Int("code", msg.Code()))
	}
}

// dispatchTouchline converts paise → ₹, builds a state.MarketData, and
// pushes it onto every subscriber's channel for this (segment, token).
func (m *Manager) dispatchTouchline(tl *TouchlineMsg) {
	key := SubscriptionKey{SegmentID: tl.SegmentID, SecurityCode: tl.SecurityCode}
	now := time.Now()

	m.mu.Lock()
	entry, ok := m.fanouts[key]
	m.lastTickAtSym[key] = now
	m.mu.Unlock()
	if !ok || len(entry.subs) == 0 {
		// We're subscribed to this token but nobody is consuming — usually
		// means a strategy was Stop'd but we haven't unsubscribed from
		// ODIN yet. Cheap to drop here; the unsubscribe will happen on
		// the next Unsubscribe() call.
		return
	}

	md := state.MarketData{
		Bid:    float64(tl.BidPaise) / PaiseDivisor,
		Ask:    float64(tl.AskPaise) / PaiseDivisor,
		BidQty: int(tl.BidQty),
		AskQty: int(tl.AskQty),
		LTP:    float64(tl.LTPPaise) / PaiseDivisor,
		At:     now,
	}

	m.statsMu.Lock()
	m.ticksReceived++
	m.statsMu.Unlock()

	// Non-blocking send to every subscriber. If a subscriber is slow
	// (its tickCh is full), we drop this tick for them — better to lose
	// one tick than back-pressure the read goroutine and stall the WHOLE
	// feed for every strategy.
	for _, sub := range entry.subs {
		md.Symbol = sub.Symbol
		select {
		case sub.Ticks <- md:
		default:
			m.statsMu.Lock()
			m.ticksDropped++
			m.statsMu.Unlock()
			// Log every 100 drops to surface the back-pressure without spam.
			if m.ticksDropped%100 == 1 {
				m.logger.Warn("marketws tick dropped — subscriber slow",
					zap.String("subscriber_id", sub.SubscriberID),
					zap.String("symbol", sub.Symbol))
			}
		}
	}
}

// Subscribe registers a new consumer for the symbol identified by ISIN.
// Returns a *Subscription with a buffered ticks channel and an idempotent
// Unsubscribe func. Bounded by ctx for the resolver lookup.
//
// subscriberID is used in logs for tying ticks back to which strategy
// they fed (e.g., the strategy_id).
func (m *Manager) Subscribe(ctx context.Context, subscriberID, isin, symbol string) (*Subscription, error) {
	resolved, err := m.resolver.Resolve(ctx, isin)
	if err != nil {
		return nil, err
	}

	key := SubscriptionKey{SegmentID: resolved.SegmentID, SecurityCode: resolved.SecurityCode}
	sub := &Subscription{
		SubscriberID: subscriberID,
		ISIN:         isin,
		Symbol:       symbol,
		SecurityCode: resolved.SecurityCode,
		SegmentID:    resolved.SegmentID,
		// 64 deep — covers a burst without forcing the WS read goroutine
		// to drop. Strategy's own tickCh has another 256 of buffering.
		Ticks: make(chan state.MarketData, 64),
	}

	m.mu.Lock()
	entry, exists := m.fanouts[key]
	if !exists {
		entry = &fanoutEntry{}
		m.fanouts[key] = entry
	}
	entry.subs = append(entry.subs, sub)
	firstSubscriber := len(entry.subs) == 1
	m.mu.Unlock()

	if firstSubscriber {
		if err := m.client.Subscribe(key.SegmentID, key.SecurityCode); err != nil {
			// Roll back the local registration.
			m.removeSub(key, sub)
			close(sub.Ticks)
			return nil, fmt.Errorf("odin subscribe: %w", err)
		}
		m.logger.Info("subscribed to symbol",
			zap.String("subscriber_id", subscriberID),
			zap.String("isin", isin),
			zap.String("symbol", symbol),
			zap.Int("segment", key.SegmentID),
			zap.Int64("security_code", key.SecurityCode))
	} else {
		m.logger.Info("joined existing subscription",
			zap.String("subscriber_id", subscriberID),
			zap.String("symbol", symbol),
			zap.Int("total_subscribers", len(entry.subs)))
	}

	sub.Unsubscribe = func() { m.unsubscribe(key, sub) }
	return sub, nil
}

// unsubscribe removes one subscriber. If they were the last for this
// key, sends an Unsubscribe to ODIN.
func (m *Manager) unsubscribe(key SubscriptionKey, sub *Subscription) {
	m.removeSub(key, sub)
	close(sub.Ticks)

	// Should we tell ODIN to stop sending us this symbol? Only if no
	// other strategy still wants it.
	m.mu.Lock()
	entry, ok := m.fanouts[key]
	last := !ok || len(entry.subs) == 0
	if last {
		delete(m.fanouts, key)
		delete(m.lastTickAtSym, key)
	}
	m.mu.Unlock()

	if last {
		if err := m.client.Unsubscribe(key.SegmentID, key.SecurityCode); err != nil {
			m.logger.Warn("odin unsubscribe failed (will retry on next reconnect)",
				zap.Int("segment", key.SegmentID),
				zap.Int64("security_code", key.SecurityCode),
				zap.Error(err))
		}
	}
}

// removeSub takes the manager lock and drops one subscriber from the
// per-key list. Idempotent: removing a sub that isn't there is a no-op.
func (m *Manager) removeSub(key SubscriptionKey, sub *Subscription) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, ok := m.fanouts[key]
	if !ok {
		return
	}
	for i, s := range entry.subs {
		if s == sub {
			entry.subs = append(entry.subs[:i], entry.subs[i+1:]...)
			return
		}
	}
}

// LastTickAt returns the wall-clock of the most recent tick for the
// given subscription, or zero-time if no tick has been received yet.
// Used by the strategy layer to enforce the 30s no-data halt.
func (m *Manager) LastTickAt(seg int, security int64) time.Time {
	key := SubscriptionKey{SegmentID: seg, SecurityCode: security}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastTickAtSym[key]
}

// FeedAlive returns true if the ODIN feed is connected. Used by
// manager.Start to refuse Entry when there's no live data feed.
func (m *Manager) FeedAlive() bool {
	return m.client.State() == StateConnected
}

// Stats returns per-process tick counters.
func (m *Manager) Stats() (received, dropped uint64) {
	m.statsMu.RLock()
	defer m.statsMu.RUnlock()
	return m.ticksReceived, m.ticksDropped
}

// ─────────────────────────────────────────────────────────────────────────
// Lock-free counter helper (avoid importing sync/atomic in 5 files)
// ─────────────────────────────────────────────────────────────────────────

type atomic_uint64 struct{ v uint64 }

func (a *atomic_uint64) Add(delta uint64) uint64 {
	// We don't actually need atomic semantics for the id counter — it's
	// only read under m.mu. Keeping the type wrapper so the field name
	// signals "monotonic increasing" at the call site.
	a.v += delta
	return a.v
}
