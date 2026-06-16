// Package notifications is the gateway's bridge between the
// `manthan.notifications` Kafka topic and the per-user WebSocket clients
// connected to `/ws/notifications`.
//
// Why the gateway and not trade-execution (which produces the events):
//   - The gateway is the user-facing tier — all push channels to the
//     frontend already terminate here (/ws/matches, etc.).
//   - Notification fan-out is cross-service: rules-engine AND
//     trade-execution publish to the same Kafka topic; the gateway is
//     the natural single consumer.
//   - Decouples notifications from execution: a notification spike or a
//     slow WS client cannot back-pressure the order path.
//
// Architecture:
//
//   manthan.notifications (Kafka)            users connected to /ws/notifications
//                │                                            ▲
//                ▼                                            │
//          Consumer.Run     ──▶  Broadcaster.Broadcast  ──▶  per-user channel
//          (reads + json-                                    (non-blocking;
//           unmarshal)                                       a slow client
//                                                            drops, never
//                                                            blocks others)
package notifications

import (
	"sync"

	"go.uber.org/zap"
)

// Notification mirrors the shape published by both rules-engine and
// trade-execution to the `manthan.notifications` topic. Keep JSON tags
// in lockstep with services/rules-engine/internal/manthan/notification_publisher.go
// — adding a field on one side requires adding it here too.
type Notification struct {
	Type       string `json:"type"`        // SESSION_EXPIRED | JWT_EXPIRING | MANUAL_EXIT | ...
	Severity   string `json:"severity"`    // info | warning | error | critical
	UserID     string `json:"user_id"`     // routing key — only delivered to this user's WS connections
	StrategyID string `json:"strategy_id"` // optional
	SignalID   string `json:"signal_id"`   // optional
	Symbol     string `json:"symbol"`      // optional
	Title      string `json:"title"`       // one-line user-readable headline
	Message    string `json:"message"`     // longer human description
	ActionHint string `json:"action_hint"` // optional UI hint (e.g. "RELOGIN")
	Timestamp  string `json:"timestamp"`   // ISO-8601 set by the producer
}

// subscriberChanBuffer is the per-subscriber buffer. Sized so a small
// burst (banner + toast + log) doesn't drop, but bounded so one slow
// WebSocket client cannot back up the whole broadcaster.
const subscriberChanBuffer = 16

// Broadcaster is an in-process per-user fan-out. It is goroutine-safe
// and intentionally NON-blocking: a slow WS client misses messages
// rather than wedging the consumer.
//
// Lifetime: one Broadcaster per gateway process. Subscribers (one per WS
// connection) come and go; the Consumer registers no subscribers — it
// only calls Broadcast.
type Broadcaster struct {
	mu      sync.RWMutex
	subs    map[string]map[*subscription]struct{} // user_id -> set of subs
	logger  *zap.Logger
	dropped uint64 // count of messages dropped because a sub's buffer was full
}

// subscription is one client's view. Cancel removes it from the registry
// and closes the channel — safe to call multiple times.
type subscription struct {
	ch       chan Notification
	cancel   func()
	once     sync.Once
}

// NewBroadcaster builds an empty broadcaster. logger is required.
func NewBroadcaster(logger *zap.Logger) *Broadcaster {
	return &Broadcaster{
		subs:   make(map[string]map[*subscription]struct{}),
		logger: logger.Named("notifications.broadcaster"),
	}
}

// Subscribe returns a channel that will receive every Notification whose
// UserID matches userID, plus a cancel func the caller MUST call when
// done (idempotent). The channel is non-blocking on the producer side —
// messages are dropped if the caller doesn't read fast enough.
func (b *Broadcaster) Subscribe(userID string) (<-chan Notification, func()) {
	sub := &subscription{
		ch: make(chan Notification, subscriberChanBuffer),
	}

	b.mu.Lock()
	if b.subs[userID] == nil {
		b.subs[userID] = make(map[*subscription]struct{})
	}
	b.subs[userID][sub] = struct{}{}
	count := len(b.subs[userID])
	b.mu.Unlock()

	sub.cancel = func() {
		sub.once.Do(func() {
			b.mu.Lock()
			if subs, ok := b.subs[userID]; ok {
				delete(subs, sub)
				if len(subs) == 0 {
					delete(b.subs, userID)
				}
			}
			b.mu.Unlock()
			close(sub.ch)
		})
	}

	b.logger.Debug("subscribed",
		zap.String("user_id", userID),
		zap.Int("user_subs", count))

	return sub.ch, sub.cancel
}

// Broadcast hands the notification to every subscriber matching
// n.UserID. Non-blocking: if a subscriber's buffer is full, the message
// for that subscriber is dropped (counter incremented) and the others
// still get it.
//
// A notification with empty UserID is dropped with a Warn — every
// producer in the codebase sets UserID; an empty one is a bug.
func (b *Broadcaster) Broadcast(n Notification) {
	if n.UserID == "" {
		b.logger.Warn("dropping notification with empty user_id",
			zap.String("type", n.Type),
			zap.String("title", n.Title))
		return
	}
	b.mu.RLock()
	subs, ok := b.subs[n.UserID]
	if !ok || len(subs) == 0 {
		b.mu.RUnlock()
		return
	}
	// Copy the subscriber set under the read lock so we don't hold it
	// while sending (sends are non-blocking but I/O on slow paths can
	// surprise you; minimize lock scope).
	targets := make([]*subscription, 0, len(subs))
	for s := range subs {
		targets = append(targets, s)
	}
	b.mu.RUnlock()

	for _, s := range targets {
		select {
		case s.ch <- n:
		default:
			b.dropped++
			b.logger.Warn("notification dropped — subscriber buffer full",
				zap.String("user_id", n.UserID),
				zap.String("type", n.Type))
		}
	}
}

// Stats returns (active_users, total_subscribers, dropped_messages).
// Cheap — for a /readyz dashboard.
func (b *Broadcaster) Stats() (users int, subs int, dropped uint64) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	users = len(b.subs)
	for _, m := range b.subs {
		subs += len(m)
	}
	return users, subs, b.dropped
}
