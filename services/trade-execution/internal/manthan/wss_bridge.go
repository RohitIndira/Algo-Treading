package manthan

import (
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

// OrderUpdate is the WSS status update routed to a pending Manthan order.
type OrderUpdate struct {
	BrokerOrderID string
	Status        string  // raw broker status: EXECUTED, TRADED, REJECTED, CANCELLED, PENDING, OPEN, etc.
	FilledQty     int
	AvgFillPrice  float64
	TriggerPrice  float64
	Reason        string  // rejection/cancel reason
	Timestamp     time.Time
}

// WSSBridge connects the shared broker WebSocket to Manthan order handlers.
//
// Two overlapping sets:
//
//	pending — broker_order_id → live channel. Populated by Register when
//	          EntryHandler places an order; the channel is consumed by the
//	          same handler waiting for the fill/reject. Cleaned on Unregister.
//
//	known   — broker_order_id → struct{}. The set of orders that are
//	          Manthan's concern regardless of whether a handler is currently
//	          waiting. Populated by Register AND by MarkKnown on boot
//	          recovery. Cleaned only by MarkTerminated. This exists so that
//	          HandleUpdate can still say "yes this is Manthan" for orders
//	          placed by a PREVIOUS process — otherwise a restart drops all
//	          WSS→Kafka fanout for existing orders (see WSSKafkaBridge and
//	          the 2026-07-17 postmortem).
//
// Thread-safe: accessed from WSS goroutine (writes) and entry handler goroutines (reads).
type WSSBridge struct {
	mu      sync.RWMutex
	pending map[string]chan *OrderUpdate // broker_order_id → channel (live handler)
	known   map[string]struct{}          // broker_order_id → is-a-Manthan-order marker (survives handler exit)
	logger  *zap.Logger
}

func NewWSSBridge(logger *zap.Logger) *WSSBridge {
	return &WSSBridge{
		pending: make(map[string]chan *OrderUpdate),
		known:   make(map[string]struct{}),
		logger:  logger,
	}
}

// Register creates a buffered channel for a broker order and returns it.
// The caller (EntryHandler) selects on this channel to receive fill/reject/cancel.
// Buffer size 5: covers PENDING → OPEN → EXECUTED sequence without blocking WSS goroutine.
//
// Also adds the broker_order_id to the `known` set so subsequent restarts
// can find it via MarkKnown recovery.
func (b *WSSBridge) Register(brokerOrderID string) <-chan *OrderUpdate {
	b.mu.Lock()
	defer b.mu.Unlock()

	ch := make(chan *OrderUpdate, 5)
	b.pending[brokerOrderID] = ch
	b.known[brokerOrderID] = struct{}{}

	b.logger.Debug("WSS bridge registered",
		zap.String("broker_order_id", brokerOrderID))
	return ch
}

// Unregister removes the live callback channel. Call after order reaches
// terminal state or after timeout. Closes the channel to unblock any waiting
// select. Leaves the `known` marker intact — an ENTRY that filled still has
// SL orders whose WSS updates should route as Manthan; only MarkTerminated
// clears `known`.
func (b *WSSBridge) Unregister(brokerOrderID string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if ch, ok := b.pending[brokerOrderID]; ok {
		close(ch)
		delete(b.pending, brokerOrderID)
	}
}

// MarkKnown adds a broker_order_id to the `known` set without creating a
// channel. Called at boot for recovery (see Repository.ListActiveManthan…
// and manthan_init.go's Boot() recovery loop) so WSS events for orders
// placed in a PREVIOUS process still get correctly identified as Manthan
// — which is what enables WSSKafkaBridge.PublishFill to fire and populate
// order.events with real traded_price.
//
// Safe to call for a broker_order_id already in pending (no-op on `known`).
func (b *WSSBridge) MarkKnown(brokerOrderID string) {
	if brokerOrderID == "" {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.known[brokerOrderID] = struct{}{}
}

// MarkTerminated removes a broker_order_id from BOTH `pending` and `known`.
// Call when the order is truly done — CANCELLED / REJECTED / SL_FILLED /
// full EXIT. Prevents unbounded growth of `known` over long uptime.
func (b *WSSBridge) MarkTerminated(brokerOrderID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if ch, ok := b.pending[brokerOrderID]; ok {
		close(ch)
		delete(b.pending, brokerOrderID)
	}
	delete(b.known, brokerOrderID)
}

// KnownCount returns the size of the `known` set. Cheap read used by tests
// and boot-log to confirm recovery landed the expected number of orders.
func (b *WSSBridge) KnownCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.known)
}

// HandleUpdate is called by statusservice when a WSS message arrives for a
// broker order. Returns true when this is a Manthan order — either because
// a live handler is waiting on it (in `pending`) OR the boot-recovery
// marked it as Manthan (in `known`). Non-Manthan orders return false and
// existing status handlers take over.
//
// Channel push happens only when there's a live handler; boot-recovered
// orders have no channel to push to, which is exactly right — the WSS→Kafka
// fanout in WSSKafkaBridge still fires because HandleUpdate returned true.
func (b *WSSBridge) HandleUpdate(brokerOrderID, status string, filledQty int, avgPrice, triggerPrice float64, reason string) bool {
	b.mu.RLock()
	ch, hasChan := b.pending[brokerOrderID]
	_, isKnown := b.known[brokerOrderID]
	b.mu.RUnlock()

	if !hasChan && !isKnown {
		return false // not a Manthan order — let existing handlers process it
	}

	if hasChan {
		update := &OrderUpdate{
			BrokerOrderID: brokerOrderID,
			Status:        status,
			FilledQty:     filledQty,
			AvgFillPrice:  avgPrice,
			TriggerPrice:  triggerPrice,
			Reason:        reason,
			Timestamp:     time.Now(),
		}
		// Non-blocking send — channel full = slow handler, better to drop
		// than to jam the WSS goroutine.
		select {
		case ch <- update:
			b.logger.Debug("WSS update routed to Manthan handler",
				zap.String("broker_order_id", brokerOrderID),
				zap.String("status", status),
				zap.Int("filled_qty", filledQty))
		default:
			b.logger.Warn("WSS bridge channel full — update dropped",
				zap.String("broker_order_id", brokerOrderID),
				zap.String("status", status))
		}
	}

	return true
}

// IsRegistered checks if a broker order ID has a LIVE channel waiter (i.e.
// EntryHandler is currently blocked on it). "Not registered" doesn't mean
// "not Manthan" — a boot-recovered order is Manthan (in `known`) but has
// no channel. Callers deciding routing should use HandleUpdate's return
// value instead of this method.
func (b *WSSBridge) IsRegistered(brokerOrderID string) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	_, ok := b.pending[brokerOrderID]
	return ok
}

// PendingCount returns the number of orders waiting for WSS updates.
func (b *WSSBridge) PendingCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.pending)
}

// ────────────────────────────────────────────────────────────────────
// Codify (Indira) WSS OrderStatus enum
// ────────────────────────────────────────────────────────────────────
// Values marked "verified" were observed emitted by the broker during
// live capture 2026-07-09 (see scripts/codify_learn/03_wss_full_capture.py).
//
// Values marked "unverified" have never been observed but are retained
// for defensive coverage in case broker vocabulary differs in edge cases
// we haven't exercised (partial fills, SL fires, AMO acceptance, etc).
// Do NOT remove without additional capture evidence — false negatives
// here mean a fill event silently gets ignored.
//
// Verified terminal states:
//   EXECUTED     — fill (comes on MessageType=TRD_MSG with TradedPrice)
//   CANCELLED    — user-cancel success
//   A.REJECTED   — exchange/broker rejection (OMSOrderStatus=15 fresh,
//                                             =10 cancel-of-dead)
//   ORDER ERROR  — instant reject (Reason contains "price freeze")
//
// Verified transitional state (expect follow-up within ~200ms):
//   ADMIN PENDING — pre-reject state, becomes A.REJECTED
//
// Verified active state:
//   PENDING      — sitting at exchange (Codify never emits "OPEN")

// IsTerminalStatus returns true if the broker status means the order is done.
func IsTerminalStatus(status string) bool {
	switch status {
	case "EXECUTED", "TRADED", "COMPLETE", "FILLED", // EXECUTED verified; others defensive
		"REJECTED", "A.REJECTED", // A.REJECTED verified; REJECTED defensive
		"ORDER ERROR", // verified — price freeze instant reject
		"CANCELLED":   // verified
		return true
	}
	return false
}

// IsFilledWSStatus returns true if the broker status means order was filled.
func IsFilledWSStatus(status string) bool {
	switch status {
	case "EXECUTED", // verified
		"TRADED", "COMPLETE", "FILLED": // defensive — never observed
		return true
	}
	return false
}

// IsPartialWSStatus returns true if broker reports partial fill.
// NOTE: neither value has been observed in live capture; strings are
// educated guesses from Codify docs. Update after we see a real partial.
func IsPartialWSStatus(status string) bool {
	switch status {
	case "PARTIALLY TRADED", "PARTIALLY EXECUTED":
		return true
	}
	return false
}

// IsRejectedWSStatus returns true if broker rejected the order.
// ORDER ERROR is the "price freeze" path — same terminal semantics as
// A.REJECTED but arrives as a single event (no ADMIN PENDING lead-in).
func IsRejectedWSStatus(status string) bool {
	switch status {
	case "A.REJECTED", // verified — codes 10 and 15
		"REJECTED",     // defensive — never observed
		"ORDER ERROR": // verified — price freeze instant reject
		return true
	}
	return false
}

// IsPriceFreezeReject returns true iff the WSS event is specifically the
// "price too far from LTP" rejection. Used to drive log-spam suppression:
// production sees 100+ of these per minute during AMO retry storms and we
// want to log-collapse them without hiding real rejections.
// Detection: OrderStatus=ORDER ERROR AND Reason mentions "price freeze".
func IsPriceFreezeReject(status, reason string) bool {
	if status != "ORDER ERROR" {
		return false
	}
	// Reason from live capture: " The order has been cancelled due to price freeze."
	return strings.Contains(strings.ToLower(reason), "price freeze")
}
