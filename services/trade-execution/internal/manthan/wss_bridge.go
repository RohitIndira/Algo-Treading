package manthan

import (
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
// When EntryHandler places a LIMIT BUY, it registers a callback channel keyed
// by broker_order_id. When the WSS fires a status update for that order,
// statusservice routes it here → we push to the channel → EntryHandler
// receives it instantly (no polling).
//
// Thread-safe: accessed from WSS goroutine (writes) and entry handler goroutines (reads).
type WSSBridge struct {
	mu      sync.RWMutex
	pending map[string]chan *OrderUpdate // broker_order_id → channel
	logger  *zap.Logger
}

func NewWSSBridge(logger *zap.Logger) *WSSBridge {
	return &WSSBridge{
		pending: make(map[string]chan *OrderUpdate),
		logger:  logger,
	}
}

// Register creates a buffered channel for a broker order and returns it.
// The caller (EntryHandler) selects on this channel to receive fill/reject/cancel.
// Buffer size 5: covers PENDING → OPEN → EXECUTED sequence without blocking WSS goroutine.
func (b *WSSBridge) Register(brokerOrderID string) <-chan *OrderUpdate {
	b.mu.Lock()
	defer b.mu.Unlock()

	ch := make(chan *OrderUpdate, 5)
	b.pending[brokerOrderID] = ch

	b.logger.Debug("WSS bridge registered",
		zap.String("broker_order_id", brokerOrderID))
	return ch
}

// Unregister removes a callback channel. Call after order reaches terminal state
// or after timeout. Closes the channel to unblock any waiting select.
func (b *WSSBridge) Unregister(brokerOrderID string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if ch, ok := b.pending[brokerOrderID]; ok {
		close(ch)
		delete(b.pending, brokerOrderID)
	}
}

// HandleUpdate is called by statusservice when a WSS message arrives for a
// broker order. If the order is registered (Manthan order), the update is
// pushed to the channel. If not registered (non-Manthan order), it's ignored.
//
// Returns true if the update was routed to a Manthan handler.
func (b *WSSBridge) HandleUpdate(brokerOrderID, status string, filledQty int, avgPrice, triggerPrice float64, reason string) bool {
	b.mu.RLock()
	ch, ok := b.pending[brokerOrderID]
	b.mu.RUnlock()

	if !ok {
		return false // not a Manthan order — let existing handlers process it
	}

	update := &OrderUpdate{
		BrokerOrderID: brokerOrderID,
		Status:        status,
		FilledQty:     filledQty,
		AvgFillPrice:  avgPrice,
		TriggerPrice:  triggerPrice,
		Reason:        reason,
		Timestamp:     time.Now(),
	}

	// Non-blocking send — if channel is full (unlikely with buffer 5), log and drop.
	// This prevents WSS goroutine from blocking on slow Manthan handlers.
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

	return true
}

// IsRegistered checks if a broker order ID is a Manthan order.
// Used by statusservice to decide routing.
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

// IsTerminalStatus returns true if the broker status means the order is done.
func IsTerminalStatus(status string) bool {
	switch status {
	case "EXECUTED", "TRADED", "COMPLETE", "FILLED",
		"REJECTED", "A.REJECTED",
		"CANCELLED":
		return true
	}
	return false
}

// IsFilledWSStatus returns true if the broker status means order was filled.
func IsFilledWSStatus(status string) bool {
	switch status {
	case "EXECUTED", "TRADED", "COMPLETE", "FILLED":
		return true
	}
	return false
}

// IsPartialWSStatus returns true if broker reports partial fill.
func IsPartialWSStatus(status string) bool {
	switch status {
	case "PARTIALLY TRADED", "PARTIALLY EXECUTED":
		return true
	}
	return false
}

// IsRejectedWSStatus returns true if broker rejected the order.
func IsRejectedWSStatus(status string) bool {
	switch status {
	case "REJECTED", "A.REJECTED":
		return true
	}
	return false
}
