// Package wss owns the broker WebSocket listener for orderstatus svc.
//
// One shared TCP connection to broker per binary (not per-user), same pattern
// as pkg/indira.WSClient's shared-connection mode. On boot, StartSubscription
// gets called once per active user; the shared client fans events for all
// users to a single Updates channel; the Listener consumes and appends to
// broker_events.
//
// Chunk B (2026-07-10): listener + subscription plumbing only. Auth wiring
// is a TODO — real auth comes via user-config gRPC in a follow-up. For now,
// StartSubscription is exposed so a caller with an *indira.AuthContext can
// hand-wire tests.
package wss

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"go.uber.org/zap"

	indira "github.com/RohitIndira/Algo-Treading/pkg/indira"

	"github.com/RohitIndira/Algo-Treading/services/orderstatus/internal/publisher"
	"github.com/RohitIndira/Algo-Treading/services/orderstatus/internal/store"
)

// Listener orchestrates:
//   - one shared *indira.WSClient across all subscribed users
//   - a consumer goroutine reading from the shared Updates channel
//   - broker_events.Insert for every event
type Listener struct {
	httpClient *indira.Client
	writer     *store.Writer
	pub        *publisher.Publisher // nil = no Kafka fan-out (dev / test paths)
	logger     *zap.Logger

	mu       sync.Mutex
	wsClient *indira.WSClient // nil until first StartSubscription
	subs     map[string]*indira.AuthContext // userID → auth (for reconnect)

	runningCtx    context.Context
	runningCancel context.CancelFunc
}

// NewListener wires the WSS event pipeline:
//
//	WSS event → broker_events INSERT (writer) → order.events publish (pub)
//
// Passing a nil pub is legal — the listener still persists to broker_events,
// just skips the Kafka fan-out. Useful for tests or a boot without Kafka.
func NewListener(httpClient *indira.Client, writer *store.Writer, pub *publisher.Publisher, logger *zap.Logger) *Listener {
	return &Listener{
		httpClient: httpClient,
		writer:     writer,
		pub:        pub,
		logger:     logger,
		subs:       make(map[string]*indira.AuthContext),
	}
}

// StartSubscription adds a user to the shared WSS. First call also boots the
// shared *indira.WSClient + spawns the consumer goroutine.
//
// TODO(auth-wiring): callers today pass in an AuthContext directly. Chunk B.5
// will replace this with a user-config gRPC lookup so orderstatus svc doesn't
// depend on encrypted-token access. See docs/orderstatus_service_design.md.
func (l *Listener) StartSubscription(ctx context.Context, userID string, auth *indira.AuthContext) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Dedupe — same user calling twice just refreshes auth.
	l.subs[userID] = auth

	// First subscription boots the shared client.
	if l.wsClient == nil {
		l.wsClient = indira.NewWSClient(l.httpClient, auth)
		if err := l.wsClient.Connect(ctx); err != nil {
			l.wsClient = nil
			return fmt.Errorf("wss connect: %w", err)
		}
		l.runningCtx, l.runningCancel = context.WithCancel(context.Background())
		go l.consumeLoop(l.runningCtx)
		l.logger.Info("wss shared client booted",
			zap.String("first_user", userID))
	}

	// Subscribe the user (Subscribe is idempotent on the pkg/indira side).
	if err := l.wsClient.Subscribe(ctx, auth); err != nil {
		return fmt.Errorf("wss subscribe user=%s: %w", userID, err)
	}
	l.logger.Info("wss subscription added",
		zap.String("user_id", userID),
		zap.Int("total_subs", len(l.subs)))
	return nil
}

// Close stops the consumer goroutine and closes the shared WSS client.
func (l *Listener) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.runningCancel != nil {
		l.runningCancel()
	}
	if l.wsClient != nil {
		l.wsClient.Close()
		l.wsClient = nil
	}
}

// consumeLoop reads every event off the shared Updates channel and writes it
// to broker_events. Runs in its own goroutine until ctx is cancelled or the
// channel closes.
func (l *Listener) consumeLoop(ctx context.Context) {
	l.logger.Info("wss consumer loop started")
	defer l.logger.Info("wss consumer loop stopped")

	for {
		select {
		case <-ctx.Done():
			return
		case ws, ok := <-l.wsClient.Updates:
			if !ok {
				// Channel closed — shared client is gone.
				return
			}
			if ws == nil {
				continue
			}
			l.handleWSEvent(ctx, ws)
		}
	}
}

// handleWSEvent converts one WSOrderStatus into a store.Event and writes it.
// Errors are logged but do NOT crash the loop — losing one event is better
// than dropping every subsequent event.
func (l *Listener) handleWSEvent(ctx context.Context, ws *indira.WSOrderStatus) {
	brokerOrderID := strings.TrimSpace(ws.UniqueCode)
	if brokerOrderID == "" || brokerOrderID == "0" {
		return
	}

	// Resolve userID — pkg/indira sends events with UserID as FlexInt; some
	// paths populate UCC (client code) instead. Prefer UCC (matches our
	// user_id VARCHAR keying elsewhere).
	userID := strings.TrimSpace(ws.UCC)
	if userID == "" {
		userID = strings.TrimSpace(string(ws.UserID))
	}
	if userID == "" {
		// Nothing to attribute this event to — log + skip.
		l.logger.Warn("wss event with no user_id, skipping",
			zap.String("broker_order_id", brokerOrderID))
		return
	}

	// Serialize the full raw event for forensics. json.Marshal on a WSOrderStatus
	// re-quotes strings correctly (FlexInt custom MarshalJSON handles the
	// number/string dance).
	rawJSON, err := json.Marshal(ws)
	if err != nil {
		l.logger.Error("wss event marshal failed",
			zap.String("broker_order_id", brokerOrderID), zap.Error(err))
		return
	}

	ev := &store.Event{
		BrokerOrderID:   brokerOrderID,
		ExchangeOrderID: strings.TrimSpace(ws.OrderNumber),
		EventSeq:        eventSeqFrom(ws),
		Source:          store.SourceWSS,
		EventType:       eventTypeFromStatus(ws.OrderStatus),
		Status:          strings.TrimSpace(strings.ToUpper(ws.OrderStatus)),
		OMSStatusCode:   0, // pkg/indira doesn't expose OMSOrderStatus yet; add when needed
		UserID:          userID,
		Symbol:          strings.TrimSpace(ws.Symbol),
		Exchange:        strings.TrimSpace(string(ws.Exchange)),
		BuySell:         strings.TrimSpace(ws.BuySell),
		OrderType:       strings.TrimSpace(ws.OrderType),
		Product:         strings.TrimSpace(ws.Product),
		OrderPrice:      parseFloat(ws.OrderPrice),
		TriggerPrice:    ws.TriggerPrice,
		Quantity:        ws.OrderOriginalQty,
		FilledQty:       ws.FilledQty(), // FIX 7: per-trade qty (TradeQty on TRD_MSG); TradedQTY is 0 on real fills
		TradedPrice:     parseFloat(ws.TradedPrice),
		PendingQty:      flexInt(ws.PendingQty),
		Reason:          strings.TrimSpace(ws.Reason),
		RawPayload:      rawJSON,
		BrokerTsMs:      0, // WSS doesn't include a millis stamp; broker_ts_ms stays 0 for WSS path
	}

	id, inserted, err := l.writer.Insert(ctx, ev)
	if err != nil {
		l.logger.Error("broker_events insert failed",
			zap.String("broker_order_id", brokerOrderID),
			zap.Int64("event_seq", ev.EventSeq),
			zap.String("status", ev.Status),
			zap.Error(err))
		return
	}
	if !inserted {
		// Dedup — REST poll wrote this same event first. Silent.
		return
	}

	l.logger.Debug("broker_events row appended",
		zap.String("broker_order_id", brokerOrderID),
		zap.String("status", ev.Status),
		zap.String("event_type", string(ev.EventType)),
		zap.Int64("event_seq", ev.EventSeq))

	// Fan out to Kafka order.events. Only new events (inserted=true) go here —
	// dedup at the source means downstream consumers each see every event
	// exactly once regardless of WSS/REST race conditions. Stamp published_at
	// only on a confirmed publish; a failed inline publish leaves the row for
	// the durability worker to retry.
	if l.pub != nil && l.pub.Publish(ctx, ev) {
		if err := l.writer.MarkPublished(ctx, id); err != nil {
			l.logger.Warn("mark published failed — durability worker will retry",
				zap.String("broker_order_id", brokerOrderID),
				zap.Int64("id", id), zap.Error(err))
		}
	}
}

// eventSeqFrom picks the best available deterministic sequence key for
// this event. Preference:
//  1. broker's MessageSequenceNumber (monotonic per user connection)
//  2. exchange timestamp parsed from LastModifiedTimeStamp / OrderEntryTime
//  3. fall back to the composite (OrderSequenceNumber, MessageSequenceNumber)
//
// Whatever we pick, REST paths (reconciler.go, orderbook poll) must produce
// the SAME value from the equivalent REST-side field for dedup to work.
// TODO(reconciler): confirm the REST orderbook has an equivalent when we
// wire Chunk D.
func eventSeqFrom(ws *indira.WSOrderStatus) int64 {
	if v := flexInt(ws.MessageSequenceNumber); v > 0 {
		return int64(v)
	}
	if ws.OrderSequenceNumber > 0 {
		return int64(ws.OrderSequenceNumber)
	}
	// No good source — return a monotonic proxy. Not ideal but keeps the
	// idempotency contract from being "0 == 0" for every mystery event.
	return int64(len(ws.UniqueCode))*1000 + int64(flexInt(ws.PendingQty))
}

// eventTypeFromStatus maps the verified 6 WSS OrderStatus values (see
// docs/codify_wss_state_machine.md) to our broker_events.event_type enum.
func eventTypeFromStatus(status string) store.EventType {
	switch strings.TrimSpace(strings.ToUpper(status)) {
	case "EXECUTED":
		return store.EventFilled
	case "CANCELLED":
		return store.EventCancelled
	case "A.REJECTED", "REJECTED", "ORDER ERROR":
		return store.EventRejected
	case "PENDING", "ADMIN PENDING":
		return store.EventStatusChanged
	default:
		// Unknown status — persist as STATUS_CHANGED so we don't lose it,
		// but the row's `status` column preserves the exact broker string.
		return store.EventStatusChanged
	}
}

func flexInt(f indira.FlexInt) int {
	v, _ := strconv.Atoi(string(f))
	return v
}

func parseFloat(s string) float64 {
	v, _ := strconv.ParseFloat(s, 64)
	return v
}
