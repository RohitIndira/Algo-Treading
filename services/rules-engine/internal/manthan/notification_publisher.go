package manthan

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// NotificationPublisher emits user-facing notifications to the Kafka topic
// `manthan.notifications`. Frontend / email / push consumers subscribe and
// route to whatever channel makes sense per user.
//
// Best-effort by design: a notification publish failure is logged but never
// fails the calling transaction. The DB state update is the source of truth;
// notifications are an out-of-band convenience.
//
// Off by default. Enable with MANTHAN_NOTIFICATIONS_ENABLED=true.
type NotificationPublisher struct {
	writer  *kafka.Writer
	enabled bool
	logger  *zap.Logger
}

func NewNotificationPublisher(brokers []string, enabled bool, logger *zap.Logger) *NotificationPublisher {
	if !enabled || len(brokers) == 0 {
		return &NotificationPublisher{enabled: false, logger: logger}
	}
	return &NotificationPublisher{
		enabled: true,
		logger:  logger,
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        "manthan.notifications",
			Balancer:     &kafka.LeastBytes{},
			BatchTimeout: 1 * time.Millisecond,
			RequiredAcks: kafka.RequireAll,
		},
	}
}

func (p *NotificationPublisher) Close() error {
	if p == nil || p.writer == nil {
		return nil
	}
	return p.writer.Close()
}

// Notification is the JSON shape on the manthan.notifications topic.
type Notification struct {
	Type       string `json:"type"`        // event class — same vocabulary as manthan_position_events.event_type
	Severity   string `json:"severity"`    // info | warning | critical
	UserID     string `json:"user_id"`     // who to notify
	StrategyID string `json:"strategy_id"` // which strategy this relates to
	SignalID   string `json:"signal_id"`   // links to the originating decision
	Symbol     string `json:"symbol"`
	Title      string `json:"title"`        // one-line user-readable headline
	Message    string `json:"message"`      // longer human description
	ActionHint string `json:"action_hint"`  // optional — what the user might want to do
	Timestamp  string `json:"timestamp"`    // ISO-8601
}

// Publish writes one notification to Kafka. Returns nil on disabled publisher
// (silently no-op) or success; logs and returns the error otherwise so the
// caller can decide whether to swallow it (typical) or propagate.
func (p *NotificationPublisher) Publish(ctx context.Context, n Notification) error {
	if p == nil || !p.enabled || p.writer == nil {
		return nil
	}
	if n.Timestamp == "" {
		n.Timestamp = time.Now().UTC().Format(time.RFC3339Nano)
	}
	if n.Severity == "" {
		n.Severity = "info"
	}

	body, err := json.Marshal(n)
	if err != nil {
		return fmt.Errorf("marshal notification: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(n.UserID), // user_id key → per-user partition affinity
		Value: body,
		Headers: []kafka.Header{
			{Key: "type", Value: []byte(n.Type)},
			{Key: "severity", Value: []byte(n.Severity)},
			{Key: "user_id", Value: []byte(n.UserID)},
		},
	}

	writeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := p.writer.WriteMessages(writeCtx, msg); err != nil {
		p.logger.Warn("Notification publish failed",
			zap.String("type", n.Type),
			zap.String("user_id", n.UserID),
			zap.String("symbol", n.Symbol),
			zap.Error(err))
		return err
	}
	p.logger.Info("Notification published",
		zap.String("type", n.Type),
		zap.String("severity", n.Severity),
		zap.String("user_id", n.UserID),
		zap.String("symbol", n.Symbol))
	return nil
}

// NotifyManualExit is a convenience helper for the most common case driven
// by the projector when MANUAL_EXIT_DETECTED is applied.
func (p *NotificationPublisher) NotifyManualExit(ctx context.Context, userID, strategyID, signalID, symbol string, dbQty int) {
	_ = p.Publish(ctx, Notification{
		Type:       "MANUAL_EXIT_DETECTED",
		Severity:   "info",
		UserID:     userID,
		StrategyID: strategyID,
		SignalID:   signalID,
		Symbol:     symbol,
		Title:      fmt.Sprintf("%s position manually exited", symbol),
		Message: fmt.Sprintf(
			"You sold the %d %s shares the algo was managing outside the system. The strategy will not re-buy this stock for 3 days.",
			dbQty, symbol),
		ActionHint: "If this was unintentional, you can clear the override by re-creating the strategy.",
	})
}

// NotifyManualPartialExit fires when broker shows fewer shares than our DB
// but more than zero. Algo continues to manage the remaining shares.
func (p *NotificationPublisher) NotifyManualPartialExit(ctx context.Context, userID, strategyID, signalID, symbol string, oldQty, newQty int) {
	_ = p.Publish(ctx, Notification{
		Type:       "MANUAL_PARTIAL_EXIT_DETECTED",
		Severity:   "warning",
		UserID:     userID,
		StrategyID: strategyID,
		SignalID:   signalID,
		Symbol:     symbol,
		Title:      fmt.Sprintf("%s position partially reduced", symbol),
		Message: fmt.Sprintf(
			"You sold %d of %d %s shares manually. The algo will continue to trail SL on the remaining %d shares.",
			oldQty-newQty, oldQty, symbol, newQty),
	})
}

// NotifyManualSLCancelled fires when broker shows our SL order is gone but
// the underlying shares are still held — the user manually cancelled SL
// without exiting the position. Safety monitor will re-place but the user
// should know.
func (p *NotificationPublisher) NotifyManualSLCancelled(ctx context.Context, userID, strategyID, signalID, symbol string) {
	_ = p.Publish(ctx, Notification{
		Type:       "MANUAL_SL_CANCELLED_DETECTED",
		Severity:   "warning",
		UserID:     userID,
		StrategyID: strategyID,
		SignalID:   signalID,
		Symbol:     symbol,
		Title:      fmt.Sprintf("%s stop-loss was cancelled", symbol),
		Message: fmt.Sprintf(
			"Your stop-loss order on %s was cancelled outside the algo. The system will automatically re-place it on the next safety check.",
			symbol),
	})
}
