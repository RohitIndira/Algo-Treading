// Package alert is orderstatus svc's operator-attention path.
//
// It publishes OPS_ALERT messages on manthan.notifications in the exact shape
// trade-execution's JWTExpiryNotifier.PublishOpsAlert uses, so the admin
// console and the app render them without a contract change. Every alert is
// also logged at Error with a stable message ("broadcast feed alert") so a
// log search finds it when Kafka is the thing that is down.
package alert

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// Topic is the notifications stream the admin console and the app consume.
const Topic = "manthan.notifications"

// Alerter is what the broadcast listener needs. A nil *Publisher is a valid
// Alerter: it only logs.
type Alerter interface {
	Alert(ctx context.Context, kind, userID, message string)
}

// Publisher writes OPS_ALERT messages. Dedup: one alert per (kind, user) per
// dedupWindow so a flapping condition never becomes a storm.
type Publisher struct {
	writer      *kafka.Writer // nil → log only
	logger      *zap.Logger
	dedupWindow time.Duration

	mu     sync.Mutex
	lastAt map[string]time.Time
}

// New wraps a *kafka.Writer configured for Topic. writer may be nil (log only).
func New(writer *kafka.Writer, logger *zap.Logger) *Publisher {
	return &Publisher{writer: writer, logger: logger, dedupWindow: 5 * time.Minute, lastAt: map[string]time.Time{}}
}

// Alert publishes one OPS_ALERT. kind is a stable upper-snake token
// (BROADCAST_FEED_STALE, BROADCAST_ANCHOR_LOST, BROADCAST_TOKEN_FAILING…).
func (p *Publisher) Alert(ctx context.Context, kind, userID, message string) {
	logger := zap.NewNop()
	if p != nil && p.logger != nil {
		logger = p.logger
	}
	logger.Error("broadcast feed alert",
		zap.String("kind", kind), zap.String("user_id", userID), zap.String("message", message))
	if p == nil || p.writer == nil {
		return
	}

	key := kind + "|" + userID
	p.mu.Lock()
	if last, ok := p.lastAt[key]; ok && time.Since(last) < p.dedupWindow {
		p.mu.Unlock()
		return
	}
	p.lastAt[key] = time.Now()
	p.mu.Unlock()

	notif := struct {
		Type       string `json:"type"`
		Severity   string `json:"severity"`
		UserID     string `json:"user_id"`
		Symbol     string `json:"symbol"`
		Kind       string `json:"kind"`
		Title      string `json:"title"`
		Message    string `json:"message"`
		ActionHint string `json:"action_hint"`
		Timestamp  string `json:"timestamp"`
	}{
		Type:       "OPS_ALERT",
		Severity:   "critical",
		UserID:     userID,
		Kind:       kind,
		Title:      "Broadcast order feed needs attention: " + kind,
		Message:    message,
		ActionHint: "Check orderstatus logs and /metrics (broadcast_*); the per-user sockets and REST polls are unaffected",
		Timestamp:  time.Now().UTC().Format(time.RFC3339Nano),
	}
	body, err := json.Marshal(notif)
	if err != nil {
		return
	}
	writeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := p.writer.WriteMessages(writeCtx, kafka.Message{
		Key:   []byte(userID),
		Value: body,
		Headers: []kafka.Header{
			{Key: "type", Value: []byte("OPS_ALERT")},
			{Key: "severity", Value: []byte("critical")},
			{Key: "user_id", Value: []byte(userID)},
		},
	}); err != nil {
		logger.Warn("OPS_ALERT publish failed", zap.String("kind", kind), zap.Error(err))
	}
}
