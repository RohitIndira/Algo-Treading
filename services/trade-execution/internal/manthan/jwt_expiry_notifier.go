package manthan

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// JWTExpiryNotifier polls active broker JWTs and publishes a user-facing
// alert to manthan.notifications when one is close to expiring.
//
// Why this exists
// ───────────────
// Indira issues short-lived JWTs (~24h). When the token expires, the
// protective replayer's 15:35 IST AMO submission and the live SafetyMonitor
// both fail with AU004 — the position is then unprotected until the operator
// notices and re-logs in. Indira does not expose a refresh-token flow, so
// human-in-the-loop is the only path. This notifier surfaces the warning
// while there's still time to act.
//
// The frontend (paperWSServer / live-orders socket) already consumes
// manthan.notifications and renders banners; emitting a JWT_EXPIRING event
// reuses that same UX path.
type JWTExpiryNotifier struct {
	brokers []string
	logger  *zap.Logger
	writer  *kafka.Writer
	enabled bool

	// activeUserIDs returns the users currently holding live Manthan orders.
	// Same provider used by Reconciler — no need to enumerate dormant users.
	activeUserIDs func() []string

	// getAuth returns the user's current creds (UserID + decrypted bearer).
	getAuth func(userID string) *BrokerAuth

	// alertWindow: warn when JWT exp - now < alertWindow.
	alertWindow time.Duration

	// pollInterval: how often we sweep all active users.
	pollInterval time.Duration

	// dedupWindow: don't re-alert the same user within this window.
	dedupWindow time.Duration

	dedupMu       sync.Mutex
	lastAlertedAt map[string]time.Time
}

// JWTExpiryNotifierConfig — caller-tunable knobs. Sensible defaults applied
// when zero-valued.
type JWTExpiryNotifierConfig struct {
	AlertWindow  time.Duration // default 8h
	PollInterval time.Duration // default 30min
	DedupWindow  time.Duration // default 2h
}

// NewJWTExpiryNotifier wires the notifier to its data sources. If brokers is
// empty, the publisher silently no-ops (useful for dev where Kafka isn't
// reachable).
func NewJWTExpiryNotifier(
	brokers []string,
	activeUserIDs func() []string,
	getAuth func(userID string) *BrokerAuth,
	logger *zap.Logger,
	cfg JWTExpiryNotifierConfig,
) *JWTExpiryNotifier {
	if cfg.AlertWindow <= 0 {
		cfg.AlertWindow = 8 * time.Hour
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 30 * time.Minute
	}
	if cfg.DedupWindow <= 0 {
		cfg.DedupWindow = 2 * time.Hour
	}

	n := &JWTExpiryNotifier{
		brokers:       brokers,
		logger:        logger,
		activeUserIDs: activeUserIDs,
		getAuth:       getAuth,
		alertWindow:   cfg.AlertWindow,
		pollInterval:  cfg.PollInterval,
		dedupWindow:   cfg.DedupWindow,
		lastAlertedAt: make(map[string]time.Time),
	}

	if len(brokers) > 0 {
		n.writer = &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        "manthan.notifications",
			Balancer:     &kafka.LeastBytes{},
			RequiredAcks: kafka.RequireAll,
			BatchTimeout: 1 * time.Millisecond,
		}
		n.enabled = true
	}
	return n
}

// Start runs the poll loop until ctx is cancelled.
//
// Quiet hours: alerts are suppressed between 21:00 and 06:00 IST so we don't
// wake the operator at midnight; the morning run picks up anything from
// overnight. The 15:35 EOD cron tolerates 1 expired JWT per user — Phase A
// will skip that user and log a warning.
func (n *JWTExpiryNotifier) Start(ctx context.Context) {
	if n == nil {
		return
	}
	n.logger.Info("JWT expiry notifier started",
		zap.Duration("poll_interval", n.pollInterval),
		zap.Duration("alert_window", n.alertWindow),
		zap.Duration("dedup_window", n.dedupWindow))

	// Run once on startup so an already-expiring token surfaces immediately.
	n.sweep(ctx)

	t := time.NewTicker(n.pollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			n.logger.Info("JWT expiry notifier stopped")
			return
		case <-t.C:
			n.sweep(ctx)
		}
	}
}

// Close flushes the Kafka writer.
func (n *JWTExpiryNotifier) Close() error {
	if n == nil || n.writer == nil {
		return nil
	}
	return n.writer.Close()
}

func (n *JWTExpiryNotifier) sweep(ctx context.Context) {
	if n == nil {
		return
	}
	// Quiet hours: 21:00–06:00 IST.
	loc, _ := time.LoadLocation("Asia/Kolkata")
	if loc == nil {
		loc = time.FixedZone("IST", 5*60*60+30*60)
	}
	hour := time.Now().In(loc).Hour()
	if hour >= 21 || hour < 6 {
		return
	}

	users := n.activeUserIDs()
	if len(users) == 0 {
		return
	}

	for _, uid := range users {
		auth := n.getAuth(uid)
		if auth == nil || auth.BearerToken == "" {
			continue
		}
		exp, err := decodeJWTExp(auth.BearerToken)
		if err != nil {
			n.logger.Debug("JWT decode failed — skipping",
				zap.String("user", uid), zap.Error(err))
			continue
		}
		remaining := time.Until(exp)
		if remaining <= 0 {
			n.publishAlert(ctx, uid, exp, "JWT already expired — re-login required to keep protection live")
			continue
		}
		if remaining < n.alertWindow {
			n.publishAlert(ctx, uid, exp,
				fmt.Sprintf("Broker session expires in %s — re-login before next pre-open to keep SL protection live",
					remaining.Round(time.Minute)))
		}
	}
}

// publishAlert emits a JWT_EXPIRING notification, respecting dedup.
func (n *JWTExpiryNotifier) publishAlert(ctx context.Context, userID string, exp time.Time, message string) {
	if !n.enabled {
		return
	}
	n.dedupMu.Lock()
	if last, ok := n.lastAlertedAt[userID]; ok && time.Since(last) < n.dedupWindow {
		n.dedupMu.Unlock()
		return
	}
	n.lastAlertedAt[userID] = time.Now()
	n.dedupMu.Unlock()

	notif := struct {
		Type       string `json:"type"`
		Severity   string `json:"severity"`
		UserID     string `json:"user_id"`
		Title      string `json:"title"`
		Message    string `json:"message"`
		ActionHint string `json:"action_hint"`
		ExpiresAt  string `json:"expires_at"`
		Timestamp  string `json:"timestamp"`
	}{
		Type:       "JWT_EXPIRING",
		Severity:   "warning",
		UserID:     userID,
		Title:      "Broker session expiring",
		Message:    message,
		ActionHint: "Re-login from the trading app to refresh the session",
		ExpiresAt:  exp.UTC().Format(time.RFC3339),
		Timestamp:  time.Now().UTC().Format(time.RFC3339Nano),
	}

	body, err := json.Marshal(notif)
	if err != nil {
		n.logger.Warn("JWT_EXPIRING marshal failed", zap.Error(err))
		return
	}

	writeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	err = n.writer.WriteMessages(writeCtx, kafka.Message{
		Key:   []byte(userID),
		Value: body,
		Headers: []kafka.Header{
			{Key: "type", Value: []byte("JWT_EXPIRING")},
			{Key: "severity", Value: []byte("warning")},
			{Key: "user_id", Value: []byte(userID)},
		},
	})
	if err != nil {
		n.logger.Warn("JWT_EXPIRING publish failed",
			zap.String("user", userID), zap.Error(err))
		return
	}
	n.logger.Info("JWT_EXPIRING notification published",
		zap.String("user", userID),
		zap.Time("expires_at", exp))
}

// decodeJWTExp pulls the `exp` claim from a JWT without verifying the
// signature (we trust the broker that minted it; if the token is invalid the
// next API call will fail with AU004 anyway).
func decodeJWTExp(token string) (time.Time, error) {
	parts := strings.Split(token, ".")
	if len(parts) < 2 {
		return time.Time{}, fmt.Errorf("invalid JWT shape")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		// Some signers pad with `=` — tolerate.
		payload, err = base64.StdEncoding.DecodeString(parts[1])
		if err != nil {
			return time.Time{}, fmt.Errorf("payload b64: %w", err)
		}
	}
	var claims struct {
		Exp int64 `json:"exp"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return time.Time{}, fmt.Errorf("claims unmarshal: %w", err)
	}
	if claims.Exp == 0 {
		return time.Time{}, fmt.Errorf("no exp claim")
	}
	return time.Unix(claims.Exp, 0), nil
}
