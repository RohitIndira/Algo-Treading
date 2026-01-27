package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

// PnLStreamMeta describes the current PnL stream for a user.
// A stream is a logical sequence of PnL snapshots tied to a
// specific strategy configuration/version.
type PnLStreamMeta struct {
	StreamID   string `json:"stream_id"`
	Channel    string `json:"channel"`
	StrategyID string `json:"strategy_id"`
	UpdatedAt  int64  `json:"updated_at"`
}

// PnLStreamManager tracks the active PnL stream per user and
// publishes control-plane events to Redis when streams rotate.
//
// It is intentionally lightweight: in-memory index for fast
// lookups, with best-effort persistence to Redis so that other
// components (e.g. API gateway) can discover the current stream
// for a user.
type PnLStreamManager struct {
	mu     sync.RWMutex
	active map[string]*PnLStreamMeta // userID -> meta
	cache  *RedisCache
	logger *zap.Logger
	ttl    time.Duration
}

// NewPnLStreamManager constructs a new manager. The ttl is used when
// persisting the "active" mapping into Redis; pass 0 to use the
// RedisCache default TTL.
func NewPnLStreamManager(cache *RedisCache, logger *zap.Logger, ttl time.Duration) *PnLStreamManager {
	return &PnLStreamManager{
		active: make(map[string]*PnLStreamMeta),
		cache:  cache,
		logger: logger,
		ttl:    ttl,
	}
}

// GetOrCreateStream returns the currently active PnL stream for a
// user, creating a new one if none exists yet.
func (m *PnLStreamManager) GetOrCreateStream(ctx context.Context, userID, strategyID string) (*PnLStreamMeta, error) {
	if userID == "" {
		return nil, fmt.Errorf("userID cannot be empty")
	}

	m.mu.RLock()
	meta, ok := m.active[userID]
	m.mu.RUnlock()
	if ok && meta != nil {
		return meta, nil
	}

	// No active stream in memory yet; rotate to create one.
	return m.RotateStream(ctx, userID, strategyID)
}

// RotateStream creates a new PnL stream for the given user and
// strategy. It updates in-memory state, persists the active mapping
// to Redis, and emits a control-plane switch event.
func (m *PnLStreamManager) RotateStream(ctx context.Context, userID, strategyID string) (*PnLStreamMeta, error) {
	if userID == "" {
		return nil, fmt.Errorf("userID cannot be empty")
	}

	// Snapshot previous stream (if any)
	m.mu.Lock()
	old := m.active[userID]

	streamID := fmt.Sprintf("%d", time.Now().UnixNano())
	channel := fmt.Sprintf("user:%s:pnl:%s", userID, streamID)
	meta := &PnLStreamMeta{
		StreamID:   streamID,
		Channel:    channel,
		StrategyID: strategyID,
		UpdatedAt:  time.Now().Unix(),
	}
	m.active[userID] = meta
	m.mu.Unlock()

	// 1) Persist mapping in Redis so other components can discover it.
	if m.cache != nil {
		key := fmt.Sprintf("user:%s:pnl:active", userID)
		if err := m.cache.Set(ctx, key, meta, m.ttl); err != nil {
			m.logger.Warn("Failed to persist active PnL stream to Redis",
				zap.String("user_id", userID),
				zap.Error(err))
		}
	}

	// 2) Publish control-plane switch event. This is best-effort; we
	// don't fail the rotation if publishing fails.
	ctrlEvent := map[string]any{
		"type":          "switch",
		"user_id":       userID,
		"new_stream_id": meta.StreamID,
		"new_channel":   meta.Channel,
		"strategy_id":   meta.StrategyID,
	}
	if old != nil {
		ctrlEvent["old_stream_id"] = old.StreamID
		ctrlEvent["old_channel"] = old.Channel
	}

	if m.cache != nil {
		payload, err := json.Marshal(ctrlEvent)
		if err != nil {
			m.logger.Warn("Failed to marshal PnL stream switch event",
				zap.String("user_id", userID),
				zap.Error(err))
		} else {
			ctrlChannel := fmt.Sprintf("user:%s:pnl:control", userID)
			if err := m.cache.Publish(ctx, ctrlChannel, string(payload)); err != nil {
				m.logger.Warn("Failed to publish PnL stream switch event",
					zap.String("user_id", userID),
					zap.String("channel", ctrlChannel),
					zap.Error(err))
			}
		}
	}

	m.logger.Info("Rotated PnL stream",
		zap.String("user_id", userID),
		zap.String("strategy_id", strategyID),
		zap.String("stream_id", meta.StreamID),
		zap.String("channel", meta.Channel))

	return meta, nil
}

