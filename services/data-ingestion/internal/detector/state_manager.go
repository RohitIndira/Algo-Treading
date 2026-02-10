package detector

import (
	"context"
	"fmt"
	"time"

	redispkg "github.com/RohitIndira/Algo-Treading/pkg/database/redis"
	"github.com/RohitIndira/Algo-Treading/pkg/logger"
	"go.uber.org/zap"
)

// StateManager manages persistent state for breakout tracking in Redis
// This prevents duplicate breakout events across service restarts
type StateManager struct {
	client *redispkg.Client
	logger *logger.Logger
}

// NewStateManager creates a new state manager
func NewStateManager(client *redispkg.Client, lgr *logger.Logger) *StateManager {
	return &StateManager{
		client: client,
		logger: lgr,
	}
}

// IsBreakoutProcessed checks if this specific breakout timestamp has already been processed
// Key format: 52w:breakouts:exchange:token:YYYY-MM-DD
// Value: exact timestamp of the breakout
// This allows multiple breakouts for the same stock on the same day (if price keeps increasing)
func (s *StateManager) IsBreakoutProcessed(ctx context.Context, exchange, token, date, timestamp string) (bool, error) {
	key := s.getBreakoutTimestampKey(exchange, token, date)

	// Get stored timestamp (if exists)
	storedTimestamp, err := s.client.Get(ctx, key).Result()
	if err != nil {
		// Key doesn't exist - this is a new breakout
		if err.Error() == "redis: nil" {
			return false, nil
		}
		s.logger.Error("Failed to check breakout state",
			zap.String("key", key),
			zap.Error(err))
		return false, err
	}

	// Compare timestamps
	if storedTimestamp != timestamp {
		// Different timestamp = NEW breakout (same day, higher price)
		s.logger.Info("New higher breakout detected",
			zap.String("exchange", exchange),
			zap.String("token", token),
			zap.String("old_timestamp", storedTimestamp),
			zap.String("new_timestamp", timestamp))
		return false, nil
	}

	// Same timestamp = already processed
	return true, nil
}

// MarkBreakoutProcessed marks a breakout timestamp as processed
// Stores the exact timestamp to allow multiple breakouts same day
// Sets TTL to 24 hours (auto-cleanup next day)
func (s *StateManager) MarkBreakoutProcessed(ctx context.Context, exchange, token, date, timestamp string) error {
	key := s.getBreakoutTimestampKey(exchange, token, date)

	// Store the exact timestamp
	if err := s.client.Set(ctx, key, timestamp, 24*time.Hour).Err(); err != nil {
		s.logger.Error("Failed to mark breakout as processed",
			zap.String("key", key),
			zap.String("timestamp", timestamp),
			zap.Error(err))
		return err
	}

	s.logger.Debug("Marked breakout timestamp as processed",
		zap.String("exchange", exchange),
		zap.String("token", token),
		zap.String("date", date),
		zap.String("timestamp", timestamp))

	return nil
}

// GetTodayBreakouts returns all breakouts processed today
// Useful for monitoring and debugging
func (s *StateManager) GetTodayBreakouts(ctx context.Context) ([]string, error) {
	today := s.getTodayIST()
	key := s.getBreakoutSetKey(today)

	members, err := s.client.SetMembers(ctx, key)
	if err != nil {
		return nil, err
	}

	return members, nil
}

// CleanupOldBreakouts removes breakout sets older than 7 days
// This is a safety cleanup in case TTL doesn't work properly
func (s *StateManager) CleanupOldBreakouts(ctx context.Context) error {
	s.logger.Info("Running cleanup of old breakout sets")

	loc, _ := time.LoadLocation("Asia/Kolkata")
	now := time.Now().In(loc)

	// Delete sets from 8-30 days ago (keep last 7 days)
	for i := 8; i <= 30; i++ {
		date := now.AddDate(0, 0, -i).Format("2006-01-02")
		key := s.getBreakoutSetKey(date)

		if err := s.client.Delete(ctx, key); err != nil {
			s.logger.Warn("Failed to delete old breakout set",
				zap.String("key", key),
				zap.Error(err))
			// Continue with other deletions
		} else {
			s.logger.Debug("Deleted old breakout set", zap.String("key", key))
		}
	}

	return nil
}

// getBreakoutSetKey returns the Redis key for a date's breakout set
func (s *StateManager) getBreakoutSetKey(date string) string {
	return fmt.Sprintf("52w:breakouts:%s", date)
}

// getBreakoutTimestampKey returns the Redis key for storing a breakout's timestamp
// Allows multiple breakouts per stock per day (tracks by timestamp)
func (s *StateManager) getBreakoutTimestampKey(exchange, token, date string) string {
	return fmt.Sprintf("52w:breakouts:%s:%s:%s", exchange, token, date)
}

// getTodayIST returns today's date in IST (YYYY-MM-DD)
func (s *StateManager) getTodayIST() string {
	loc, _ := time.LoadLocation("Asia/Kolkata")
	return time.Now().In(loc).Format("2006-01-02")
}

// GetBreakoutCount returns the count of breakouts processed for a given date
func (s *StateManager) GetBreakoutCount(ctx context.Context, date string) (int64, error) {
	key := s.getBreakoutSetKey(date)
	count, err := s.client.SCard(ctx, key).Result()
	if err != nil {
		return 0, err
	}
	return count, nil
}
