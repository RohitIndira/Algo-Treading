package cash52w

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
	"go.uber.org/zap"
)

// ============================================================================
// POSITION TRACKER - Persistent Position State Management
// ============================================================================
//
// Manages the full lifecycle of 52W positions:
// - OPEN: Initial entry
// - PARTIAL_EXIT: One or more exit levels triggered
// - CLOSED: Fully exited
//
// Tracks:
// - Original entry quantity & price
// - Remaining quantity after partial exits
// - Realized P&L from partial exits
// - Unrealized P&L on remaining position
// - Exit history (which levels triggered, when, at what price)
//
// Future: Persist to Kafka topic for crash recovery
// ============================================================================

// PositionStatus represents the current state of a position
type PositionStatus string

const (
	StatusOpen        PositionStatus = "OPEN"
	StatusPartialExit PositionStatus = "PARTIAL_EXIT"
	StatusClosed      PositionStatus = "CLOSED"
)

// ExitRecord tracks a single exit event
type ExitRecord struct {
	Timestamp      time.Time `json:"timestamp"`
	LevelType      string    `json:"level_type"`       // "PROFIT" / "STOPLOSS" / "FORCE_EXIT"
	LevelNumber    int       `json:"level_number"`     // 1, 2, 3
	ExitPrice      float64   `json:"exit_price"`
	Quantity       int32     `json:"quantity"`
	RealizedPnL    float64   `json:"realized_pnl"`     // Actual profit/loss on this exit
	RealizedPnLPct float64   `json:"realized_pnl_pct"` // Percentage return
}

// TrackedPosition represents a position with full history
type TrackedPosition struct {
	// Identity
	UserID   string `json:"user_id"`
	Token    string `json:"token"`
	Symbol   string `json:"symbol"`
	Exchange string `json:"exchange"`

	// Entry details
	EntryTime     time.Time `json:"entry_time"`
	EntryPrice    float64   `json:"entry_price"`
	EntryQuantity int32     `json:"entry_quantity"`

	// Current state
	Status            PositionStatus `json:"status"`
	RemainingQuantity int32          `json:"remaining_quantity"`
	
	// P&L tracking
	RealizedPnL       float64 `json:"realized_pnl"`         // Total from all exits
	UnrealizedPnL     float64 `json:"unrealized_pnl"`       // Current on remaining qty
	TotalPnL          float64 `json:"total_pnl"`            // Realized + Unrealized
	
	// Exit history
	ExitHistory []ExitRecord `json:"exit_history"`
	
	// Timestamps
	LastUpdated time.Time `json:"last_updated"`
	ClosedTime  time.Time `json:"closed_time,omitempty"`
}

// PositionTracker manages all tracked positions
type PositionTracker struct {
	mu        sync.RWMutex
	positions map[string]map[string]*TrackedPosition // userID -> token -> position
	logger    *zap.Logger
	kafkaPub  KafkaPublisher // Interface for Kafka publishing
}

// KafkaPublisher interface for publishing position state events
type KafkaPublisher interface {
	Publish(ctx context.Context, topic string, key []byte, value []byte) error
}

// NewPositionTracker creates a new position tracker
func NewPositionTracker(logger *zap.Logger, kafkaPub KafkaPublisher) *PositionTracker {
	return &PositionTracker{
		positions: make(map[string]map[string]*TrackedPosition),
		logger:    logger,
		kafkaPub:  kafkaPub,
	}
}

// TrackNewPosition records a new position entry
func (pt *PositionTracker) TrackNewPosition(
	userID, token, symbol, exchange string,
	entryPrice float64,
	quantity int32,
) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	if pt.positions[userID] == nil {
		pt.positions[userID] = make(map[string]*TrackedPosition)
	}

	pos := &TrackedPosition{
		UserID:            userID,
		Token:             token,
		Symbol:            symbol,
		Exchange:          exchange,
		EntryTime:         time.Now(),
		EntryPrice:        entryPrice,
		EntryQuantity:     quantity,
		Status:            StatusOpen,
		RemainingQuantity: quantity,
		RealizedPnL:       0,
		UnrealizedPnL:     0,
		TotalPnL:          0,
		ExitHistory:       []ExitRecord{},
		LastUpdated:       time.Now(),
	}

	pt.positions[userID][token] = pos

	pt.logger.Info("Position tracked",
		zap.String("user_id", userID),
		zap.String("symbol", symbol),
		zap.String("token", token),
		zap.Int32("quantity", quantity),
		zap.Float64("entry_price", entryPrice))
	
	// Publish position state to Kafka
	pt.publishPositionState(context.Background(), pos)
}

// publishPositionState publishes a position state event to Kafka
func (pt *PositionTracker) publishPositionState(ctx context.Context, pos *TrackedPosition) {
	if pt.kafkaPub == nil {
		return
	}

	data, err := json.Marshal(pos)
	if err != nil {
		pt.logger.Error("Failed to marshal position state",
			zap.Error(err),
			zap.String("user_id", pos.UserID),
			zap.String("token", pos.Token))
		return
	}

	// Use token as key for partitioning
	key := []byte(pos.Token)

	if err := pt.kafkaPub.Publish(ctx, "position-states", key, data); err != nil {
		pt.logger.Error("Failed to publish position state to Kafka",
			zap.Error(err),
			zap.String("user_id", pos.UserID),
			zap.String("token", pos.Token),
			zap.String("status", string(pos.Status)))
	} else {
		pt.logger.Debug("Position state published to Kafka",
			zap.String("user_id", pos.UserID),
			zap.String("token", pos.Token),
			zap.String("symbol", pos.Symbol),
			zap.String("status", string(pos.Status)))
	}
}

// RecordExit records a partial or full exit
func (pt *PositionTracker) RecordExit(
	userID, token string,
	levelType string,
	levelNumber int,
	exitPrice float64,
	quantity int32,
) error {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	userPositions, ok := pt.positions[userID]
	if !ok {
		return fmt.Errorf("user %s has no tracked positions", userID)
	}

	pos, ok := userPositions[token]
	if !ok {
		return fmt.Errorf("position not found: user=%s token=%s", userID, token)
	}

	if quantity > pos.RemainingQuantity {
		return fmt.Errorf("exit quantity (%d) exceeds remaining quantity (%d)",
			quantity, pos.RemainingQuantity)
	}

	// Calculate realized P&L for this exit
	pnlPerShare := exitPrice - pos.EntryPrice
	realizedPnL := pnlPerShare * float64(quantity)
	realizedPnLPct := 0.0
	if pos.EntryPrice > 0 {
		realizedPnLPct = (pnlPerShare / pos.EntryPrice) * 100.0
	}

	// Record the exit
	exitRecord := ExitRecord{
		Timestamp:      time.Now(),
		LevelType:      levelType,
		LevelNumber:    levelNumber,
		ExitPrice:      exitPrice,
		Quantity:       quantity,
		RealizedPnL:    realizedPnL,
		RealizedPnLPct: realizedPnLPct,
	}

	pos.ExitHistory = append(pos.ExitHistory, exitRecord)
	pos.RemainingQuantity -= quantity
	pos.RealizedPnL += realizedPnL
	pos.LastUpdated = time.Now()

	// Update status
	if pos.RemainingQuantity == 0 {
		pos.Status = StatusClosed
		pos.ClosedTime = time.Now()
	} else {
		pos.Status = StatusPartialExit
	}

	pt.logger.Info("Exit recorded",
		zap.String("user_id", userID),
		zap.String("symbol", pos.Symbol),
		zap.String("token", token),
		zap.String("level_type", levelType),
		zap.Int("level_number", levelNumber),
		zap.Int32("exit_qty", quantity),
		zap.Int32("remaining_qty", pos.RemainingQuantity),
		zap.Float64("realized_pnl", realizedPnL),
		zap.Float64("realized_pnl_pct", realizedPnLPct),
		zap.String("status", string(pos.Status)))

	// Publish updated position state to Kafka
	pt.publishPositionState(context.Background(), pos)

	return nil
}

// UpdateUnrealizedPnL updates the unrealized P&L based on current market price
func (pt *PositionTracker) UpdateUnrealizedPnL(userID, token string, currentPrice float64) error {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	userPositions, ok := pt.positions[userID]
	if !ok {
		return nil // No positions for this user
	}

	pos, ok := userPositions[token]
	if !ok {
		return nil // Position not found
	}

	if pos.RemainingQuantity == 0 {
		pos.UnrealizedPnL = 0
		pos.TotalPnL = pos.RealizedPnL
		return nil
	}

	// Calculate unrealized P&L on remaining quantity
	pnlPerShare := currentPrice - pos.EntryPrice
	pos.UnrealizedPnL = pnlPerShare * float64(pos.RemainingQuantity)
	pos.TotalPnL = pos.RealizedPnL + pos.UnrealizedPnL
	pos.LastUpdated = time.Now()

	return nil
}

// GetPosition retrieves a tracked position
func (pt *PositionTracker) GetPosition(userID, token string) (*TrackedPosition, bool) {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	userPositions, ok := pt.positions[userID]
	if !ok {
		return nil, false
	}

	pos, ok := userPositions[token]
	if !ok {
		return nil, false
	}

	// Return a copy to prevent external modification
	posCopy := *pos
	posCopy.ExitHistory = make([]ExitRecord, len(pos.ExitHistory))
	copy(posCopy.ExitHistory, pos.ExitHistory)

	return &posCopy, true
}

// GetUserPositions returns all positions for a user
func (pt *PositionTracker) GetUserPositions(userID string, includeClosed bool) []*TrackedPosition {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	userPositions, ok := pt.positions[userID]
	if !ok {
		return nil
	}

	result := make([]*TrackedPosition, 0, len(userPositions))
	for _, pos := range userPositions {
		// Skip closed positions unless requested
		if pos.Status == StatusClosed && !includeClosed {
			continue
		}

		posCopy := *pos
		posCopy.ExitHistory = make([]ExitRecord, len(pos.ExitHistory))
		copy(posCopy.ExitHistory, pos.ExitHistory)
		result = append(result, &posCopy)
	}

	return result
}

// GetAllPositions returns all tracked positions (for monitoring/debugging)
func (pt *PositionTracker) GetAllPositions() map[string]map[string]*TrackedPosition {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	// Deep copy
	result := make(map[string]map[string]*TrackedPosition)
	for userID, userPositions := range pt.positions {
		result[userID] = make(map[string]*TrackedPosition)
		for token, pos := range userPositions {
			posCopy := *pos
			posCopy.ExitHistory = make([]ExitRecord, len(pos.ExitHistory))
			copy(posCopy.ExitHistory, pos.ExitHistory)
			result[userID][token] = &posCopy
		}
	}

	return result
}

// GetStats returns position tracking statistics
func (pt *PositionTracker) GetStats() map[string]interface{} {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	totalUsers := len(pt.positions)
	totalPositions := 0
	openPositions := 0
	partialExitPositions := 0
	closedPositions := 0

	for _, userPositions := range pt.positions {
		for _, pos := range userPositions {
			totalPositions++
			switch pos.Status {
			case StatusOpen:
				openPositions++
			case StatusPartialExit:
				partialExitPositions++
			case StatusClosed:
				closedPositions++
			}
		}
	}

	return map[string]interface{}{
		"total_users":            totalUsers,
		"total_positions":        totalPositions,
		"open_positions":         openPositions,
		"partial_exit_positions": partialExitPositions,
		"closed_positions":       closedPositions,
	}
}

// CleanupOldPositions removes fully closed positions older than the specified duration
func (pt *PositionTracker) CleanupOldPositions(olderThan time.Duration) int {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	cutoff := time.Now().Add(-olderThan)
	removed := 0

	for userID, userPositions := range pt.positions {
		for token, pos := range userPositions {
			if pos.Status == StatusClosed && pos.ClosedTime.Before(cutoff) {
				delete(userPositions, token)
				removed++
			}
		}
		
		// Remove user entry if no positions remain
		if len(userPositions) == 0 {
			delete(pt.positions, userID)
		}
	}

	if removed > 0 {
		pt.logger.Info("Cleaned up old positions",
			zap.Int("removed_count", removed),
			zap.Duration("older_than", olderThan))
	}

	return removed
}

// ExportToJSON exports all positions as JSON (for backup/debugging)
func (pt *PositionTracker) ExportToJSON() ([]byte, error) {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	return json.MarshalIndent(pt.positions, "", "  ")
}

// ImportFromJSON imports positions from JSON (for restoration)
func (pt *PositionTracker) ImportFromJSON(data []byte) error {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	var imported map[string]map[string]*TrackedPosition
	if err := json.Unmarshal(data, &imported); err != nil {
		return fmt.Errorf("failed to unmarshal positions: %w", err)
	}

	pt.positions = imported
	pt.logger.Info("Imported positions from JSON",
		zap.Int("user_count", len(imported)))

	return nil
}
