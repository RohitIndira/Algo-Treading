package detector

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/RohitIndira/Algo-Treading/pkg/logger"
	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/internal/models"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// BreakoutDetector identifies 52-week high breakouts from market data
// Detection logic:
// 1. week_52_high_date must equal today (IST)
// 2. LTP must be valid (> 0)
// 3. Not already processed today (persistent dedupe via StateManager)
type BreakoutDetector struct {
	stateMgr *StateManager
	logger   *logger.Logger
}

// NewBreakoutDetector creates a new breakout detector
func NewBreakoutDetector(stateMgr *StateManager, lgr *logger.Logger) *BreakoutDetector {
	return &BreakoutDetector{
		stateMgr: stateMgr,
		logger:   lgr,
	}
}

// DetectBreakout checks if a market snapshot represents a 52-week breakout
// Returns a breakout event if detected, nil otherwise
func (d *BreakoutDetector) DetectBreakout(ctx context.Context, snap *models.MarketSnapshot) (*models.BreakoutEvent, error) {
	// Validate snapshot
	if !snap.IsValid() {
		d.logger.Debug("Invalid market snapshot",
			zap.String("symbol", snap.Symbol),
			zap.String("token", snap.Token),
			zap.Float64("ltp", snap.LTP))
		return nil, nil
	}

	// Normalize exchange
	exchange := strings.ToUpper(snap.Exchange)
	if exchange == "" {
		d.logger.Debug("Missing exchange in snapshot",
			zap.String("token", snap.Token))
		return nil, nil
	}
	snap.Exchange = exchange

	// Get today's date in IST
	today := d.getTodayIST()

	// Extract the date from the snapshot
	snapDate, ok := snap.GetDateIST()
	if !ok {
		d.logger.Debug("Could not extract date from snapshot",
			zap.String("token", snap.Token),
			zap.String("last_updated", snap.LastUpdated),
			zap.Int64("timestamp", snap.Timestamp))
		return nil, nil
	}

	// Only process snapshots from today
	if snapDate != today {
		d.logger.Debug("Snapshot not from today",
			zap.String("token", snap.Token),
			zap.String("snap_date", snapDate),
			zap.String("today", today))
		return nil, nil
	}

	// Check if week_52_high_date matches today
	if snap.Week52HighDate == "" {
		return nil, nil
	}

	if snap.Week52HighDate != today {
		d.logger.Debug("52-week high date not today",
			zap.String("token", snap.Token),
			zap.String("week_52_high_date", snap.Week52HighDate),
			zap.String("today", today))
		return nil, nil
	}

	// Check timestamp - must be present for proper dedupe
	if snap.Week52HighTimestamp == "" {
		d.logger.Warn("Missing 52-week high timestamp",
			zap.String("token", snap.Token),
			zap.String("symbol", snap.Symbol))
		return nil, nil
	}

	// Check persistent state with timestamp to avoid duplicates
	// This allows multiple breakouts same day (if price keeps increasing)
	alreadyProcessed, err := d.stateMgr.IsBreakoutProcessed(ctx, exchange, snap.Token, today, snap.Week52HighTimestamp)
	if err != nil {
		d.logger.Error("Failed to check breakout state",
			zap.String("token", snap.Token),
			zap.Error(err))
		return nil, err
	}

	if alreadyProcessed {
		d.logger.Debug("Breakout timestamp already processed",
			zap.String("token", snap.Token),
			zap.String("exchange", exchange),
			zap.String("timestamp", snap.Week52HighTimestamp))
		return nil, nil
	}

	// Create breakout event
	eventID := uuid.New().String()
	event := models.NewBreakoutEventFromSnapshot(snap, eventID)

	d.logger.Info("52-week breakout detected",
		zap.String("event_id", eventID),
		zap.String("symbol", snap.Symbol),
		zap.String("token", snap.Token),
		zap.String("exchange", exchange),
		zap.Float64("ltp", snap.LTP),
		zap.Float64("week_52_high", snap.Week52High),
		zap.String("week_52_high_date", snap.Week52HighDate),
		zap.String("week_52_high_timestamp", snap.Week52HighTimestamp))

	// Mark as processed with timestamp
	if err := d.stateMgr.MarkBreakoutProcessed(ctx, exchange, snap.Token, today, snap.Week52HighTimestamp); err != nil {
		d.logger.Error("Failed to mark breakout as processed",
			zap.String("token", snap.Token),
			zap.String("timestamp", snap.Week52HighTimestamp),
			zap.Error(err))
		return nil, err
	}

	return event, nil
}

// getTodayIST returns today's date in IST timezone (YYYY-MM-DD)
func (d *BreakoutDetector) getTodayIST() string {
	loc, _ := time.LoadLocation("Asia/Kolkata")
	return time.Now().In(loc).Format("2006-01-02")
}

// ValidateBreakoutCriteria performs additional validation on breakout criteria
// This can be extended with more sophisticated checks (volume, price movement, etc.)
func (d *BreakoutDetector) ValidateBreakoutCriteria(snap *models.MarketSnapshot) error {
	// Ensure LTP is close to 52W high (within 1%)
	if snap.Week52High > 0 {
		percentDiff := ((snap.Week52High - snap.LTP) / snap.Week52High) * 100
		if percentDiff > 1.0 {
			return fmt.Errorf("LTP (%.2f) is %.2f%% below 52W high (%.2f)",
				snap.LTP, percentDiff, snap.Week52High)
		}
	}

	// Ensure volume is reasonable (if available)
	if snap.Volume < 0 {
		return fmt.Errorf("invalid volume: %.0f", snap.Volume)
	}

	return nil
}
