package models

import (
	"encoding/json"
	"time"
)

// BreakoutEvent represents a 52-week high breakout event published to Kafka
// This matches the structure expected by the rules-engine service
type BreakoutEvent struct {
	// Identification
	Symbol   string `json:"symbol"`
	Token    string `json:"token"`
	Exchange string `json:"exchange"`

	// Price data at breakout
	LTP            float64 `json:"ltp"`
	Open           float64 `json:"open,omitempty"`
	High           float64 `json:"high,omitempty"`
	Low            float64 `json:"low,omitempty"`
	Close          float64 `json:"close,omitempty"`
	PrevClose      float64 `json:"prev_close,omitempty"`
	Volume         float64 `json:"volume,omitempty"`
	PercentChange  float64 `json:"percent_change,omitempty"`
	PercentValue   float64 `json:"percent_value,omitempty"`

	// 52-week context
	Week52High          float64 `json:"week_52_high"`
	Week52Low           float64 `json:"week_52_low"`
	Week52HighDate      string  `json:"week_52_high_date"`       // YYYY-MM-DD in IST
	Week52LowDate       string  `json:"week_52_low_date,omitempty"`
	Week52HighTimestamp string  `json:"week_52_high_timestamp"`  // Exact breakout time from odin-streamer
	Week52LowTimestamp  string  `json:"week_52_low_timestamp,omitempty"`

	// Additional metadata
	DayHigh      float64 `json:"day_high,omitempty"`
	DayLow       float64 `json:"day_low,omitempty"`
	AvgVolume5D  float64 `json:"avg_volume_5d,omitempty"`

	// Flags
	IsNewWeek52High bool `json:"is_new_week_52_high"`
	IsNewWeek52Low  bool `json:"is_new_week_52_low"`

	// Timestamps (IST)
	Timestamp   int64  `json:"timestamp"`    // Unix milliseconds
	LastUpdated string `json:"last_updated"` // RFC3339 in IST

	// Event metadata (added by data-ingestion)
	DetectedAt time.Time `json:"detected_at"` // When we detected this breakout
	EventID    string    `json:"event_id"`    // Unique event identifier
}

// NewBreakoutEventFromSnapshot creates a breakout event from a market snapshot
func NewBreakoutEventFromSnapshot(snap *MarketSnapshot, eventID string) *BreakoutEvent {
	return &BreakoutEvent{
		Symbol:              snap.Symbol,
		Token:               snap.Token,
		Exchange:            snap.Exchange,
		LTP:                 snap.LTP,
		Open:                snap.Open,
		High:                snap.High,
		Low:                 snap.Low,
		Close:               snap.Close,
		PrevClose:           snap.PrevClose,
		Volume:              snap.Volume,
		PercentChange:       snap.PercentChange,
		PercentValue:        snap.PercentValue,
		Week52High:          snap.Week52High,
		Week52Low:           snap.Week52Low,
		Week52HighDate:      snap.Week52HighDate,
		Week52LowDate:       snap.Week52LowDate,
		Week52HighTimestamp: snap.Week52HighTimestamp, // Exact breakout time from Redis
		Week52LowTimestamp:  snap.Week52LowTimestamp,
		DayHigh:             snap.DayHigh,
		DayLow:              snap.DayLow,
		AvgVolume5D:         snap.AvgVolume5D,
		IsNewWeek52High:     snap.IsNewWeek52High,
		IsNewWeek52Low:      snap.IsNewWeek52Low,
		Timestamp:           snap.Timestamp,
		LastUpdated:         snap.LastUpdated,
		DetectedAt:          time.Now(),
		EventID:             eventID,
	}
}

// ToJSON serializes the breakout event to JSON
func (b *BreakoutEvent) ToJSON() ([]byte, error) {
	return json.Marshal(b)
}

// Key returns the Kafka message key (token)
func (b *BreakoutEvent) Key() []byte {
	return []byte(b.Token)
}
