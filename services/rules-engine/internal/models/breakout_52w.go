package models

import (
	"encoding/json"
	"time"
)

// Breakout52WEvent represents a 52-week high breakout signal coming from
// the `market.data.52w_breakouts` Kafka topic. This mirrors the payload
// published by the data-ingestion Redis watcher.
type Breakout52WEvent struct {
	Symbol         string  `json:"symbol"`
	Token          string  `json:"token"`
	Exchange       string  `json:"exchange"`
	LTP            float64 `json:"ltp"`
	Week52High     float64 `json:"week_52_high"`
	Week52Low      float64 `json:"week_52_low"`
	Week52HighDate string  `json:"week_52_high_date"`
	Week52LowDate  string  `json:"week_52_low_date"`
	
	// Week52HighTimestamp is the exact time when the 52-week high occurred.
	// This is CRITICAL for time-based allocation - we only allocate positions
	// for breakouts that happened AFTER the user enabled the strategy.
	Week52HighTimestamp time.Time `json:"-"` // Custom unmarshal below
	
	Timestamp      int64   `json:"timestamp"`
	LastUpdated    string  `json:"last_updated"`
	IsNewWeek52High bool   `json:"is_new_week_52_high"`
	IsNewWeek52Low  bool   `json:"is_new_week_52_low"`
}

// UnmarshalJSON implements custom JSON unmarshaling to parse the timestamp string
func (b *Breakout52WEvent) UnmarshalJSON(data []byte) error {
	// Use an auxiliary struct to avoid recursion
	type Alias Breakout52WEvent
	aux := &struct {
		Week52HighTimestampStr string `json:"week_52_high_timestamp"`
		*Alias
	}{
		Alias: (*Alias)(b),
	}
	
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	
	// Parse the timestamp string into time.Time
	// Support multiple formats: RFC3339, RFC3339Nano, and custom IST format
	if aux.Week52HighTimestampStr != "" {
		formats := []string{
			time.RFC3339Nano,
			time.RFC3339,
			"2006-01-02T15:04:05-07:00",
			"2006-01-02T15:04:05.999999999-07:00",
			"2006-01-02T15:04:05.999999999Z07:00",
			"2006-01-02T15:04:05Z07:00",
		}
		
		var parsed bool
		for _, format := range formats {
			if t, err := time.Parse(format, aux.Week52HighTimestampStr); err == nil {
				b.Week52HighTimestamp = t
				parsed = true
				break
			}
		}
		
		if !parsed {
			// If parsing fails, leave as zero time (will be skipped in allocation check)
			b.Week52HighTimestamp = time.Time{}
		}
	}
	
	return nil
}
