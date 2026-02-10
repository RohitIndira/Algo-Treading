package models

import "time"

// MarketSnapshot represents the real-time market data stored in Redis
// under keys: market:nse:<token>, market:bse:<token>
//
// This model matches the data structure described in the requirements:
// - All numeric fields for prices, volumes, etc.
// - String fields for metadata (symbol, exchange, dates)
// - Boolean flags for 52-week breakout detection
type MarketSnapshot struct {
	// Basic identification
	Symbol   string `json:"symbol"`
	Token    string `json:"token"`
	Exchange string `json:"exchange"`

	// Price data
	LTP        float64 `json:"ltp"`          // Last Traded Price
	Open       float64 `json:"open"`         // Opening price
	High       float64 `json:"high"`         // Day high
	Low        float64 `json:"low"`          // Day low
	Close      float64 `json:"close"`        // Closing price
	PrevClose  float64 `json:"prev_close"`   // Previous close
	DayHigh    float64 `json:"day_high"`     // Intraday high
	DayLow     float64 `json:"day_low"`      // Intraday low

	// Volume data
	Volume       float64 `json:"volume"`         // Trading volume
	AvgVolume5D  float64 `json:"avg_volume_5d"`  // 5-day average volume

	// Change metrics
	PercentChange float64 `json:"percent_change"` // Percentage change
	PercentValue  float64 `json:"percent_value"`  // Absolute change value

	// 52-week data
	Week52High          float64 `json:"week_52_high"`            // 52-week high price
	Week52Low           float64 `json:"week_52_low"`             // 52-week low price
	Week52HighDate      string  `json:"week_52_high_date"`       // Date of 52W high (YYYY-MM-DD in IST)
	Week52LowDate       string  `json:"week_52_low_date"`        // Date of 52W low (YYYY-MM-DD in IST)
	Week52HighTimestamp string  `json:"week_52_high_timestamp"`  // Exact timestamp of 52W high (RFC3339 in IST)
	Week52LowTimestamp  string  `json:"week_52_low_timestamp"`   // Exact timestamp of 52W low (RFC3339 in IST)

	// Breakout flags (set by data provider)
	IsNewWeek52High bool `json:"is_new_week_52_high"` // Flag indicating new 52W high
	IsNewWeek52Low  bool `json:"is_new_week_52_low"`  // Flag indicating new 52W low

	// Timestamps (IST timezone)
	Timestamp   int64  `json:"timestamp"`    // Unix timestamp in milliseconds
	LastUpdated string `json:"last_updated"` // RFC3339 timestamp string (IST)
}

// IsValid performs basic validation on the market snapshot
func (m *MarketSnapshot) IsValid() bool {
	if m.Symbol == "" || m.Token == "" {
		return false
	}
	if m.LTP <= 0 {
		return false
	}
	if m.Exchange == "" {
		return false
	}
	return true
}

// GetDateIST extracts the date portion (YYYY-MM-DD) from LastUpdated or Timestamp
// Returns the date in IST timezone
func (m *MarketSnapshot) GetDateIST() (string, bool) {
	loc, _ := time.LoadLocation("Asia/Kolkata")
	
	// Try parsing LastUpdated first (RFC3339 format)
	if m.LastUpdated != "" {
		if t, err := time.Parse(time.RFC3339Nano, m.LastUpdated); err == nil {
			return t.In(loc).Format("2006-01-02"), true
		}
		if t, err := time.Parse(time.RFC3339, m.LastUpdated); err == nil {
			return t.In(loc).Format("2006-01-02"), true
		}
	}

	// Fallback to Timestamp (milliseconds)
	if m.Timestamp > 0 {
		t := time.Unix(0, m.Timestamp*int64(time.Millisecond)).In(loc)
		return t.Format("2006-01-02"), true
	}

	return "", false
}

// RedisKey returns the Redis key for this market snapshot
func (m *MarketSnapshot) RedisKey() string {
	return "market:" + m.Exchange + ":" + m.Token
}
