package models

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
	Timestamp      int64   `json:"timestamp"`
	LastUpdated    string  `json:"last_updated"`
	IsNewWeek52High bool   `json:"is_new_week_52_high"`
	IsNewWeek52Low  bool   `json:"is_new_week_52_low"`
}
