package models

import (
	"encoding/json"
	"time"
)

// MarketEvent represents a market event from Kafka (market depth based)
type MarketEvent struct {
	EventID    string     `json:"event_id"`
	EventType  string     `json:"event_type"`
	Timestamp  time.Time  `json:"timestamp"`
	StockData  StockData  `json:"stock_data"`
	MarketData MarketData `json:"market_data"`
}

// MongoDBID represents MongoDB's _id field structure
type MongoDBID struct {
	OID string `json:"$oid"`
}

// UnmarshalJSON custom unmarshaling to handle both event_id and MongoDB's _id
func (e *MarketEvent) UnmarshalJSON(data []byte) error {
	// Create a temporary struct with all possible field mappings
	type Alias MarketEvent
	aux := &struct {
		MongoID interface{} `json:"_id"` // Can be string or object
		*Alias
	}{
		Alias: (*Alias)(e),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	// If EventID is empty, try to get it from MongoDB's _id
	if e.EventID == "" && aux.MongoID != nil {
		switch v := aux.MongoID.(type) {
		case string:
			e.EventID = v
		case map[string]interface{}:
			// Handle {"$oid": "..."}
			if oid, ok := v["$oid"].(string); ok {
				e.EventID = oid
			}
		}
	}

	return nil
}

// StockData contains stock information
type StockData struct {
	StockCode   int64  `json:"stock_code"` // Primary stock code (based on active exchange)
	NSECode     int64  `json:"nse_code"`   // NSE-specific code (for Redis lookup)
	BSECode     int64  `json:"bse_code"`   // BSE-specific code (for Redis lookup)
	Exchange    string `json:"exchange"`
	Symbol      string `json:"symbol"`
	ISIN        string `json:"isin"`
	CompanyName string `json:"company_name"`
}

// DepthMetrics contains market depth analysis metrics
type DepthMetrics struct {
	SpreadPct       float64 `json:"spread_pct"`
	BidAskRatio     float64 `json:"bid_ask_ratio"`
	TotalBidQty     int64   `json:"total_bid_qty"`
	TotalAskQty     int64   `json:"total_ask_qty"`
	ImbalanceRatio  float64 `json:"imbalance_ratio"`   // (bid_qty - ask_qty) / (bid_qty + ask_qty)
	LTPPositionType string  `json:"ltp_position_type"` // "between_spread", "above_ask", "below_bid"
}

// MarketData contains market depth data
type MarketData struct {
	LastTradedPrice float64  `json:"last_traded_price"`
	PctChange       float64  `json:"pct_change"`
	PriceMap        PriceMap `json:"price_map"`
	// Market depth - bid/ask levels (required)
	BidPrices     []float64 `json:"bid_prices"`
	BidQuantities []int     `json:"bid_quantities"`
	AskPrices     []float64 `json:"ask_prices"`
	AskQuantities []int     `json:"ask_quantities"`
	// Calculated depth metrics
	DepthMetrics DepthMetrics `json:"depth_metrics"`
}

// PriceMap contains OHLCV data
type PriceMap struct {
	Open   float64 `json:"open"`
	High   float64 `json:"high"`
	Low    float64 `json:"low"`
	Volume int64   `json:"volume"`
}

// Validate validates a market event
func (e *MarketEvent) Validate() error {
	if e.EventID == "" {
		return ErrInvalidEventID
	}
	// Auto-set event_type if missing
	if e.EventType == "" {
		e.EventType = "market_depth" // Default for market depth events
	}
	if e.StockData.StockCode <= 0 {
		return ErrInvalidStockCode
	}
	if e.StockData.Exchange == "" {
		return ErrInvalidExchange
	}
	// Market depth must have bid/ask prices and quantities
	if len(e.MarketData.BidPrices) == 0 || len(e.MarketData.AskPrices) == 0 {
		return ErrMissingMarketDepth
	}
	return nil
}
