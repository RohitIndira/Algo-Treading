package models

import (
	"fmt"
	"strconv"
	"time"
)

// MongoDBEvent represents the actual MongoDB document structure
type MongoDBEvent struct {
	ID            interface{}            `json:"_id"`
	Stock         interface{}            `json:"stock"`    // Can be string or number
	Code          interface{}            `json:"code"`     // NSE stock code (when NSE is active)
	BSECode       interface{}            `json:"bsecode"`  // BSE stock code (when only BSE is active)
	Token         interface{}            `json:"token"`    // Token field set by data-ingestion (code for NSE, bsecode for BSE)
	Symbol        string                 `json:"symbol"`   // Stock symbol
	Exchange      string                 `json:"exchange"` // Exchange (NSE/BSE)
	NewsID        string                 `json:"news_id"`
	NewsLink      string                 `json:"news link"` // Note: space in field name
	Impact        string                 `json:"impact"`
	ImpactScore   interface{}            `json:"impact score"` // Note: space in field name
	Sentiment     string                 `json:"sentiment"`
	Category      string                 `json:"category"`
	ShortSummary  string                 `json:"short summary"` // Note: space in field name
	DtTm          interface{}            `json:"dt_tm"`
	Company       string                 `json:"company"`     // ISIN
	CompanyName   string                 `json:"companyname"` // Company name
	SymbolMap     map[string]interface{} `json:"symbolmap"`
	LastTraded    interface{}            `json:"LastTradedPrice"`
	PctChange     interface{}            `json:"pct_change"`
	DocumentDate  string                 `json:"document_date"`
	NewsFirst     interface{}            `json:"NewsFirstPrice"`
	NewsPctChange interface{}            `json:"news_pct_change"`
	PriceMap      map[string]interface{} `json:"pricemap"`
}

// ToMarketEvent converts MongoDB event to MarketEvent
func (m *MongoDBEvent) ToMarketEvent() (*MarketEvent, error) {
	event := &MarketEvent{}

	// Extract event_id from MongoDB _id
	event.EventID = m.extractEventID()
	if event.EventID == "" {
		return nil, fmt.Errorf("failed to extract event ID")
	}

	// Set default event type
	event.EventType = "market_depth"

	// Parse timestamp
	event.Timestamp = m.extractTimestamp()

	// Map stock data
	event.StockData = m.mapStockData()

	// Map market data
	event.MarketData = m.mapMarketData()

	return event, nil
}

func (m *MongoDBEvent) extractEventID() string {
	if m.ID == nil {
		return ""
	}

	switch v := m.ID.(type) {
	case string:
		return v
	case map[string]interface{}:
		if oid, ok := v["$oid"].(string); ok {
			return oid
		}
	}
	return ""
}

func (m *MongoDBEvent) extractTimestamp() time.Time {
	if m.DtTm == nil {
		return time.Now()
	}

	switch v := m.DtTm.(type) {
	case string:
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			return t
		}
	case map[string]interface{}:
		if dateStr, ok := v["$date"].(string); ok {
			if t, err := time.Parse(time.RFC3339, dateStr); err == nil {
				return t
			}
		}
	}

	return time.Now()
}

func (m *MongoDBEvent) mapStockData() StockData {
	sd := StockData{
		ISIN:        m.Company,
		CompanyName: m.CompanyName,
	}

	// Use the exchange field directly from the event (set by data-ingestion service)
	if m.Exchange != "" {
		sd.Exchange = m.Exchange
	} else {
		// Fallback: determine exchange from symbolmap (backward compatibility)
		if m.SymbolMap != nil {
			if nse, ok := m.SymbolMap["NSE"]; ok && nse != nil && nse != "" {
				sd.Exchange = "NSE"
			} else {
				sd.Exchange = "BSE"
			}
		} else {
			sd.Exchange = "NSE" // Default
		}
	}

	// Use the symbol field directly from the event
	if m.Symbol != "" {
		sd.Symbol = m.Symbol
	} else {
		// Fallback: extract symbol from symbolmap or stock field
		sd.Symbol = m.extractStockSymbol()
	}

	// Extract BOTH NSE code and BSE code for Redis lookup
	// NSE code from 'code' field
	if m.Code != nil {
		sd.NSECode = m.toInt64(m.Code)
	}

	// BSE code from 'bsecode' field
	if m.BSECode != nil {
		sd.BSECode = m.toInt64(m.BSECode)
	}

	// Priority 1: Use the token field directly (set by data-ingestion service)
	// This field contains the correct code for NSE or bsecode for BSE
	if m.Token != nil {
		sd.StockCode = m.toInt64(m.Token)
	}

	// Fallback: Priority-based stock code extraction for backward compatibility
	if sd.StockCode == 0 {
		if sd.Exchange == "NSE" || sd.Exchange == "NSE_EQ" {
			// For NSE, use 'code' field
			sd.StockCode = sd.NSECode
		} else if sd.Exchange == "BSE" || sd.Exchange == "BSE_EQ" {
			// For BSE-only, use 'bsecode' field
			sd.StockCode = sd.BSECode
		}

		// Last resort fallback: try symbolmap or stock field
		if sd.StockCode == 0 {
			if m.SymbolMap != nil {
				if bse, ok := m.SymbolMap["BSE"]; ok {
					sd.StockCode = m.toInt64(bse)
				}
			}
			if sd.StockCode == 0 {
				sd.StockCode = m.toInt64(m.Stock)
			}
		}
	}

	return sd
}

func (m *MongoDBEvent) extractStockSymbol() string {
	if m.Stock == nil {
		return ""
	}

	switch v := m.Stock.(type) {
	case string:
		return v
	case int, int32, int64, float32, float64:
		// Stock is a number, convert to string
		return fmt.Sprintf("%v", v)
	}
	return ""
}

func (m *MongoDBEvent) extractCompanyName() string {
	if m.SymbolMap != nil {
		if name, ok := m.SymbolMap["Company_Name"].(string); ok {
			return name
		}
	}
	return ""
}

func (m *MongoDBEvent) mapMarketData() MarketData {
	md := MarketData{
		LastTradedPrice: m.toFloat64(m.LastTraded),
		PctChange:       m.toFloat64(m.PctChange),
	}

	// Map price map
	if m.PriceMap != nil {
		md.PriceMap = PriceMap{
			Open:   m.toFloat64(m.PriceMap["Open"]),
			High:   m.toFloat64(m.PriceMap["High"]),
			Low:    m.toFloat64(m.PriceMap["Low"]),
			Volume: m.toInt64(m.PriceMap["Volume"]),
		}
	}

	return md
}

// Helper function to convert interface{} to int64
func (m *MongoDBEvent) toInt64(v interface{}) int64 {
	if v == nil {
		return 0
	}

	switch val := v.(type) {
	case int:
		return int64(val)
	case int32:
		return int64(val)
	case int64:
		return val
	case float32:
		return int64(val)
	case float64:
		return int64(val)
	case string:
		if i, err := strconv.ParseInt(val, 10, 64); err == nil {
			return i
		}
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return int64(f)
		}
	}
	return 0
}

// Helper function to convert interface{} to float64
func (m *MongoDBEvent) toFloat64(v interface{}) float64 {
	if v == nil {
		return 0
	}

	switch val := v.(type) {
	case int:
		return float64(val)
	case int32:
		return float64(val)
	case int64:
		return float64(val)
	case float32:
		return float64(val)
	case float64:
		return val
	case string:
		// Handle "Post Market News" and other non-numeric strings
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f
		}
	}
	return 0
}
