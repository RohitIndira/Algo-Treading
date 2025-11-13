package models

import (
	"fmt"
	"strconv"
	"time"
)

// MongoDBEvent represents the actual MongoDB document structure
type MongoDBEvent struct {
	ID            interface{}            `json:"_id"`
	Stock         interface{}            `json:"stock"` // Can be string or number
	NewsID        string                 `json:"news_id"`
	NewsLink      string                 `json:"news link"` // Note: space in field name
	Impact        string                 `json:"impact"`
	ImpactScore   interface{}            `json:"impact score"` // Note: space in field name
	Sentiment     string                 `json:"sentiment"`
	Category      string                 `json:"category"`
	ShortSummary  string                 `json:"short summary"` // Note: space in field name
	DtTm          interface{}            `json:"dt_tm"`
	Company       string                 `json:"company"` // ISIN
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
	event.EventType = "news"

	// Parse timestamp
	event.Timestamp = m.extractTimestamp()

	// Map stock data
	event.StockData = m.mapStockData()

	// Map news data
	event.NewsData = m.mapNewsData()

	// Map analysis
	event.Analysis = m.mapAnalysis()

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
		Symbol:      m.extractStockSymbol(),
		ISIN:        m.Company,
		CompanyName: m.extractCompanyName(),
	}

	// Extract stock code from symbolmap.BSE or stock field
	if m.SymbolMap != nil {
		if bse, ok := m.SymbolMap["BSE"]; ok {
			sd.StockCode = m.toInt64(bse)
		}

		// Determine exchange (prefer NSE if available)
		if nse, ok := m.SymbolMap["NSE"]; ok && nse != nil && nse != "" {
			sd.Exchange = "NSE"
			if nseStr, ok := nse.(string); ok && nseStr != "" {
				sd.Symbol = nseStr
			}
		} else {
			sd.Exchange = "BSE"
		}
	}

	// If no stock code yet, try stock field directly
	if sd.StockCode == 0 {
		sd.StockCode = m.toInt64(m.Stock)
	}

	// Fallback: use stock field value as exchange if no symbolmap
	if sd.Exchange == "" {
		sd.Exchange = "NSE" // Default
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

func (m *MongoDBEvent) mapNewsData() NewsData {
	// Parse document date
	var docDate time.Time
	if m.DocumentDate != "" {
		// Try multiple formats
		formats := []string{
			"2006-01-02 15:04:05",
			time.RFC3339,
			"2006-01-02T15:04:05Z",
		}
		for _, format := range formats {
			if t, err := time.Parse(format, m.DocumentDate); err == nil {
				docDate = t
				break
			}
		}
	}

	return NewsData{
		NewsID:       m.NewsID,
		NewsLink:     m.NewsLink,
		Category:     m.Category,
		ShortSummary: m.ShortSummary,
		DocumentDate: docDate,
	}
}

func (m *MongoDBEvent) mapAnalysis() Analysis {
	return Analysis{
		Sentiment:   m.Sentiment,
		Impact:      m.Impact,
		ImpactScore: int32(m.toInt64(m.ImpactScore)),
	}
}

func (m *MongoDBEvent) mapMarketData() MarketData {
	md := MarketData{
		LastTradedPrice: m.toFloat64(m.LastTraded),
		PctChange:       m.toFloat64(m.PctChange),
		NewsFirstPrice:  m.toFloat64(m.NewsFirst),
		NewsPctChange:   m.toFloat64(m.NewsPctChange),
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
