package models

import (
	"time"
)

// MarketEvent represents a market event from Kafka
type MarketEvent struct {
	EventID    string     `json:"event_id"`
	EventType  string     `json:"event_type"`
	Timestamp  time.Time  `json:"timestamp"`
	StockData  StockData  `json:"stock_data"`
	NewsData   NewsData   `json:"news_data"`
	Analysis   Analysis   `json:"analysis"`
	MarketData MarketData `json:"market_data"`
}

// StockData contains stock information
type StockData struct {
	StockCode   int64  `json:"stock_code"`
	Exchange    string `json:"exchange"`
	Symbol      string `json:"symbol"`
	ISIN        string `json:"isin"`
	CompanyName string `json:"company_name"`
}

// NewsData contains news information
type NewsData struct {
	NewsID       string    `json:"news_id"`
	NewsLink     string    `json:"news_link"`
	Category     string    `json:"category"`
	ShortSummary string    `json:"short_summary"`
	DocumentDate time.Time `json:"document_date"`
}

// Analysis contains sentiment analysis
type Analysis struct {
	Sentiment   string `json:"sentiment"`
	Impact      string `json:"impact"`
	ImpactScore int32  `json:"impact_score"`
}

// MarketData contains market data
type MarketData struct {
	LastTradedPrice float64  `json:"last_traded_price"`
	PctChange       float64  `json:"pct_change"`
	NewsFirstPrice  float64  `json:"news_first_price"`
	NewsPctChange   float64  `json:"news_pct_change"`
	PriceMap        PriceMap `json:"price_map"`
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
	if e.EventType == "" {
		return ErrInvalidEventType
	}
	if e.StockData.StockCode <= 0 {
		return ErrInvalidStockCode
	}
	if e.StockData.Exchange == "" {
		return ErrInvalidExchange
	}
	if e.Analysis.ImpactScore < 0 || e.Analysis.ImpactScore > 10 {
		return ErrInvalidImpactScore
	}
	return nil
}

// GetSentimentValue converts sentiment string to normalized value
func (a *Analysis) GetSentimentValue() string {
	// Normalize sentiment values
	switch a.Sentiment {
	case "Positive", "positive", "POSITIVE":
		return "positive"
	case "Negative", "negative", "NEGATIVE":
		return "negative"
	case "Neutral", "neutral", "NEUTRAL":
		return "neutral"
	default:
		return "neutral"
	}
}
