package models

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// NewsDocument represents the raw news document structure from MongoDB
type NewsDocument struct {
	ID           interface{} `bson:"_id"`
	NewsID       interface{} `bson:"news_id"`
	Stock        interface{} `bson:"stock"`
	Company      interface{} `bson:"company"` // Usually ISIN string
	DtTm         interface{} `bson:"dt_tm"`   // Can be primitive.DateTime or time.Time
	ImpactScore  interface{} `bson:"impact score"`
	Sentiment    interface{} `bson:"sentiment"`
	Category     interface{} `bson:"category"`
	ShortSummary interface{} `bson:"short summary"`
	Source       interface{} `bson:"source"`
	Duplicate    interface{} `bson:"duplicate"`
	DocumentDate interface{} `bson:"document_date,omitempty"`
}

// NewsPayload represents the enriched news data sent to Kafka
type NewsPayload struct {
	Stock        interface{} `json:"stock"`
	Company      interface{} `json:"company"`
	NewsID       interface{} `json:"news_id"`
	DtTm         interface{} `json:"dt_tm"`
	ImpactScore  interface{} `json:"impact score"`
	Sentiment    interface{} `json:"sentiment"`
	Category     interface{} `json:"category"`
	BSECode      string      `json:"bsecode"`
	NSECode      interface{} `json:"nsecode"` // Can be nil
	MCap         float64     `json:"mcap"`
	MCapType     string      `json:"mcaptype"`
	Exchange     string      `json:"exchange"`
	DocumentDate interface{} `json:"document_date,omitempty"`
}

// Helper to safely get string from interface
func GetString(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// Helper to safely get time from interface (supporting primitive.DateTime and time.Time)
func GetTime(v interface{}) (time.Time, bool) {
	switch t := v.(type) {
	case primitive.DateTime:
		return t.Time(), true
	case time.Time:
		return t, true
	default:
		return time.Time{}, false
	}
}
