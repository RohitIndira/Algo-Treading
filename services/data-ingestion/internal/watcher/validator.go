package watcher

import (
	"fmt"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/internal/models"
)

// Validator handles business rules for news filtering
type Validator struct{}

// NewValidator creates a new Validator
func NewValidator() *Validator {
	return &Validator{}
}

// ValidateNews checks if the document passes all filtering criteria
func (v *Validator) ValidateNews(doc models.NewsDocument) (bool, string) {
	// 1. Check dt_tm
	if doc.DtTm == nil {
		return false, "missing dt_tm field"
	}

	newsTime, ok := models.GetTime(doc.DtTm)
	if !ok {
		return false, "invalid dt_tm format"
	}

	// Ensure we work with the UTC representation to get the raw clock values
	newsTime = newsTime.UTC()

	// Re-interpret the time as IST
	istLocation := time.FixedZone("IST", 5*60*60+30*60)
	
	// Create new time with the raw clock values but in IST
	newsTime = time.Date(
		newsTime.Year(), newsTime.Month(), newsTime.Day(),
		newsTime.Hour(), newsTime.Minute(), newsTime.Second(),
		newsTime.Nanosecond(), istLocation,
	)

	// Filter out news older than 24 hours to verify relevance (24/7 news cycle)
	if time.Since(newsTime) > 24*time.Hour {
		return false, fmt.Sprintf("news too old: %v", newsTime)
	}
	if newsTime.After(time.Now().Add(10 * time.Minute)) { // Allow 10 min clock skew
		return false, fmt.Sprintf("news from future: %v", newsTime)
	}

	// 2. Check duplicate field
	if duplicate, ok := doc.Duplicate.(bool); ok && duplicate {
		return false, "duplicate is true"
	}

	// 3. Check source field
	if source := models.GetString(doc.Source); source == "TelegramBrokerReport" {
		return false, "source is TelegramBrokerReport"
	}

	// 4. Check impact score exists and is not null
	if doc.ImpactScore == nil {
		return false, "impact score is null or missing"
	}

	// 5. Check short summary exists and is not null
	if doc.ShortSummary == nil {
		return false, "short summary is null or missing"
	}
	if models.GetString(doc.ShortSummary) == "" {
		return false, "short summary is empty"
	}

	// 6. Check company field exists (needed for Redis lookup)
	if doc.Company == nil {
		return false, "company field is null or missing"
	}
	if isin := models.GetString(doc.Company); isin == "" {
		return false, "company ISIN is invalid"
	}

	return true, ""
}
