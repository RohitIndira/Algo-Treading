package watcher

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/RohitIndira/Algo-Treading/pkg/database/mongodb"
	"github.com/RohitIndira/Algo-Treading/pkg/logger"
	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/internal/publisher"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

// MongoWatcher listens to a MongoDB collection change stream and forwards inserts to a publisher
type MongoWatcher struct {
	client              *mongodb.Client
	collection          string
	companiesDB         string
	companiesCollection string
	pub                 publisher.Publisher
	lgr                 *logger.Logger
	resumeToken         bson.Raw
	mu                  sync.RWMutex
	processedIDs        map[string]time.Time // Track processed document IDs to prevent duplicates
}

// NewMongoWatcher creates a new watcher
func NewMongoWatcher(client *mongodb.Client, collection string, pub publisher.Publisher, lgr *logger.Logger) (*MongoWatcher, error) {
	if client == nil {
		return nil, fmt.Errorf("mongodb client is nil")
	}
	return &MongoWatcher{
		client:              client,
		collection:          collection,
		companiesDB:         "OdinMasterData",
		companiesCollection: "CompanyMaster",
		pub:                 pub,
		lgr:                 lgr,
		processedIDs:        make(map[string]time.Time),
	}, nil
}

// validateNewsDocument checks if the document passes all filtering criteria
func (w *MongoWatcher) validateNewsDocument(ctx context.Context, doc bson.M) (bool, string) {
	// 1. Check dt_tm is today's date
	dtTm, ok := doc["dt_tm"]
	if !ok {
		return false, "missing dt_tm field"
	}

	var newsTime time.Time
	switch v := dtTm.(type) {
	case primitive.DateTime:
		newsTime = v.Time()
	case time.Time:
		newsTime = v
	default:
		return false, "invalid dt_tm format"
	}

	// Check if news is from today (IST timezone - UTC+5:30)
	istLocation := time.FixedZone("IST", 5*60*60+30*60)
	today := time.Now().In(istLocation)
	newsDate := newsTime.In(istLocation)

	if newsDate.Year() != today.Year() || newsDate.YearDay() != today.YearDay() {
		return false, fmt.Sprintf("dt_tm not today: %s", newsDate.Format("2006-01-02"))
	}

	// 2. Document Quality Filters
	// Check duplicate field
	if duplicate, ok := doc["duplicate"].(bool); ok && duplicate {
		return false, "duplicate is true"
	}

	// Check impact score exists and is not null
	impactScore, hasImpact := doc["impact score"]
	if !hasImpact || impactScore == nil {
		return false, "impact score is null or missing"
	}

	// Check short summary exists and is not null
	shortSummary, hasSummary := doc["short summary"]
	if !hasSummary || shortSummary == nil {
		return false, "short summary is null or missing"
	}
	if summaryStr, ok := shortSummary.(string); ok && summaryStr == "" {
		return false, "short summary is empty"
	}

	// 3. Get company ISIN and fetch mcap
	company, hasCompany := doc["company"]
	if !hasCompany || company == nil {
		return false, "company field is null or missing"
	}

	companyISIN, ok := company.(string)
	if !ok || companyISIN == "" {
		return false, "company ISIN is invalid"
	}

	// Fetch company details from OdinMasterData.CompanyMaster
	var companyDetails bson.M
	companyDB := w.client.Client.Database(w.companiesDB)
	err := companyDB.Collection(w.companiesCollection).FindOne(ctx, bson.M{"isin": companyISIN}).Decode(&companyDetails)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return false, fmt.Sprintf("company not found in CompanyMaster: %s", companyISIN)
		}
		w.lgr.Error("error fetching company details", zap.Error(err), zap.String("isin", companyISIN))
		return false, "error fetching company details"
	}

	// Check mcap > 250
	mcap, hasMcap := companyDetails["mcap"]
	if !hasMcap || mcap == nil {
		return false, "company mcap is null or missing"
	}

	var mcapValue float64
	switch v := mcap.(type) {
	case float64:
		mcapValue = v
	case float32:
		mcapValue = float64(v)
	case int:
		mcapValue = float64(v)
	case int32:
		mcapValue = float64(v)
	case int64:
		mcapValue = float64(v)
	default:
		return false, "mcap is not a numeric value"
	}

	if mcapValue <= 250 {
		return false, fmt.Sprintf("mcap %.2f <= 250", mcapValue)
	}

	// 4. Determine exchange status first to know which token field to validate
	var bseStatusStr, nseStatusStr string
	if v, ok := companyDetails["BSEStatus"].(string); ok {
		bseStatusStr = v
	}
	if v, ok := companyDetails["NSEStatus"].(string); ok {
		nseStatusStr = v
	}

	nseActive := strings.EqualFold(strings.TrimSpace(nseStatusStr), "Active")
	bseActive := strings.EqualFold(strings.TrimSpace(bseStatusStr), "Active")

	// Validate appropriate token field exists based on which exchange is active
	if nseActive {
		// NSE is active - need code field for token
		code, hasCode := companyDetails["code"]
		if !hasCode || code == nil {
			return false, "NSE is active but code field is null or missing"
		}
		doc["code"] = code
	} else if bseActive {
		// BSE only is active - need bsecode field for token
		bseCode, hasBseCode := companyDetails["bsecode"]
		if !hasBseCode || bseCode == nil {
			return false, "BSE is active but bsecode field is null or missing"
		}
		// Note: bsecode will be added to doc later when setting token
	} else {
		// For DELISTED or UNLISTED, we don't require token fields
	}

	// Set symbol based on listing status
	if nseSym, ok := companyDetails["nsesymbol"]; ok && nseSym != nil {
		if nseSymStr, ok := nseSym.(string); ok && nseSymStr != "" {
			doc["symbol"] = nseSymStr
		}
	}
	if doc["symbol"] == nil {
		if bseSym, ok := companyDetails["BSESymbol"]; ok && bseSym != nil {
			doc["symbol"] = bseSym
		}
	}

	if logoURL, ok := companyDetails["logo_url"]; ok {
		doc["logo_url"] = logoURL
	}
	if companyName, ok := companyDetails["companyname"]; ok {
		doc["companyname"] = companyName
	}
	doc["mcap"] = mcapValue

	// Now set exchange and token fields based on active exchange
	// Rules:
	// - If NSE is Active (with or without BSE) -> NSE with code field as token
	// - If only BSE is Active -> BSE with bsecode field as token
	// - If both Delisted -> DELISTED (no token)
	// - Otherwise -> UNLISTED (no token)
	exchangeShort := "UNLISTED"
	exchangeBroker := "UNLISTED"
	var rawToken interface{}

	if nseActive {
		// NSE is active (with or without BSE) - use NSE and code field as token
		exchangeShort = "NSE"
		exchangeBroker = "NSE_EQ"
		// Token should be the code field for NSE
		if tok, ok := companyDetails["code"]; ok && tok != nil {
			rawToken = tok
		}
	} else if bseActive {
		// Only BSE is active - use BSE and bsecode field as token
		exchangeShort = "BSE"
		exchangeBroker = "BSE_EQ"
		// Token should be the bsecode field for BSE
		if bseCode, ok := companyDetails["bsecode"]; ok && bseCode != nil {
			rawToken = bseCode
			doc["bsecode"] = bseCode
		}
	} else if strings.EqualFold(strings.TrimSpace(nseStatusStr), "Delisted") && strings.EqualFold(strings.TrimSpace(bseStatusStr), "Delisted") {
		exchangeShort = "DELISTED"
		exchangeBroker = "DELISTED"
	}

	// Set both fields so downstream consumers can use whichever format they expect
	doc["exchange"] = exchangeShort
	doc["exchange_broker"] = exchangeBroker

	// Normalize token to an integer (int64) if possible so consumers expecting numeric tokens will find it
	if rawToken != nil {
		switch v := rawToken.(type) {
		case int64:
			doc["token"] = v
		case int32:
			doc["token"] = int64(v)
		case int:
			doc["token"] = int64(v)
		case float64:
			doc["token"] = int64(v)
		case float32:
			doc["token"] = int64(v)
		case string:
			if iv, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64); err == nil {
				doc["token"] = iv
			} else {
				// fallback to raw string if it can't be parsed
				doc["token"] = v
			}
		default:
			// keep raw value as last resort
			doc["token"] = v
		}
	}

	return true, ""
}

// Run starts the change stream and blocks until context is done
func (w *MongoWatcher) Run(ctx context.Context) error {
	pipeline := mongo.Pipeline{
		bson.D{{Key: "$match", Value: bson.D{{Key: "operationType", Value: "insert"}}}},
	}

	// Configure change stream options
	opts := options.ChangeStream().
		SetFullDocument(options.UpdateLookup) // Ensure we get full document for inserts

	// If we have a resume token, use it to prevent reprocessing
	w.mu.RLock()
	if w.resumeToken != nil {
		opts.SetResumeAfter(w.resumeToken)
		w.lgr.Info("resuming change stream from saved token")
	}
	w.mu.RUnlock()

	cs, err := w.client.WatchCollection(ctx, w.collection, pipeline, opts)
	if err != nil {
		return fmt.Errorf("failed to watch collection: %w", err)
	}
	defer cs.Close(ctx)

	w.lgr.Info("started mongo watcher with filters", zap.String("collection", w.collection))

	// Start a goroutine to periodically clean old processed IDs (older than 1 hour)
	cleanupTicker := time.NewTicker(10 * time.Minute)
	defer cleanupTicker.Stop()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-cleanupTicker.C:
				w.cleanupOldProcessedIDs()
			}
		}
	}()

	for cs.Next(ctx) {
		var event bson.M
		if err := cs.Decode(&event); err != nil {
			w.lgr.Error("failed to decode change event", zap.Error(err))
			continue
		}

		// fullDocument holds the inserted document
		full, ok := event["fullDocument"]
		if !ok {
			w.lgr.Warn("change event missing fullDocument")
			continue
		}

		// Convert to bson.M for validation
		doc, ok := full.(bson.M)
		if !ok {
			w.lgr.Error("fullDocument is not bson.M")
			continue
		}

		// Get document ID for deduplication
		docID := ""
		if id, exists := doc["_id"]; exists {
			docID = fmt.Sprintf("%v", id)
		}

		// Check if we've already processed this document
		if docID != "" && w.isDuplicate(docID) {
			w.lgr.Warn("skipping duplicate document from change stream", zap.String("docID", docID))
			continue
		}

		// Mark as processing IMMEDIATELY to prevent race condition with duplicate events
		if docID != "" {
			w.markAsProcessed(docID)
		}

		w.lgr.Info("processing new document", zap.String("docID", docID))

		// Validate document against all filtering criteria
		vctx, vcancel := context.WithTimeout(ctx, 3*time.Second)
		valid, reason := w.validateNewsDocument(vctx, doc)
		vcancel()

		if !valid {
			w.lgr.Info("document filtered out", zap.String("reason", reason), zap.String("docID", docID))
			continue
		}

		// CRITICAL: Verify document has ALL required fields before publishing
		// This prevents publishing incomplete documents if validation didn't set all fields
		_, hasExchange := doc["exchange"]
		_, hasToken := doc["token"]
		_, hasMcap := doc["mcap"]
		_, hasCompany := doc["company"]

		if !hasExchange || !hasToken || !hasMcap || !hasCompany {
			w.lgr.Warn("skipping publish - missing required fields",
				zap.String("docID", docID),
				zap.Bool("hasExchange", hasExchange),
				zap.Bool("hasToken", hasToken),
				zap.Bool("hasMcap", hasMcap),
				zap.Bool("hasCompany", hasCompany))
			continue
		}

		// marshal to extended JSON for Kafka payload
		payload, err := bson.MarshalExtJSON(doc, false, false)
		if err != nil {
			w.lgr.Error("failed to marshal fullDocument", zap.Error(err))
			continue
		}

		// attempt to get _id as key
		var key []byte
		if id, exists := doc["_id"]; exists {
			kb, err := bson.MarshalExtJSON(id, false, false)
			if err == nil {
				key = kb
			}
		}

		// publish with short timeout
		pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		if err := w.pub.Publish(pctx, key, payload); err != nil {
			w.lgr.Error("failed to publish message", zap.Error(err))
		} else {
			// Log published document details
			exchange := "UNKNOWN"
			if ex, ok := doc["exchange"].(string); ok {
				exchange = ex
			}
			token := "UNKNOWN"
			if tok, ok := doc["token"]; ok {
				token = fmt.Sprintf("%v", tok)
			}
			company := "UNKNOWN"
			if comp, ok := doc["company"].(string); ok {
				company = comp
			}

			w.lgr.Info("published news to kafka",
				zap.String("company", company),
				zap.String("exchange", exchange),
				zap.String("token", token),
				zap.Float64("mcap", doc["mcap"].(float64)),
			)
		}
		cancel()

		// Save resume token after successful processing
		if resumeToken := cs.ResumeToken(); resumeToken != nil {
			w.mu.Lock()
			w.resumeToken = resumeToken
			w.mu.Unlock()
		}
	}

	if err := cs.Err(); err != nil {
		return fmt.Errorf("change stream error: %w", err)
	}

	w.lgr.Info("mongo watcher stopped")
	return nil
}

// isDuplicate checks if a document ID has been processed recently
func (w *MongoWatcher) isDuplicate(docID string) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	_, exists := w.processedIDs[docID]
	return exists
}

// markAsProcessed marks a document ID as processed
func (w *MongoWatcher) markAsProcessed(docID string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.processedIDs[docID] = time.Now()
}

// cleanupOldProcessedIDs removes processed IDs older than 1 hour to prevent memory growth
func (w *MongoWatcher) cleanupOldProcessedIDs() {
	w.mu.Lock()
	defer w.mu.Unlock()

	cutoff := time.Now().Add(-1 * time.Hour)
	for id, timestamp := range w.processedIDs {
		if timestamp.Before(cutoff) {
			delete(w.processedIDs, id)
		}
	}

	w.lgr.Debug("cleaned up old processed IDs", zap.Int("remaining", len(w.processedIDs)))
}
