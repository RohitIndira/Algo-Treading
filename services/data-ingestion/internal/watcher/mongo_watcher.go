package watcher

import (
	"context"
	"fmt"
	"time"

	"github.com/RohitIndira/Algo-Treading/pkg/database/mongodb"
	"github.com/RohitIndira/Algo-Treading/pkg/logger"
	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/internal/publisher"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
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

	// 4. Check co_code field exists (stock code validation)
	coCode, hasCoCode := companyDetails["co_code"]
	if !hasCoCode || coCode == nil {
		return false, "company co_code is null or missing"
	}

	// Enrich document with company details (co_code, symbol, mcap, etc.)
	doc["co_code"] = coCode
	doc["code"] = coCode // Also set as 'code' for compatibility

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

	return true, ""
}

// Run starts the change stream and blocks until context is done
func (w *MongoWatcher) Run(ctx context.Context) error {
	pipeline := mongo.Pipeline{
		{{"$match", bson.D{{"operationType", "insert"}}}},
	}

	cs, err := w.client.WatchCollection(ctx, w.collection, pipeline)
	if err != nil {
		return fmt.Errorf("failed to watch collection: %w", err)
	}
	defer cs.Close(ctx)

	w.lgr.Info("started mongo watcher with filters", zap.String("collection", w.collection))

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

		// Validate document against all filtering criteria
		vctx, vcancel := context.WithTimeout(ctx, 3*time.Second)
		valid, reason := w.validateNewsDocument(vctx, doc)
		vcancel()

		if !valid {
			w.lgr.Debug("document filtered out", zap.String("reason", reason))
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
			w.lgr.Info("published news to kafka",
				zap.String("company", doc["company"].(string)),
				zap.Float64("mcap", doc["mcap"].(float64)),
			)
		}
		cancel()
	}

	if err := cs.Err(); err != nil {
		return fmt.Errorf("change stream error: %w", err)
	}

	w.lgr.Info("mongo watcher stopped")
	return nil
}
