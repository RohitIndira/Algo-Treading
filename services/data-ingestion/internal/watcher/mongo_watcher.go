package watcher

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/RohitIndira/Algo-Treading/pkg/database/mongodb"
	"github.com/RohitIndira/Algo-Treading/pkg/logger"
	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/internal/data"
	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/internal/publisher"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

// MongoWatcher listens to a MongoDB collection change stream and forwards inserts to a publisher
type MongoWatcher struct {
	client       *mongodb.Client
	collection   string
	redisManager *data.RedisManager
	pub          publisher.Publisher
	lgr          *logger.Logger
	resumeToken  bson.Raw
	mu           sync.RWMutex
	processedIDs map[string]time.Time // Track processed document IDs to prevent duplicates
	validator    *Validator
}

// NewMongoWatcher creates a new watcher
func NewMongoWatcher(client *mongodb.Client, collection string, redisManager *data.RedisManager, pub publisher.Publisher, lgr *logger.Logger) (*MongoWatcher, error) {
	if client == nil {
		return nil, fmt.Errorf("mongodb client is nil")
	}
	if redisManager == nil {
		return nil, fmt.Errorf("redis manager is nil")
	}
	return &MongoWatcher{
		client:       client,
		collection:   collection,
		redisManager: redisManager,
		pub:          pub,
		lgr:          lgr,
		processedIDs: make(map[string]time.Time),
		validator:    NewValidator(),
	}, nil
}

// enrichAndPublish processes a single news document: database lookup -> enrichment -> publish
func (w *MongoWatcher) enrichAndPublish(ctx context.Context, doc models.NewsDocument, docID string, receivedAt time.Time) {
	// Get company ISIN
	companyISIN := models.GetString(doc.Company)

	// Redis Lookup
	companyDetails, err := w.redisManager.GetCompanyDetails(ctx, companyISIN)
	if err != nil {
		w.lgr.Error("failed to get company details from redis", zap.Error(err), zap.String("isin", companyISIN))
		return
	}
	if companyDetails == nil {
		// Company not found in Redis (or not active) -> Skip
		w.lgr.Debug("company not found in redis or inactive", zap.String("isin", companyISIN))
		return
	}

	// Construct payload
	payloadMap := models.NewsPayload{
		Stock:        doc.Stock,
		Company:      doc.Company,
		NewsID:       doc.NewsID,
		DtTm:         doc.DtTm,
		ImpactScore:  doc.ImpactScore,
		Sentiment:    doc.Sentiment,
		Category:     doc.Category,
		BSECode:      companyDetails.BSECode,
		NSECode:      nil,
		MCap:         companyDetails.MCap,
		MCapType:     companyDetails.MCapType,
		Exchange:     companyDetails.Exchange,
		DocumentDate: doc.DocumentDate,
	}

	if companyDetails.NSECode != "" {
		payloadMap.NSECode = companyDetails.NSECode
	}

	// Publish to Kafka
	// marshal to extended JSON for Kafka payload
	payload, err := bson.MarshalExtJSON(payloadMap, false, false)
	if err != nil {
		w.lgr.Error("failed to marshal payload", zap.Error(err))
		return
	}

	// attempt to get _id as key
	var key []byte
	if doc.ID != nil {
		kb, err := bson.MarshalExtJSON(doc.ID, false, false)
		if err == nil {
			key = kb
		}
	}

	// Publish
	pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := w.pub.Publish(pctx, key, payload); err != nil {
		w.lgr.Error("failed to publish message", zap.Error(err))
	} else {
		w.lgr.Info("published news to kafka",
			zap.String("docID", docID),
			zap.String("isin", companyISIN),
			zap.String("exchange", companyDetails.Exchange),
			zap.Time("receivedAt", receivedAt),
			zap.Duration("latency", time.Since(receivedAt)),
		)
	}
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

	w.lgr.Info("started mongo watcher", zap.String("collection", w.collection))

	// Start a goroutine to periodically clean old processed IDs (older than 1 hour)
	cleanupTicker := time.NewTicker(10 * time.Minute)
	defer cleanupTicker.Stop()
	go func() {
		defer w.lgr.RecoverAndLog("mongo-watcher-cleanup-ticker")
		for {
			select {
			case <-ctx.Done():
				return
			case <-cleanupTicker.C:
				w.cleanupOldProcessedIDs()
			}
		}
	}()

	var wg sync.WaitGroup

	// Semaphore to limit concurrency (max 50 concurrent enrichments)
	maxConcurrency := 50
	sem := make(chan struct{}, maxConcurrency)

	for cs.Next(ctx) {
		var event bson.M
		if err := cs.Decode(&event); err != nil {
			w.lgr.Error("failed to decode change event", zap.Error(err))
			continue
		}

		// fullDocument holds the inserted document
		full, ok := event["fullDocument"]
		if !ok {
			continue
		}

		// Decode into NewsDocument model
		// First marshal back to bytes then unmarshal to struct to avoid manual mapping
		// Or assume bson.M and map manually. Using struct is cleaner if we can unmarshal.
		// Since 'full' is likely map[string]interface{} or search specific bson types,
		// standard BSON decoding from the stream 'Decode' works best if we decoded directly into a struct wrapping event.
		// But we decode to bson.M above.
		// Let's re-marshal to bytes and unmarshal to NewsDocument for type safety.
		bytes, err := bson.Marshal(full)
		if err != nil {
			w.lgr.Error("failed to marshal full document", zap.Error(err))
			continue
		}

		var doc models.NewsDocument
		if err := bson.Unmarshal(bytes, &doc); err != nil {
			w.lgr.Error("failed to unmarshal into NewsDocument", zap.Error(err))
			continue
		}

		// Get document ID for deduplication
		docID := ""
		if doc.ID != nil {
			docID = fmt.Sprintf("%v", doc.ID)
		}

		// Check if we've already processed this document
		if docID != "" && w.isDuplicate(docID) {
			continue
		}

		// Mark as processed
		if docID != "" {
			w.markAsProcessed(docID)
		}

		w.lgr.Info("received document", zap.String("docID", docID))

		receivedAt := time.Now()

		// Validate basic criteria
		if valid, reason := w.validator.ValidateNews(doc); !valid {
			w.lgr.Info("document filtered out", zap.String("reason", reason), zap.String("docID", docID))
			continue
		}

		// Acquire semaphore
		select {
		case sem <- struct{}{}:
			// Process in a new goroutine
			wg.Add(1)
			go func(d models.NewsDocument, id string, t time.Time) {
				defer w.lgr.RecoverAndLog("mongo-watcher-enrich-and-publish")
				defer wg.Done()
				defer func() { <-sem }() // Release semaphore
				w.enrichAndPublish(ctx, d, id, t)
			}(doc, docID, receivedAt)
		case <-ctx.Done():
			return nil
		}

		// Save resume token
		if resumeToken := cs.ResumeToken(); resumeToken != nil {
			w.mu.Lock()
			w.resumeToken = resumeToken
			w.mu.Unlock()
		}
	}

	if err := cs.Err(); err != nil {
		return fmt.Errorf("change stream error: %w", err)
	}

	// Wait for all processing goroutines to finish
	wg.Wait()
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
}
