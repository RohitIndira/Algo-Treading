// replay is a test utility that fetches the last N news documents from MongoDB
// and republishes them to the Kafka topic, bypassing the 24-hour age filter.
package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	kafkapkg "github.com/RohitIndira/Algo-Treading/pkg/kafka"
	"github.com/RohitIndira/Algo-Treading/pkg/logger"
	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/config"
	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/internal/data"
	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/internal/models"

	"github.com/RohitIndira/Algo-Treading/pkg/database/mongodb"
	"go.mongodb.org/mongo-driver/bson"
	mongoopts "go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

func main() {
	n := flag.Int("n", 3, "number of recent news documents to replay")
	flag.Parse()

	cfg := config.Load()

	lgr, err := logger.NewWithDefaults("replay")
	if err != nil {
		panic(err)
	}
	defer lgr.Sync()

	lgr.Info("replay: starting", zap.Int("count", *n))

	ctx := context.Background()

	// MongoDB
	mongoClient, err := mongodb.New(ctx, mongodb.Config{
		URI:            cfg.MongoURI,
		Database:       cfg.MongoDatabase,
		ConnectTimeout: cfg.MongoConnectTimeout,
	})
	if err != nil {
		lgr.Fatal("failed to connect to mongodb", zap.Error(err))
	}
	defer mongoClient.Close(ctx)

	// Kafka producer
	producer, err := kafkapkg.NewProducer(kafkapkg.ProducerConfig{
		Brokers:     cfg.KafkaBrokers,
		Topic:       cfg.KafkaTopic,
		BatchSize:   100,
		MaxAttempts: 3,
	})
	if err != nil {
		lgr.Fatal("failed to create kafka producer", zap.Error(err))
	}
	defer producer.Close()

	// Redis manager (for company enrichment)
	redisMgr, err := data.NewRedisManager(cfg.RedisURI, cfg.RedisPassword, cfg.RedisDB, lgr, mongoClient)
	if err != nil {
		lgr.Fatal("failed to create redis manager", zap.Error(err))
	}
	defer redisMgr.Close()

	// Fetch last N documents sorted by _id descending
	coll := mongoClient.Collection(cfg.MongoCollection)
	opts := mongoopts.Find().
		SetSort(bson.D{{Key: "_id", Value: -1}}).
		SetLimit(int64(*n))

	cursor, err := coll.Find(ctx, bson.D{}, opts)
	if err != nil {
		lgr.Fatal("failed to query mongodb", zap.Error(err))
	}
	defer cursor.Close(ctx)

	var docs []models.NewsDocument
	if err := cursor.All(ctx, &docs); err != nil {
		lgr.Fatal("failed to decode documents", zap.Error(err))
	}

	lgr.Info("replay: fetched documents", zap.Int("found", len(docs)))

	published := 0
	for _, doc := range docs {
		docID := fmt.Sprintf("%v", doc.ID)

		if err := enrichAndPublish(ctx, doc, docID, producer, redisMgr, cfg.KafkaTopic, lgr); err != nil {
			lgr.Error("replay: failed to publish", zap.String("docID", docID), zap.Error(err))
			continue
		}
		published++
	}

	lgr.Info("replay: done", zap.Int("published", published), zap.Int("total", len(docs)))
}

func enrichAndPublish(
	ctx context.Context,
	doc models.NewsDocument,
	docID string,
	producer *kafkapkg.Producer,
	redisMgr *data.RedisManager,
	topic string,
	lgr *logger.Logger,
) error {
	companyISIN := models.GetString(doc.Company)
	if companyISIN == "" {
		return fmt.Errorf("empty company ISIN, skipping")
	}

	companyDetails, err := redisMgr.GetCompanyDetails(ctx, companyISIN)
	if err != nil {
		return fmt.Errorf("redis lookup failed: %w", err)
	}
	if companyDetails == nil {
		return fmt.Errorf("company not found in redis (isin=%s)", companyISIN)
	}

	payload := models.NewsPayload{
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
		payload.NSECode = companyDetails.NSECode
	}

	msgBytes, err := bson.MarshalExtJSON(payload, false, false)
	if err != nil {
		return fmt.Errorf("marshal failed: %w", err)
	}

	var key []byte
	if doc.ID != nil {
		if kb, err := bson.MarshalExtJSON(doc.ID, false, false); err == nil {
			key = kb
		}
	}

	pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := producer.WriteMessage(pctx, key, msgBytes); err != nil {
		return fmt.Errorf("kafka write failed: %w", err)
	}

	lgr.Info("replay: published",
		zap.String("docID", docID),
		zap.String("isin", companyISIN),
		zap.String("exchange", companyDetails.Exchange),
		zap.String("topic", topic),
	)
	return nil
}
