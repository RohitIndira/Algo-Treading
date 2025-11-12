package watcher

import (
	"context"
	"fmt"
	"time"

	"github.com/RohitIndira/Algo-Treading/pkg/database/mongodb"
	"github.com/RohitIndira/Algo-Treading/pkg/logger"
	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/internal/publisher"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/zap"
)

// MongoWatcher listens to a MongoDB collection change stream and forwards inserts to a publisher
type MongoWatcher struct {
	client     *mongodb.Client
	collection string
	pub        publisher.Publisher
	lgr        *logger.Logger
}

// NewMongoWatcher creates a new watcher
func NewMongoWatcher(client *mongodb.Client, collection string, pub publisher.Publisher, lgr *logger.Logger) (*MongoWatcher, error) {
	if client == nil {
		return nil, fmt.Errorf("mongodb client is nil")
	}
	return &MongoWatcher{client: client, collection: collection, pub: pub, lgr: lgr}, nil
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

	w.lgr.Info("started mongo watcher", zap.String("collection", w.collection))

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

		// marshal to extended JSON for Kafka payload
		payload, err := bson.MarshalExtJSON(full, false, false)
		if err != nil {
			w.lgr.Error("failed to marshal fullDocument", zap.Error(err))
			continue
		}

		// attempt to get _id as key
		var key []byte
		if m, ok := full.(bson.M); ok {
			if id, exists := m["_id"]; exists {
				kb, err := bson.MarshalExtJSON(id, false, false)
				if err == nil {
					key = kb
				}
			}
		}

		// publish with short timeout
		pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		if err := w.pub.Publish(pctx, key, payload); err != nil {
			w.lgr.Error("failed to publish message", zap.Error(err))
		}
		cancel()
	}

	if err := cs.Err(); err != nil {
		return fmt.Errorf("change stream error: %w", err)
	}

	w.lgr.Info("mongo watcher stopped")
	return nil
}
