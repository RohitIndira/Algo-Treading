package backfill

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

// RawNewsDoc holds the fields we need from CAG_CHATBOT.NewsImpactDashboard.
// Field names with spaces ("impact score", "short summary") are handled via
// explicit bson tags — the Go BSON driver supports spaces in key names.
type RawNewsDoc struct {
	ID           interface{} `bson:"_id"`
	NewsID       interface{} `bson:"news_id"`
	ISIN         string      `bson:"company"` // company field holds the ISIN
	DtTm         time.Time   `bson:"dt_tm"`
	ImpactScore  interface{} `bson:"impact score"` // space in Mongo field name
	Sentiment    string      `bson:"sentiment"`
	Category     string      `bson:"category"`
	ShortSummary string      `bson:"short summary"` // space in Mongo field name
	Duplicate    bool        `bson:"duplicate"`
}

// ImpactScoreInt32 safely converts the ImpactScore interface{} to int32.
// The field can arrive as int32, int64, float64, or primitive.Decimal128 from BSON.
func (d *RawNewsDoc) ImpactScoreInt32() int32 {
	if d.ImpactScore == nil {
		return 0
	}
	switch v := d.ImpactScore.(type) {
	case int32:
		return v
	case int64:
		return int32(v)
	case float64:
		return int32(v)
	case int:
		return int32(v)
	default:
		return 0
	}
}

// NewsReader queries CAG_CHATBOT.NewsImpactDashboard for historical news within
// a time window. The from/to times must already have the IST offset stripped
// (produced by AMNTimeRange) so they match the Z-tagged wall-clock values that
// MongoDB stores in dt_tm.
type NewsReader struct {
	coll   *mongo.Collection
	logger *zap.Logger
}

// NewNewsReader creates a NewsReader connected to CAG_CHATBOT.NewsImpactDashboard.
func NewNewsReader(client *mongo.Client, logger *zap.Logger) *NewsReader {
	return &NewsReader{
		coll:   client.Database("CAG_CHATBOT").Collection("NewsImpactDashboard"),
		logger: logger,
	}
}

// ReadSince returns all non-duplicate news documents with dt_tm ∈ [from, to).
// Results are ordered ascending by dt_tm so we process oldest news first.
func (r *NewsReader) ReadSince(ctx context.Context, from, to time.Time) ([]RawNewsDoc, error) {
	filter := bson.D{
		{Key: "dt_tm", Value: bson.D{
			{Key: "$gte", Value: primitive.NewDateTimeFromTime(from)},
			{Key: "$lt", Value: primitive.NewDateTimeFromTime(to)},
		}},
		{Key: "duplicate", Value: false},
	}

	projection := bson.D{
		{Key: "company", Value: 1},
		{Key: "dt_tm", Value: 1},
		{Key: "impact score", Value: 1},
		{Key: "sentiment", Value: 1},
		{Key: "category", Value: 1},
		{Key: "short summary", Value: 1},
		{Key: "news_id", Value: 1},
		{Key: "duplicate", Value: 1},
	}

	opts := options.Find().
		SetProjection(projection).
		SetSort(bson.D{{Key: "dt_tm", Value: 1}})

	cursor, err := r.coll.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("news_reader: find: %w", err)
	}
	defer cursor.Close(ctx)

	var docs []RawNewsDoc
	for cursor.Next(ctx) {
		var doc RawNewsDoc
		if err := cursor.Decode(&doc); err != nil {
			r.logger.Warn("news_reader: failed to decode document, skipping", zap.Error(err))
			continue
		}
		// Skip empty ISINs — nothing to match against CompanyMaster.
		if doc.ISIN == "" {
			continue
		}
		docs = append(docs, doc)
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("news_reader: cursor: %w", err)
	}

	r.logger.Info("news_reader: fetched documents",
		zap.Int("count", len(docs)),
		zap.Time("from", from),
		zap.Time("to", to))

	return docs, nil
}
