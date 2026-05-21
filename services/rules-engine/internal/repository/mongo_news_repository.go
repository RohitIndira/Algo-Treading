package repository

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

// MongoNewsRepository reads historical news from the collection data-ingestion
// watches (CAG_CHATBOT.NewsImpactDashboard by default) and the company master
// (OdinMasterData.CompanyMaster) needed to enrich those raw documents.
//
// Why enrichment is required: data-ingestion does NOT store the enriched form
// of a news event — it joins the raw document against CompanyMaster (by ISIN)
// in-flight and publishes the enriched result to Kafka. The raw collection
// therefore has only an ISIN (`company`), no stock code / exchange. The
// after-market backfill reads the raw collection, so it must reproduce that
// join itself — see LoadCompanyMaster + EnrichWithCompany.
type MongoNewsRepository struct {
	client     *mongo.Client
	collection *mongo.Collection
	dbName     string
	collName   string
	logger     *zap.Logger
}

// CompanyInfo is the subset of an OdinMasterData.CompanyMaster row needed to
// turn a raw news document into a tradable event.
type CompanyInfo struct {
	ISIN     string
	NSECode  int64
	BSECode  int64
	Exchange string // "NSE" or "BSE" — whichever segment is Active
	MCap     float64
	MCapType string
}

// NewMongoNewsRepository connects to MongoDB and scopes one collection handle
// to the news collection. The same client also serves CompanyMaster reads.
func NewMongoNewsRepository(ctx context.Context, uri, database, collectionName string, logger *zap.Logger) (*MongoNewsRepository, error) {
	if uri == "" {
		return nil, fmt.Errorf("mongo news repository: empty URI")
	}
	if database == "" || collectionName == "" {
		return nil, fmt.Errorf("mongo news repository: empty database/collection")
	}

	connectCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	client, err := mongo.Connect(connectCtx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, fmt.Errorf("mongo news repository: connect: %w", err)
	}

	pingCtx, cancelPing := context.WithTimeout(ctx, 5*time.Second)
	defer cancelPing()
	if err := client.Ping(pingCtx, nil); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, fmt.Errorf("mongo news repository: ping: %w", err)
	}

	return &MongoNewsRepository{
		client:     client,
		collection: client.Database(database).Collection(collectionName),
		dbName:     database,
		collName:   collectionName,
		logger:     logger,
	}, nil
}

// Close disconnects the MongoDB client. Safe on nil / repeated calls.
func (r *MongoNewsRepository) Close(ctx context.Context) error {
	if r == nil || r.client == nil {
		return nil
	}
	return r.client.Disconnect(ctx)
}

// rawNewsDoc is the BSON-tagged view of one NewsImpactDashboard document.
//
// A dedicated struct (rather than models.MongoDBEvent) is required because
// MongoDBEvent carries only `json` tags — and several Mongo field names
// contain spaces ("impact score", "short summary", "news link") or differ
// from the lowercased Go field name ("dt_tm", "news_id", "_id"), so the BSON
// decoder cannot populate MongoDBEvent directly.
type rawNewsDoc struct {
	ID           primitive.ObjectID     `bson:"_id"`
	NewsID       string                 `bson:"news_id"`
	Company      string                 `bson:"company"` // ISIN
	Stock        string                 `bson:"stock"`   // trading symbol
	SymbolMap    map[string]interface{} `bson:"symbolmap"`
	DtTm         time.Time              `bson:"dt_tm"`
	ImpactScore  interface{}            `bson:"impact score"`
	Impact       string                 `bson:"impact"`
	Sentiment    string                 `bson:"sentiment"`
	Category     string                 `bson:"category"`
	ShortSummary string                 `bson:"short summary"`
	NewsLink     string                 `bson:"news link"`
	DocumentDate string                 `bson:"document_date"`
}

// toMongoDBEvent maps the raw doc into models.MongoDBEvent so the existing,
// well-tested ToMarketEvent conversion can be reused. Stock-identity fields
// (exchange, codes, mcap) are left empty here — EnrichWithCompany fills them.
func (d *rawNewsDoc) toMongoDBEvent() *models.MongoDBEvent {
	companyName := ""
	if d.SymbolMap != nil {
		if cn, ok := d.SymbolMap["Company_Name"].(string); ok {
			companyName = cn
		}
	}
	return &models.MongoDBEvent{
		ID:        d.ID.Hex(),
		NewsID:    d.NewsID,
		Stock:     d.Stock,
		Symbol:    d.Stock,
		SymbolMap: d.SymbolMap,
		// dt_tm is IST wall-clock stored with a 'Z' suffix — not a true UTC
		// instant. The Go driver decodes it into a UTC time.Time whose
		// wall-clock reads correctly; re-stamp it with the IST offset so the
		// downstream event timestamp is the real instant. extractTimestamp
		// parses the RFC3339 string (offset included).
		DtTm:         istWallClock(d.DtTm).Format(time.RFC3339),
		ImpactScore:  d.ImpactScore,
		Impact:       d.Impact,
		Sentiment:    d.Sentiment,
		Category:     d.Category,
		ShortSummary: d.ShortSummary,
		NewsLink:     d.NewsLink,
		Company:      d.Company,
		CompanyName:  companyName,
		DocumentDate: d.DocumentDate,
	}
}

// FindInRange streams NewsImpactDashboard documents whose dt_tm is within
// [start, end] (inclusive) ascending, invoking onEach for each one as a
// *models.MongoDBEvent (un-enriched — caller must EnrichWithCompany).
//
// Returns the number of documents read. A doc that fails to decode is skipped
// (logged), not fatal. An error from onEach aborts the scan.
func (r *MongoNewsRepository) FindInRange(
	ctx context.Context,
	start, end time.Time,
	onEach func(*models.MongoDBEvent) error,
) (scanned int, err error) {
	if r == nil || r.collection == nil {
		return 0, fmt.Errorf("mongo news repository: not initialized")
	}
	if !end.After(start) {
		return 0, nil
	}

	// dt_tm is an IST wall-clock time stored with a 'Z' suffix, so its BSON
	// datetime value numerically equals the IST clock reading. The query
	// bounds must therefore be the IST wall-clock components reinterpreted as
	// UTC — NOT the true-UTC conversion (which would shift the window 5h30m).
	rangeFilter := bson.M{
		"$gte": primitive.NewDateTimeFromTime(istQueryBound(start)),
		"$lte": primitive.NewDateTimeFromTime(istQueryBound(end)),
	}
	filter := bson.M{"$or": []bson.M{{"dt_tm": rangeFilter}, {"dttm": rangeFilter}}}
	// The embedding vector is large and unused — project it out to keep the
	// cursor light.
	findOpts := options.Find().
		SetSort(bson.D{{Key: "dt_tm", Value: 1}}).
		SetProjection(bson.M{"embedding_shortsummary": 0, "embedding": 0})

	cursor, err := r.collection.Find(ctx, filter, findOpts)
	if err != nil {
		return 0, fmt.Errorf("mongo news repository: find on %s.%s: %w", r.dbName, r.collName, err)
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var raw rawNewsDoc
		if decErr := cursor.Decode(&raw); decErr != nil {
			r.logger.Warn("mongo news repository: skipping un-decodable document", zap.Error(decErr))
			continue
		}
		scanned++
		if cbErr := onEach(raw.toMongoDBEvent()); cbErr != nil {
			return scanned, cbErr
		}
	}
	if cErr := cursor.Err(); cErr != nil {
		return scanned, fmt.Errorf("mongo news repository: cursor: %w", cErr)
	}
	return scanned, nil
}

// LoadCompanyMaster loads the full OdinMasterData.CompanyMaster collection into
// an ISIN-keyed map. Inactive companies (neither NSE nor BSE segment Active)
// are omitted — a news event for such a company is not tradable.
//
// CompanyMaster is a few thousand rows; loading it once per backfill keeps the
// per-event enrichment a pure in-memory map lookup.
func (r *MongoNewsRepository) LoadCompanyMaster(ctx context.Context) (map[string]CompanyInfo, error) {
	if r == nil || r.client == nil {
		return nil, fmt.Errorf("mongo news repository: not initialized")
	}
	coll := r.client.Database("OdinMasterData").Collection("CompanyMaster")
	cursor, err := coll.Find(ctx, bson.M{})
	if err != nil {
		return nil, fmt.Errorf("mongo news repository: company master find: %w", err)
	}
	defer cursor.Close(ctx)

	out := make(map[string]CompanyInfo)
	for cursor.Next(ctx) {
		var d struct {
			ISIN      string      `bson:"isin"`
			Code      interface{} `bson:"code"`
			BSECode   interface{} `bson:"bsecode"`
			MCap      float64     `bson:"mcap"`
			MCapType  string      `bson:"mcaptype"`
			NSEStatus string      `bson:"NSEStatus"`
			BSEStatus string      `bson:"BSEStatus"`
		}
		if err := cursor.Decode(&d); err != nil {
			continue
		}
		if d.ISIN == "" {
			continue
		}
		exchange := ""
		switch {
		case strings.EqualFold(strings.TrimSpace(d.NSEStatus), "Active"):
			exchange = "NSE"
		case strings.EqualFold(strings.TrimSpace(d.BSEStatus), "Active"):
			exchange = "BSE"
		default:
			continue // inactive on both segments — not tradable
		}
		out[d.ISIN] = CompanyInfo{
			ISIN:     d.ISIN,
			NSECode:  toInt64(d.Code),
			BSECode:  toInt64(d.BSECode),
			Exchange: exchange,
			MCap:     d.MCap,
			MCapType: d.MCapType,
		}
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("mongo news repository: company master cursor: %w", err)
	}
	return out, nil
}

// EnrichWithCompany fills the stock-identity fields (exchange, codes, market
// cap) on a raw news event from the CompanyMaster map — the same join
// data-ingestion performs for live news. Returns false when the event's ISIN
// is unknown or its company is inactive, in which case the event is not
// tradable and the caller should skip it.
func EnrichWithCompany(ev *models.MongoDBEvent, companies map[string]CompanyInfo) bool {
	if ev == nil || ev.Company == "" {
		return false
	}
	info, ok := companies[ev.Company]
	if !ok || info.Exchange == "" {
		return false
	}
	ev.Exchange = info.Exchange
	ev.Code = info.NSECode
	ev.NSECode = info.NSECode
	ev.BSECode = info.BSECode
	// Token is what ToMarketEvent uses first for StockCode: the NSE code on
	// NSE, the BSE code on BSE.
	if info.Exchange == "NSE" {
		ev.Token = info.NSECode
	} else {
		ev.Token = info.BSECode
	}
	ev.MCap = info.MCap
	ev.MCapType = info.MCapType
	return true
}

// istZone is UTC+5:30. NewsImpactDashboard.dt_tm stores IST wall-clock time
// tagged with a 'Z' suffix, so timestamp handling must route through this zone.
var istZone = time.FixedZone("IST", 5*60*60+30*60)

// istWallClock reinterprets a dt_tm value read from MongoDB as the real IST
// instant. The Go driver decodes the BSON datetime into a UTC time.Time whose
// wall-clock reading already IS the IST time (because the value was stored as
// IST-tagged-'Z'); this re-stamps that wall-clock with the IST offset so the
// resulting instant is correct.
func istWallClock(t time.Time) time.Time {
	t = t.UTC()
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), istZone)
}

// istQueryBound converts an IST window bound into the value to compare against
// dt_tm. Because dt_tm's stored BSON datetime numerically equals the IST clock
// reading, the bound must be the IST wall-clock components reinterpreted as
// UTC — not the true-UTC conversion of the instant.
func istQueryBound(t time.Time) time.Time {
	t = t.In(istZone)
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC)
}

// toInt64 best-effort converts a BSON-decoded numeric/string value to int64.
func toInt64(v interface{}) int64 {
	switch x := v.(type) {
	case nil:
		return 0
	case int:
		return int64(x)
	case int32:
		return int64(x)
	case int64:
		return x
	case float32:
		return int64(x)
	case float64:
		return int64(x)
	case string:
		s := strings.TrimSpace(x)
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			return n
		}
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return int64(f)
		}
	}
	return 0
}
