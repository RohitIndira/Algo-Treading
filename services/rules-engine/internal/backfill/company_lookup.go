package backfill

import (
	"context"
	"fmt"
	"strconv"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

// CompanyInfo holds the fields we need from OdinMasterData.CompanyMaster.
type CompanyInfo struct {
	ISIN        string
	NSECode     int64   // parsed from string nsecode field
	BSECode     int64   // parsed from string bsecode field
	MCap        float64 // market cap in crores
	MCapType    string  // "Small", "Mid", "Large"
	Exchange    string  // "NSE", "BSE", "DELISTED", "UNLISTED"
	Symbol      string
	CompanyName string
}

// companyMasterDoc mirrors the raw document from OdinMasterData.CompanyMaster.
//
// IMPORTANT: CompanyMaster does NOT have "nsecode"/"exchange"/"symbol"/
// "company_name" fields. The real schema is:
//   - code           → NSE token (also the Redis key market:nse:<code>)
//   - bsecode        → BSE token
//   - nsesymbol      → NSE trading symbol
//   - companyname    → company name
//   - nselistedflag  → "Y"/"N"  (used to derive Exchange="NSE")
//   - bselistedflag  → "Y"/"N"
// code/bsecode arrive as either numbers (int32/int64/float64) or strings
// depending on the document, so they are decoded as interface{} and coerced.
type companyMasterDoc struct {
	ISIN          string      `bson:"isin"`
	Code          interface{} `bson:"code"`    // NSE token (mixed int/string)
	BSECode       interface{} `bson:"bsecode"` // BSE token (mixed int/string)
	MCap          float64     `bson:"mcap"`
	MCapType      string      `bson:"mcaptype"`
	NSESymbol     string      `bson:"nsesymbol"`
	CompanyName   string      `bson:"companyname"`
	NSEListedFlag string      `bson:"nselistedflag"`
	BSEListedFlag string      `bson:"bselistedflag"`
}

// toInt64 coerces a mixed-type BSON numeric/string value to int64.
func toInt64(v interface{}) int64 {
	switch x := v.(type) {
	case int32:
		return int64(x)
	case int64:
		return x
	case float64:
		return int64(x)
	case string:
		n, _ := strconv.ParseInt(x, 10, 64)
		return n
	default:
		return 0
	}
}

// CompanyLookup fetches company metadata from OdinMasterData.CompanyMaster.
type CompanyLookup struct {
	coll   *mongo.Collection
	logger *zap.Logger
}

// NewCompanyLookup connects to OdinMasterData.CompanyMaster.
func NewCompanyLookup(client *mongo.Client, logger *zap.Logger) *CompanyLookup {
	return &CompanyLookup{
		coll:   client.Database("OdinMasterData").Collection("CompanyMaster"),
		logger: logger,
	}
}

// BatchLookup fetches company data for a set of ISINs in a single $in query.
// Returns map[ISIN]CompanyInfo. ISINs with no matching document are omitted.
func (cl *CompanyLookup) BatchLookup(ctx context.Context, isins []string) (map[string]CompanyInfo, error) {
	if len(isins) == 0 {
		return map[string]CompanyInfo{}, nil
	}

	// Deduplicate ISINs before querying.
	seen := make(map[string]struct{}, len(isins))
	unique := isins[:0]
	for _, isin := range isins {
		if _, ok := seen[isin]; !ok {
			seen[isin] = struct{}{}
			unique = append(unique, isin)
		}
	}

	filter := bson.D{{Key: "isin", Value: bson.D{{Key: "$in", Value: unique}}}}
	projection := bson.D{
		{Key: "isin", Value: 1},
		{Key: "code", Value: 1},     // NSE token (= Redis key market:nse:<code>)
		{Key: "bsecode", Value: 1},  // BSE token
		{Key: "mcap", Value: 1},
		{Key: "mcaptype", Value: 1},
		{Key: "nsesymbol", Value: 1},
		{Key: "companyname", Value: 1},
		{Key: "nselistedflag", Value: 1},
		{Key: "bselistedflag", Value: 1},
	}

	cursor, err := cl.coll.Find(ctx, filter, options.Find().SetProjection(projection))
	if err != nil {
		return nil, fmt.Errorf("company_lookup: find: %w", err)
	}
	defer cursor.Close(ctx)

	result := make(map[string]CompanyInfo, len(unique))
	for cursor.Next(ctx) {
		var doc companyMasterDoc
		if err := cursor.Decode(&doc); err != nil {
			cl.logger.Warn("company_lookup: decode error, skipping", zap.Error(err))
			continue
		}
		if doc.ISIN == "" {
			continue
		}

		// Derive a single Exchange tag. AMN backfill is NSE-only, so NSE listing
		// takes priority; BSE is recorded as a fallback for completeness.
		exchange := ""
		if doc.NSEListedFlag == "Y" {
			exchange = "NSE"
		} else if doc.BSEListedFlag == "Y" {
			exchange = "BSE"
		}

		result[doc.ISIN] = CompanyInfo{
			ISIN:        doc.ISIN,
			NSECode:     toInt64(doc.Code),
			BSECode:     toInt64(doc.BSECode),
			MCap:        doc.MCap,
			MCapType:    doc.MCapType,
			Exchange:    exchange,
			Symbol:      doc.NSESymbol,
			CompanyName: doc.CompanyName,
		}
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("company_lookup: cursor: %w", err)
	}

	cl.logger.Info("company_lookup: resolved ISINs",
		zap.Int("requested", len(unique)),
		zap.Int("found", len(result)))

	return result, nil
}

// UniqueISINs extracts the deduplicated ISIN list from a news doc slice.
func UniqueISINs(docs []RawNewsDoc) []string {
	seen := make(map[string]struct{}, len(docs))
	out := make([]string, 0, len(docs))
	for _, d := range docs {
		if d.ISIN == "" {
			continue
		}
		if _, ok := seen[d.ISIN]; !ok {
			seen[d.ISIN] = struct{}{}
			out = append(out, d.ISIN)
		}
	}
	return out
}
