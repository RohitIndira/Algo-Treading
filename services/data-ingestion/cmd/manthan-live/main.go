// Reads Manthan data from the live Google Sheet, runs the full pipeline,
// and publishes results to Postgres (all candidates) + Kafka (eligible only).
//
// Usage:
//   export MANTHAN_SHEET_ID=...  (default: the live sheet ID below)
//   export MANTHAN_CREDS=...     (default: local service-account JSON)
//   export KAFKA_BROKERS=localhost:9092  (optional; skip Kafka if unset)
//   go run ./services/data-ingestion/cmd/manthan-live/
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	goredis "github.com/go-redis/redis/v8"
	_ "github.com/lib/pq"
	"go.uber.org/zap"

	"github.com/RohitIndira/Algo-Treading/pkg/database/mongodb"
	projectlogger "github.com/RohitIndira/Algo-Treading/pkg/logger"
	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/config"
	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/internal/data"
	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/internal/manthan"
)

const (
	defaultSheetID = "1E_MzQNQFNvnmR8wMZMCyzPKc-SjOei4wwQp4QAey5sc"
	defaultCreds   = "/home/rohitt/Algo-Treading/services/data-ingestion/credentials/manthan-sheet.json"
)

func main() {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	sheetID := os.Getenv("MANTHAN_SHEET_ID")
	if sheetID == "" {
		sheetID = defaultSheetID
	}
	creds := os.Getenv("MANTHAN_CREDS")
	if creds == "" {
		creds = defaultCreds
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// --- Sheets ---
	reader := manthan.NewGSheetReader(sheetID, creds, logger)
	if err := reader.Connect(ctx); err != nil {
		logger.Fatal("Sheets connect failed", zap.Error(err))
	}
	raw, err := reader.ReadAll(ctx)
	if err != nil {
		logger.Fatal("Sheets read failed", zap.Error(err))
	}

	pipeline := manthan.NewPipeline(logger)
	result := pipeline.Run(raw)

	// --- DB + Kafka publisher ---
	cfg := config.Load()
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		cfg.PGHost, cfg.PGPort, cfg.PGUser, cfg.PGPassword, cfg.PGDatabase, cfg.PGSSLMode)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		logger.Fatal("DB open failed", zap.Error(err))
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		logger.Fatal("DB ping failed", zap.Error(err))
	}

	var brokers []string
	if b := strings.TrimSpace(os.Getenv("KAFKA_BROKERS")); b != "" {
		brokers = strings.Split(b, ",")
	}

	// Build a CompanyResolver backed by Redis + MongoDB. The resolver does
	// the lazy fetch-and-cache: trade-execution's broker_adapter reads
	// `isin:<ISIN>` from Redis at order time, so we MUST guarantee that key
	// exists for every signal we publish. The publisher uses this resolver
	// as a pre-flight gate — un-resolvable ISINs get downgraded to
	// FILTER_REJECTED before Kafka.
	//
	// Failure is non-fatal: if MongoDB or Redis is unavailable we fall back
	// to publishing without the gate (legacy behaviour) and warn loudly.
	var resolver manthan.CompanyResolver
	mongoClient, mongoErr := mongodb.New(ctx, mongodb.Config{
		URI:            cfg.MongoURI,
		Database:       cfg.MongoDatabase,
		ConnectTimeout: cfg.MongoConnectTimeout,
	})
	if mongoErr != nil {
		logger.Warn("MongoDB connect failed — pre-flight ISIN gate disabled",
			zap.Error(mongoErr))
	} else {
		defer mongoClient.Close(context.Background())
		dataLogger, lgrErr := projectlogger.NewWithDefaults("manthan-live-resolver")
		if lgrErr != nil {
			logger.Warn("data-logger init failed — pre-flight ISIN gate disabled",
				zap.Error(lgrErr))
		} else {
			redisMgr, rErr := data.NewRedisManager(cfg.RedisURI, cfg.RedisPassword, cfg.RedisDB, dataLogger, mongoClient)
			if rErr != nil {
				logger.Warn("Redis connect failed — pre-flight ISIN gate disabled",
					zap.Error(rErr))
			} else {
				defer redisMgr.Close()
				resolver = redisMgr
				logger.Info("Pre-flight ISIN gate ENABLED — ELIGIBLE signals will be checked against MongoDB CompanyMaster")
			}
		}
	}

	pub := manthan.NewPublisher(manthan.PublisherConfig{
		DB:           db,
		KafkaBrokers: brokers,
		KafkaTopic:   "manthan.signals",
		Resolver:     resolver,
	}, logger)
	defer pub.Close()

	// Publish EMA allocations to the SAME Redis that rules-engine + rebalancer
	// read from (configured via REDIS_URI / REDIS_PASSWORD in .env). The
	// previous hardcoded localhost:6379 caused a silent split-brain — manthan-
	// live wrote to local but downstream consumers looked at the remote Indira
	// Redis, so the EMA map was effectively missing and consumers fell back to
	// a hardcoded default.
	emaRdb := goredis.NewClient(&goredis.Options{
		Addr:     cfg.RedisURI,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})
	if emaRdb.Ping(ctx).Err() == nil {
		// Defensive unit normalization: the allocator consumes FRACTIONS
		// (0.0–1.0). If the sheet's Allocation column ever carries percent
		// values (30 instead of 0.30), publishing raw would inflate every
		// per-stock allocation 100x. Normalize and say so.
		for k, v := range result.IndexAllocation {
			if v > 1.0 {
				result.IndexAllocation[k] = v / 100.0
			}
		}
		emaJSON, _ := json.Marshal(result.IndexAllocation)
		emaRdb.Set(ctx, "manthan:ema:allocations", string(emaJSON), 24*time.Hour)
		logger.Info("EMA allocations published to Redis",
			zap.String("redis", cfg.RedisURI),
			zap.Any("allocations", result.IndexAllocation))
		emaRdb.Close()
	} else {
		logger.Warn("Redis not available — EMA allocations not cached",
			zap.String("redis", cfg.RedisURI))
	}

	pubStats, err := pub.Publish(ctx, result)
	if err != nil {
		logger.Fatal("Publish failed", zap.Error(err))
	}
	fmt.Printf("\n=== PUBLISH SUMMARY ===\n")
	fmt.Printf("DB wrote — eligible:        %d\n", pubStats.EligibleWritten)
	fmt.Printf("DB wrote — filter rejected: %d\n", pubStats.FilterRejectedWritten)
	fmt.Printf("DB wrote — data dropped:    %d\n", pubStats.DroppedWritten)
	fmt.Printf("Kafka published:            %d\n", pubStats.KafkaPublished)
	if pubStats.KafkaError != nil {
		fmt.Printf("Kafka error:                %v\n", pubStats.KafkaError)
	}

	fmt.Printf("\n=== LIVE MANTHAN PIPELINE ===\n")
	fmt.Printf("Sheet ID: %s\n", sheetID)
	fmt.Printf("ATH_Entry=Buy stocks:  %d\n", result.Stats.BuySignalTotal)
	fmt.Printf("Dropped (data):        %d\n", result.Stats.MissingData)
	fmt.Printf("Failed filters:        %d\n", result.Stats.FilterFailed)
	fmt.Printf("Rejected (caps):       %d\n", result.Stats.CapRejected)
	fmt.Printf("FINAL ELIGIBLE:        %d\n", result.Stats.Eligible)

	fmt.Printf("\n=== PER-FILTER REJECTS ===\n")
	fmt.Printf("MCap range:       %d\n", result.Stats.FailedMCap)
	fmt.Printf("PE > 60 / <=0:    %d\n", result.Stats.FailedPE)
	fmt.Printf("PAT <= 0:         %d\n", result.Stats.FailedPAT)
	fmt.Printf("FScore < 60:      %d\n", result.Stats.FailedFScore)
	fmt.Printf("BarNo < 20:       %d\n", result.Stats.FailedBarNo)
	fmt.Printf("20BarVol <= 1Cr:  %d\n", result.Stats.FailedVolume)

	// Missing-data drops
	fmt.Printf("\n=== DROPPED (incomplete data) ===\n")
	for _, d := range result.Drops {
		fmt.Printf("  %-14s %s\n", d.Symbol, d.Reason)
	}

	// Filter rejects
	fmt.Printf("\n=== FILTER REJECTED ===\n")
	for _, s := range result.FilteredOut {
		fmt.Printf("  %-14s reason=%-35s MCap=%7.0f PE=%6.2f FS=%3.0f PAT=%8.2f BarNo=%d\n",
			s.Symbol, s.FilterReason, s.MarketCap, s.PE, s.FScore, s.PAT, s.BarNo)
	}

	// Final eligible
	fmt.Printf("\n=== FINAL ELIGIBLE STOCKS (%d) ===\n", len(result.Eligible))
	if len(result.Eligible) == 0 {
		fmt.Println("  (none today)")
	}
	for _, s := range result.Eligible {
		fmt.Printf("\n  %s — %s\n", s.Symbol, s.CompanyName)
		fmt.Printf("    Industry=%q  Bucket=%s  Index=%s  Alloc=%.1f%%\n",
			s.Industry, s.MCapBucket, s.IndexName, s.Allocation*100)
		fmt.Printf("    MCap=%.0f Cr  PE=%.2f  FScore=%.0f  PAT=%.2f Cr\n",
			s.MarketCap, s.PE, s.FScore, s.PAT)
		fmt.Printf("    ATH=%.2f  Price=%.2f  52WHi=%.2f\n",
			s.ATHClose, s.LatestPrice, s.Week52High)
		fmt.Printf("    BarNo=%d  20BarVal=%.0f  >1Cr=%v\n",
			s.BarNo, s.AvgVal20Bars, s.AvgVal20BarsGt1Cr)
	}

	// Index allocation (for audit)
	fmt.Printf("\n=== INDEX ALLOCATION (live) ===\n")
	type kv struct {
		K string
		V float64
	}
	list := make([]kv, 0, len(result.IndexAllocation))
	for k, v := range result.IndexAllocation {
		list = append(list, kv{k, v})
	}
	sort.Slice(list, func(i, j int) bool { return list[i].V > list[j].V })
	for _, item := range list {
		fmt.Printf("  %-15s %.1f%%\n", item.K, item.V*100)
	}
}
