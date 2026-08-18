// Reads Manthan data from the live Google Sheet, runs the full pipeline,
// and publishes results to Postgres (all candidates) + Kafka (eligible only).
//
// Usage:
//   export MANTHAN_SHEET_ID=...  (default: the live sheet ID below)
//   export MANTHAN_CREDS=...     (default: local service-account JSON)
//   export KAFKA_BROKERS=localhost:9092  (optional; skip Kafka if unset)
//   go run ./services/data-ingestion/cmd/manthan-live/
//
// WATCH MODE (event-driven ingestion, 2026-08-18):
//   MANTHAN_WATCH=1            → after the initial run, stay alive and poll a
//                                fingerprint of the BuySignal tab; when the
//                                operator edits the sheet, the pipeline re-runs
//                                AUTOMATICALLY within one poll interval. The
//                                publisher's per-day idempotency guarantees
//                                only NEW symbols reach Kafka, and rules-engine
//                                fans every new signal out to ALL active
//                                strategies — no manual re-run needed.
//   MANTHAN_WATCH_INTERVAL=120 → poll seconds (default 120)
//   Watch polls only Mon–Fri 08:45–15:25 IST (outside the window edits wait
//   for the window; the daily 09:00 IST pm2 cron_restart still guarantees the
//   fresh-morning load).
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

	if os.Getenv("MANTHAN_WATCH") == "1" {
		watchLoop(logger)
		return
	}
	if err := runPipelineOnce(logger); err != nil {
		logger.Fatal("pipeline run failed", zap.Error(err))
	}
}

// runPipelineOnce executes one full sheet→pipeline→DB/Kafka/EMA cycle.
// Errors are returned (not Fatal'd) so watch mode survives a bad cycle —
// a transient Sheets/DB error just waits for the next trigger.
func runPipelineOnce(logger *zap.Logger) error {
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
		return fmt.Errorf("sheets connect: %w", err)
	}
	raw, err := reader.ReadAll(ctx)
	if err != nil {
		return fmt.Errorf("sheets read: %w", err)
	}

	pipeline := manthan.NewPipeline(logger)
	result := pipeline.Run(raw)

	// --- DB + Kafka publisher ---
	cfg := config.Load()
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		cfg.PGHost, cfg.PGPort, cfg.PGUser, cfg.PGPassword, cfg.PGDatabase, cfg.PGSSLMode)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("db open: %w", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		return fmt.Errorf("db ping: %w", err)
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
		return fmt.Errorf("publish: %w", err)
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
	return nil
}

// watchLoop is the event-driven mode: run once, then re-run automatically
// whenever the BuySignal tab changes. Change detection is a SHA-256 over the
// tab's raw values (one Sheets API read per poll — well inside quota); the
// Sheets-readonly scope has no Drive metadata access, so hashing content is
// the reliable signal. A cycle failure never kills the loop.
func watchLoop(logger *zap.Logger) {
	interval := 120 * time.Second
	if v := os.Getenv("MANTHAN_WATCH_INTERVAL"); v != "" {
		if n, err := time.ParseDuration(v + "s"); err == nil && n >= 30*time.Second {
			interval = n
		}
	}
	sheetID := os.Getenv("MANTHAN_SHEET_ID")
	if sheetID == "" {
		sheetID = defaultSheetID
	}
	creds := os.Getenv("MANTHAN_CREDS")
	if creds == "" {
		creds = defaultCreds
	}

	logger.Info("manthan-live WATCH mode — sheet edits trigger the pipeline automatically",
		zap.Duration("poll", interval), zap.String("sheet", sheetID))

	var lastSyms []string
	haveBaseline := false
	buySymbols := func() ([]string, bool) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		r := manthan.NewGSheetReader(sheetID, creds, logger)
		if err := r.Connect(ctx); err != nil {
			logger.Warn("watch: sheets connect failed — will retry", zap.Error(err))
			return nil, false
		}
		syms, err := r.BuySymbols(ctx)
		if err != nil {
			logger.Warn("watch: buy-symbol read failed — will retry", zap.Error(err))
			return nil, false
		}
		return syms, true
	}

	// Initial run + baseline symbol set.
	if err := runPipelineOnce(logger); err != nil {
		logger.Error("watch: initial run failed — will retry on next change/tick", zap.Error(err))
	} else if syms, ok := buySymbols(); ok {
		lastSyms, haveBaseline = syms, true
	}

	ist, _ := time.LoadLocation("Asia/Kolkata")
	if ist == nil {
		ist = time.FixedZone("IST", 5*3600+1800)
	}
	for {
		time.Sleep(interval)
		now := time.Now().In(ist)
		if now.Weekday() == time.Saturday || now.Weekday() == time.Sunday {
			continue
		}
		mod := now.Hour()*60 + now.Minute()
		if mod < 8*60+45 || mod > 15*60+25 {
			continue // outside the tradeable window — edits wait for the window
		}
		syms, ok := buySymbols()
		if !ok {
			continue
		}
		if !haveBaseline {
			// The initial run never established a baseline (e.g. it failed):
			// treat the first successful read as a change so the pipeline
			// catches up, then baseline from it.
			logger.Info("watch: establishing baseline — running pipeline",
				zap.Int("buy_symbols", len(syms)))
		} else {
			added, removed := diffSymbols(lastSyms, syms)
			if len(added) == 0 && len(removed) == 0 {
				continue // price recalcs / cosmetic edits — NOT a stock change
			}
			logger.Info("watch: BUY LIST CHANGED — running pipeline",
				zap.Strings("added", added),
				zap.Strings("removed", removed),
				zap.Int("total_now", len(syms)))
		}
		if err := runPipelineOnce(logger); err != nil {
			logger.Error("watch: pipeline run failed — baseline kept, will retry next tick", zap.Error(err))
			continue // lastSyms unchanged → next tick sees the same diff and retries
		}
		lastSyms, haveBaseline = syms, true
	}
}

// diffSymbols returns (added, removed) between two SORTED symbol slices.
func diffSymbols(old, new []string) (added, removed []string) {
	oldSet := make(map[string]struct{}, len(old))
	for _, s := range old {
		oldSet[s] = struct{}{}
	}
	newSet := make(map[string]struct{}, len(new))
	for _, s := range new {
		newSet[s] = struct{}{}
		if _, ok := oldSet[s]; !ok {
			added = append(added, s)
		}
	}
	for _, s := range old {
		if _, ok := newSet[s]; !ok {
			removed = append(removed, s)
		}
	}
	return added, removed
}
