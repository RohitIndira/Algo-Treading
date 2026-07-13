// rebalancer is a manual CLI that drives one Manthan rebalance pass.
//
// Reads:
//   - user-config gRPC (active MANTHAN strategies + trade_configs + broker auth) — Phase 0.2 + 0.6a
//   - signals_db.manthan_signals (today's eligible signals; Phase 0.5 migrates to gRPC)
//   - Indira broker (fund-limit + position-book + holdings, real capital)
//   - Redis manthan:ema:allocations (today's per-index EMA targets)
//   - trading_db.manthan_positions / manthan_cooldown / manthan_signal_decisions
//
// Writes:
//   - Kafka trade-signals (MANTHAN_ENTRY for each approved entry)
//
// Usage:
//   go run ./services/rebalancer/cmd/                # all active users, real publish
//   go run ./services/rebalancer/cmd/ --dry-run      # print plan, no publish
//   go run ./services/rebalancer/cmd/ --user ND03920 # one user only
//
// All env via .env; defaults match docker-compose dev setup.
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	goredis "github.com/go-redis/redis/v8"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
	"github.com/RohitIndira/Algo-Treading/services/rebalancer/internal"
)

func main() {
	dryRun := flag.Bool("dry-run", false, "print the plan; no Kafka publish, no broker order")
	userFilter := flag.String("user", "", "rebalance only this user_id (default: all active)")
	flag.Parse()

	loadEnv()

	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// --- DB connections ---
	// Phase 0.6a (2026-06-23) removed the execution_db connection —
	// the only thing that used it was ResolveBrokerAuth, which now goes
	// through user-config gRPC. rebalancer is no longer coupled to
	// trade-execution's DB schema.
	//
	// DB names are env-driven (TRADING_DB, MARKET_DB) so Phase 3 of the
	// database redesign can flip `.env` without a code change. Defaults
	// match the current dev/staging layout; post-cutover both .env files
	// will set TRADING_DB=stockk_trading and MARKET_DB=stockk_market.
	tradingDBName := getEnv("TRADING_DB", "trading_db")
	marketDBName := getEnv("MARKET_DB", "signals_db")
	tradingDB := mustOpen(logger, tradingDBName, buildDSN(tradingDBName))
	defer tradingDB.Close()
	marketDB := mustOpen(logger, marketDBName, buildDSN(marketDBName))
	defer marketDB.Close()

	// --- Indira broker client ---
	brokerBase := getEnv("INDIRA_BASE_URL", "https://livemiddleware.indiratrade.com")
	broker := indiraClient.NewClient(indiraClient.Config{BaseURL: brokerBase})

	// --- Redis (for EMA targets) ---
	// REDIS_ADDR wins; REDIS_URI is kept as a legacy alias for older .env
	// files. Default is localhost — production redis IPs must come from .env,
	// NEVER from a code default (one stale literal cost us an incident).
	rdb := goredis.NewClient(&goredis.Options{
		Addr:     getEnv("REDIS_ADDR", getEnv("REDIS_URI", "localhost:6379")),
		Password: getEnv("REDIS_PASSWORD", ""),
		DB:       0,
	})
	if pingErr := rdb.Ping(ctx).Err(); pingErr != nil {
		logger.Warn("redis ping failed — EMA fallback to defaults",
			zap.Error(pingErr))
	}
	defer rdb.Close()

	// --- Kafka writer (only if not dry-run) ---
	var kafkaWriter *kafka.Writer
	if !*dryRun {
		kafkaWriter = &kafka.Writer{
			Addr:         kafka.TCP(strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ",")...),
			Topic:        "trade-signals",
			Balancer:     &kafka.LeastBytes{},
			BatchTimeout: 1 * time.Millisecond,
			RequiredAcks: kafka.RequireAll,
		}
		defer kafkaWriter.Close()
	}

	// --- Dial user-config gRPC ---
	// Phase 0.2 migrated strategies fetch from direct SQL (cross-bounded-
	// context anti-pattern) to gRPC. user-config is the single owner of the
	// strategies table; rebalancer asks via the RPC. CLI semantics: fail
	// fast if the address is unreachable, no retry.
	ucAddr := getEnv("USER_CONFIG_GRPC_ADDR", "localhost:50051")
	ucClient, err := internal.NewUserConfigClient(ctx, ucAddr)
	if err != nil {
		logger.Fatal("dial user-config gRPC failed",
			zap.String("addr", ucAddr), zap.Error(err))
	}
	defer ucClient.Close()

	// --- Load active strategies ---
	strategies, err := internal.LoadActiveStrategies(ctx, ucClient)
	if err != nil {
		logger.Fatal("load active strategies failed", zap.Error(err))
	}
	if *userFilter != "" {
		filtered := strategies[:0]
		for _, s := range strategies {
			if s.UserID == *userFilter {
				filtered = append(filtered, s)
			}
		}
		strategies = filtered
	}
	if len(strategies) == 0 {
		logger.Info("no active MANTHAN strategies — nothing to do")
		return
	}
	logger.Info("rebalancer starting",
		zap.Int("strategies", len(strategies)),
		zap.Bool("dry_run", *dryRun))

	// --- Today's eligible signals (shared across all users) ---
	signals, err := internal.LoadEligibleSignals(ctx, marketDB)
	if err != nil {
		logger.Fatal("load eligible signals failed", zap.Error(err))
	}
	logger.Info("today's eligible signals", zap.Int("count", len(signals)))
	// IMPORTANT: do NOT early-exit when signals is empty.
	//
	// The rebalancer has two independent phases:
	//
	//   1. TopUp existing positions — driven by EMA-target × per_call vs
	//      current-invested. The EMA sheet refreshes EVERY DAY after market
	//      hours, so today's target can differ from yesterday's. Capital
	//      also moves with P&L, recomputing per_call. Either change creates
	//      a positive gap that Phase 1 fills — completely independent of
	//      whether any new ATH-breakout signal arrived today.
	//
	//   2. Allocate fresh entries — only fires when new eligible signals
	//      exist (Allocate naturally returns empty when len(signals)==0).
	//
	// Earlier this returned here when signals were empty, silently freezing
	// top-ups on every signal-less day. Most trading days have no new ATH
	// signals; under the old behaviour positions stayed at their initial
	// allocation forever even as EMA targets shifted in the sheet.

	// --- Per-strategy rebalance ---
	results := []internal.AllocResult{}
	for _, cfg := range strategies {
		logger.Info("─── strategy",
			zap.String("user", cfg.UserID),
			zap.String("strategy_id", cfg.StrategyID),
			zap.String("name", cfg.StrategyName),
			zap.Float64("total_capital", cfg.TotalCapital),
			zap.Int("max_positions", cfg.MaxPositions))

		if cfg.BearerToken == "" {
			logger.Warn("no broker auth — skipping (no user_credentials row)",
				zap.String("user", cfg.UserID))
			continue
		}

		auth := &indiraClient.AuthContext{
			UserId:      cfg.UserID,
			BearerToken: cfg.BearerToken,
			AppId:       cfg.AppID,
			Source:      cfg.Source,
		}
		cap := internal.FetchCurrentCapital(ctx, broker, auth, tradingDB, cfg.StrategyID, logger)
		logger.Info("capital",
			zap.Float64("cash", cap.Cash),
			zap.Float64("positions", cap.PositionsValue),
			zap.Float64("holdings", cap.HoldingsValue),
			zap.Float64("total", cap.Total),
			zap.String("source", cap.Source))

		snap, err := internal.LoadPortfolioSnapshot(ctx, tradingDB, rdb, cfg, cap)
		if err != nil {
			logger.Error("load snapshot failed — skipping user",
				zap.String("user", cfg.UserID), zap.Error(err))
			continue
		}
		logger.Info("snapshot",
			zap.Float64("per_call", snap.PerCall),
			zap.Int("active_positions", len(snap.ActivePositions)),
			zap.Int("cooldowns", len(snap.CooldownSymbols)),
			zap.Int("user_overrides", len(snap.OverrideSymbols)))

		// Phase 1: top up existing positions whose index EMA target × per_call
		// > already_invested. This bumps deployed[idx] + position invested
		// before the new-entry allocator runs, so Phase 2 sees post-topup
		// state and won't double-deploy capital.
		topups, topupSkipped := internal.TopUpExistingPositions(snap, logger)

		// Phase 2: take fresh entries from today's eligible signals.
		res := internal.Allocate(snap, signals, logger)
		res.TopUps = topups
		res.Skipped = append(topupSkipped, res.Skipped...)
		results = append(results, res)
	}

	// --- Publish + summary ---
	// tradingDB is needed for Phase A (write manthan_signal_decisions
	// PROPOSED) and Phase C (mark DISPATCHED) inside PublishOrders. Required
	// — the rules-engine projector reads decision rows when projecting
	// ENTRY_FILLED into manthan_positions.
	published, pubErr := internal.PublishOrders(ctx, kafkaWriter, tradingDB, results, *dryRun, logger)
	printSummary(results, published, *dryRun)
	if pubErr != nil {
		logger.Error("at least one publish failed", zap.Error(pubErr))
		os.Exit(2)
	}
}

func printSummary(results []internal.AllocResult, published int, dryRun bool) {
	fmt.Printf("\n=== REBALANCE SUMMARY%s ===\n", map[bool]string{true: " (DRY-RUN)", false: ""}[dryRun])
	totalTopUps, totalPlanned, totalSkipped := 0, 0, 0
	for _, r := range results {
		fmt.Printf("\nUser: %s   per_call=₹%.2f   capital=₹%.2f (%s)\n",
			r.Cfg.UserID, r.PerCall, r.Capital.Total, r.Capital.Source)
		fmt.Printf("  Topped up (%d):\n", len(r.TopUps))
		for _, p := range r.TopUps {
			fmt.Printf("    %-12s +qty=%-4d add=₹%-9.0f index=%s ema=%.0f%%   %s\n",
				p.Symbol, p.Quantity, p.InvestedAmt, p.IndexName, p.EMAFraction*100, p.Reason)
		}
		fmt.Printf("  Planned (%d):\n", len(r.Planned))
		for _, p := range r.Planned {
			fmt.Printf("    %-12s qty=%-4d invested=₹%-10.0f index=%s ema=%.0f%%   %s\n",
				p.Symbol, p.Quantity, p.InvestedAmt, p.IndexName, p.EMAFraction*100, p.Reason)
		}
		fmt.Printf("  Skipped (%d):\n", len(r.Skipped))
		for _, s := range r.Skipped {
			fmt.Printf("    %-12s %s\n", s.Symbol, s.Reason)
		}
		totalTopUps += len(r.TopUps)
		totalPlanned += len(r.Planned)
		totalSkipped += len(r.Skipped)
	}
	fmt.Printf("\nTotal: topups=%d  planned=%d  skipped=%d  published=%d\n",
		totalTopUps, totalPlanned, totalSkipped, published)
	if dryRun {
		fmt.Println("DRY-RUN: no Kafka messages were published. Re-run without --dry-run to commit.")
	}
}

// --- helpers ---

// loadEnv loads environment from .env files. Order matters: godotenv.Overload
// makes LATER files win, so we load shared/legacy files first and the
// rebalancer's own .env LAST. The rebalancer .env is authoritative — every
// var main.go reads is documented in services/rebalancer/.env.example.
//
// Files in load order (each one optional; missing files silently skipped):
//   1. ./.env                              — repo-root shared (legacy)
//   2. services/data-ingestion/.env        — shared Kafka/Redis (legacy)
//   3. services/rebalancer/.env            — rebalancer-specific (WINS)
func loadEnv() {
	for _, p := range []string{
		".env",
		"services/data-ingestion/.env",
		"services/rebalancer/.env",
	} {
		if abs, err := filepath.Abs(p); err == nil {
			_ = godotenv.Overload(abs)
		}
	}
}

func mustOpen(logger *zap.Logger, name, dsn string) *sql.DB {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		logger.Fatal("db open failed", zap.String("db", name), zap.Error(err))
	}
	if err := db.Ping(); err != nil {
		logger.Fatal("db ping failed", zap.String("db", name), zap.Error(err))
	}
	return db
}

func buildDSN(dbname string) string {
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		getEnv("POSTGRES_HOST", "localhost"),
		getEnv("POSTGRES_PORT", "5432"),
		getEnv("POSTGRES_USER", "postgres"),
		getEnv("POSTGRES_PASSWORD", "postgres"),
		dbname,
		getEnv("POSTGRES_SSL_MODE", "disable"),
	)
}

func getEnv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
