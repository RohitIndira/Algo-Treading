package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
	"go.uber.org/zap"

	"github.com/RohitIndira/Algo-Treading/services/api-gateway/config"
	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/admin"
	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/algos"
	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/auth"
	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/grpc_clients"
	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/handlers"
	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/livealgos"
	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/middleware"
	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/notifications"
	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/performance"
	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/portfolio"
	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/router"
)

func main() {
	// Load config
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	log.Println("Starting API Gateway...")

	// gRPC client: user-config-service
	userConfigClient, err := grpc_clients.NewUserConfigClient(
		cfg.Services.UserConfigAddr,
		cfg.Server.GRPCTimeout,
	)
	if err != nil {
		log.Fatalf("Failed to initialize user config client: %v", err)
	}
	defer userConfigClient.Close()

	log.Printf("Connected to User Config Service at %s", cfg.Services.UserConfigAddr)

	userConfigHandler := handlers.NewUserConfigHandler(userConfigClient)

	// Initialize Redis client for WebSocket pub/sub. Address is env-configurable
	// (REDIS_LOCAL_ADDR) so non-default deployments aren't pinned to :6379.
	localRedisAddr := os.Getenv("REDIS_LOCAL_ADDR")
	if localRedisAddr == "" {
		localRedisAddr = "localhost:6379"
	}
	redisClient := redis.NewClient(&redis.Options{
		Addr:     localRedisAddr,
		Password: os.Getenv("REDIS_LOCAL_PASSWORD"),
		DB:       0,
	})

	// Test Redis connection
	ctx := context.Background()
	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Printf("Warning: Redis connection failed: %v (WebSocket features will not work)", err)
	} else {
		log.Println("Connected to Redis for WebSocket pub/sub")
	}
	defer redisClient.Close()

	// Initialize logger for WebSocket handler
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	// Notifications bridge: Kafka `manthan.notifications` → /ws/notifications.
	// Broadcaster is in-process per-user fan-out; consumer pumps Kafka
	// into it. Bridge is best-effort — if Kafka is down we log and run
	// without it (the WS route stays mounted; the broadcaster just has
	// no producer feeding it).
	notifBroadcaster := notifications.NewBroadcaster(logger)
	notifConsumer, ncErr := notifications.NewConsumer(notifications.Config{
		Brokers: cfg.Services.KafkaBrokers,
		Topic:   cfg.Services.NotificationsTopic,
	}, notifBroadcaster, logger)
	if ncErr != nil {
		log.Printf("Warning: notifications consumer init failed: %v (notifications WS will be silent)", ncErr)
	} else {
		go func() {
			if rErr := notifConsumer.Run(context.Background()); rErr != nil {
				log.Printf("notifications consumer stopped: %v", rErr)
			}
		}()
		log.Printf("Notifications bridge running: Kafka %v / topic=%s → /ws/notifications",
			cfg.Services.KafkaBrokers, cfg.Services.NotificationsTopic)
	}

	// Initialize WebSocket handler
	websocketHandler := handlers.NewWebSocketHandler(redisClient, notifBroadcaster, logger)

	// Initialize Paper Trading handler
	paperTradingHandler := handlers.NewPaperTradingHandler(cfg.Services.TradeExecutionPaperURL)

	// Initialize Manthan handler — Manthan data is split across two Postgres DBs:
	//
	//   MANTHAN_SIGNALS_DB   — holds signals_db.manthan_signals
	//                          (written by data-ingestion / manthan-live).
	//                          Default: signals_db.
	//   MANTHAN_POSITIONS_DB — holds trading_db.manthan_positions
	//                          and trading_db.manthan_cooldown
	//                          (written by the rules-engine publisher).
	//                          Default: trading_db.
	//
	// Handler is nil-safe: failure of either DB just disables the respective
	// section in the /manthan/overview response.
	//
	// Optionally also connects to the external Redis (Indira's market data feed)
	// to enrich positions + signals with live LTP. Env: EXT_REDIS_ADDR / EXT_REDIS_PASSWORD.
	var manthanHandler *handlers.ManthanHandler
	var healthHandler *handlers.HealthHandler
	var perfHandler *handlers.PerformanceHandler
	// Hoisted so livealgos handler can reuse for chart data in
	// GetStrategyDetails (algo_performance_daily → DetailsChart).
	var liveAlgosPerfStore performance.Store
	var liveAlgosPerfClientMap map[string]string
	var liveAlgosNAVStore *livealgos.NAVStore // true deployment NAV curve (stockk_market)
	var extRedis *redis.Client                // hoisted: reused by the market-quote handler
	// positionsDB, ordersDB + their DB names hoisted so downstream setup
	// (live-algos store in DB.1, portfolio token lookup in PF.D) can reuse
	// the same pools instead of dialling a second time.
	//
	// positionsSSotDB is the CQRS SSOT for lifecycle P&L (positions_db,
	// written by services/positions). Distinct from positionsDB (trading_db)
	// which still holds strategies + trade_configs + legacy manthan_positions.
	// signalsDB hoisted so the live-algos store can enrich per-symbol
	// isin/industry/mcap from signals_db.manthan_signals.
	var positionsDB *sql.DB
	var positionsSSotDB *sql.DB
	var ordersDB *sql.DB
	var signalsDB *sql.DB
	var positionsDBName, ordersDBName, positionsSSotDBName string
	{
		pgHost := envOr("POSTGRES_HOST", "localhost")
		pgPort := envOr("POSTGRES_PORT", "5432")
		pgUser := envOr("POSTGRES_USER", "postgres")
		pgPass := envOr("POSTGRES_PASSWORD", "postgres")
		pgSSL := envOr("POSTGRES_SSLMODE", "disable")
		signalsDBName := envOr("MANTHAN_SIGNALS_DB", "signals_db")
		positionsDBName = envOr("MANTHAN_POSITIONS_DB", "trading_db")
		positionsSSotDBName = envOr("POSITIONS_DB", "positions_db")
		ordersDBName = envOr("MANTHAN_ORDERS_DB", "execution_db")
		perfDBName := envOr("MANTHAN_PERF_DB", "stockk_market")

		openPG := func(dbName string) (*sql.DB, error) {
			connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
				pgHost, pgPort, pgUser, pgPass, dbName, pgSSL)
			db, err := sql.Open("postgres", connStr)
			if err != nil {
				return nil, err
			}
			db.SetMaxOpenConns(5)
			db.SetMaxIdleConns(2)
			pCtx, pCancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer pCancel()
			if err := db.PingContext(pCtx); err != nil {
				_ = db.Close()
				return nil, err
			}
			return db, nil
		}

		if db, err := openPG(signalsDBName); err != nil {
			log.Printf("Warning: Manthan signals DB (%s) open/ping failed: %v", signalsDBName, err)
		} else {
			log.Printf("Manthan signals DB connected (%s)", signalsDBName)
			signalsDB = db
			defer signalsDB.Close()
		}

		if db, err := openPG(positionsDBName); err != nil {
			log.Printf("Warning: Manthan positions DB (%s) open/ping failed: %v", positionsDBName, err)
		} else {
			log.Printf("Manthan positions DB connected (%s)", positionsDBName)
			positionsDB = db
			defer positionsDB.Close()
		}

		// Positions SSOT (positions_db, written by services/positions). This
		// is what live-algos Positions() reads — the legacy manthan_positions
		// path had unreliable realized_pnl. Nil-safe: on connect failure the
		// live-algos details tier returns an empty position list rather than
		// falling back to the stale table (that regression is the whole point
		// of switching).
		if db, err := openPG(positionsSSotDBName); err != nil {
			log.Printf("Warning: Positions SSOT DB (%s) open/ping failed: %v", positionsSSotDBName, err)
		} else {
			log.Printf("Positions SSOT DB connected (%s)", positionsSSotDBName)
			positionsSSotDB = db
			defer positionsSSotDB.Close()
		}

		if db, err := openPG(ordersDBName); err != nil {
			log.Printf("Warning: Manthan orders DB (%s) open/ping failed: %v", ordersDBName, err)
		} else {
			log.Printf("Manthan orders DB connected (%s)", ordersDBName)
			ordersDB = db
			defer ordersDB.Close()
		}

		// Performance data DB — algo_performance_daily + benchmark_daily.
		// Populated by the perf-etl service (currently the one-off Python
		// script /tmp/perf_etl.py). If unreachable, the /performance
		// endpoint returns 500 but the rest of the gateway keeps working.
		perfDB, err := openPG(perfDBName)
		if err != nil {
			log.Printf("Warning: Manthan performance DB (%s) open/ping failed: %v (performance route disabled)", perfDBName, err)
		} else {
			log.Printf("Manthan performance DB connected (%s)", perfDBName)
			defer perfDB.Close()
			perfStore := performance.NewPostgresStore(perfDB)
			liveAlgosNAVStore = livealgos.NewNAVStore(perfDB)
			// Maps algo id → reference client id in the sheet. Grows as
			// we add more algos; for launch there's exactly one entry.
			perfClientMap := map[string]string{
				"algo_manthan_v1": "A844",
			}
			perfHandler = handlers.NewPerformanceHandler(perfStore, perfClientMap)
			// Share with the livealgos details handler so /users/me/live-algos/{sid}
			// can serve the Manthan-Momentum growth chart (algo_performance_daily
			// rebased to strategy created_at).
			liveAlgosPerfStore = perfStore
			liveAlgosPerfClientMap = perfClientMap
		}

		// Optional external Redis for live LTP (assigns the hoisted var)
		if extAddr := os.Getenv("EXT_REDIS_ADDR"); extAddr != "" {
			extRedis = redis.NewClient(&redis.Options{
				Addr:         extAddr,
				Password:     os.Getenv("EXT_REDIS_PASSWORD"),
				DB:           0,
				PoolSize:     10,
				MinIdleConns: 2,
				ReadTimeout:  2 * time.Second,
			})
			pCtx, pCancel := context.WithTimeout(context.Background(), 2*time.Second)
			if err := extRedis.Ping(pCtx).Err(); err != nil {
				log.Printf("Warning: external Redis ping failed: %v (live LTP disabled)", err)
				_ = extRedis.Close()
				extRedis = nil
			} else {
				log.Printf("External Redis connected for live LTP (%s)", extAddr)
				defer extRedis.Close()
			}
			pCancel()
		}

		if signalsDB != nil || positionsDB != nil || ordersDB != nil {
			manthanHandler = handlers.NewManthanHandler(signalsDB, positionsDB, ordersDB, redisClient, extRedis)
		}

		// Health probe handler — bootstraps the health_probes table on each
		// reachable DB and exposes /livez, /readyz, /health. Built here so
		// it can reuse the same DB/Redis/gRPC handles the rest of the app
		// uses (probes the SAME pool the production code uses, not a sidecar).
		healthHandler = handlers.NewHealthHandler(
			signalsDB, positionsDB, ordersDB,
			redisClient, extRedis,
			userConfigClient,
			nil, /* logger optional; main.go uses log.Printf */
		)
	}

	// CORS config
	corsConfig := middleware.CORSConfig{
		AllowedOrigins: cfg.CORS.AllowedOrigins,
		AllowedMethods: cfg.CORS.AllowedMethods,
		AllowedHeaders: cfg.CORS.AllowedHeaders,
	}

	// Market-quote handler — serves live quotes off the ext-Redis tick feed.
	// Reuses the same extRedis client built above (nil-safe: route is
	// gated on the handler in the router).
	var marketHandler *handlers.MarketHandler
	if extRedis != nil {
		marketHandler = handlers.NewMarketHandler(extRedis)
	}

	// Algos — Explore screen catalog. Today: static in-memory list (just
	// Manthan). When the catalog grows or moves to a DB, only the NewStaticCatalog
	// call below changes; the handler + router wiring stay identical.
	algosCatalog := algos.NewStaticCatalog()
	// Overlay REAL computed performance onto the catalog cards (primaryReturn
	// windows, maxDrawdown, sortino) instead of hardcoded constants. Nil-safe:
	// if the perf store isn't wired the catalog keeps its defaults.
	if sc, ok := algosCatalog.(*algos.StaticCatalog); ok && liveAlgosPerfStore != nil {
		sc.SetStatsProvider(newAlgoStatsProvider(liveAlgosPerfStore, liveAlgosPerfClientMap, algoStatsTTL, positionsSSotDB))
		log.Println("Algo catalog: live performance stats enabled (algo_performance_daily)")
	} else {
		log.Println("Algo catalog: perf store unavailable — serving catalog default stats")
	}
	algosHandler := handlers.NewAlgosHandler(algosCatalog)

	// Live Algos — user's deployed strategies dashboard AND the Details
	// page endpoints (strategy detail / holdings / trades / stock P&L).
	//
	// The LIST endpoint (GET /users/me/live-algos) needs only user-config
	// gRPC + the static catalog. The DETAILS endpoints additionally need:
	//
	//   store: *sql.DB pool against stockk_trading (positions + orders)
	//   ltp:   go-redis client pointed at the staging LTP feed. In dev
	//          this is the SSH tunnel at localhost:6380 (managed by the
	//          systemd --user staging-redis-tunnel.service). In prod,
	//          it will be the local redis_live directly.
	//
	// Both are nil-safe — if the DB pool or Redis client fail to init,
	// the router skips registering the details routes and the LIST
	// endpoint keeps working normally.
	// livealgos store reads from FOUR DB pools:
	//   positionsDB     (trading_db)   — strategies + trade_configs (StrategyMeta)
	//   positionsSSotDB (positions_db) — positions.* (CQRS SSOT, replaces the
	//                                    legacy manthan_positions read whose
	//                                    realized_pnl was unreliable)
	//   ordersDB        (execution_db) — manthan_orders (Stock P&L drilldown)
	//   signalsDB       (signals_db)   — manthan_signals for per-symbol
	//                                    isin/industry/mcap enrichment
	// signalsDB is nil-safe inside the store (empty industry/mcap on rows).
	// positionsSSotDB nil returns an empty position list — which for a
	// clean-cutover strategy is the correct display (see docs/db_ownership.md).
	var liveAlgosStore livealgos.Store
	if positionsDB != nil && ordersDB != nil {
		liveAlgosStore = livealgos.NewPostgresStore(positionsDB, positionsSSotDB, ordersDB, signalsDB)
		log.Printf("Live-algos store wired (strategies=%s, positions_ssot=%s, orders=%s, signals=%s)",
			positionsDBName, positionsSSotDBName, ordersDBName,
			envOr("MANTHAN_SIGNALS_DB", "signals_db"))
	} else {
		log.Printf("Warning: live-algos details store disabled — need both positionsDB (%v) and ordersDB (%v)",
			positionsDB != nil, ordersDB != nil)
	}

	var liveAlgosLTP *livealgos.LTPStore
	if ltpAddr := envOr("LIVEALGOS_LTP_REDIS_ADDR", "localhost:6380"); ltpAddr != "" {
		ltpClient := redis.NewClient(&redis.Options{
			Addr:         ltpAddr,
			Password:     os.Getenv("LIVEALGOS_LTP_REDIS_PASSWORD"),
			DB:           0,
			PoolSize:     10,
			MinIdleConns: 2,
			ReadTimeout:  2 * time.Second,
		})
		pCtx, pCancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := ltpClient.Ping(pCtx).Err(); err != nil {
			log.Printf("Warning: LTP redis ping (%s) failed: %v (details endpoints report ltpStatus=UNAVAILABLE until probe recovers)", ltpAddr, err)
			// Keep the client + store around so the runtime probe can
			// flip HEALTHY if the tunnel comes back — no need to bounce
			// api-gateway. See livealgos.LTPStore.Start.
			liveAlgosLTP = livealgos.NewLTPStore(ltpClient)
		} else {
			log.Printf("Live-algos LTP redis connected (%s)", ltpAddr)
			liveAlgosLTP = livealgos.NewLTPStore(ltpClient)
			liveAlgosLTP.MarkHealthy(true) // seed healthy before Start's first tick
		}
		defer ltpClient.Close()
		// Start the 5s probe. Detects silent tunnel half-close per
		// reference_ltp_tunnel_silent_fail — a single failing PING
		// flips the store UNAVAILABLE so subsequent responses carry
		// ltpStatus=UNAVAILABLE instead of returning 0 LTPs.
		probeCtx, probeCancel := context.WithCancel(context.Background())
		defer probeCancel()
		go liveAlgosLTP.Start(probeCtx, 5*time.Second)
		pCancel()
	}

	liveAlgosHandler := handlers.NewLiveAlgosHandler(userConfigClient, algosCatalog, liveAlgosStore, liveAlgosLTP, liveAlgosPerfStore, liveAlgosPerfClientMap, liveAlgosNAVStore)
	// True per-deployment NAV snapshots — feeds the details chart + metrics.
	if pgStore, ok := liveAlgosStore.(*livealgos.PostgresStore); ok {
		startNAVSnapshots(context.Background(), pgStore, liveAlgosLTP, liveAlgosNAVStore)
	}

	// ── Portfolio handler (PF.D) ────────────────────────────────────────
	// Dials portfolio svc's gRPC, composes an Enricher over the same
	// LTP client the live-algos handler uses, and registers the three
	// /users/me/portfolio/* routes below. Nil-safe: if portfolio svc is
	// unreachable at boot the routes stay unregistered and requests get
	// a clean 404. See docs/portfolio_service_design.md.
	var portfolioHandler *handlers.PortfolioHandler
	// Token lookup uses ordersDB (execution_db.manthan_orders) — the
	// authoritative writer for exchange_token. Previously read from
	// stockk_trading which was a silent replica; DB.1 killed that path.
	// Portfolio svc gRPC is :9008 (see deployments ecosystem + portfolio
	// cmd/main.go). The old default of :9005 pointed at RULES-ENGINE's gRPC
	// port — every /users/me/portfolio/* call 500'd with "connection refused"
	// while the strategy-scoped endpoints worked, which surfaced to users as
	// an "allocation data mismatch" between screens (2026-08-18).
	if pfAddr := envOr("PORTFOLIO_GRPC_ADDR", "localhost:9008"); pfAddr != "" && ordersDB != nil {
		pfClient, err := grpc_clients.NewPortfolioClient(pfAddr, 5*time.Second)
		if err != nil {
			log.Printf("Warning: portfolio gRPC dial (%s) failed: %v (portfolio endpoints disabled)", pfAddr, err)
		} else {
			defer pfClient.Close()
			// LTPSource interface satisfied by *livealgos.LTPStore. Nil is fine —
			// enricher's LTPSource nil-guard treats it as UNAVAILABLE.
			enricher := portfolio.NewEnricher(
				portfolio.NewTradingDBTokenLookup(ordersDB),
				liveAlgosLTP,
			)
			portfolioHandler = handlers.NewPortfolioHandler(pfClient, enricher)
			log.Printf("Portfolio gRPC client wired (%s) → /users/me/portfolio/* enabled", pfAddr)
		}
	}

	// JWT verifier for the protected subrouter.
	//
	// PATTERN 4 (cached introspection) — bridge until Codifi shares the
	// HS512 signing secret. Every JWT is validated against Codifi's
	// verify endpoint on first-see, then cached for 5 min so subsequent
	// requests with the same JWT cost ~1 ms instead of ~200 ms.
	//
	// The blacklist (populated by /auth/logout) closes the revocation
	// gap — logout takes effect INSTANTLY on our gateway, even though
	// Codifi keeps trusting the JWT until its natural exp.
	//
	// When Codifi finally shares the HS512 signing secret, swap this
	// ONE line to:
	//     verifier := auth.NewLocalKeyVerifier(os.Getenv("CODIFI_JWT_SECRET"))
	// Every protected route instantly moves from network-hop verification
	// to local HMAC math — no handler, middleware, or router changes.
	//
	// CODIFI_VERIFY_URL env var lets ops point at a staging Codifi
	// without a code change. Default is production Codifi.
	// AUTH_MODE=noop bypasses Codifi introspection and accepts ANY well-formed
	// JWT carrying a userId claim — TESTING ONLY (e.g. eyeballing protected
	// endpoints without a live SSO login). Default is real introspection.
	var verifier auth.Verifier
	if envOr("AUTH_MODE", "introspection") == "noop" {
		log.Println("⚠ AUTH_MODE=noop — accepting any well-formed JWT with a userId claim (TESTING ONLY, NOT FOR PRODUCTION)")
		verifier = auth.NewNoopVerifier()
	} else {
		verifier = auth.NewIntrospectionVerifier(auth.IntrospectionConfig{
			// PRIMARY (2026-08-05): unified session verify — validates BOTH
			// web-SSO and mobile-APP tokens with Authorization: Bearer +
			// source header. Same response envelope as the legacy SSO verify.
			UnifiedVerifyURL: envOr("CODIFI_UNIFIED_VERIFY_URL",
				"https://livemiddleware.indiratrade.com/auth-services/api/auth/v1/verify/token"),
			// LEGACY SSO endpoint (rollback path only):
			VerifyURL: envOr("CODIFI_VERIFY_URL",
				"https://livemiddleware.indiratrade.com/auth-services/api/auth/verify/token"),
			// APP tokens (mobile mPin / biometric login) — piggybacks on the
			// /AccountInfo data endpoint because Codifi's dedicated
			// /auth/v1/verify/token is over-strict (rejects tokens the
			// mobile app itself uses successfully). See introspection_verifier.go
			// for the full write-up.
			AppVerifyURL: envOr("CODIFI_APP_VERIFY_URL",
				"https://livemiddleware.indiratrade.com/user-services/api/user/v1/AccountInfo"),
			// HTTPTimeout, CacheTTL, NegativeTTL, CleanupPeriod all use
			// defaults defined in NewIntrospectionVerifier — 3s / 5m / 30s / 1m.
		})
	}

	// Router
	// Auto-capture: any request whose token passes verification AND is newer
	// than the stored broker session refreshes user_credentials automatically
	// (store -> USER_CREDENTIALS_UPDATED -> trade-execution wake -> SL re-arm).
	// Kills the daily "app validated but never pushed the fresh token" gap.
	tokenCapture := middleware.NewTokenCapture(userConfigClient)

	// ── Admin console (spec M1) ──────────────────────────────────────
	// Rides the existing trading_db handle (admin_users / admin_sessions /
	// admin_audit — migration 019). Absent handle → admin surface absent,
	// loudly: an admin console silently missing is an ops trap.
	var adminHTTP *admin.HTTP
	if positionsDB != nil {
		adminHTTP = admin.NewHTTP(admin.NewService(admin.NewStore(positionsDB)))
		log.Printf("Admin console enabled (trading_db=%s)", positionsDBName)
	} else {
		log.Printf("⚠ Admin console DISABLED — trading_db handle unavailable")
	}

	r := router.NewRouter(userConfigHandler, websocketHandler, paperTradingHandler, manthanHandler, healthHandler, marketHandler, algosHandler, perfHandler, liveAlgosHandler, portfolioHandler, verifier, tokenCapture, corsConfig, adminHTTP)

	// Debug: list all routes
	_ = r.(*mux.Router).Walk(func(route *mux.Route, router *mux.Router, ancestors []*mux.Route) error {
		path, _ := route.GetPathTemplate()
		methods, _ := route.GetMethods()
		log.Printf("ROUTE → %v %v", methods, path)
		return nil
	})

	// HTTP server
	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Server.HTTPPort),
		Handler:      r,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start server
	go func() {
		log.Printf("API Gateway listening on port %d", cfg.Server.HTTPPort)
		log.Printf("Health check: http://localhost:%d/api/v1/health", cfg.Server.HTTPPort)

		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down API Gateway...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("Forced shutdown: %v", err)
	}

	log.Println("API Gateway stopped")
}

// envOr returns the value of the named environment variable, or fallback if empty.
func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
