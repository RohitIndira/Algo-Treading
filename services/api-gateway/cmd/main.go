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
	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/grpc_clients"
	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/algos"
	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/auth"
	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/handlers"
	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/livealgos"
	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/middleware"
	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/notifications"
	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/performance"
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

	// gRPC client: hft-engine. Non-fatal — grpc.Dial is lazy, so this only
	// errors on a malformed address; if it does, HFT routes stay disabled
	// (router nil-checks the handler) but the rest of the gateway runs.
	var hftHandler *handlers.HFTHandler
	hftClient, err := grpc_clients.NewHFTClient(
		cfg.Services.HFTEngineAddr,
		cfg.Server.GRPCTimeout,
	)
	if err != nil {
		log.Printf("Warning: HFT engine client init failed: %v (HFT routes disabled)", err)
	} else {
		defer hftClient.Close()
		hftHandler = handlers.NewHFTHandler(hftClient)
		log.Printf("Connected to HFT Engine at %s", cfg.Services.HFTEngineAddr)
	}

	// Initialize handlers. hftClient is passed through to UserConfigHandler
	// so CreateStrategy can auto-fire Entry on HFT_BIDDING strategies with
	// activate_immediately=true — turning the create→start dance into a
	// single round-trip. nil-safe: if hftClient init failed above, auto-start
	// is silently skipped (operator can still POST /hft/{id}/start manually).
	userConfigHandler := handlers.NewUserConfigHandler(userConfigClient, hftClient)

	// Initialize Redis client for WebSocket pub/sub
	redisClient := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
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
	//   MANTHAN_SIGNALS_DB   — holds market_data.manthan_signals
	//                          (written by data-ingestion / manthan-live).
	//                          Default: market_data.
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
	var extRedis *redis.Client // hoisted: reused by the market-quote handler
	{
		pgHost := envOr("POSTGRES_HOST", "localhost")
		pgPort := envOr("POSTGRES_PORT", "5432")
		pgUser := envOr("POSTGRES_USER", "postgres")
		pgPass := envOr("POSTGRES_PASSWORD", "postgres")
		pgSSL := envOr("POSTGRES_SSLMODE", "disable")
		signalsDBName := envOr("MANTHAN_SIGNALS_DB", "market_data")
		positionsDBName := envOr("MANTHAN_POSITIONS_DB", "trading_db")
		ordersDBName := envOr("MANTHAN_ORDERS_DB", "trading_execution")
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

		signalsDB, err := openPG(signalsDBName)
		if err != nil {
			log.Printf("Warning: Manthan signals DB (%s) open/ping failed: %v", signalsDBName, err)
		} else {
			log.Printf("Manthan signals DB connected (%s)", signalsDBName)
			defer signalsDB.Close()
		}

		positionsDB, err := openPG(positionsDBName)
		if err != nil {
			log.Printf("Warning: Manthan positions DB (%s) open/ping failed: %v", positionsDBName, err)
		} else {
			log.Printf("Manthan positions DB connected (%s)", positionsDBName)
			defer positionsDB.Close()
		}

		ordersDB, err := openPG(ordersDBName)
		if err != nil {
			log.Printf("Warning: Manthan orders DB (%s) open/ping failed: %v", ordersDBName, err)
		} else {
			log.Printf("Manthan orders DB connected (%s)", ordersDBName)
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
			// Maps algo id → reference client id in the sheet. Grows as
			// we add more algos; for launch there's exactly one entry.
			perfHandler = handlers.NewPerformanceHandler(perfStore, map[string]string{
				"algo_manthan_v1": "A844",
			})
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
	var liveAlgosStore livealgos.Store
	tradingDBName := envOr("STOCKK_TRADING_DB", "stockk_trading")
	if tradingDB, err := sql.Open("postgres", fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		envOr("POSTGRES_HOST", "localhost"),
		envOr("POSTGRES_PORT", "5432"),
		envOr("POSTGRES_USER", "postgres"),
		envOr("POSTGRES_PASSWORD", "postgres"),
		tradingDBName,
		envOr("POSTGRES_SSLMODE", "disable"),
	)); err != nil {
		log.Printf("Warning: %s DB open failed: %v (details endpoints disabled)", tradingDBName, err)
	} else {
		tradingDB.SetMaxOpenConns(5)
		tradingDB.SetMaxIdleConns(2)
		pCtx, pCancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := tradingDB.PingContext(pCtx); err != nil {
			log.Printf("Warning: %s DB ping failed: %v (details endpoints disabled)", tradingDBName, err)
			_ = tradingDB.Close()
		} else {
			log.Printf("Live-algos trading DB connected (%s)", tradingDBName)
			defer tradingDB.Close()
			liveAlgosStore = livealgos.NewPostgresStore(tradingDB)
		}
		pCancel()
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
			log.Printf("Warning: LTP redis ping (%s) failed: %v (details endpoints degrade to zero LTP)", ltpAddr, err)
			_ = ltpClient.Close()
		} else {
			log.Printf("Live-algos LTP redis connected (%s)", ltpAddr)
			defer ltpClient.Close()
			liveAlgosLTP = livealgos.NewLTPStore(ltpClient)
		}
		pCancel()
	}

	liveAlgosHandler := handlers.NewLiveAlgosHandler(userConfigClient, algosCatalog, liveAlgosStore, liveAlgosLTP)

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
	verifier := auth.NewIntrospectionVerifier(auth.IntrospectionConfig{
		// SSO tokens (web login via SSO gateway):
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

	// Router
	r := router.NewRouter(userConfigHandler, websocketHandler, paperTradingHandler, manthanHandler, hftHandler, healthHandler, marketHandler, algosHandler, perfHandler, liveAlgosHandler, verifier, corsConfig)

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
