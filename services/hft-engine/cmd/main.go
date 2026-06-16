// hft-engine — high-frequency bidding strategy executor.
//
// One process per host. Spawns one goroutine per active strategy (Phase 2+).
// Phase 1 boots the service, opens DBs, starts the gRPC server, and waits
// for shutdown signal. No trading.
//
// Run:
//
//	go run ./services/hft-engine/cmd/
//
// gRPC port:    :9090 (configurable via HFT_GRPC_PORT)
// HTTP port:    :9091 (livez/readyz; configurable via HFT_HTTP_PORT)
//
// Smoke-test once running:
//
//	grpcurl -plaintext localhost:9090 hft_engine.HFTService/Ping
//	curl http://localhost:9091/livez
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	pb "github.com/RohitIndira/Algo-Treading/api/proto/hft_engine"
	goredis "github.com/go-redis/redis/v8"

	indiraPkg "github.com/RohitIndira/Algo-Treading/pkg/indira"
	"github.com/RohitIndira/Algo-Treading/services/hft-engine/internal/audit"
	"github.com/RohitIndira/Algo-Treading/services/hft-engine/internal/broker"
	"github.com/RohitIndira/Algo-Treading/services/hft-engine/internal/config"
	"github.com/RohitIndira/Algo-Treading/services/hft-engine/internal/grpcserver"
	hftlog "github.com/RohitIndira/Algo-Treading/services/hft-engine/internal/logger"
	"github.com/RohitIndira/Algo-Treading/services/hft-engine/internal/manager"
	"github.com/RohitIndira/Algo-Treading/services/hft-engine/internal/marketws"
	"github.com/RohitIndira/Algo-Treading/services/hft-engine/internal/orderws"
	"github.com/RohitIndira/Algo-Treading/services/hft-engine/internal/repo"
	"github.com/RohitIndira/Algo-Treading/services/hft-engine/internal/state"
)

func main() {
	// ── 1. Load .env (best-effort) ────────────────────────────────────────
	// Try repo root then service-local. If neither exists we still run from
	// process env, which is how PM2 + systemd will invoke us in prod.
	//
	// godotenv.Load (NOT Overload) so that shell / PM2 / systemd env vars
	// WIN over the file. The file is just a dev fallback; in production
	// ops staff override values without editing checked-in files.
	for _, p := range []string{
		".env",
		filepath.Join("services", "hft-engine", ".env"),
	} {
		if abs, err := filepath.Abs(p); err == nil {
			if err := godotenv.Load(abs); err == nil {
				log.Printf("loaded .env from %s (shell env overrides)", abs)
				break
			}
		}
	}

	// ── 2. Build typed config + structured logger ─────────────────────────
	cfg := config.Load()
	logger := hftlog.New(cfg.Env)
	defer logger.Sync()

	logger.Info("hft-engine starting",
		zap.String("env", cfg.Env),
		zap.String("default_mode", cfg.DefaultMode),
		zap.Int("grpc_port", cfg.GRPCPort),
		zap.Int("http_port", cfg.HTTPPort),
		zap.String("trading_db", cfg.TradingDB.Name),
		zap.String("trading_exec_db", cfg.TradingExecDB.Name))

	// ── 3. Open the two Postgres connections ──────────────────────────────
	tradingDB := mustOpenDB(logger, "trading_db", cfg.TradingDB.DSN())
	defer tradingDB.Close()

	tradingExecDB := mustOpenDB(logger, "trading_execution", cfg.TradingExecDB.DSN())
	defer tradingExecDB.Close()

	// ── 4. Wire repo + audit + broker + manager ──────────────────────────
	r := repo.New(tradingDB, tradingExecDB, cfg.EncryptionKey)
	auditWriter := audit.New(r, logger, audit.Config{}) // defaults: 1000 buf, 200 batch, 1s flush
	rootCtx, rootCancel := context.WithCancel(context.Background())
	auditWriter.Start(rootCtx)

	// Broker selection. Phase 3: branch on cfg.DefaultMode.
	//
	//   PAPER → PaperBroker (logs, never touches Indira). Safe default.
	//   LIVE  → IndiraBroker (real REST orders against pkg/indira).
	//
	// The DEFAULT remains PAPER. A strategy's own cfg.Mode is layered on
	// top in manager.Start — a strategy declared LIVE will be REFUSED if
	// the engine itself is PAPER (no silent demotion to paper).
	// Forward-declare a manager pointer so the orderws router closure can
	// see it even though manager.New runs a few lines below. The closure
	// reads the pointer at event-arrival time (after manager.New has
	// assigned it), not at construction time.
	var managerRef *manager.Manager

	// orderws.Service is built only in LIVE mode. PAPER never has real
	// fills to route. The router closure forwards each event to the
	// manager's RouteFill, which fans out to the owning Runner.
	var (
		br  broker.Broker
		ows *orderws.Service
	)
	switch cfg.DefaultMode {
	case "LIVE":
		indiraClient := indiraPkg.NewClient(indiraPkg.Config{BaseURL: cfg.IndiraBaseURL})
		refresh := func(ctx context.Context, userID string) (*broker.AuthContext, error) {
			creds, err := r.LoadCredentials(ctx, userID)
			if err != nil {
				return nil, err
			}
			return &broker.AuthContext{
				UserID:      creds.UserID,
				AppID:       creds.AppID,
				Source:      creds.Source,
				BearerToken: creds.BearerToken,
			}, nil
		}
		br = broker.NewIndiraBroker(indiraClient, refresh, broker.IndiraConfig{}, logger)

		ows = orderws.New(indiraClient, func(ev state.FillEvent) {
			if managerRef != nil {
				managerRef.RouteFill(ev)
			}
		}, logger)
		ows.Start(rootCtx)

		logger.Info("broker wired", zap.String("impl", "IndiraBroker"),
			zap.String("base_url", cfg.IndiraBaseURL))
		logger.Info("order-status WS service started (subscribers attach on Entry)")
	default:
		br = broker.NewPaperBroker(logger)
		logger.Info("broker wired", zap.String("impl", "PaperBroker"))
	}

	// ── 4b. External Redis + ODIN broadcast client + marketws Manager ────
	// External Redis holds isin:{ISIN} → ODIN security_code mapping. Ping
	// it once so a misconfigured EXT_REDIS_ADDR fails fast at boot, not
	// per-Entry.
	extRedis := goredis.NewClient(&goredis.Options{
		Addr:     cfg.ExtRedisAddr,
		Password: cfg.ExtRedisPassword,
		DB:       cfg.ExtRedisDB,
	})
	if perr := extRedis.Ping(rootCtx).Err(); perr != nil {
		logger.Warn("external Redis ping failed — Entry will fail until reachable",
			zap.String("addr", cfg.ExtRedisAddr),
			zap.Error(perr))
	} else {
		logger.Info("external Redis connected", zap.String("addr", cfg.ExtRedisAddr))
	}
	defer extRedis.Close()
	resolver := marketws.NewResolver(extRedis)

	odinCfg := marketws.OdinClientConfig{
		Scheme: cfg.OdinBcastScheme,
		Host:   cfg.OdinBcastHost,
		Port:   cfg.OdinBcastPort,
		UserID: cfg.OdinBcastUserID,
		APIKey: cfg.OdinBcastAPIKey,
	}
	odinClient := marketws.New(odinCfg, logger)
	mws := marketws.NewManager(odinClient, resolver, logger)
	odinClient.SetListener(mws.OnMessage)

	// Run the ODIN client as a background goroutine. Its Run() blocks
	// until rootCtx is cancelled OR auth is rejected. We don't fatal on
	// run errors — manager.Start refuses Entry when the feed isn't up.
	go func() {
		if err := odinClient.Run(rootCtx); err != nil && rootCtx.Err() == nil {
			logger.Error("ODIN client terminated", zap.Error(err))
		}
	}()
	logger.Info("ODIN broadcast client launched",
		zap.String("host", cfg.OdinBcastHost),
		zap.Int("port", cfg.OdinBcastPort),
		zap.String("scheme", cfg.OdinBcastScheme),
		zap.Bool("creds_present", cfg.OdinBcastUserID != "" && cfg.OdinBcastAPIKey != ""))

	mgr := manager.New(rootCtx, r, auditWriter, br, mws, ows, state.Mode(cfg.DefaultMode), logger)
	managerRef = mgr // armed: orderws router can now find the manager

	// ── 5. Start the gRPC server ──────────────────────────────────────────
	grpcServer := grpc.NewServer()
	pb.RegisterHFTServiceServer(grpcServer, grpcserver.New(mgr, logger))
	// reflection enables `grpcurl -plaintext localhost:9090 list` for ops.
	reflection.Register(grpcServer)

	grpcListener, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.GRPCPort))
	if err != nil {
		logger.Fatal("gRPC listen failed", zap.Error(err))
	}

	go func() {
		logger.Info("gRPC listening", zap.Int("port", cfg.GRPCPort))
		if err := grpcServer.Serve(grpcListener); err != nil {
			logger.Error("gRPC server exited", zap.Error(err))
		}
	}()

	// ── 6. Start a tiny HTTP server for /livez + /readyz ──────────────────
	httpMux := http.NewServeMux()
	httpMux.HandleFunc("/livez", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	httpMux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		// Phase 1: ready if both DBs ping ok. Phase 7+: also check Redis, Indira reachability.
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := tradingDB.PingContext(ctx); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"status":"not_ready","detail":"trading_db ping failed"}`))
			return
		}
		if err := tradingExecDB.PingContext(ctx); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"status":"not_ready","detail":"trading_execution ping failed"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(fmt.Sprintf(`{"status":"ok","running_strategies":%d}`, mgr.Count())))
	})

	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.HTTPPort),
		Handler: httpMux,
	}
	go func() {
		logger.Info("HTTP (probes) listening", zap.Int("port", cfg.HTTPPort))
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP server exited", zap.Error(err))
		}
	}()

	// ── 7. Wait for SIGINT/SIGTERM, then shut down cleanly ────────────────
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	logger.Info("shutdown signal received", zap.String("signal", sig.String()))

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	// Stop HTTP first (least important).
	_ = httpServer.Shutdown(shutdownCtx)

	// GracefulStop lets in-flight RPCs finish; bounded by shutdownCtx.
	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-shutdownCtx.Done():
		logger.Warn("gRPC graceful stop timed out — forcing")
		grpcServer.Stop()
	}

	// orderws BEFORE audit so any in-flight fill events get a chance to
	// route + write their audit rows before the writer drains.
	if ows != nil {
		ows.Stop()
	}

	// Audit writer last — drains its channel into DB before exit.
	auditWriter.Stop()

	rootCancel()
	logger.Info("hft-engine stopped cleanly")
}

// mustOpenDB opens a Postgres connection and pings it. Fatals on failure
// because there's nothing the engine can do without its databases.
func mustOpenDB(logger *zap.Logger, name, dsn string) *sql.DB {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		logger.Fatal("db open failed", zap.String("db", name), zap.Error(err))
	}
	// Tight defaults — hft-engine doesn't need many idle connections.
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute)

	if err := db.Ping(); err != nil {
		logger.Fatal("db ping failed", zap.String("db", name), zap.Error(err))
	}
	logger.Info("db connected", zap.String("db", name))
	return db
}
