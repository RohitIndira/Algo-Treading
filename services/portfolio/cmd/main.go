// Portfolio svc — read-only query side of the positions CQRS split.
//
// Design: docs/portfolio_service_design.md
//
// Chunk PF.A (2026-07-13): skeleton only. Connects to positions_db,
// exposes /health, binds gRPC listener but registers no service methods
// yet. Query methods land in PF.B; gRPC server methods in PF.C.
//
// Two concerns kept OUT by design:
//   - LTP / unrealized P&L — api-gateway does this (§3 of design doc).
//   - JWT verification    — api-gateway does this (portfolio svc trusts
//                            the user_id passed on gRPC requests).
package main

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	pb "github.com/RohitIndira/Algo-Treading/api/proto/portfolio"
	"github.com/RohitIndira/Algo-Treading/services/portfolio/internal/server"
	"github.com/RohitIndira/Algo-Treading/services/portfolio/internal/store"
)

func main() {
	logger, _ := zap.NewProduction()
	defer func() { _ = logger.Sync() }()

	logger.Info("portfolio svc starting",
		zap.String("version", "0.1.0-pf.c"),
		zap.String("chunk", "PF.C"),
		zap.String("pid", fmt.Sprintf("%d", os.Getpid())))

	// ── DB: positions_db (read-only pool) ──────────────────────────────
	// Runs as user 'positions_reader' in prod (see migrations/001).
	// Local dev falls back to 'postgres' via env — same as every other
	// service.
	db, err := openDB(logger)
	if err != nil {
		logger.Fatal("open positions_db failed", zap.Error(err))
	}
	defer db.Close()

	// ── HTTP health ────────────────────────────────────────────────────
	// Kept minimal — only pings the DB. Kafka / gRPC readiness handled
	// separately (there's no Kafka producer in portfolio svc, and gRPC
	// readiness is by Serve()).
	httpPort := getEnv("HTTP_PORT", "8095")
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := db.PingContext(ctx); err != nil {
			http.Error(w, "positions_db unreachable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	httpSrv := &http.Server{
		Addr:              ":" + httpPort,
		Handler:           mux,
		ReadHeaderTimeout: 3 * time.Second,
	}
	go func() {
		logger.Info("http server listening", zap.String("port", httpPort))
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("http server failed", zap.Error(err))
		}
	}()

	// ── gRPC server ────────────────────────────────────────────────────
	// PF.A: bind + reflection only, no service registered yet. api-gateway
	// clients would fail with Unimplemented if they called us today —
	// intentional. PF.C registers real handlers.
	grpcPort := getEnv("GRPC_PORT", "9005")
	lis, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		logger.Fatal("grpc listen failed", zap.Error(err))
	}
	grpcSrv := grpc.NewServer()
	reflection.Register(grpcSrv) // grpcurl introspection

	// PF.C: register PortfolioService with the store-backed handlers.
	portfolioStore := store.New(db, logger)
	pb.RegisterPortfolioServiceServer(grpcSrv, server.New(portfolioStore, logger))

	go func() {
		logger.Info("grpc server listening",
			zap.String("port", grpcPort),
			zap.String("registered", "portfolio.PortfolioService"))
		if err := grpcSrv.Serve(lis); err != nil {
			logger.Error("grpc server failed", zap.Error(err))
		}
	}()

	logger.Info("portfolio svc ready",
		zap.String("chunk", "PF.C"),
		zap.String("next", "AG.LTP — api-gateway LTP liveness probe / PF.D — HTTP proxy handlers"))

	// ── Graceful shutdown ──────────────────────────────────────────────
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	logger.Info("shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		logger.Error("http shutdown failed", zap.Error(err))
	}
	grpcSrv.GracefulStop()
	logger.Info("portfolio svc stopped")
}

func openDB(logger *zap.Logger) (*sql.DB, error) {
	dsn := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		getEnv("POSTGRES_HOST", "localhost"),
		getEnv("POSTGRES_PORT", "5432"),
		getEnv("POSITIONS_DB_USER", getEnv("POSTGRES_USER", "postgres")),
		getEnv("POSITIONS_DB_PASSWORD", getEnv("POSTGRES_PASSWORD", "postgres")),
		getEnv("POSITIONS_DB", "positions_db"),
		getEnv("POSTGRES_SSL_MODE", "disable"),
	)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	// Portfolio queries fan out from UI — bigger pool than positions svc's
	// 10/5 defaults.
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(5 * time.Minute)

	pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		return nil, fmt.Errorf("ping positions_db: %w", err)
	}
	logger.Info("connected to positions_db",
		zap.String("db", getEnv("POSITIONS_DB", "positions_db")),
		zap.String("user", getEnv("POSITIONS_DB_USER", getEnv("POSTGRES_USER", "postgres"))))
	return db, nil
}

func getEnv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
