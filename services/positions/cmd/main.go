// positions svc — owns manthan + user_manual position lifecycle,
// realized_pnl computation, and reconciler drift detection.
//
// Design: docs/positions_service_design.md
//
// Chunk P.A (2026-07-13): skeleton only — connects to own DB + Kafka,
// exposes /health, then idles. Consumer + state machine land in Chunks P.B-P.G.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

func main() {
	logger, _ := zap.NewProduction()
	defer func() { _ = logger.Sync() }()

	logger.Info("positions svc starting",
		zap.String("version", "0.1.0-skeleton"),
		zap.String("chunk", "P.A"),
		zap.String("pid", fmt.Sprintf("%d", os.Getpid())))

	// ── DB: positions_db ───────────────────────────────────────────────
	dbURL := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		getEnv("POSTGRES_HOST", "localhost"),
		getEnv("POSTGRES_PORT", "5432"),
		getEnv("POSTGRES_USER", "postgres"),
		getEnv("POSTGRES_PASSWORD", "postgres"),
		getEnv("POSITIONS_DB", "positions_db"),
		getEnv("POSTGRES_SSL_MODE", "disable"),
	)
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		logger.Fatal("open db failed", zap.Error(err))
	}
	defer db.Close()

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.PingContext(context.Background()); err != nil {
		logger.Fatal("db ping failed", zap.Error(err))
	}
	logger.Info("connected to positions_db")

	// ── Kafka producers ────────────────────────────────────────────────
	// Two topics per §5.2 of the design doc.
	brokers := splitCsv(getEnv("KAFKA_BROKERS", "localhost:9092"))

	positionEventsWriter := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        "position.events",
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 1 * time.Millisecond,
		RequiredAcks: kafka.RequireAll,
	}
	defer positionEventsWriter.Close()

	driftWriter := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        "positions.drift.detected",
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 1 * time.Millisecond,
		RequiredAcks: kafka.RequireAll,
	}
	defer driftWriter.Close()

	logger.Info("kafka producers ready",
		zap.Strings("brokers", brokers),
		zap.Strings("topics", []string{"position.events", "positions.drift.detected"}))

	// ── HTTP health check ──────────────────────────────────────────────
	port := getEnv("HTTP_PORT", "8092")
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := db.PingContext(ctx); err != nil {
			http.Error(w, "db unreachable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 3 * time.Second,
	}
	go func() {
		logger.Info("http server listening", zap.String("port", port))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("http server failed", zap.Error(err))
		}
	}()

	// ── TODO Chunks P.B/P.B.5/P.C/P.D/P.E/P.G — wire up here ──────────
	//   P.B    order.events Kafka consumer + position_events writer
	//   P.B.5  gRPC client to trade-execution.LookupOrderMeta
	//   P.C    state machine: FILLED BUY→INSERT, SELL→UPDATE + realized_pnl
	//   P.D    position.events publisher wired into state machine
	//   P.E    (in rules-engine) consumes position.events → cooldown INSERT
	//   P.G    reconciler drift detection → positions.drift.detected publisher

	// silence "declared and not used" until the chunks above land
	_ = positionEventsWriter
	_ = driftWriter

	logger.Info("positions svc ready — awaiting subsequent chunks",
		zap.String("chunk", "P.A"),
		zap.String("next", "P.B — order.events consumer + audit writer"))

	// ── Graceful shutdown ──────────────────────────────────────────────
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	logger.Info("shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("http shutdown failed", zap.Error(err))
	}
	logger.Info("positions svc stopped")
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func splitCsv(s string) []string {
	out := []string{}
	cur := ""
	for _, r := range s {
		if r == ',' {
			if cur != "" {
				out = append(out, cur)
				cur = ""
			}
			continue
		}
		cur += string(r)
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}
