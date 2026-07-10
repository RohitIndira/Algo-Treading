// orderstatus svc — observes broker order state (WSS + REST) and publishes
// order.events to Kafka for downstream consumers (positions svc,
// trade-execution's wait-for-fill loop, api-gateway's live-orders push).
//
// This binary is the "read side" of the CQRS split defined in
// docs/orderstatus_service_design.md. Trade-execution places orders and
// INSERTs manthan_orders in trading_execution DB; orderstatus watches broker
// and appends to broker_events in order_status_db. The two never cross.
//
// Chunk A (2026-07-10): skeleton only — boots, connects to DB + Kafka,
// exposes /health, then idles. WSS listener + reconciler + publisher land in
// Chunks B/C/D.
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

	indira "github.com/RohitIndira/Algo-Treading/pkg/indira"

	"github.com/RohitIndira/Algo-Treading/services/orderstatus/internal/store"
	"github.com/RohitIndira/Algo-Treading/services/orderstatus/internal/wss"
)

func main() {
	logger, _ := zap.NewProduction()
	defer func() { _ = logger.Sync() }()

	logger.Info("orderstatus svc starting",
		zap.String("version", "0.1.0-skeleton"),
		zap.String("pid", fmt.Sprintf("%d", os.Getpid())))

	// ── DB connection: order_status_db ─────────────────────────────────
	dbURL := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		getEnv("POSTGRES_HOST", "localhost"),
		getEnv("POSTGRES_PORT", "5432"),
		getEnv("POSTGRES_USER", "postgres"),
		getEnv("POSTGRES_PASSWORD", "postgres"),
		getEnv("ORDER_STATUS_DB", "order_status_db"),
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
	logger.Info("connected to order_status_db")

	// ── Kafka producer: order.events topic ─────────────────────────────
	brokers := splitCsv(getEnv("KAFKA_BROKERS", "localhost:9092"))
	kafkaWriter := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        "order.events",
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 1 * time.Millisecond,
		RequiredAcks: kafka.RequireAll,
	}
	defer kafkaWriter.Close()
	logger.Info("kafka producer ready",
		zap.Strings("brokers", brokers),
		zap.String("topic", "order.events"))

	// ── HTTP health check ──────────────────────────────────────────────
	port := getEnv("HTTP_PORT", "8090")
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

	// ── WSS listener (Chunk B) ─────────────────────────────────────────
	// broker_events writer + listener orchestrator. Actual subscriptions
	// require an *indira.AuthContext per user — hooked in during Chunk B.5
	// (user-config gRPC lookup) or provided by external callers via test paths.
	brokerEvents := store.NewWriter(db, logger)
	indiraClient := indira.NewDefaultClient()
	listener := wss.NewListener(indiraClient, brokerEvents, logger)
	defer listener.Close()

	// silence "declared and not used" during Chunk B — kafkaWriter is used in
	// Chunk C when we publish order.events on every INSERT.
	_ = kafkaWriter

	logger.Info("orderstatus svc ready",
		zap.String("chunk", "B"),
		zap.String("waiting_for", "auth wiring (Chunk B.5) then subscriptions via listener.StartSubscription"))
	_ = listener // silence unused until auth wiring lands

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
	logger.Info("orderstatus svc stopped")
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
