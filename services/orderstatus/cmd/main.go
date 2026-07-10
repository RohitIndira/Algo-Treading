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

	"github.com/RohitIndira/Algo-Treading/services/orderstatus/internal/publisher"
	"github.com/RohitIndira/Algo-Treading/services/orderstatus/internal/store"
	"github.com/RohitIndira/Algo-Treading/services/orderstatus/internal/userconfig"
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

	// ── WSS listener (Chunks B + C) ────────────────────────────────────
	// Pipeline: WSS event → broker_events INSERT → order.events publish
	brokerEvents := store.NewWriter(db, logger)
	orderEventsPub := publisher.NewPublisher(kafkaWriter, logger)
	indiraClient := indira.NewDefaultClient()
	listener := wss.NewListener(indiraClient, brokerEvents, orderEventsPub, logger)
	defer listener.Close()

	// ── Auth wiring (Chunk B.5): user-config gRPC → subscriptions ──────
	// Dial user-config with a 10s timeout. Fail fast — orderstatus svc
	// can't do useful work without knowing which users to subscribe.
	ucAddr := getEnv("USER_CONFIG_GRPC_ADDR", "localhost:50051")
	ucCtx, ucCancel := context.WithTimeout(context.Background(), 10*time.Second)
	ucClient, err := userconfig.NewClient(ucCtx, ucAddr)
	ucCancel()
	if err != nil {
		logger.Fatal("user-config dial failed", zap.String("addr", ucAddr), zap.Error(err))
	}
	defer func() { _ = ucClient.Close() }()
	logger.Info("user-config gRPC ready", zap.String("addr", ucAddr))

	// Fetch active users and start a subscription per user. One user may
	// own multiple strategies — user-config's dedup gives us unique IDs.
	subCtx, subCancel := context.WithTimeout(context.Background(), 30*time.Second)
	activeUsers, err := ucClient.ListActiveUserIDs(subCtx)
	if err != nil {
		subCancel()
		logger.Fatal("ListActiveUserIDs failed", zap.Error(err))
	}
	logger.Info("active users to subscribe", zap.Int("count", len(activeUsers)))

	subscribed, skipped, failed := 0, 0, 0
	for _, userID := range activeUsers {
		auth, err := ucClient.FetchUserCredentials(subCtx, userID)
		if err != nil {
			logger.Warn("fetch credentials failed — skipping user",
				zap.String("user_id", userID), zap.Error(err))
			failed++
			continue
		}
		if auth == nil {
			// No broker creds on file (user never SSO'd, or credentials wiped).
			// Silent skip — will retry on next boot.
			logger.Debug("no credentials on file — skipping user",
				zap.String("user_id", userID))
			skipped++
			continue
		}
		if err := listener.StartSubscription(subCtx, userID, auth); err != nil {
			logger.Warn("subscribe failed — will not receive events for user",
				zap.String("user_id", userID), zap.Error(err))
			failed++
			continue
		}
		subscribed++
	}
	subCancel()

	logger.Info("wss subscription pass complete",
		zap.Int("subscribed", subscribed),
		zap.Int("skipped_no_creds", skipped),
		zap.Int("failed", failed))

	logger.Info("orderstatus svc ready",
		zap.String("chunk", "C"),
		zap.String("next", "Chunk D — move reconciler + safety_monitor into this binary"))

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
