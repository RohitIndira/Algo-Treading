// orderstatus svc — observes broker order state (WSS + REST) and publishes
// order.events to Kafka for downstream consumers (positions svc,
// trade-execution's wait-for-fill loop, api-gateway's live-orders push).
//
// This binary is the "read side" of the CQRS split defined in
// docs/orderstatus_service_design.md. Trade-execution places orders and
// INSERTs manthan_orders in execution_db DB; orderstatus watches broker
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
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	indira "github.com/RohitIndira/Algo-Treading/pkg/indira"

	"github.com/RohitIndira/Algo-Treading/services/orderstatus/internal/alert"
	"github.com/RohitIndira/Algo-Treading/services/orderstatus/internal/publisher"
	"github.com/RohitIndira/Algo-Treading/services/orderstatus/internal/reconciler"
	"github.com/RohitIndira/Algo-Treading/services/orderstatus/internal/store"
	"github.com/RohitIndira/Algo-Treading/services/orderstatus/internal/tradeexec"
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
		Addr:  kafka.TCP(brokers...),
		Topic: "order.events",
		// Hash (not LeastBytes): partition is chosen by hashing Message.Key
		// (= broker_order_id), so every event for one order lands on the SAME
		// partition and keeps strict per-order FIFO ordering. LeastBytes routes
		// by partition load and IGNORES the key, which would scatter one order's
		// events across partitions and let positions svc see an exit before its
		// entry. Per-order ordering is the only ordering any consumer needs.
		Balancer:     &kafka.Hash{},
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

	// Prometheus: broadcast_* and Go runtime metrics.
	mux.Handle("/metrics", promhttp.Handler())

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

	// ── Reconciler (Chunk D — Layers 2 + 3 of the truth hierarchy) ─────
	// Layer 2: REST orderbook poll every 15s per user — catches WSS misses
	// Layer 3: full sweep every 5min + immediate startup catch-up
	//
	// Both paths INSERT into broker_events with source=REST_*; dedup with the
	// WSS path on (broker_order_id, event_seq); publish only NEW rows to
	// order.events.
	rec := reconciler.New(
		indiraClient,
		brokerEvents,
		orderEventsPub,
		func(ctx context.Context, userID string) (*indira.AuthContext, error) {
			return ucClient.FetchUserCredentials(ctx, userID)
		},
		ucClient.ListActiveUserIDs,
		reconciler.Config{
			FastPollInterval: 15 * time.Second,
			SlowPollInterval: 5 * time.Minute,
		},
		logger,
	)
	recCtx, recCancel := context.WithCancel(context.Background())
	defer recCancel()
	rec.Start(recCtx)

	// ── Dealer broadcast listener (2026-10-26) ─────────────────────────
	// One subscription to the broker's broadcast socket sees every client's
	// order frames, session or not. Frames for managed users go through the
	// SAME ingest as the per-user socket (source WSS_BROADCAST, dedup on
	// broker_order_id + event_seq). BROADCAST_FEED_MODE:
	//   off        → not started; service behaves exactly as before
	//   shadow     → connect + compare, write NOTHING (default)
	//   own_orders → write/publish only frames for orders in manthan_orders
	//                (trade-execution LookupOrderMeta; TRADE_EXEC_GRPC_ADDR)
	//   all        → write/publish every managed-client frame
	// Rollback: BROADCAST_FEED_MODE=off + restart. Read-only toward the broker.
	// A bad value must never crash-loop the whole order pipeline (PM2 would
	// give up after max_restarts): log loudly and run with the feed off.
	bmode, err := wss.ParseMode(getEnv("BROADCAST_FEED_MODE", "shadow"))
	if err != nil {
		logger.Error("bad BROADCAST_FEED_MODE — running with the broadcast feed OFF", zap.Error(err))
		bmode = wss.ModeOff
	}
	var broadcast *wss.BroadcastListener
	bctx, bcancel := context.WithCancel(context.Background())
	defer bcancel()
	if bmode != wss.ModeOff {
		alertWriter := &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        alert.Topic,
			Balancer:     &kafka.Hash{},
			BatchTimeout: 1 * time.Millisecond,
			RequiredAcks: kafka.RequireAll,
		}
		defer alertWriter.Close()
		alerter := alert.New(alertWriter, logger)

		var own wss.OwnOrderLookup
		if teAddr := os.Getenv("TRADE_EXEC_GRPC_ADDR"); teAddr != "" {
			teClient, err := tradeexec.New(teAddr)
			if err != nil {
				logger.Error("trade-execution dial failed — own_orders mode will skip every frame", zap.Error(err))
			} else {
				defer func() { _ = teClient.Close() }()
				own = teClient
			}
		} else if bmode == wss.ModeOwnOrders {
			logger.Error("BROADCAST_FEED_MODE=own_orders but TRADE_EXEC_GRPC_ADDR is unset — every frame will be skipped")
		}

		broadcast = wss.NewBroadcastListener(wss.BroadcastConfig{
			Mode:       bmode,
			AnchorUser: os.Getenv("BROADCAST_ANCHOR_USER"),
		}, indiraClient, listener, ucClient, own, brokerEvents, alerter, logger)
		broadcast.Start(bctx)
		logger.Info("broadcast listener enabled",
			zap.String("mode", string(bmode)),
			zap.String("anchor_user", os.Getenv("BROADCAST_ANCHOR_USER")),
			zap.Bool("own_order_lookup", own != nil))
	} else {
		logger.Info("broadcast listener off (BROADCAST_FEED_MODE=off)")
	}

	// Durability backstop: republish any broker_events rows whose inline
	// publish failed (published_at IS NULL) so a transient Kafka outage never
	// permanently drops an event from order.events. Scans in id order every 1s.
	go orderEventsPub.RunWorker(recCtx, brokerEvents, 1*time.Second)
	logger.Info("order.events durability worker started")

	logger.Info("orderstatus svc ready",
		zap.String("chunk", "D"),
		zap.String("next", "Chunk E — trade-execution consumes order.events"))

	// ── Graceful shutdown ──────────────────────────────────────────────
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	logger.Info("shutdown signal received")

	// Broadcast first: sends the close frame and stops its goroutines.
	bcancel()
	if broadcast != nil {
		broadcast.Wait()
	}

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
