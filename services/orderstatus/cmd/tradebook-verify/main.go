// Command tradebook-verify is an ops/verification tool: it fetches one user's
// live tradebook and runs it through the REAL reconciler tradebook path
// (insert into order_status_db.broker_events + publish to order.events Kafka),
// bypassing the 15:31 IST schedule. Used to verify Fix 3 end-to-end with a
// fresh broker JWT. Not part of the running service.
//
// Env:
//
//	INDIRA_TOKEN   (required) — bearer JWT
//	INDIRA_USER    (default S4450)
//	INDIRA_APPID   (required) — broker appId
//	INDIRA_SOURCE  (optional) — WEB/AND/IOS; if unset, tries WEB then AND then IOS
//	KAFKA_BROKERS  (default localhost:9092)
//	POSTGRES_*, ORDER_STATUS_DB — order_status_db connection (same as the service)
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"time"

	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	indira "github.com/RohitIndira/Algo-Treading/pkg/indira"
	"github.com/RohitIndira/Algo-Treading/services/orderstatus/internal/publisher"
	"github.com/RohitIndira/Algo-Treading/services/orderstatus/internal/reconciler"
	"github.com/RohitIndira/Algo-Treading/services/orderstatus/internal/store"
)

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func main() {
	logger, _ := zap.NewDevelopment()
	defer func() { _ = logger.Sync() }()

	token := os.Getenv("INDIRA_TOKEN")
	appID := os.Getenv("INDIRA_APPID")
	userID := env("INDIRA_USER", "S4450")
	if token == "" || appID == "" {
		logger.Fatal("INDIRA_TOKEN and INDIRA_APPID are required")
	}

	client := indira.NewDefaultClient()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Determine a working source (drives the sso header). Try the given one,
	// else WEB→AND→IOS until the tradebook call stops returning AU004.
	sources := []string{env("INDIRA_SOURCE", "WEB")}
	if os.Getenv("INDIRA_SOURCE") == "" {
		sources = []string{"WEB", "AND", "IOS"}
	}
	var auth *indira.AuthContext
	var trades []indira.TradeBook
	for _, src := range sources {
		a := &indira.AuthContext{UserId: userID, AppId: appID, Source: src, BearerToken: token}
		tb, err := client.GetTradeBook(ctx, a)
		if err != nil {
			logger.Warn("GetTradeBook failed for source — trying next", zap.String("source", src), zap.Error(err))
			continue
		}
		auth, trades = a, tb
		logger.Info("GetTradeBook OK", zap.String("source", src), zap.Int("trades", len(tb)))
		break
	}
	if auth == nil {
		logger.Fatal("GetTradeBook failed for every source — check token/appId/source")
	}

	// Print the raw shape of the first trade — closes the TradeTime TZ residual
	// with a live sample and shows the real ExchTrdId magnitude.
	for i := range trades {
		raw, _ := json.Marshal(trades[i])
		seq, seqErr := trades[i].ExchTradeID()
		fmt.Printf("\n[trade %d] ordId=%s tradeTime=%q exchTrdId->int64=%d (err=%v) tradedPrice=%.4f tradedQty=%d\n  raw=%s\n",
			i, trades[i].OrdId, trades[i].TradeTime, seq, seqErr, trades[i].TradedPrice, trades[i].TradedQty, string(raw))
		if i >= 2 {
			fmt.Printf("  ... (%d more)\n", len(trades)-3)
			break
		}
	}

	// Wire the real reconciler path: real DB writer + real Kafka publisher.
	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		env("POSTGRES_HOST", "localhost"), env("POSTGRES_PORT", "5432"),
		env("POSTGRES_USER", "postgres"), env("POSTGRES_PASSWORD", "postgres"),
		env("ORDER_STATUS_DB", "order_status_db"), env("POSTGRES_SSL_MODE", "disable"))
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		logger.Fatal("open db", zap.Error(err))
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		logger.Fatal("ping db", zap.Error(err))
	}

	kw := &kafka.Writer{
		Addr:         kafka.TCP(env("KAFKA_BROKERS", "localhost:9092")),
		Topic:        "order.events",
		Balancer:     &kafka.Hash{},
		BatchTimeout: 1 * time.Millisecond,
		RequiredAcks: kafka.RequireAll,
	}
	defer kw.Close()

	writer := store.NewWriter(db, logger)
	pub := publisher.NewPublisher(kw, logger)

	rec := reconciler.New(
		client, writer, pub,
		func(context.Context, string) (*indira.AuthContext, error) { return auth, nil },
		func(context.Context) ([]string, error) { return []string{userID}, nil },
		reconciler.Config{}, logger,
	)

	logger.Info("running one tradebook sweep (bypassing 15:31 schedule)")
	rec.RunTradebookSweepOnce(ctx)
	logger.Info("tradebook sweep done — check broker_events + order.events")
}
