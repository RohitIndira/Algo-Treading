package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/RohitIndira/Algo-Treading/pkg/database/mongodb"
	kafkapkg "github.com/RohitIndira/Algo-Treading/pkg/kafka"
	"github.com/RohitIndira/Algo-Treading/pkg/logger"
	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/config"
	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/internal/data"
	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/internal/publisher"
	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/internal/watcher"

	"go.uber.org/zap"
)

func main() {
	// Route the standard library logger to stdout so stdlib log.* output shares
	// the single stream PM2 captures alongside the zap logger.
	log.SetOutput(os.Stdout)

	cfg := config.Load()

	lgr, err := logger.NewWithDefaults("data-ingestion")
	if err != nil {
		panic(err)
	}
	defer lgr.Sync()

	lgr.Info("Starting data-ingestion service")

	// Initialize MongoDB client
	ctx, cancel := context.WithTimeout(context.Background(), cfg.MongoConnectTimeout)
	defer cancel()

	mongoClient, err := mongodb.New(ctx, mongodb.Config{URI: cfg.MongoURI, Database: cfg.MongoDatabase, ConnectTimeout: cfg.MongoConnectTimeout})
	if err != nil {
		lgr.Fatal("failed to connect to mongodb", zap.Error(err))
	}
	defer mongoClient.Close(context.Background())

	lgr.Info("Connected to MongoDB", zap.String("database", cfg.MongoDatabase), zap.String("collection", cfg.MongoCollection))

	// Initialize Kafka producer
	prodCfg := kafkapkg.ProducerConfig{
		Brokers:     cfg.KafkaBrokers,
		Topic:       cfg.KafkaTopic,
		BatchSize:   100,
		MaxAttempts: 3,
	}
	producer, err := kafkapkg.NewProducer(prodCfg)
	if err != nil {
		lgr.Fatal("failed to create kafka producer", zap.Error(err))
	}
	defer producer.Close()
	// Ensure Kafka topic exists (auto-create if missing)
	if err := kafkapkg.EnsureTopicExists(cfg.KafkaBrokers, cfg.KafkaTopic, 1, 1); err != nil {
		lgr.Fatal("failed to ensure topic exists", zap.Error(err))
	}
	lgr.Info("Connected to Kafka", zap.Strings("brokers", cfg.KafkaBrokers), zap.String("topic", cfg.KafkaTopic))

	// Create publisher wrapper
	pub := publisher.NewKafkaPublisher(producer, cfg.KafkaTopic)

	// Initialize Redis Manager
	redisMgr, err := data.NewRedisManager(cfg.RedisURI, cfg.RedisPassword, cfg.RedisDB, lgr, mongoClient)
	if err != nil {
		lgr.Fatal("failed to create redis manager", zap.Error(err))
	}
	defer redisMgr.Close()

	// Start Redis Scheduler for Company Master Data
	// Run in background with a separate context that is cancelled on shutdown
	schedCtx, schedCancel := context.WithCancel(context.Background())
	defer schedCancel()
	redisMgr.StartScheduler(schedCtx, mongoClient)

	// Start watcher
	ctxRun, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()

	w, err := watcher.NewMongoWatcher(mongoClient, cfg.MongoCollection, redisMgr, pub, lgr)
	if err != nil {
		lgr.Fatal("failed to create watcher", zap.Error(err))
	}

	// watcherDone closes when the run loop exits for ANY reason — error,
	// normal return, or recovered panic — so we can tell the difference
	// between "we were asked to stop" and "the watcher silently died" below.
	// Without this, a watcher crash leaves main() blocked on <-sig forever:
	// the process stays "online" in PM2 while doing nothing.
	watcherDone := make(chan struct{})
	go func() {
		defer close(watcherDone)
		defer func() {
			if rec := recover(); rec != nil {
				lgr.Error("PANIC in watcher run loop — ingestion has stopped",
					zap.Any("panic", rec),
					zap.String("stacktrace", string(debug.Stack())))
			}
		}()
		if err := w.Run(ctxRun); err != nil {
			lgr.Error("watcher stopped with error — ingestion has stopped", zap.Error(err))
		}
	}()

	// Wait for termination
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	crashed := false
	select {
	case <-sig:
		lgr.Info("received shutdown signal")
	case <-watcherDone:
		crashed = true
		lgr.Error("watcher exited unexpectedly — shutting down so the process supervisor can restart it")
	}

	lgr.Info("shutting down data-ingestion service")
	// allow graceful shutdown
	tctx, tcancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer tcancel()
	cancelRun()
	// give components time to cleanup
	<-tctx.Done()

	if crashed {
		// os.Exit skips deferred calls, so flush the logger explicitly —
		// otherwise the crash log above may never reach the file/pm2 capture.
		lgr.Sync()
		os.Exit(1)
	}
}
