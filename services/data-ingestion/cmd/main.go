package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	kafkapkg "github.com/RohitIndira/Algo-Treading/pkg/kafka"
	"github.com/RohitIndira/Algo-Treading/pkg/logger"
	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/config"
	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/internal/publisher"
	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/internal/watcher"

	"go.uber.org/zap"
)

func main() {

	cfg := config.Load()

	lgr, err := logger.NewWithDefaults("data-ingestion")
	if err != nil {
		panic(err)
	}
	defer lgr.Sync()

	lgr.Info("Starting data-ingestion service (B2C Market Data)")

	// Initialize Kafka producer build Producerconfig or pkg/kafka
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

	// Validate B2C configuration
	if cfg.B2CBridgePath == "" {
		lgr.Fatal("B2C_BRIDGE_PATH environment variable is not set")
	}
	if len(cfg.B2CTokens) == 0 {
		lgr.Fatal("B2C_TOKENS environment variable is not set (comma-separated list of tokens)")
	}

	lgr.Info("B2C Configuration loaded",
		zap.String("bridge_path", cfg.B2CBridgePath),
		zap.Strings("tokens", cfg.B2CTokens),
	)

	// Start watcher
	ctxRun, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()

	w, err := watcher.NewB2CWatcher(cfg.B2CBridgePath, cfg.B2CTokens, pub, lgr)
	if err != nil {
		lgr.Fatal("failed to create B2C watcher", zap.Error(err))
	}

	go func() {
		if err := w.Run(ctxRun); err != nil {
			lgr.Error("watcher stopped with error", zap.Error(err))
			cancelRun()
		}
	}()

	// Wait for termination
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	lgr.Info("shutting down data-ingestion service")
	// allow graceful shutdown
	tctx, tcancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer tcancel()
	cancelRun()
	// give components time to cleanup
	<-tctx.Done()
}
