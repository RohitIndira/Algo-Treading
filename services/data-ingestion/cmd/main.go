package main

import (
	"context"
	"fmt"
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
	fmt.Println("In the main function of main.go file in data-ingestion service")
	cfg := config.Load() // calls `Load()` from `config` package
	fmt.Println("Configuration loaded successfully")

	lgr, err := logger.NewWithDefaults("data-ingestion")
	if err != nil {
		panic(err)
	}
	defer lgr.Sync()
	fmt.Println("Starting data-ingestion service for Jobbing strategy (Live Market Data)")
	lgr.Info("Starting data-ingestion service for Jobbing strategy (Live Market Data)")

	// Context for the entire application
	ctxRun, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()

	// ========================================================================
	// B2C LIVE MARKET DATA SETUP (JOBBING STRATEGY)
	// ========================================================================
	lgr.Info("Initializing B2C live market data pipeline for Jobbing strategy")

	// Kafka producer for live market data
	marketDataProdCfg := kafkapkg.ProducerConfig{
		Brokers:     cfg.KafkaBrokers,
		Topic:       cfg.KafkaTopic,
		BatchSize:   100,
		MaxAttempts: 3,
	}
	marketDataProducer, err := kafkapkg.NewProducer(marketDataProdCfg)
	if err != nil {
		lgr.Fatal("failed to create market-data kafka producer", zap.Error(err))
	}
	defer marketDataProducer.Close()

	if err := kafkapkg.EnsureTopicExists(cfg.KafkaBrokers, cfg.KafkaTopic, 3, 1); err != nil {
		lgr.Fatal("failed to ensure market.data.live topic exists", zap.Error(err))
	}
	lgr.Info("Connected to Kafka (live market data)",
		zap.Strings("brokers", cfg.KafkaBrokers),
		zap.String("topic", cfg.KafkaTopic))

	marketDataPub := publisher.NewKafkaPublisher(marketDataProducer, cfg.KafkaTopic)

	// Start B2C watcher for live market data (Jobbing strategy)
	if cfg.B2CBridgePath == "" {
		lgr.Warn("B2C_BRIDGE_PATH not set, skipping B2C market data ingestion")
	} else {
		jobbingWatcher, err := watcher.NewB2CWatcher(
			cfg.B2CBridgePath,
			cfg.B2CTokens,
			cfg.StocksDBPath,
			marketDataPub,
			lgr,
		)
		if err != nil {
			lgr.Fatal("failed to create B2C watcher", zap.Error(err))
		}

		go func() {
			lgr.Info("Starting B2C live market data watcher")
			if err := jobbingWatcher.Run(ctxRun); err != nil {
				lgr.Error("B2C watcher stopped with error", zap.Error(err))
			}
		}()
	}

	// ========================================================================
	// GRACEFUL SHUTDOWN
	// ========================================================================
	lgr.Info("All data ingestion pipelines started successfully")
	lgr.Info("Service is running. Press Ctrl+C to stop.")

	// Wait for termination signal
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	lgr.Info("Shutdown signal received, stopping all pipelines...")

	// Cancel all running goroutines
	cancelRun()

	// Allow graceful shutdown with timeout
	tctx, tcancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer tcancel()

	// Wait for graceful shutdown or timeout
	<-tctx.Done()

	if tctx.Err() == context.DeadlineExceeded {
		lgr.Warn("Graceful shutdown timed out")
	} else {
		lgr.Info("All components shut down gracefully")
	}

	lgr.Info("Data-ingestion service stopped")
}
