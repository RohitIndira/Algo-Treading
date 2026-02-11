package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"go.uber.org/zap"

	redisdb "github.com/RohitIndira/Algo-Treading/pkg/database/redis"
	"github.com/RohitIndira/Algo-Treading/services/paper-execution/internal/consumer"
	"github.com/RohitIndira/Algo-Treading/services/paper-execution/internal/publisher"
	"github.com/RohitIndira/Algo-Treading/services/paper-execution/internal/service"
)

type Config struct {
	KafkaBrokers []string
	KafkaGroupID string
	TopicSignals string
	TopicExec    string
	TopicPnL     string

	RedisAddr     string
	RedisPassword string
	RedisDB       int

	StrategyID   string
	TradingMode  string
	SLPct        float64
	TSLPct       float64
	PollInterval time.Duration

	EmitPnLSnapshots    bool
	PnLSnapshotInterval time.Duration
}

func main() {
	_ = godotenv.Load()

	logger, _ := zap.NewProduction()
	defer logger.Sync()

	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	logger.Info("Starting Paper Execution Service",
		zap.String("strategy_id", cfg.StrategyID),
		zap.String("trading_mode", cfg.TradingMode),
		zap.Strings("kafka_brokers", cfg.KafkaBrokers),
		zap.String("topic_signals", cfg.TopicSignals),
		zap.String("topic_exec", cfg.TopicExec),
		zap.String("redis_addr", cfg.RedisAddr),
		zap.Duration("poll_interval", cfg.PollInterval))

	redisClient, err := redisdb.New(redisdb.Config{
		Address:  cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
		PoolSize: 50,
	})
	if err != nil {
		logger.Fatal("Failed to connect to Redis", zap.Error(err))
	}
	defer redisClient.Close()

	pub := publisher.NewKafkaPublisher(cfg.KafkaBrokers, cfg.TopicExec, cfg.TopicPnL, logger)
	defer pub.Close()

	sim := service.NewSimulator(service.Config{
		StrategyID:          cfg.StrategyID,
		TradingMode:         cfg.TradingMode,
		SLPct:               cfg.SLPct,
		TSLPct:              cfg.TSLPct,
		PollInterval:        cfg.PollInterval,
		EmitPnLSnapshots:    cfg.EmitPnLSnapshots,
		PnLSnapshotInterval: cfg.PnLSnapshotInterval,
	}, redisClient, pub, logger)

	cons := consumer.NewTradeSignalConsumer(cfg.KafkaBrokers, cfg.TopicSignals, cfg.KafkaGroupID, sim, logger)
	defer cons.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := cons.Start(ctx); err != nil {
			logger.Error("trade-signals consumer error", zap.Error(err))
		}
	}()

	go sim.Start(ctx)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh
	logger.Info("Shutting down paper-execution")
	cancel()
	time.Sleep(1 * time.Second)
}

func loadConfig() (Config, error) {
	kafkaBrokersStr := getenv("KAFKA_BROKERS", "localhost:9092")
	brokers := []string{}
	for _, b := range strings.Split(kafkaBrokersStr, ",") {
		b = strings.TrimSpace(b)
		if b != "" {
			brokers = append(brokers, b)
		}
	}
	if len(brokers) == 0 {
		return Config{}, fmt.Errorf("KAFKA_BROKERS is empty")
	}

	redisDB, _ := strconv.Atoi(getenv("MARKET_REDIS_DB", "0"))

	slPct, _ := strconv.ParseFloat(getenv("SL_PCT", "10"), 64)
	tslPct, _ := strconv.ParseFloat(getenv("TSL_PCT", "20"), 64)
	pi, err := time.ParseDuration(getenv("POLL_INTERVAL", "5s"))
	if err != nil {
		return Config{}, fmt.Errorf("invalid POLL_INTERVAL: %w", err)
	}
	psi, err := time.ParseDuration(getenv("PNL_SNAPSHOT_INTERVAL", "30s"))
	if err != nil {
		return Config{}, fmt.Errorf("invalid PNL_SNAPSHOT_INTERVAL: %w", err)
	}

	emitPnL := strings.ToLower(strings.TrimSpace(getenv("EMIT_PNL_SNAPSHOTS", "false"))) == "true"

	return Config{
		KafkaBrokers:        brokers,
		KafkaGroupID:        getenv("KAFKA_GROUP_ID", "paper-execution-service"),
		TopicSignals:        getenv("KAFKA_TOPIC_TRADE_SIGNALS", "trade-signals"),
		TopicExec:           getenv("KAFKA_TOPIC_PAPER_EXECUTIONS", "paper-executions"),
		TopicPnL:            getenv("KAFKA_TOPIC_PAPER_PNL", "paper-pnl"),
		RedisAddr:           getenv("MARKET_REDIS_ADDR", "localhost:6379"),
		RedisPassword:       getenv("MARKET_REDIS_PASSWORD", ""),
		RedisDB:             redisDB,
		StrategyID:          getenv("STRATEGY_ID", "JOBBING"),
		TradingMode:         strings.ToUpper(strings.TrimSpace(getenv("TRADING_MODE", "PAPER"))),
		SLPct:               slPct,
		TSLPct:              tslPct,
		PollInterval:        pi,
		EmitPnLSnapshots:    emitPnL,
		PnLSnapshotInterval: psi,
	}, nil
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
