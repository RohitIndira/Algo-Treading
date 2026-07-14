package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	pb "github.com/RohitIndira/Algo-Treading/api/proto/user_config"
	"github.com/RohitIndira/Algo-Treading/pkg/database/postgres"
	"github.com/RohitIndira/Algo-Treading/pkg/logger"
	"github.com/RohitIndira/Algo-Treading/services/user-config/config"
	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/repository"
	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/scheduler"
	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/server"
	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/service"
	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/tradeexec"
	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/worker"
	goredis "github.com/go-redis/redis/v8"
	"github.com/jmoiron/sqlx"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	// Load .env file if it exists
	if err := godotenv.Load(); err != nil {
		fmt.Printf("Note: .env file not found, using system environment variables\n")
	}

	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Initialize logger
	lgr, err := logger.NewWithDefaults("user-config-service")
	if err != nil {
		log.Fatalf("Failed to initialize logger: %v", err)
	}
	defer lgr.Sync()

	lgr.Info("Starting User Config Service")

	// Initialize database connection
	dbClient, err := postgres.New(cfg.Database)
	if err != nil {
		lgr.Fatal("Failed to connect to database", zap.Error(err))
	}
	defer dbClient.Close()

	lgr.Info("Database connection established")

	// Connect to the trade-execution DB (execution_db) to store/read user credentials
	execDBClient, err := postgres.New(cfg.ExecutionDB)
	if err != nil {
		lgr.Warn("Failed to connect to execution DB — credentials will NOT be saved", zap.Error(err))
	}
	if execDBClient != nil {
		defer execDBClient.Close()
		lgr.Info("Execution DB connection established", zap.String("db", cfg.ExecutionDB.Database))
	}

	// Initialize Kafka writer
	var kafkaWriter *kafka.Writer
	if cfg.Kafka.Enabled {
		kafkaWriter = kafka.NewWriter(kafka.WriterConfig{
			Brokers:      cfg.Kafka.Brokers,
			Topic:        cfg.Kafka.Topic,
			Balancer:     &kafka.LeastBytes{},
			RequiredAcks: int(kafka.RequireOne),
			Async:        false,
			BatchSize:    1,
			BatchTimeout: 10 * time.Millisecond,
		})
		defer kafkaWriter.Close()
		lgr.Info("Kafka writer initialized", zap.String("topic", cfg.Kafka.Topic))
	} else {
		lgr.Warn("Kafka is disabled")
	}

	// Initialize repository (repository expects *sqlx.DB, but we have *sql.DB)
	// We need to use jmoiron/sqlx to wrap it
	sqlxDB := sqlx.NewDb(dbClient.DB, "postgres")
	repo := repository.NewStrategyRepository(sqlxDB)

	// Initialize credentials repository (writes to execution_db DB)
	var credsRepo repository.CredentialsRepository
	if execDBClient != nil {
		execSqlxDB := sqlx.NewDb(execDBClient.DB, "postgres")
		credsRepo = repository.NewCredentialsRepository(execSqlxDB, cfg.EncryptionKey)
		lgr.Info("Credentials repository initialized (execution_db)")
	} else {
		credsRepo = repository.NewNoopCredentialsRepository()
		lgr.Warn("Using no-op credentials repository — credentials will not be persisted")
	}

	// Optional ext-Redis client. user-config reads `symbol:{TICKER}` master
	// data for symbol → ISIN resolution during strategy validation. If the
	// env isn't set or the dial fails, the service still works — callers
	// then have to supply ISIN directly.
	var extRedis *goredis.Client
	if addr := os.Getenv("EXT_REDIS_ADDR"); addr != "" {
		extRedis = goredis.NewClient(&goredis.Options{
			Addr:         addr,
			Password:     os.Getenv("EXT_REDIS_PASSWORD"),
			DB:           0,
			ReadTimeout:  2 * time.Second,
			WriteTimeout: 2 * time.Second,
		})
		pCtx, pCancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := extRedis.Ping(pCtx).Err(); err != nil {
			lgr.Warn("ext-Redis ping failed — symbol→ISIN resolution disabled",
				zap.String("addr", addr), zap.Error(err))
			_ = extRedis.Close()
			extRedis = nil
		} else {
			lgr.Info("ext-Redis connected for symbol→ISIN resolution", zap.String("addr", addr))
			defer extRedis.Close()
		}
		pCancel()
	}

	// Initialize service
	svc := service.NewStrategyService(repo, credsRepo, kafkaWriter, cfg.Kafka.Topic, extRedis)

	// Trade-execution HTTP client for the atomic SQUARE_OFF_AT_MARKET path
	// of Deactivate/Delete. Nil-safe — if TRADE_EXECUTION_HTTP_URL is unset,
	// SQUARE_OFF requests get a clear error and KEEP_POSITIONS_OPEN keeps
	// working as before. See internal/tradeexec/client.go for the contract.
	if tradeExecURL := os.Getenv("TRADE_EXECUTION_HTTP_URL"); tradeExecURL != "" {
		svc.SetTradeExecClient(tradeexec.New(tradeExecURL))
		lgr.Info("Trade-execution client wired for SQUARE_OFF_AT_MARKET path",
			zap.String("url", tradeExecURL))
	} else {
		lgr.Warn("TRADE_EXECUTION_HTTP_URL unset — SQUARE_OFF_AT_MARKET requests will return an error; KEEP_POSITIONS_OPEN still works")
	}

	// Initialize Outbox Worker
	if cfg.Kafka.Enabled && kafkaWriter != nil {
		outboxWorker := worker.NewOutboxWorker(repo, kafkaWriter, 500*time.Millisecond)
		workerCtx, workerCancel := context.WithCancel(context.Background())
		defer workerCancel()
		
		go outboxWorker.Start(workerCtx)
		lgr.Info("Outbox worker started")
	}

	// Start EOD deactivation scheduler (deactivates all active strategies at 15:30 IST)
	eodScheduler := scheduler.NewEODDeactivationScheduler(svc)
	eodCtx, eodCancel := context.WithCancel(context.Background())
	defer eodCancel()
	go eodScheduler.Start(eodCtx)
	lgr.Info("EOD deactivation scheduler started")

	// Initialize gRPC server
	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(10*1024*1024), // 10MB
		grpc.MaxSendMsgSize(10*1024*1024), // 10MB
	)

	// Register service
	pb.RegisterUserConfigServiceServer(grpcServer, server.NewUserConfigServer(svc))

	// Register reflection service for debugging
	reflection.Register(grpcServer)

	// Start gRPC server
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Server.Port))
	if err != nil {
		lgr.Fatal("Failed to listen", zap.Error(err))
	}

	lgr.Info("Starting gRPC server", zap.Int("port", cfg.Server.Port))

	// Start server in goroutine
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			lgr.Fatal("Failed to serve", zap.Error(err))
		}
	}()

	lgr.Info("User Config Service started successfully")

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	lgr.Info("Shutting down server...")

	// Graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Stop gRPC server
	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()

	select {
	case <-ctx.Done():
		lgr.Warn("Shutdown timeout exceeded, forcing shutdown")
		grpcServer.Stop()
	case <-stopped:
		lgr.Info("Server stopped gracefully")
	}

	lgr.Info("User Config Service shut down complete")
}
