package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	pb "github.com/RohitIndira/Algo-Treading/api/proto/user_config"
	"github.com/RohitIndira/Algo-Treading/pkg/correlation"
	"github.com/RohitIndira/Algo-Treading/pkg/database/postgres"
	"github.com/RohitIndira/Algo-Treading/pkg/logger"
	"github.com/RohitIndira/Algo-Treading/services/user-config/config"
	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/repository"
	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/scheduler"
	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/server"
	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/service"
	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/worker"
	"github.com/jmoiron/sqlx"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

// recoveryInterceptor recovers a panic in any unary RPC handler and logs it
// with full context (method, correlation id, stack trace) instead of letting
// it crash the whole process. Unlike net/http, grpc-go has NO built-in
// per-RPC panic recovery — without this, one bad request takes down strategy
// CRUD, the outbox worker, and the EOD scheduler together.
func recoveryInterceptor(lgr *logger.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp interface{}, err error) {
		defer func() {
			if rec := recover(); rec != nil {
				lgr.Error("PANIC RECOVERED in gRPC handler",
					zap.String("correlation_id", correlation.FromContext(ctx)),
					zap.String("method", info.FullMethod),
					zap.Any("panic", rec),
					zap.String("stacktrace", string(debug.Stack())),
				)
				err = status.Error(codes.Internal, "internal server error")
			}
		}()
		return handler(ctx, req)
	}
}

// correlationServerInterceptor extracts the correlation ID from incoming gRPC
// metadata and stores it in the handler context. A new ID is generated when
// none is present so every RPC is always traceable.
func correlationServerInterceptor(
	ctx context.Context,
	req interface{},
	_ *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if vals := md.Get(correlation.GRPCMetadataKey); len(vals) > 0 && vals[0] != "" {
			ctx = correlation.WithContext(ctx, vals[0])
		}
	}
	if correlation.FromContext(ctx) == "" {
		ctx = correlation.WithContext(ctx, correlation.NewID())
	}
	return handler(ctx, req)
}

func main() {
	// Route the standard library logger to stdout so stdlib log.* output shares
	// the single stream PM2 captures alongside the zap logger.
	log.SetOutput(os.Stdout)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

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

	// Connect to the trade-execution DB (trading_execution) to store/read user credentials
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

	// Initialize credentials repository (writes to trading_execution DB)
	var credsRepo repository.CredentialsRepository
	if execDBClient != nil {
		execSqlxDB := sqlx.NewDb(execDBClient.DB, "postgres")
		credsRepo = repository.NewCredentialsRepository(execSqlxDB, cfg.EncryptionKey)
		lgr.Info("Credentials repository initialized (trading_execution)")
	} else {
		credsRepo = repository.NewNoopCredentialsRepository()
		lgr.Warn("Using no-op credentials repository — credentials will not be persisted")
	}

	// Initialize service
	svc := service.NewStrategyService(repo, credsRepo, kafkaWriter, cfg.Kafka.Topic)

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
	serverOpts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(10 * 1024 * 1024),
		grpc.MaxSendMsgSize(10 * 1024 * 1024),
		// recoveryInterceptor must run outermost so it catches panics from
		// correlationServerInterceptor and every handler beneath it.
		grpc.ChainUnaryInterceptor(recoveryInterceptor(lgr), correlationServerInterceptor),
	}
	certFile := os.Getenv("GRPC_TLS_CERT")
	keyFile := os.Getenv("GRPC_TLS_KEY")
	if certFile != "" && keyFile != "" {
		creds, err := credentials.NewServerTLSFromFile(certFile, keyFile)
		if err != nil {
			lgr.Fatal("Failed to load gRPC server TLS credentials", zap.Error(err))
		}
		serverOpts = append(serverOpts, grpc.Creds(creds))
		lgr.Info("gRPC server TLS enabled", zap.String("cert", certFile))
	} else {
		lgr.Warn("gRPC server TLS disabled — set GRPC_TLS_CERT and GRPC_TLS_KEY to enable")
	}
	grpcServer := grpc.NewServer(serverOpts...)

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

	// Start server in goroutine. lgr.Fatal already exits on a returned error;
	// the recover here only guards against an actual panic in Serve, which
	// would otherwise crash the process with an unstructured stack trace.
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				lgr.Error("PANIC in gRPC server — service is non-functional",
					zap.Any("panic", rec),
					zap.String("stacktrace", string(debug.Stack())))
				lgr.Sync()
				os.Exit(1)
			}
		}()
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
