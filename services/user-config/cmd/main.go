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

	pb "github.com/RohitIndira/Algo-Treading/api/proto/user_config"
	"github.com/RohitIndira/Algo-Treading/pkg/database/postgres"
	"github.com/RohitIndira/Algo-Treading/pkg/logger"
	"github.com/RohitIndira/Algo-Treading/services/user-config/config"
	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/repository"
	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/server"
	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/service"
	"github.com/jmoiron/sqlx"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
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

	// Initialize service
	svc := service.NewStrategyService(repo, kafkaWriter, cfg.Kafka.Topic)

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
