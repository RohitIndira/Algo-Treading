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
	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/consumer"
	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/publisher"
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

	// Initialize repository (repository expects *sqlx.DB, but we have *sql.DB)
	// We need to use jmoiron/sqlx to wrap it
	sqlxDB := sqlx.NewDb(dbClient.DB, "postgres")
	repo := repository.NewStrategyRepository(sqlxDB)

	// Initialize Kafka writers
	var kafkaWriter *kafka.Writer
	var jobbingWriter *kafka.Writer
	var cash52wWriter *kafka.Writer
	var cash52wConsumer *consumer.Cash52WConfigConsumer
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

		// Optional dedicated writer for jobbing configs
		if cfg.Kafka.JobbingTopic != "" && cfg.Kafka.JobbingTopic != cfg.Kafka.Topic {
			jobbingWriter = kafka.NewWriter(kafka.WriterConfig{
				Brokers:      cfg.Kafka.Brokers,
				Topic:        cfg.Kafka.JobbingTopic,
				Balancer:     &kafka.LeastBytes{},
				RequiredAcks: int(kafka.RequireOne),
				Async:        false,
				BatchSize:    1,
				BatchTimeout: 10 * time.Millisecond,
			})
			defer jobbingWriter.Close()
			lgr.Info("Kafka writer initialized for jobbing configs", zap.String("topic", cfg.Kafka.JobbingTopic))
		}

		// Optional dedicated writer for Cash 52W strategy configs so
		// downstream services can subscribe to a focused stream.
		if cfg.Kafka.Cash52WConfigTopic != "" && cfg.Kafka.Cash52WConfigTopic != cfg.Kafka.Topic {
			cash52wWriter = kafka.NewWriter(kafka.WriterConfig{
				Brokers:      cfg.Kafka.Brokers,
				Topic:        cfg.Kafka.Cash52WConfigTopic,
				Balancer:     &kafka.LeastBytes{},
				RequiredAcks: int(kafka.RequireOne),
				Async:        false,
				BatchSize:    1,
				BatchTimeout: 10 * time.Millisecond,
			})
			defer cash52wWriter.Close()
			lgr.Info("Kafka writer initialized for Cash52W configs", zap.String("topic", cfg.Kafka.Cash52WConfigTopic))

			// Also start a consumer to keep the cash52w_configs table in sync
			// from the user-configs.cash52w topic. This ensures that 52W
			// configuration can be rebuilt purely from Kafka events if needed.
			cash52wConsumer = consumer.NewCash52WConfigConsumer(
				cfg.Kafka.Brokers,
				cfg.Kafka.Cash52WConfigTopic,
				"user-config-cash52w-sync-v1",
				repo,
				lgr.Logger,
			)
		}
	} else {
		lgr.Warn("Kafka is disabled")
	}

	// Initialize Phase 1 ConfigPublisher for enhanced Cash52W configs
	var configPublisher *publisher.ConfigPublisher
	if cfg.Kafka.Enabled {
		var err error
		configPublisher, err = publisher.NewConfigPublisher(cfg.Kafka.Brokers, cfg.Kafka.Cash52WConfigTopic, lgr)
		if err != nil {
			lgr.Fatal("Failed to create config publisher", zap.Error(err))
		}
		defer configPublisher.Close()
	}

	// Initialize service
	svc := service.NewStrategyService(repo, kafkaWriter, cfg.Kafka.Topic, jobbingWriter, cfg.Kafka.JobbingTopic, cash52wWriter, cfg.Kafka.Cash52WConfigTopic, configPublisher)

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

	// Start Cash52W config consumer if configured
	if cash52wConsumer != nil {
		go func() {
			if err := cash52wConsumer.Start(context.Background()); err != nil {
				lgr.Error("Cash52W config consumer exited with error", zap.Error(err))
			}
		}()
	}

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

	// Close Kafka consumer
	if cash52wConsumer != nil {
		if err := cash52wConsumer.Close(); err != nil {
			lgr.Warn("Failed to close Cash52W config consumer", zap.Error(err))
		}
	}

	lgr.Info("User Config Service shut down complete")
}
