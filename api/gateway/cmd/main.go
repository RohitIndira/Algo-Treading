package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/gorilla/mux"
	"go.uber.org/zap"

	"github.com/RohitIndira/Algo-Treading/api/gateway/config"
	"github.com/RohitIndira/Algo-Treading/api/gateway/internal/grpc_clients"
	"github.com/RohitIndira/Algo-Treading/api/gateway/internal/handlers"
	"github.com/RohitIndira/Algo-Treading/api/gateway/internal/middleware"
	"github.com/RohitIndira/Algo-Treading/api/gateway/internal/router"
)

func main() {
	// Load config
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	log.Println("Starting API Gateway...")

	// gRPC client: user-config-service
	userConfigClient, err := grpc_clients.NewUserConfigClient(
		cfg.Services.UserConfigAddr,
		cfg.Server.GRPCTimeout,
	)
	if err != nil {
		log.Fatalf("Failed to initialize user config client: %v", err)
	}
	defer userConfigClient.Close()

	log.Printf("Connected to User Config Service at %s", cfg.Services.UserConfigAddr)

	// HTTP proxy handler for user-login-service. The gateway injects the
	// INTERNAL_API_KEY header so frontend clients do not need to know this
	// secret; only the gateway and user-login-service share it.
	authProxyHandler := handlers.NewAuthProxyHandler(
		cfg.Services.UserLoginServiceURL,
		cfg.Services.InternalAPIKey,
	)
	userConfigHandler := handlers.NewUserConfigHandler(userConfigClient)

	// gRPC client: trade-execution-service
	tradeExecClient, err := grpc_clients.NewTradeExecutionClient(
		cfg.Services.TradeExecutionAddr,
		cfg.Server.GRPCTimeout,
	)
	if err != nil {
		log.Fatalf("Failed to initialize trade execution client: %v", err)
	}
	defer tradeExecClient.Close()

	tradeExecHandler := handlers.NewTradeExecutionHandler(tradeExecClient)

	log.Printf("User Login Service URL: %s", cfg.Services.UserLoginServiceURL)

	// Initialize Redis client for WebSocket pub/sub
	//
	// In production this should point to the same Redis instance that
	// data-ingestion and rules-engine use for market data and realtime
	// password and DB configurable via environment variables so the
	// gateway can run in different environments without code changes.
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	redisPassword := os.Getenv("REDIS_PASSWORD")
	redisDB := 0
	if v := os.Getenv("REDIS_DB"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			redisDB = parsed
		}
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: redisPassword,
		DB:       redisDB,
	})

	// Test Redis connection
	ctx := context.Background()
	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Printf("Warning: Redis connection failed (%s/%d): %v (WebSocket features will not work)", redisAddr, redisDB, err)
	} else {
		log.Printf("Connected to Redis for WebSocket pub/sub at %s/%d", redisAddr, redisDB)
	}
	defer redisClient.Close()

	// Initialize logger for WebSocket handler
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	// Initialize WebSocket handler
	websocketHandler := handlers.NewWebSocketHandler(redisClient, logger)

	// CORS config
	corsConfig := middleware.CORSConfig{
		AllowedOrigins: cfg.CORS.AllowedOrigins,
		AllowedMethods: cfg.CORS.AllowedMethods,
		AllowedHeaders: cfg.CORS.AllowedHeaders,
	}

	// Router
	r := router.NewRouter(userConfigHandler, authProxyHandler, websocketHandler, tradeExecHandler, corsConfig)

	// Debug: list all routes
	_ = r.(*mux.Router).Walk(func(route *mux.Route, router *mux.Router, ancestors []*mux.Route) error {
		path, _ := route.GetPathTemplate()
		methods, _ := route.GetMethods()
		log.Printf("ROUTE → %v %v", methods, path)
		return nil
	})

	// HTTP server
	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Server.HTTPPort),
		Handler:      r,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start server
	go func() {
		log.Printf("API Gateway listening on port %d", cfg.Server.HTTPPort)
		log.Printf("Health check: http://localhost:%d/api/v1/health", cfg.Server.HTTPPort)

		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down API Gateway...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("Forced shutdown: %v", err)
	}

	log.Println("API Gateway stopped")
}
