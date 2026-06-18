package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/gorilla/mux"
	"github.com/RohitIndira/Algo-Treading/api/gateway/config"
	pkglogger "github.com/RohitIndira/Algo-Treading/pkg/logger"
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

	// Initialize handlers
	userConfigHandler := handlers.NewUserConfigHandler(userConfigClient)

	// Initialize Redis client for WebSocket pub/sub
	redisClient := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	// Test Redis connection
	ctx := context.Background()
	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Printf("Warning: Redis connection failed: %v (WebSocket features will not work)", err)
	} else {
		log.Println("Connected to Redis for WebSocket pub/sub")
	}
	defer redisClient.Close()

	// Initialize logger for WebSocket handler
	pkgLgr, _ := pkglogger.NewWithDefaults("api-gateway")
	defer pkgLgr.Close()
	logger := pkgLgr.Logger

	// Initialize WebSocket handler
	websocketHandler := handlers.NewWebSocketHandler(redisClient, logger)

	// Initialize Paper Trading handler
	paperTradingHandler := handlers.NewPaperTradingHandler(cfg.Services.TradeExecutionPaperURL)

	// AMN preview: stateless proxy to the rules-engine HTTP server.
	// The rules-engine owns MongoDB — gateway stays DB-free.
	amnPreviewHandler := handlers.NewAMNPreviewHandler(cfg.Services.RulesEngineURL)
	log.Printf("AMN preview proxying to rules-engine at %s", cfg.Services.RulesEngineURL)

	// CORS config
	corsConfig := middleware.CORSConfig{
		AllowedOrigins: cfg.CORS.AllowedOrigins,
		AllowedMethods: cfg.CORS.AllowedMethods,
		AllowedHeaders: cfg.CORS.AllowedHeaders,
	}

	// Auth config
	authConfig := middleware.AuthConfig{
		VerifyURL: cfg.Auth.VerifyURL,
		Timeout:   cfg.Auth.Timeout,
	}

	log.Printf("Auth middleware configured: verify URL=%s timeout=%s", authConfig.VerifyURL, authConfig.Timeout)

	// Router
	r := router.NewRouter(userConfigHandler, websocketHandler, paperTradingHandler, amnPreviewHandler, corsConfig, authConfig)

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
