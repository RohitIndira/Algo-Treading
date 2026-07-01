package router

import (
	"net/http"

	"github.com/RohitIndira/Algo-Treading/api/gateway/internal/handlers"
	"github.com/RohitIndira/Algo-Treading/api/gateway/internal/middleware"
	"github.com/gorilla/mux"
	"go.uber.org/zap"
)

func NewRouter(
	userConfigHandler *handlers.UserConfigHandler,
	websocketHandler *handlers.WebSocketHandler,
	paperHandler *handlers.PaperTradingHandler,
	amnPreviewHandler *handlers.AMNPreviewHandler,
	corsConfig middleware.CORSConfig,
	authConfig middleware.AuthConfig,
	logger *zap.Logger,
) http.Handler {

	r := mux.NewRouter()

	// Outermost: catches a panic from every middleware/handler below so a
	// crash is logged with full context instead of just resetting the conn.
	r.Use(middleware.Recovery(logger))
	// Assign/propagate X-Correlation-ID before anything else touches the context
	r.Use(middleware.CorrelationID())
	// Security headers on every response
	r.Use(middleware.SecurityHeaders())
	// Cap request bodies at 1 MB
	r.Use(middleware.RequestSizeLimit(1 << 20))
	// Rate limit: 100 req/s per IP, burst of 200
	r.Use(middleware.NewRateLimiter(100, 200).Middleware())
	// Access log for every request (status, latency, user, mutating tag)
	r.Use(middleware.AccessLog(logger))
	// CORS middleware
	r.Use(middleware.CORS(corsConfig))

	// /api/v1 prefix
	api := r.PathPrefix("/api/v1").Subrouter()
	// Auth verification — runs on all /api/v1 routes; health and OPTIONS are
	// skipped inside the middleware itself.
	api.Use(middleware.Auth(authConfig))

	// Global OPTIONS route for preflight
	api.Methods(http.MethodOptions).HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// Health check
	api.HandleFunc("/health", userConfigHandler.HealthCheck).Methods("GET")

	// Strategy CRUD
	api.HandleFunc("/strategies", userConfigHandler.CreateStrategy).Methods("POST")
	api.HandleFunc("/strategies/{strategy_id}", userConfigHandler.GetStrategy).Methods("GET")
	api.HandleFunc("/strategies/{strategy_id}", userConfigHandler.UpdateStrategy).Methods("PUT")
	api.HandleFunc("/strategies/{strategy_id}", userConfigHandler.DeleteStrategy).Methods("DELETE")
	api.HandleFunc("/strategies/{strategy_id}/activate", userConfigHandler.ActivateStrategy).Methods("POST")
	api.HandleFunc("/strategies/{strategy_id}/deactivate", userConfigHandler.DeactivateStrategy).Methods("POST")

	// User strategies
	api.HandleFunc("/users/{user_id}/strategies", userConfigHandler.ListUserStrategies).Methods("GET")

	// Paper trading REST endpoints
	if paperHandler != nil {
		api.HandleFunc("/paper-trades/positions", paperHandler.GetPaperPositions).Methods("GET")
		api.HandleFunc("/paper-trades/closed-orders", paperHandler.GetClosedPaperOrders).Methods("GET")
		api.HandleFunc("/paper-trades/force-exit-all", paperHandler.ForceExitAll).Methods("POST")
		api.HandleFunc("/paper-trades/force-exit-strategy", paperHandler.ForceExitStrategy).Methods("POST")
		api.HandleFunc("/paper-trades/ws-info", paperHandler.GetPaperWSInfo).Methods("GET")
		// Live orders endpoints
		api.HandleFunc("/live-orders", paperHandler.GetLiveOrders).Methods("GET")
		api.HandleFunc("/live-orders/closed-orders", paperHandler.GetClosedLiveOrders).Methods("GET")
		api.HandleFunc("/live-orders/force-exit-all", paperHandler.ForceExitAllLive).Methods("POST")
		api.HandleFunc("/live-orders/force-exit-strategy", paperHandler.ForceExitStrategyLive).Methods("POST")
		api.HandleFunc("/live-orders/indira-positions", paperHandler.GetIndiraPositions).Methods("GET")
		api.HandleFunc("/live-orders/subscribe-broker-ws", paperHandler.SubscribeBrokerWS).Methods("POST")
		api.HandleFunc("/live-orders/price-watches", paperHandler.GetPriceWatches).Methods("GET")
		api.HandleFunc("/live-orders/cancel-price-watch", paperHandler.CancelPriceWatch).Methods("POST")
		// Auto square-off config (trade-execution native)
		api.HandleFunc("/auto-square-off/config", paperHandler.SetAutoSquareOffConfig).Methods("POST")
		api.HandleFunc("/auto-square-off/config", paperHandler.GetAutoSquareOffConfig).Methods("GET")
		// Dashboard
		api.HandleFunc("/dashboard-stats", paperHandler.GetDashboardStats).Methods("GET")
	}

	// AMN backfill preview — returns news items matching strategy conditions
	if amnPreviewHandler != nil {
		api.HandleFunc("/amn-preview", amnPreviewHandler.Preview).Methods("POST")
	}

	// WebSocket routes for live match feed
	r.HandleFunc("/ws/matches", websocketHandler.HandleMatchesFeed)        // Single user
	r.HandleFunc("/ws/matches/all", websocketHandler.HandleAllMatchesFeed) // All users

	return r
}
