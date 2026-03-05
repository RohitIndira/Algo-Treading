package router

import (
	"net/http"

	"github.com/RohitIndira/Algo-Treading/api/gateway/internal/handlers"
	"github.com/RohitIndira/Algo-Treading/api/gateway/internal/middleware"
	"github.com/gorilla/mux"
)

func NewRouter(
	userConfigHandler *handlers.UserConfigHandler,
	websocketHandler *handlers.WebSocketHandler,
	paperHandler *handlers.PaperTradingHandler,
	corsConfig middleware.CORSConfig,
) http.Handler {

	r := mux.NewRouter()

	// CORS middleware
	r.Use(middleware.CORS(corsConfig))

	// /api/v1 prefix
	api := r.PathPrefix("/api/v1").Subrouter()

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
		api.HandleFunc("/paper-trades/ws-info", paperHandler.GetPaperWSInfo).Methods("GET")
		// Live orders endpoints
		api.HandleFunc("/live-orders", paperHandler.GetLiveOrders).Methods("GET")
		api.HandleFunc("/live-orders/closed-orders", paperHandler.GetClosedLiveOrders).Methods("GET")
		api.HandleFunc("/live-orders/force-exit-all", paperHandler.ForceExitAllLive).Methods("POST")
		api.HandleFunc("/live-orders/indira-positions", paperHandler.GetIndiraPositions).Methods("GET")
		// Dashboard
		api.HandleFunc("/dashboard-stats", paperHandler.GetDashboardStats).Methods("GET")
	}

	// WebSocket routes for live match feed
	r.HandleFunc("/ws/matches", websocketHandler.HandleMatchesFeed)        // Single user
	r.HandleFunc("/ws/matches/all", websocketHandler.HandleAllMatchesFeed) // All users

	return r
}
