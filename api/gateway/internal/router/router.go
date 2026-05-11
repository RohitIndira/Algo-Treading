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
	manthanHandler *handlers.ManthanHandler,
	healthHandler *handlers.HealthHandler,
	corsConfig middleware.CORSConfig,
) http.Handler {

	r := mux.NewRouter()

	// CORS middleware
	r.Use(middleware.CORS(corsConfig))

	// Health probes — three tiers per the production playbook.
	// Mounted at root (NOT under /api/v1) so k8s / ALB probes don't have to
	// know about API versioning.
	//   /livez   — process alive (always 200 if HTTP works)
	//   /readyz  — read-only probe of every dependency
	//   /health  — deep probe: readyz + DB write probe (UPSERT on health_probes)
	if healthHandler != nil {
		r.HandleFunc("/livez", healthHandler.Livez).Methods("GET")
		r.HandleFunc("/readyz", healthHandler.Readyz).Methods("GET")
		r.HandleFunc("/health", healthHandler.Health).Methods("GET")
	}

	// /api/v1 prefix
	api := r.PathPrefix("/api/v1").Subrouter()

	// Global OPTIONS route for preflight
	api.Methods(http.MethodOptions).HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// Legacy /api/v1/health — keep the gRPC pass-through for backwards
	// compatibility with existing frontends, but the production probes are
	// /livez, /readyz, /health at root.
	api.HandleFunc("/health", userConfigHandler.HealthCheck).Methods("GET")

	// Auth — frontend calls this after SSO login (and on JWT refresh) so the
	// backend has the latest broker JWT for offline flows like the protective
	// replayer's 15:35 IST cron.
	api.HandleFunc("/auth/credentials", userConfigHandler.UpdateCredentials).Methods("POST")

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
		// Dashboard
		api.HandleFunc("/dashboard-stats", paperHandler.GetDashboardStats).Methods("GET")
	}

	// Manthan aggregated overview
	if manthanHandler != nil {
		api.HandleFunc("/manthan/overview", manthanHandler.GetOverview).Methods("GET")
	}

	// WebSocket routes for live match feed
	r.HandleFunc("/ws/matches", websocketHandler.HandleMatchesFeed)        // Single user
	r.HandleFunc("/ws/matches/all", websocketHandler.HandleAllMatchesFeed) // All users

	return r
}
