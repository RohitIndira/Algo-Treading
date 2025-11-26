// package router

// import (
// 	"net/http"

// 	"github.com/RohitIndira/Algo-Treading/api/gateway/internal/handlers"
// 	"github.com/RohitIndira/Algo-Treading/api/gateway/internal/middleware"
// 	"github.com/gorilla/mux"
// )

// func NewRouter(userConfigHandler *handlers.UserConfigHandler, authProxyHandler *handlers.AuthProxyHandler, corsConfig middleware.CORSConfig) http.Handler {
// 	r := mux.NewRouter()

// 	// Apply CORS middleware
// 	r.Use(middleware.CORS(corsConfig))

// 	// API v1 routes
// 	api := r.PathPrefix("/api/v1").Subrouter()

// 	// Health check
// 	api.HandleFunc("/health", userConfigHandler.HealthCheck).Methods("GET")

// 	// Strategy routes
// 	api.HandleFunc("/strategies", userConfigHandler.CreateStrategy).Methods("POST")
// 	api.HandleFunc("/strategies/{strategy_id}", userConfigHandler.GetStrategy).Methods("GET")
// 	api.HandleFunc("/strategies/{strategy_id}", userConfigHandler.UpdateStrategy).Methods("PUT")
// 	api.HandleFunc("/strategies/{strategy_id}", userConfigHandler.DeleteStrategy).Methods("DELETE")
// 	api.HandleFunc("/strategies/{strategy_id}/activate", userConfigHandler.ActivateStrategy).Methods("POST")
// 	api.HandleFunc("/strategies/{strategy_id}/deactivate", userConfigHandler.DeactivateStrategy).Methods("POST")

// 	// User strategies
// 	api.HandleFunc("/users/{user_id}/strategies", userConfigHandler.ListUserStrategies).Methods("GET")

// 	// Auth routes (proxy to user-login-service)
// 	api.PathPrefix("/auth").HandlerFunc(authProxyHandler.ProxyRequest)
// 	api.PathPrefix("/credentials").HandlerFunc(authProxyHandler.ProxyRequest)
// 	api.PathPrefix("/session").HandlerFunc(authProxyHandler.ProxyRequest)
// 	api.PathPrefix("/history").HandlerFunc(authProxyHandler.ProxyRequest)
// 	api.PathPrefix("/totp").HandlerFunc(authProxyHandler.ProxyRequest)
// 	api.PathPrefix("/admin").HandlerFunc(authProxyHandler.ProxyRequest)

// 	return r
// }

package router

import (
	"net/http"

	"github.com/RohitIndira/Algo-Treading/api/gateway/internal/handlers"
	"github.com/RohitIndira/Algo-Treading/api/gateway/internal/middleware"
	"github.com/gorilla/mux"
)

func NewRouter(
	userConfigHandler *handlers.UserConfigHandler,
	authProxyHandler *handlers.AuthProxyHandler,
	websocketHandler *handlers.WebSocketHandler,
	corsConfig middleware.CORSConfig,
) http.Handler {

	r := mux.NewRouter()

	// CORS middleware
	r.Use(mux.CORSMethodMiddleware(r))
	r.Use(middleware.CORS(corsConfig))

	// /api/v1 prefix
	api := r.PathPrefix("/api/v1").Subrouter()

	// 🔥 FIX: Global OPTIONS route for preflight
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

	// WebSocket routes for live match feed
	r.HandleFunc("/ws/matches", websocketHandler.HandleMatchesFeed)        // Single user
	r.HandleFunc("/ws/matches/all", websocketHandler.HandleAllMatchesFeed) // All users

	// Proxy routes — no methods limit
	api.PathPrefix("/auth").HandlerFunc(authProxyHandler.ProxyRequest)
	api.PathPrefix("/credentials").HandlerFunc(authProxyHandler.ProxyRequest)
	api.PathPrefix("/session").HandlerFunc(authProxyHandler.ProxyRequest)
	api.PathPrefix("/history").HandlerFunc(authProxyHandler.ProxyRequest)
	api.PathPrefix("/totp").HandlerFunc(authProxyHandler.ProxyRequest)
	api.PathPrefix("/admin").HandlerFunc(authProxyHandler.ProxyRequest)

	return r
}
