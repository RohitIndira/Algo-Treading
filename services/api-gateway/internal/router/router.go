package router

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/auth"
	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/handlers"
	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/middleware"
	"github.com/gorilla/mux"
)

func NewRouter(
	userConfigHandler *handlers.UserConfigHandler,
	websocketHandler *handlers.WebSocketHandler,
	paperHandler *handlers.PaperTradingHandler,
	manthanHandler *handlers.ManthanHandler,
	hftHandler *handlers.HFTHandler,
	healthHandler *handlers.HealthHandler,
	marketHandler *handlers.MarketHandler,
	algosHandler *handlers.AlgosHandler,
	verifier auth.Verifier,
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
		// /live-orders/force-exit-all + /force-exit-strategy moved to the
		// protected subrouter below — they can liquidate real positions.
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

	// Algo catalog — Explore screen. Now PROTECTED (2026-07-02): user
	// reversed the earlier "public catalog" decision on the grounds
	// that even static algo details reveal product strategy to
	// unauthenticated visitors. Moved to `protected` subrouter below.
	//
	// The old registration site here is intentionally left as a
	// tombstone comment so anyone reading router.go doesn't wonder
	// where the /algos handler went — search for algosHandler.ListAlgos
	// in the protected block below.

	// Live market quote — reads the ext-Redis tick feed. Public market data,
	// no auth. Frontend passes ?symbol= (resolved via symbol→isin→token) or
	// ?token= directly.
	if marketHandler != nil {
		api.HandleFunc("/market/quote", marketHandler.GetQuote).Methods("GET")
	}

	// HFT strategy lifecycle — drives an already-created HFT_BIDDING
	// strategy in the hft-engine. Creation goes through /strategies above.
	// /hft/{id}/start and /stop moved to the protected subrouter below —
	// they can start real-money HFT runs. /state stays public (read-only).
	if hftHandler != nil {
		api.HandleFunc("/hft/{strategy_id}/state", hftHandler.GetHFTState).Methods("GET")
	}

	// ── Protected subrouter ─────────────────────────────────────────
	// Every route registered on `protected` requires a valid JWT via
	// the AuthMiddleware. Today the verifier is NoopVerifier (shape
	// checks only, no signature math). The day Codifi shares the real
	// HS512 signing secret, we swap NoopVerifier for LocalKeyVerifier
	// in main.go — a one-line change, and this whole subrouter
	// instantly gets real cryptographic auth. No routes need to move.
	//
	// Routes CURRENTLY protected in this session:
	//
	//   GET  /api/v1/whoami                       (test route — echoes userID)
	//   POST /api/v1/live-orders/force-exit-all   (can liquidate real book)
	//   POST /api/v1/live-orders/force-exit-strategy (can liquidate strategy)
	//   POST /api/v1/hft/{strategy_id}/start      (starts LIVE HFT)
	//   POST /api/v1/hft/{strategy_id}/stop       (stops LIVE HFT)
	//
	// Additional user-specific routes (dashboard, live-orders GET, etc.)
	// will migrate here in follow-up sessions once frontend adds the
	// Authorization header for each.
	protected := api.PathPrefix("").Subrouter()
	protected.Use(middleware.AuthRequired(verifier))

	// GET /api/v1/whoami — echoes the authenticated userID.
	// Zero side effects. Purpose: smoke-testing the auth middleware
	// end-to-end without touching production data. Once real user
	// handlers exist, this route can stay as a diagnostic tool or be
	// removed — it's harmless either way.
	protected.HandleFunc("/whoami", func(w http.ResponseWriter, req *http.Request) {
		userID, _ := auth.UserIDFromContext(req.Context())
		w.Header().Set("Content-Type", "application/json")
		body := map[string]interface{}{
			"infoID":    "0",
			"infoMsg":   "success",
			"timestamp": time.Now().UnixMilli(),
			"data":      map[string]string{"userID": userID},
		}
		_ = json.NewEncoder(w).Encode(body)
	}).Methods("GET")

	// POST /api/v1/auth/logout — invalidates the current JWT on OUR side.
	//
	// The frontend calls this on user logout, BEFORE deleting the JWT
	// from local storage. We pull the raw JWT out of the request context
	// (attached by AuthMiddleware) and hand it to the Verifier's Revoke
	// method, which removes it from any cache AND adds it to a short-
	// lived blacklist. Any subsequent request bearing the same JWT
	// gets rejected at step 1 of Verify.
	//
	// This closes the "revocation gap" — Codifi still trusts the JWT
	// until its natural exp, but our gateway blocks it instantly. If an
	// attacker has a stolen JWT and the real user hits logout, the
	// attacker's next request through us returns 401 immediately.
	//
	// Response is 204 No Content — logout succeeded, nothing to return.
	protected.HandleFunc("/auth/logout", func(w http.ResponseWriter, req *http.Request) {
		jwt, ok := auth.RawJWTFromContext(req.Context())
		if !ok {
			// AuthMiddleware guarantees this — but defensive check
			// costs nothing and catches configuration bugs.
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		verifier.Revoke(jwt)
		w.WriteHeader(http.StatusNoContent)
	}).Methods("POST")

	// Algo catalog — Explore screen. Moved here from public on 2026-07-02
	// per user directive that unauthenticated visitors should not see
	// product-strategy details. Frontend team must send Authorization
	// header on this call now — same shape as any other protected route.
	if algosHandler != nil {
		protected.HandleFunc("/algos", algosHandler.ListAlgos).Methods("GET")
	}

	// Money-moving paper/live routes — moved from the public `api.`
	// subrouter above. Same handlers, same paths, just gated now.
	if paperHandler != nil {
		protected.HandleFunc("/live-orders/force-exit-all", paperHandler.ForceExitAllLive).Methods("POST")
		protected.HandleFunc("/live-orders/force-exit-strategy", paperHandler.ForceExitStrategyLive).Methods("POST")
	}

	// HFT start/stop — moved from the public `api.` subrouter above.
	if hftHandler != nil {
		protected.HandleFunc("/hft/{strategy_id}/start", hftHandler.StartHFT).Methods("POST")
		protected.HandleFunc("/hft/{strategy_id}/stop", hftHandler.StopHFT).Methods("POST")
	}

	// WebSocket routes for live match feed
	r.HandleFunc("/ws/matches", websocketHandler.HandleMatchesFeed)        // Single user
	r.HandleFunc("/ws/matches/all", websocketHandler.HandleAllMatchesFeed) // All users

	// Per-user notification stream — bridges Kafka `manthan.notifications`
	// → frontend WS so the user sees broker-session-expired, manual-exit,
	// JWT-expiring, etc. and the UI can prompt re-login / show toasts.
	r.HandleFunc("/ws/notifications", websocketHandler.HandleNotificationsFeed)

	// Live HFT order/fill tape — streams every engine event (PLACE/FILL/
	// MODIFY/CANCEL/ARM/PAUSE/RESUME) for one strategy to the dashboard.
	r.HandleFunc("/ws/hft/{strategy_id}", websocketHandler.HandleHFTFeed)

	return r
}
