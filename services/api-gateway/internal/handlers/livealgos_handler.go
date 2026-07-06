package handlers

import (
	"log"
	"net/http"

	common "github.com/RohitIndira/Algo-Treading/api/proto/common"
	pb "github.com/RohitIndira/Algo-Treading/api/proto/user_config"
	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/algos"
	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/auth"
	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/grpc_clients"
	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/livealgos"
)

// LiveAlgosHandler serves GET /api/v1/users/me/live-algos — the mobile
// "Live Algos" tab.
//
// Security note (why /users/me/ not /users/{user_id}/):
//
//	The URL uses the literal keyword "me" — NOT a path parameter that
//	the client can set. Identity is extracted from the JWT via
//	auth.UserIDFromContext, which the AuthMiddleware attached to the
//	request context after cryptographic verification. There is no
//	code path that lets one user query another user's data — the
//	handler literally cannot see any user id other than the JWT's
//	own. Same pattern as GitHub's /user, Twitter's /users/me, etc.
//
// Data sources (Phase 1):
//
//	user-config gRPC     — user's strategies + capital + active flag
//	algos.Catalog        — static algo metadata (name/logo/type/style)
//	livealgos.Build      — aggregator that combines the two
//
// Data sources (Phase 2, follow-up):
//
//	trading_db.manthan_positions — open positions count per user+strategy
//	ext-Redis LTP feed           — for mark-to-market unrealized P&L
//	trade history                — for realized P&L + win rate
//	alerts subsystem             — for actionRequired
type LiveAlgosHandler struct {
	userConfig *grpc_clients.UserConfigClient
	catalog    algos.Catalog
}

// NewLiveAlgosHandler wires the handler to its dependencies. Both are
// required — a nil userConfig or catalog would produce a runtime nil
// deref on every request, which is a startup misconfiguration.
func NewLiveAlgosHandler(userConfig *grpc_clients.UserConfigClient, catalog algos.Catalog) *LiveAlgosHandler {
	if userConfig == nil {
		panic("handlers.NewLiveAlgosHandler: userConfig is required")
	}
	if catalog == nil {
		panic("handlers.NewLiveAlgosHandler: catalog is required")
	}
	return &LiveAlgosHandler{
		userConfig: userConfig,
		catalog:    catalog,
	}
}

// GetLiveAlgos handles GET /api/v1/users/me/live-algos.
//
// Response codes:
//
//	200 E_OK         — envelope with data (algos may be empty slice)
//	401 E_AUTH_*     — auth failure (handled by AuthMiddleware; won't
//	                   normally reach this handler at all)
//	500 E500         — user-config-service unreachable / gRPC error
//	500 E500         — user-config returned success=false
func (h *LiveAlgosHandler) GetLiveAlgos(w http.ResponseWriter, r *http.Request) {
	// ── Identity comes from the JWT, NOT the URL ───────────────────
	// AuthMiddleware attached this after verifying the JWT signature.
	// Impossible to spoof from the client — the URL has no user slot.
	userID, ok := auth.UserIDFromContext(r.Context())
	if !ok {
		// Should never happen: this route is under the `protected`
		// subrouter so AuthMiddleware guarantees Claims are attached.
		// Defensive check catches a mis-wired route earlier.
		respondIndiraError(w, http.StatusUnauthorized,
			"E_AUTH", "authenticated user not found in context")
		return
	}

	// ── List the user's strategies via user-config gRPC ────────────
	// ActiveOnly:false because the frontend filters locally between
	// Live/Paused/Stopped chips — we want STOPPED strategies too so
	// the user can see them and re-activate.
	//
	// PageSize is set high on purpose: a normal user has 1-2 deployed
	// strategies, so 100 easily covers everyone without triggering the
	// service's built-in cap.
	resp, err := h.userConfig.ListUserStrategies(r.Context(), &pb.ListUserStrategiesRequest{
		UserId:     userID,
		ActiveOnly: false,
		Pagination: &common.PaginationRequest{
			Page:     1,
			PageSize: 100,
		},
	})
	if err != nil {
		log.Printf("livealgos: user-config ListUserStrategies failed for %s: %v", userID, err)
		respondIndiraError(w, http.StatusInternalServerError,
			"E500", "failed to load your live algos")
		return
	}
	if !resp.Success {
		msg := "failed to load your live algos"
		if resp.Error != nil && resp.Error.Message != "" {
			msg = resp.Error.Message
		}
		respondIndiraError(w, http.StatusInternalServerError, "E500", msg)
		return
	}

	// ── Assemble the response ──────────────────────────────────────
	// Aggregator is a pure function: strategies + catalog → Response.
	// Easy to unit-test in isolation without spinning up a gRPC mock.
	payload := livealgos.Build(resp.Strategies, h.catalog)

	respondIndiraOK(w, payload)
}
