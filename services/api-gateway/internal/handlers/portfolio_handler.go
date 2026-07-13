// Portfolio HTTP handlers — the api-gateway wire endpoints for the UI's
// portfolio pages. See §6 of docs/portfolio_service_design.md.
//
// Each handler:
//   1. Pulls userID from JWT-authenticated context (mirrors live-algos).
//   2. Fetches raw data via PortfolioServiceClient (gRPC to portfolio svc).
//   3. Enriches with LTP via the api-gateway's LTP subsystem.
//   4. Wraps the result in the Indira envelope for the frontend.
//
// portfolio svc itself never touches Redis or trading_db — that's the
// entire point of the CQRS split. All enrichment code lives here.
package handlers

import (
	"log"
	"net/http"
	"strconv"

	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/auth"
	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/grpc_clients"
	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/portfolio"
)

// PortfolioHandler owns the /users/me/portfolio/* endpoints.
type PortfolioHandler struct {
	client   *grpc_clients.PortfolioClient
	enricher *portfolio.Enricher
}

// NewPortfolioHandler wires the gRPC client + enricher. Both must be
// non-nil at construction — the router only registers routes when the
// dependencies are available (see main.go).
func NewPortfolioHandler(client *grpc_clients.PortfolioClient, enricher *portfolio.Enricher) *PortfolioHandler {
	return &PortfolioHandler{client: client, enricher: enricher}
}

// ─── GET /api/v1/users/me/portfolio/summary ────────────────────────────
//
// One-shot rollup for the portfolio home tile: invested / realized (lifetime
// + today) / active + closed counts / MANTHAN vs USER_MANUAL split /
// currentValue + unrealized (both null when LTP unavailable).
//
// Internally makes TWO gRPC calls — GetPortfolioSummary + GetActivePositions
// — because unrealized P&L needs per-lot LTPs. Kept sequential; portfolio
// svc queries are single-user and cheap.
func (h *PortfolioHandler) GetSummary(w http.ResponseWriter, r *http.Request) {
	userID, ok := auth.UserIDFromContext(r.Context())
	if !ok {
		respondIndiraError(w, http.StatusUnauthorized, "E_AUTH", "authenticated user not found")
		return
	}

	sumResp, err := h.client.GetPortfolioSummary(r.Context(), userID)
	if err != nil {
		log.Printf("portfolio: GetPortfolioSummary %s: %v", userID, err)
		respondIndiraError(w, http.StatusInternalServerError, "E500", "failed to load portfolio summary")
		return
	}
	if !sumResp.GetSuccess() {
		e := sumResp.GetError()
		log.Printf("portfolio: summary server error %s: %s / %s", userID, e.GetCode(), e.GetMessage())
		respondIndiraError(w, http.StatusInternalServerError, "E500", "portfolio summary unavailable")
		return
	}

	posResp, err := h.client.GetActivePositions(r.Context(), userID)
	if err != nil {
		log.Printf("portfolio: GetActivePositions %s: %v", userID, err)
		respondIndiraError(w, http.StatusInternalServerError, "E500", "failed to load portfolio positions")
		return
	}
	if !posResp.GetSuccess() {
		// Non-fatal: return summary WITHOUT LTP enrichment. Users still
		// see invested/realized numbers; unrealized shows "—".
		out, _ := h.enricher.EnrichSummary(r.Context(), sumResp.GetSummary(), nil)
		respondIndiraOK(w, out)
		return
	}

	out, err := h.enricher.EnrichSummary(r.Context(), sumResp.GetSummary(), posResp.GetPositions())
	if err != nil {
		log.Printf("portfolio: EnrichSummary %s: %v", userID, err)
		respondIndiraError(w, http.StatusInternalServerError, "E500", "failed to enrich portfolio summary")
		return
	}
	respondIndiraOK(w, out)
}

// ─── GET /api/v1/users/me/portfolio/positions ──────────────────────────
//
// All ACTIVE lots (MANTHAN + USER_MANUAL), entry_time ASC, each enriched
// with LTP + currentValue + unrealizedPnL. Top-level ltpStatus tells the
// UI whether to trust or "—" the LTP fields.
func (h *PortfolioHandler) GetPositions(w http.ResponseWriter, r *http.Request) {
	userID, ok := auth.UserIDFromContext(r.Context())
	if !ok {
		respondIndiraError(w, http.StatusUnauthorized, "E_AUTH", "authenticated user not found")
		return
	}

	posResp, err := h.client.GetActivePositions(r.Context(), userID)
	if err != nil {
		log.Printf("portfolio: GetActivePositions %s: %v", userID, err)
		respondIndiraError(w, http.StatusInternalServerError, "E500", "failed to load portfolio positions")
		return
	}
	if !posResp.GetSuccess() {
		e := posResp.GetError()
		log.Printf("portfolio: positions server error %s: %s / %s", userID, e.GetCode(), e.GetMessage())
		respondIndiraError(w, http.StatusInternalServerError, "E500", "portfolio positions unavailable")
		return
	}

	out, err := h.enricher.EnrichPositions(r.Context(), posResp.GetPositions())
	if err != nil {
		log.Printf("portfolio: EnrichPositions %s: %v", userID, err)
		respondIndiraError(w, http.StatusInternalServerError, "E500", "failed to enrich portfolio positions")
		return
	}
	respondIndiraOK(w, out)
}

// ─── GET /api/v1/users/me/portfolio/history ────────────────────────────
//
// Paginated EXITED lots, exit_time DESC. No LTP enrichment — realized_pnl
// is the historical truth.
//
// Query params:
//   page      (default 1, clamped >=1 server-side)
//   pageSize  (default 50, clamped [1, 200] server-side)
func (h *PortfolioHandler) GetHistory(w http.ResponseWriter, r *http.Request) {
	userID, ok := auth.UserIDFromContext(r.Context())
	if !ok {
		respondIndiraError(w, http.StatusUnauthorized, "E_AUTH", "authenticated user not found")
		return
	}

	page, pageSize := parsePagination(r)

	histResp, err := h.client.GetClosedPositions(r.Context(), userID, page, pageSize)
	if err != nil {
		log.Printf("portfolio: GetClosedPositions %s: %v", userID, err)
		respondIndiraError(w, http.StatusInternalServerError, "E500", "failed to load portfolio history")
		return
	}
	if !histResp.GetSuccess() {
		e := histResp.GetError()
		log.Printf("portfolio: history server error %s: %s / %s", userID, e.GetCode(), e.GetMessage())
		respondIndiraError(w, http.StatusInternalServerError, "E500", "portfolio history unavailable")
		return
	}

	out := h.enricher.EnrichHistory(
		histResp.GetPositions(),
		page, pageSize,
		histResp.GetTotalCount(),
	)
	respondIndiraOK(w, out)
}

// parsePagination pulls page + pageSize from query string. Returns
// server-side-friendly defaults so portfolio svc's own clamp logic
// doesn't need to see zero/negative values.
func parsePagination(r *http.Request) (int32, int32) {
	page := int32(1)
	pageSize := int32(50)
	if v := r.URL.Query().Get("page"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 1 {
			page = int32(n)
		}
	}
	if v := r.URL.Query().Get("pageSize"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 1 && n <= 200 {
			pageSize = int32(n)
		}
	}
	return page, pageSize
}
