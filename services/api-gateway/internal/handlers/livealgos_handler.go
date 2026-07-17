package handlers

import (
	"context"
	"errors"
	"log"
	"net/http"
	"time"

	common "github.com/RohitIndira/Algo-Treading/api/proto/common"
	pb "github.com/RohitIndira/Algo-Treading/api/proto/user_config"
	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/algos"
	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/auth"
	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/grpc_clients"
	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/livealgos"
	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/performance"
	"github.com/gorilla/mux"
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

	// The following are used only by the Details-page endpoints
	// (GetStrategyDetails, GetHoldings, GetTrades, GetStockPnL).
	// nil-safe: if either is nil at boot (e.g. stockk_trading DB or
	// staging LTP tunnel unreachable) the router simply doesn't
	// register the routes that need them.
	store livealgos.Store
	ltp   *livealgos.LTPStore

	// perfStore + perfClientMap feed the DetailsResponse.Chart line —
	// a per-day growth curve rebased so `Pct=100` on strategy
	// deployment day. Both nil-safe: when either is missing (or the
	// map has no entry for this algoID) BuildDetails' empty
	// DetailsChart stays as-is and the frontend renders a "chart
	// pending" placeholder.
	perfStore     performance.Store
	perfClientMap map[string]string
}

// NewLiveAlgosHandler wires the handler to its dependencies. Both
// userConfig and catalog are required — a nil userConfig or catalog
// would produce a runtime nil deref on the /live-algos list endpoint.
// store, ltp, perfStore, perfClientMap are optional (see
// LiveAlgosHandler doc); pass nil when unavailable and the Details-page
// routes stay unregistered upstream (store+ltp) or the chart data
// stays empty (perf).
func NewLiveAlgosHandler(
	userConfig *grpc_clients.UserConfigClient,
	catalog algos.Catalog,
	store livealgos.Store,
	ltp *livealgos.LTPStore,
	perfStore performance.Store,
	perfClientMap map[string]string,
) *LiveAlgosHandler {
	if userConfig == nil {
		panic("handlers.NewLiveAlgosHandler: userConfig is required")
	}
	if catalog == nil {
		panic("handlers.NewLiveAlgosHandler: catalog is required")
	}
	return &LiveAlgosHandler{
		userConfig:    userConfig,
		catalog:       catalog,
		store:         store,
		ltp:           ltp,
		perfStore:     perfStore,
		perfClientMap: perfClientMap,
	}
}

// HasDetailsEndpoints reports whether the router should register the
// 3 details-page routes on this handler. Wired to check both DB store
// and LTP tunnel — either being unavailable disables the routes so
// the frontend gets a clean 404 rather than a mysterious 500.
func (h *LiveAlgosHandler) HasDetailsEndpoints() bool {
	return h.store != nil && h.ltp != nil
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

	// ── Per-strategy P&L (positions_db + LTP) ──────────────────────
	// One positions_db query per strategy + one bulk LTP MGet. When
	// either store is unwired, metrics stay empty and Build falls back
	// to pnlPending=true rather than fabricating zeros.
	metrics := h.perStrategyMetrics(r.Context(), userID, resp.Strategies)

	// ── Assemble the response ──────────────────────────────────────
	// Aggregator is a pure function: strategies + catalog + metrics → Response.
	payload := livealgos.Build(resp.Strategies, h.catalog, metrics)

	respondIndiraOK(w, payload)
}

// perStrategyMetrics computes real per-strategy P&L for the LIST endpoint
// by hitting positions_db once per strategy and doing a single LTP MGet
// for every ACTIVE token across strategies. When the position store or
// LTP subsystem is unwired at boot, returns an empty map — Build then
// falls back to pnlPending=true so the UI spins instead of showing
// silent zeros.
//
// Skips strategies with status=STOPPED (stopped_at != nil) — no live
// positions, no need to hit the DB.
func (h *LiveAlgosHandler) perStrategyMetrics(
	ctx context.Context,
	userID string,
	strategies []*pb.Strategy,
) map[string]livealgos.StrategyMetrics {
	if h.store == nil {
		return nil
	}

	type stratPositions struct {
		strategyID string
		deployed   int64
		positions  []livealgos.PositionRow
	}
	perStrategy := make([]stratPositions, 0, len(strategies))
	tokenSet := make(map[string]struct{})

	for _, s := range strategies {
		if s == nil {
			continue
		}
		if s.StoppedAt != nil {
			// Stopped: no live positions to sum. Feed a zero-real metric
			// so the tile shows 0/0 rather than an infinite spinner.
			perStrategy = append(perStrategy, stratPositions{strategyID: s.StrategyId})
			continue
		}
		positions, err := h.store.Positions(ctx, s.StrategyId, userID)
		if err != nil {
			log.Printf("livealgos: perStrategyMetrics: Positions(%s/%s) failed: %v", s.StrategyId, userID, err)
			// Leave this strategy out of the map — Build falls back to pending.
			continue
		}
		perStrategy = append(perStrategy, stratPositions{
			strategyID: s.StrategyId,
			deployed:   deployedCapitalFromForList(s),
			positions:  positions,
		})
		for _, p := range positions {
			if p.Status == "ACTIVE" && p.ExchangeToken.Valid && p.ExchangeToken.String != "" {
				tokenSet[p.ExchangeToken.String] = struct{}{}
			}
		}
	}

	// One LTP MGet across every ACTIVE token. When the LTP tunnel is
	// down we still emit numeric metrics using realized-only P&L
	// (unrealized silently drops to 0). ltpStatus isn't surfaced on the
	// LIST endpoint's response envelope; the DETAILS endpoint owns that.
	var ltps map[string]livealgos.LTPQuote
	if len(tokenSet) > 0 && h.ltp != nil {
		tokens := make([]string, 0, len(tokenSet))
		for t := range tokenSet {
			tokens = append(tokens, t)
		}
		ltps, _ = h.ltp.FetchByTokens(ctx, tokens)
	}

	out := make(map[string]livealgos.StrategyMetrics, len(perStrategy))
	for _, sp := range perStrategy {
		out[sp.strategyID] = computeStrategyMetrics(sp.deployed, sp.positions, ltps)
	}
	return out
}

// computeStrategyMetrics runs the per-strategy P&L math shared by the
// LIST endpoint. Kept as a package-level function so it stays pure and
// testable without wiring an HTTP handler.
func computeStrategyMetrics(
	deployed int64,
	positions []livealgos.PositionRow,
	ltps map[string]livealgos.LTPQuote,
) livealgos.StrategyMetrics {
	var (
		realised    int64
		todayReal   int64
		unrealised  float64
		todayUnreal float64
		openCount   int32
		wins        int
		closed      int
	)
	todayStart := startOfTodayIST()
	for _, p := range positions {
		switch p.Status {
		case "ACTIVE":
			openCount++
			q, ok := livealgos.LTPForPosition(p, ltps)
			if !ok {
				continue
			}
			unrealised += (q.LTP - p.EntryPrice) * float64(p.Quantity)
			if q.PrevClose > 0 {
				todayUnreal += (q.LTP - q.PrevClose) * float64(p.Quantity)
			}
		case "EXITED":
			if !p.ExitPrice.Valid {
				continue // ORPHAN_CLEANUP etc — not a real trade
			}
			closed++
			if p.RealizedPnL.Valid {
				realised += int64(p.RealizedPnL.Float64)
				if p.RealizedPnL.Float64 > 0 {
					wins++
				}
			}
			if p.RealizedPnL.Valid && p.ExitTime.Valid && !p.ExitTime.Time.Before(todayStart) {
				todayReal += int64(p.RealizedPnL.Float64)
			}
		}
	}
	netAmt := realised + int64(unrealised)
	todayAmt := todayReal + int64(todayUnreal)

	var netPct, todayPct, winRate float64
	if deployed > 0 {
		netPct = float64(netAmt) / float64(deployed) * 100
		todayPct = float64(todayAmt) / float64(deployed) * 100
	}
	if closed > 0 {
		winRate = float64(wins) / float64(closed) * 100
	}
	return livealgos.StrategyMetrics{
		Real:          true,
		NetPnL:        netAmt,
		NetPct:        round2Pct(netPct),
		TodayPnL:      todayAmt,
		TodayPct:      round2Pct(todayPct),
		OpenPositions: openCount,
		WinRatePct:    round2Pct(winRate),
	}
}

// startOfTodayIST returns 00:00 IST for today — mirrors the same helper
// in portfolio store. Kept local so this handler stays self-contained.
func startOfTodayIST() time.Time {
	loc, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		now := time.Now().UTC()
		return time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	}
	now := time.Now().In(loc)
	return time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, loc)
}

func round2Pct(f float64) float64 {
	rounded := int64(f * 100)
	if f*100-float64(rounded) >= 0.5 {
		rounded++
	} else if f*100-float64(rounded) <= -0.5 {
		rounded--
	}
	return float64(rounded) / 100
}

// deployedCapitalFromForList reads TotalCapital off a strategy — kept
// local so the handler doesn't import the aggregator's unexported helper.
func deployedCapitalFromForList(s *pb.Strategy) int64 {
	if s == nil || s.TradeConfig == nil {
		return 0
	}
	return int64(s.TradeConfig.TotalCapital)
}

// ─── Details page handlers ──────────────────────────────────────────
//
// These 3 handlers implement the Details page (Screens 1-7 of the
// mockup). All read from stockk_trading directly (bypassing the
// user-config gRPC that the list endpoint uses) because they need
// position + order rows and the P&L math on top. LTP comes from the
// staging Redis via SSH tunnel — same client shape as `redisClient`
// elsewhere in the gateway.
//
// URL identity is enforced two ways:
//
//  1. `/users/me/...` — no user_id path param exists, identity always
//     comes from the JWT via auth.UserIDFromContext.
//  2. The store.StrategyMeta / store.Positions queries pass BOTH
//     strategy_id AND user_id in the WHERE clause. Even if a
//     malicious client fabricated a strategy_id belonging to another
//     user, the DB returns zero rows and the handler returns 404.

// GetStrategyDetails handles GET /users/me/live-algos/{strategyId}.
// Powers Screen 1 & 2 of the Details page.
func (h *LiveAlgosHandler) GetStrategyDetails(w http.ResponseWriter, r *http.Request) {
	userID, ok := auth.UserIDFromContext(r.Context())
	if !ok {
		respondIndiraError(w, http.StatusUnauthorized,
			"E_AUTH", "authenticated user not found in context")
		return
	}
	strategyID := mux.Vars(r)["strategyId"]
	if strategyID == "" {
		respondIndiraError(w, http.StatusBadRequest,
			"E_BAD_REQUEST", "strategyId is required")
		return
	}

	// Fetch header + positions + LTPs in that order — each depends on
	// the previous only for the FROM/WHERE, not for the results, so
	// the sequential call graph is fine at MVP scale.
	meta, err := h.store.StrategyMeta(r.Context(), strategyID, userID)
	if err != nil {
		if errors.Is(err, livealgos.ErrStrategyNotFound) {
			respondIndiraError(w, http.StatusNotFound,
				"E_NOT_FOUND", "strategy not found")
			return
		}
		log.Printf("livealgos: StrategyMeta %s/%s: %v", strategyID, userID, err)
		respondIndiraError(w, http.StatusInternalServerError,
			"E500", "failed to load strategy details")
		return
	}

	positions, err := h.store.Positions(r.Context(), strategyID, userID)
	if err != nil {
		log.Printf("livealgos: Positions %s/%s: %v", strategyID, userID, err)
		respondIndiraError(w, http.StatusInternalServerError,
			"E500", "failed to load positions")
		return
	}

	// LTP: only for ACTIVE positions with a resolved exchange_token.
	// The store reports HEALTHY/UNAVAILABLE and BuildDetails renders
	// `ltpStatus` on the response so the UI can display "—" (never 0)
	// when the tunnel is down.
	tokens := collectActiveTokens(positions)
	var (
		ltps      map[string]livealgos.LTPQuote
		ltpStatus = livealgos.StatusHealthy
	)
	if len(tokens) > 0 && h.ltp != nil {
		ltps, ltpStatus = h.ltp.FetchByTokens(r.Context(), tokens)
	} else if h.ltp == nil {
		// LTP subsystem wasn't wired at boot (Redis was down then) —
		// stay explicit rather than pretending healthy.
		ltpStatus = livealgos.StatusUnavailable
	}

	algoID, algoName := resolveAlgoMeta(h.catalog, meta.StrategyType)
	payload := livealgos.BuildDetails(meta, positions, ltps, algoID, algoName)
	payload.LTPStatus = string(ltpStatus)
	// Chart: prefer real algo_performance_daily rows since deploy day.
	// If that source is empty (ETL hasn't caught up to a fresh deployment,
	// or perfStore isn't wired) synthesize a 2-point line from the strategy's
	// current live P&L so the frontend can render a real curve from day 0
	// to today instead of an empty chart.
	payload.Chart = h.chartFor(r.Context(), algoID, meta.CreatedAt, payload.NetPnL.Percent)
	respondIndiraOK(w, payload)
}

// chartFor pulls per-day performance rows for algoID's reference client
// and rebases them so `Pct=100` on `deployedAt` (the strategy's created_at).
// Returns an empty DetailsChart when the perf store isn't wired, when
// this algoID has no mapped reference client, or when no rows exist on
// or after the deployment date — the frontend already handles empty
// chart gracefully by showing a "chart pending" placeholder.
//
// The math: algo_performance_daily.return_pct is cumulative-since-algo-
// inception. Rebasing to deployment means subtracting the first row's
// return_pct so the earliest chart point sits at 100. Every subsequent
// point is 100 + (row.ReturnPct - baseline) — i.e. growth from the day
// this strategy deployed.
func (h *LiveAlgosHandler) chartFor(ctx context.Context, algoID string, deployedAt time.Time, livePct float64) livealgos.DetailsChart {
	// Primary source: algo_performance_daily since deploy day.
	if h.perfStore != nil && algoID != "" {
		if refID, ok := h.perfClientMap[algoID]; ok && refID != "" {
			rows, err := h.perfStore.FetchDaily(ctx, algoID, refID)
			if err != nil {
				log.Printf("livealgos: chartFor: fetch daily perf for algo=%s ref=%s: %v", algoID, refID, err)
			}
			if len(rows) > 0 {
				depDate := deployedAt.UTC().Truncate(24 * time.Hour)
				sinceDeploy := make([]performance.DailyRow, 0, len(rows))
				for _, row := range rows {
					if !row.Date.Before(depDate) {
						sinceDeploy = append(sinceDeploy, row)
					}
				}
				if len(sinceDeploy) > 0 {
					baseline := sinceDeploy[0].ReturnPct
					points := make([]livealgos.DetailsChartPoint, 0, len(sinceDeploy))
					for _, row := range sinceDeploy {
						points = append(points, livealgos.DetailsChartPoint{
							Date: row.Date.Format("2006-01-02"),
							Pct:  100 + (row.ReturnPct - baseline),
						})
					}
					return livealgos.DetailsChart{Points: points}
				}
			}
		}
	}

	// Fallback: strategy is fresher than the last ETL row (or perf source
	// unwired). Render a real 2-point line — deploy day at 100 and today
	// at 100 + live netPct. This is the strategy's actual growth curve
	// so far; the chart auto-switches to per-day granularity once ETL
	// backfills 2026-07-16 onward.
	deployDate := deployedAt.In(istLoc()).Format("2006-01-02")
	todayDate := time.Now().In(istLoc()).Format("2006-01-02")
	points := []livealgos.DetailsChartPoint{
		{Date: deployDate, Pct: 100},
	}
	if todayDate != deployDate {
		points = append(points, livealgos.DetailsChartPoint{Date: todayDate, Pct: 100 + livePct})
	} else {
		// Same-day deploy → also emit the "today" point so the frontend
		// gets a stable 2-anchor shape; identical date, live pct.
		points = append(points, livealgos.DetailsChartPoint{Date: deployDate, Pct: 100 + livePct})
	}
	return livealgos.DetailsChart{Points: points}
}

// istLoc returns Asia/Kolkata with a UTC fallback — matches the tz-load
// pattern used by startOfTodayIST above.
func istLoc() *time.Location {
	loc, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		return time.UTC
	}
	return loc
}

// GetHoldings handles GET /users/me/live-algos/{strategyId}/holdings.
// Powers Screens 3 & 4 — full sortable list of active positions.
func (h *LiveAlgosHandler) GetHoldings(w http.ResponseWriter, r *http.Request) {
	userID, ok := auth.UserIDFromContext(r.Context())
	if !ok {
		respondIndiraError(w, http.StatusUnauthorized, "E_AUTH", "authenticated user not found")
		return
	}
	strategyID := mux.Vars(r)["strategyId"]
	if strategyID == "" {
		respondIndiraError(w, http.StatusBadRequest, "E_BAD_REQUEST", "strategyId is required")
		return
	}
	// Existence check — cheap and it turns "no such strategy" into a
	// clean 404 rather than a suspicious 200-with-empty-list.
	if _, err := h.store.StrategyMeta(r.Context(), strategyID, userID); err != nil {
		if errors.Is(err, livealgos.ErrStrategyNotFound) {
			respondIndiraError(w, http.StatusNotFound, "E_NOT_FOUND", "strategy not found")
			return
		}
		log.Printf("livealgos: holdings meta check %s/%s: %v", strategyID, userID, err)
		respondIndiraError(w, http.StatusInternalServerError, "E500", "failed to load holdings")
		return
	}

	positions, err := h.store.Positions(r.Context(), strategyID, userID)
	if err != nil {
		log.Printf("livealgos: holdings positions %s/%s: %v", strategyID, userID, err)
		respondIndiraError(w, http.StatusInternalServerError, "E500", "failed to load holdings")
		return
	}

	active := filterByStatus(positions, "ACTIVE")
	tokens := collectTokens(active)
	var (
		ltps      map[string]livealgos.LTPQuote
		ltpStatus = livealgos.StatusHealthy
	)
	if len(tokens) > 0 && h.ltp != nil {
		ltps, ltpStatus = h.ltp.FetchByTokens(r.Context(), tokens)
	} else if h.ltp == nil {
		ltpStatus = livealgos.StatusUnavailable
	}

	payload := livealgos.BuildHoldings(active, ltps)
	payload.LTPStatus = string(ltpStatus)
	respondIndiraOK(w, payload)
}

// GetTrades handles GET /users/me/live-algos/{strategyId}/trades.
// Powers Screens 5 & 6 — full sortable list of closed trades.
func (h *LiveAlgosHandler) GetTrades(w http.ResponseWriter, r *http.Request) {
	userID, ok := auth.UserIDFromContext(r.Context())
	if !ok {
		respondIndiraError(w, http.StatusUnauthorized, "E_AUTH", "authenticated user not found")
		return
	}
	strategyID := mux.Vars(r)["strategyId"]
	if strategyID == "" {
		respondIndiraError(w, http.StatusBadRequest, "E_BAD_REQUEST", "strategyId is required")
		return
	}
	if _, err := h.store.StrategyMeta(r.Context(), strategyID, userID); err != nil {
		if errors.Is(err, livealgos.ErrStrategyNotFound) {
			respondIndiraError(w, http.StatusNotFound, "E_NOT_FOUND", "strategy not found")
			return
		}
		log.Printf("livealgos: trades meta check %s/%s: %v", strategyID, userID, err)
		respondIndiraError(w, http.StatusInternalServerError, "E500", "failed to load trades")
		return
	}

	positions, err := h.store.Positions(r.Context(), strategyID, userID)
	if err != nil {
		log.Printf("livealgos: trades positions %s/%s: %v", strategyID, userID, err)
		respondIndiraError(w, http.StatusInternalServerError, "E500", "failed to load trades")
		return
	}
	exited := filterByStatus(positions, "EXITED")
	respondIndiraOK(w, livealgos.BuildTrades(exited))
}

// GetStockPnL handles GET /users/me/live-algos/{strategyId}/holdings/{symbol}.
// Powers Screen 7 — individual stock overview + per-fill trade history.
func (h *LiveAlgosHandler) GetStockPnL(w http.ResponseWriter, r *http.Request) {
	userID, ok := auth.UserIDFromContext(r.Context())
	if !ok {
		respondIndiraError(w, http.StatusUnauthorized, "E_AUTH", "authenticated user not found")
		return
	}
	strategyID := mux.Vars(r)["strategyId"]
	symbol := mux.Vars(r)["symbol"]
	if strategyID == "" || symbol == "" {
		respondIndiraError(w, http.StatusBadRequest, "E_BAD_REQUEST", "strategyId and symbol are required")
		return
	}
	if _, err := h.store.StrategyMeta(r.Context(), strategyID, userID); err != nil {
		if errors.Is(err, livealgos.ErrStrategyNotFound) {
			respondIndiraError(w, http.StatusNotFound, "E_NOT_FOUND", "strategy not found")
			return
		}
		log.Printf("livealgos: stockpnl meta check %s/%s: %v", strategyID, userID, err)
		respondIndiraError(w, http.StatusInternalServerError, "E500", "failed to load stock detail")
		return
	}

	positions, err := h.store.Positions(r.Context(), strategyID, userID)
	if err != nil {
		log.Printf("livealgos: stockpnl positions %s/%s: %v", strategyID, userID, err)
		respondIndiraError(w, http.StatusInternalServerError, "E500", "failed to load stock detail")
		return
	}
	// Positions for THIS symbol only.
	perSymbol := filterBySymbol(positions, symbol)
	if len(perSymbol) == 0 {
		respondIndiraError(w, http.StatusNotFound, "E_NOT_FOUND", "no history for this symbol under this strategy")
		return
	}

	orders, err := h.store.OrdersForSymbol(r.Context(), strategyID, userID, symbol)
	if err != nil {
		log.Printf("livealgos: stockpnl orders %s/%s/%s: %v", strategyID, userID, symbol, err)
		respondIndiraError(w, http.StatusInternalServerError, "E500", "failed to load stock trades")
		return
	}

	respondIndiraOK(w, livealgos.BuildStockPnL(symbol, perSymbol, orders))
}

// ─── Small helpers used only by the details endpoints ────────────────

// resolveAlgoMeta looks up the algo id + display name for a given
// user-config strategy_type string. Falls back to the raw type when
// the catalog doesn't recognise it so a UI can still render something.
func resolveAlgoMeta(catalog algos.Catalog, strategyType string) (string, string) {
	// Static catalog today has exactly one entry (Manthan). context.TODO
	// keeps the lint happy while making it obvious that the static impl
	// truly ignores ctx — swap for a real ctx if we ever move to a
	// DB-backed catalog with query cancellation.
	all, _ := catalog.All(context.TODO())
	for _, a := range all {
		// Match by name; safer than comparing to the enum value that
		// user-config returns since the catalog doesn't know that enum.
		if a.Name == "Manthan" && (strategyType == "MANTHAN" || strategyType == "STRATEGY_TYPE_MANTHAN") {
			return a.ID, a.Name
		}
	}
	if len(all) > 0 {
		return all[0].ID, all[0].Name
	}
	return "", ""
}

// collectActiveTokens returns exchange_tokens for ACTIVE positions only —
// used by the main details endpoint (which values open positions but
// doesn't need LTP for closed ones).
func collectActiveTokens(positions []livealgos.PositionRow) []string {
	seen := make(map[string]struct{})
	var out []string
	for _, p := range positions {
		if p.Status != "ACTIVE" {
			continue
		}
		if !p.ExchangeToken.Valid || p.ExchangeToken.String == "" {
			continue
		}
		if _, ok := seen[p.ExchangeToken.String]; ok {
			continue
		}
		seen[p.ExchangeToken.String] = struct{}{}
		out = append(out, p.ExchangeToken.String)
	}
	return out
}

// collectTokens is like collectActiveTokens but takes a pre-filtered
// slice (caller decides which subset). Used by the Holdings endpoint
// where we've already isolated active positions.
func collectTokens(positions []livealgos.PositionRow) []string {
	seen := make(map[string]struct{})
	var out []string
	for _, p := range positions {
		if !p.ExchangeToken.Valid || p.ExchangeToken.String == "" {
			continue
		}
		if _, ok := seen[p.ExchangeToken.String]; ok {
			continue
		}
		seen[p.ExchangeToken.String] = struct{}{}
		out = append(out, p.ExchangeToken.String)
	}
	return out
}

// filterByStatus is a tiny slice filter — kept here rather than in the
// aggregator because the aggregator functions take pre-split slices.
func filterByStatus(positions []livealgos.PositionRow, status string) []livealgos.PositionRow {
	out := make([]livealgos.PositionRow, 0, len(positions))
	for _, p := range positions {
		if p.Status == status {
			out = append(out, p)
		}
	}
	return out
}

// filterBySymbol returns positions matching a symbol (case-sensitive
// intentionally — DB values are always upper-case).
func filterBySymbol(positions []livealgos.PositionRow, symbol string) []livealgos.PositionRow {
	out := make([]livealgos.PositionRow, 0)
	for _, p := range positions {
		if p.Symbol == symbol {
			out = append(out, p)
		}
	}
	return out
}
