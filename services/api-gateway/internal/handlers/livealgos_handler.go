package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"strings"
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
	// navStore (optional) serves the TRUE per-deployment curve; when nil or
	// empty the chart falls back to the reference series rebased at deploy.
	navStore *livealgos.NAVStore
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
	navStore *livealgos.NAVStore,
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
		navStore:      navStore,
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
	payload := livealgos.Build(r.Context(), resp.Strategies, h.catalog, metrics)

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
	payload.Chart = h.chartForStrategy(r.Context(), strategyID, algoID, meta.CreatedAt, payload.NetPnL.Percent)

	// ── True production metrics (2026-08-20) ──
	// TotalReturn: the header's NetPnL.Percent (realized + unrealized on
	// deployed capital). computeMetrics's realized-only figure contradicted
	// the header on the same screen — a single stop-out showed a negative
	// "total return" next to a positive net P&L.
	payload.Metrics.TotalReturnPct = payload.NetPnL.Percent
	// CAGR / MaxDrawdown / Sharpe: computed from the SAME curve the page's
	// chart renders (the algo's real daily series rebased at this user's
	// deployment date, live point included) so the tiles always match the
	// chart. Honesty gates inside — fields stay 0 until the deployment's
	// data genuinely supports them.
	dd, sharpe, cagr := metricsFromChart(payload.Chart)
	payload.Metrics.MaxDrawdownPct = dd
	payload.Metrics.SharpeRatio = sharpe
	payload.Metrics.CAGRPct = cagr
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
// chartForStrategy prefers the TRUE deployment curve (strategy_nav_daily —
// the user's own recorded NAV) and only falls back to the reference series
// rebased at the deploy date while no snapshots exist yet (pre-2026-08-20
// deployments accrue their true history from the day the snapshot job
// started; per-symbol daily closes don't exist anywhere, so the past cannot
// be reconstructed honestly). Metrics tiles derive from whatever curve is
// returned, so they always match the chart.
func (h *LiveAlgosHandler) chartForStrategy(ctx context.Context, strategyID, algoID string, deployedAt time.Time, livePct float64) livealgos.DetailsChart {
	if h.navStore != nil && strategyID != "" {
		if snaps, err := h.navStore.Series(ctx, strategyID); err != nil {
			log.Printf("livealgos: NAV series %s: %v — falling back to reference curve", strategyID, err)
		} else if len(snaps) > 0 {
			return trueCurve(snaps, deployedAt, livePct, time.Now().In(istLoc()))
		}
	}
	return h.chartFor(ctx, algoID, deployedAt, livePct)
}

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
					points := make([]livealgos.DetailsChartPoint, 0, len(sinceDeploy)+1)
					for _, row := range sinceDeploy {
						points = append(points, livealgos.DetailsChartPoint{
							Date: row.Date.Format("2006-01-02"),
							Pct:  100 + (row.ReturnPct - baseline),
						})
					}
					// Extend to TODAY with the strategy's live P&L whenever the
					// nightly ETL hasn't caught up yet. Two jobs:
					//   1. A strategy deployed yesterday has exactly ONE perf
					//      row — a single dot renders as "no equity curve" in
					//      the app (2026-08-05: chart was [{08-04, 100}]).
					//   2. Every chart stays current intraday instead of ending
					//      at the last ETL row. Once the evening sync lands
					//      today's row, the perf source wins and this no-ops.
					// livePct (NetPnL.Percent) is % since deployment — the same
					// baseline the rebased perf points use, so 100+livePct is
					// consistent with the curve.
					todayDate := time.Now().In(istLoc()).Format("2006-01-02")
					if points[len(points)-1].Date < todayDate {
						points = append(points, livealgos.DetailsChartPoint{
							Date: todayDate,
							Pct:  100 + livePct,
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
// trueCurve builds the chart from recorded NAV snapshots: a 100 anchor at
// the deploy date (when it precedes the first snapshot), one point per
// settled day (100 + that day's net P&L %), and today's LIVE value replacing
// or appending the final point so the curve always ends at what the header
// shows right now.
func trueCurve(snaps []livealgos.NAVPoint, deployedAt time.Time, livePct float64, now time.Time) livealgos.DetailsChart {
	today := now.Format("2006-01-02")
	pts := make([]livealgos.DetailsChartPoint, 0, len(snaps)+2)
	deployDate := deployedAt.Format("2006-01-02")
	if len(snaps) > 0 && deployDate < snaps[0].Date.Format("2006-01-02") {
		pts = append(pts, livealgos.DetailsChartPoint{Date: deployDate, Pct: 100})
	}
	for _, sn := range snaps {
		d := sn.Date.Format("2006-01-02")
		if d == today {
			continue // replaced by the live point below
		}
		pts = append(pts, livealgos.DetailsChartPoint{Date: d, Pct: 100 + sn.NetPnLPct})
	}
	pts = append(pts, livealgos.DetailsChartPoint{Date: today, Pct: 100 + livePct})
	return livealgos.DetailsChart{Points: pts}
}

// metricsFromChart derives MaxDrawdown / Sharpe / CAGR from the details
// chart's equity points (Pct is an equity index: 100 = deployed capital).
// The chart is the deployment's truth on this page, so metrics computed from
// it can never disagree with what the user sees.
//
// Honesty gates (same discipline as internal/performance):
//   - MaxDrawdown: ≥2 points (a single point has no drawdown);
//   - Sharpe:      ≥11 points (≥10 daily returns) — annualised, rf=0,
//     0 on zero volatility, never NaN/Inf;
//   - CAGR:        span ≥365 days — never annualise a shorter deployment.
func metricsFromChart(chart livealgos.DetailsChart) (maxDD, sharpe, cagr float64) {
	pts := chart.Points
	if len(pts) < 2 {
		return 0, 0, 0
	}
	// Max drawdown over the equity curve.
	peak := math.Inf(-1)
	for _, p := range pts {
		if p.Pct > peak {
			peak = p.Pct
		}
		if peak > 0 {
			if dd := (p.Pct - peak) / peak * 100; dd < maxDD {
				maxDD = dd
			}
		}
	}
	maxDD = round2Pct(maxDD)
	// Sharpe from daily equity returns.
	if len(pts) >= 11 {
		daily := make([]float64, 0, len(pts)-1)
		for i := 1; i < len(pts); i++ {
			if pts[i-1].Pct > 0 {
				daily = append(daily, pts[i].Pct/pts[i-1].Pct-1)
			}
		}
		if len(daily) >= 10 {
			var sum float64
			for _, r := range daily {
				sum += r
			}
			mean := sum / float64(len(daily))
			var varSum float64
			for _, r := range daily {
				varSum += (r - mean) * (r - mean)
			}
			if std := math.Sqrt(varSum / float64(len(daily))); std > 0 {
				sharpe = round2Pct(mean / std * math.Sqrt(252))
			}
		}
	}
	// CAGR only once the deployment spans a year.
	firstT, err1 := time.Parse("2006-01-02", pts[0].Date)
	lastT, err2 := time.Parse("2006-01-02", pts[len(pts)-1].Date)
	if err1 == nil && err2 == nil {
		if years := lastT.Sub(firstT).Hours() / 24 / 365.25; years >= 1 {
			if pts[0].Pct > 0 && pts[len(pts)-1].Pct > 0 {
				cagr = round2Pct((math.Pow(pts[len(pts)-1].Pct/pts[0].Pct, 1/years) - 1) * 100)
			}
		}
	}
	return maxDD, sharpe, cagr
}

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

// TimelineEvent is one card in the mobile "Algo Timeline" screen. Server
// formats the title/subtitle + IST timestamp so the app renders directly.
type TimelineEvent struct {
	Type     string `json:"type"`     // machine kind (DEPLOYED, ORDER_FILLED, …)
	Icon     string `json:"icon"`     // deployed|paused|resumed|capital_up|capital_down|deleted|order|sl|closed
	Title    string `json:"title"`    // "Algo deployed"
	Subtitle string `json:"subtitle"` // human sentence
	TS       string `json:"ts"`       // IST ISO-8601, e.g. 2026-08-07T09:00:00+05:30
	TSMillis int64  `json:"tsMillis"` // epoch millis for client-side sorting
}

// GetTimeline handles GET /users/me/live-algos/{strategyId}/timeline —
// the lifecycle + order activity feed. Newest first, IST timestamps.
func (h *LiveAlgosHandler) GetTimeline(w http.ResponseWriter, r *http.Request) {
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
	// Ownership check (404 on cross-user UUID guessing, same as siblings).
	if _, err := h.store.StrategyMeta(r.Context(), strategyID, userID); err != nil {
		if errors.Is(err, livealgos.ErrStrategyNotFound) {
			respondIndiraError(w, http.StatusNotFound, "E_NOT_FOUND", "strategy not found")
			return
		}
		log.Printf("livealgos: timeline meta %s/%s: %v", strategyID, userID, err)
		respondIndiraError(w, http.StatusInternalServerError, "E500", "failed to load timeline")
		return
	}

	rows, err := h.store.Timeline(r.Context(), strategyID, userID)
	if err != nil {
		log.Printf("livealgos: timeline %s/%s: %v", strategyID, userID, err)
		respondIndiraError(w, http.StatusInternalServerError, "E500", "failed to load timeline")
		return
	}

	ist := istLoc()
	events := make([]TimelineEvent, 0, len(rows))
	for _, row := range rows {
		icon, title, subtitle := formatTimeline(row)
		t := row.At.In(ist)
		events = append(events, TimelineEvent{
			Type:     row.Kind,
			Icon:     icon,
			Title:    title,
			Subtitle: subtitle,
			TS:       t.Format("2006-01-02T15:04:05-07:00"),
			TSMillis: row.At.UnixMilli(),
		})
	}
	respondIndiraOK(w, map[string]any{"events": events})
}

// formatTimeline maps a raw row to (icon, title, subtitle). Kept as a pure
// function so it's unit-testable without a DB.
func formatTimeline(row livealgos.TimelineRow) (icon, title, subtitle string) {
	num := func(key string) float64 {
		if row.DetailsJS == "" {
			return 0
		}
		var m map[string]float64
		_ = json.Unmarshal([]byte(row.DetailsJS), &m)
		return m[key]
	}
	switch row.Kind {
	case "DEPLOYED":
		return "deployed", "Algo deployed",
			fmt.Sprintf("Algo was made live with capital deployment of %s", rupees(num("capital")))
	case "PAUSED":
		return "paused", "Paused", "You paused the algo. It won't place any new entries."
	case "RESUMED":
		return "resumed", "Resumed", "You resumed the algo. It's scanning for entries again."
	case "CAPITAL_INCREASED":
		return "capital_up", "Capital increased",
			fmt.Sprintf("Deployed capital increased from %s to %s", rupees(num("from")), rupees(num("to")))
	case "CAPITAL_DECREASED":
		return "capital_down", "Capital decreased",
			fmt.Sprintf("Deployed capital decreased from %s to %s", rupees(num("from")), rupees(num("to")))
	case "DELETED":
		return "deleted", "Algo removed", "You removed this algo deployment."
	case "SL_ARMED":
		return "sl", "Stop-loss set",
			fmt.Sprintf("Protective SL armed for %s at %s", row.Symbol, rupees(row.Price))
	case "POSITION_CLOSED":
		return "closed", "Position closed",
			fmt.Sprintf("Sold %d %s at %s", row.Qty, row.Symbol, rupees(row.Price))
	case "ORDER_FILLED":
		return "order", "Order filled",
			fmt.Sprintf("Bought %d %s at %s", row.Qty, row.Symbol, rupees(row.Price))
	default:
		return "order", row.Kind, ""
	}
}

// rupees formats an amount as ₹1,00,000 (Indian grouping, no paise).
func rupees(v float64) string {
	neg := v < 0
	if neg {
		v = -v
	}
	n := int64(v + 0.5) // round magnitude, then re-apply sign — never round toward zero on negatives
	s := fmt.Sprintf("%d", n)
	// Indian grouping: last 3 digits, then pairs.
	if len(s) > 3 {
		head, tail := s[:len(s)-3], s[len(s)-3:]
		var parts []string
		for len(head) > 2 {
			parts = append([]string{head[len(head)-2:]}, parts...)
			head = head[:len(head)-2]
		}
		if head != "" {
			parts = append([]string{head}, parts...)
		}
		s = strings.Join(parts, ",") + "," + tail
	}
	if neg {
		return "-₹" + s
	}
	return "₹" + s
}
