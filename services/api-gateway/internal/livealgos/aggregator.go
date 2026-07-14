package livealgos

import (
	pb "github.com/RohitIndira/Algo-Treading/api/proto/user_config"
	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/algos"
)

// Build assembles the Response payload from the two inputs the handler
// gathers: the list of user-config strategies (from gRPC) and the algo
// metadata catalog (static, in-memory).
//
// Behavior notes:
//
//   - Strategies whose type is NOT one the catalog knows about get
//     skipped entirely — that means if user-config returns a
//     WEEK52_BREAKOUT strategy the mobile app has no card for yet, it
//     doesn't appear on this screen (no "unknown algo" placeholder).
//     Same-safe fallback: extend the catalog when we add strategy types.
//
//   - Strategy status is derived from active + trading_mode:
//       active=true, trading_mode is a LIVE mode   → LIVE
//       active=true, trading_mode is PAPER         → PAUSED (paper is
//                                                    "training-wheels"
//                                                    for the strategy)
//       active=false                                → STOPPED
//     Notes for the future: user-config may add an explicit `paused`
//     flag later; this function's contract stays the same, only the
//     switch changes.
//
//   - Phase 1: all P&L numbers return with pnlPending=true and
//     amount=0/percent=0. Frontend renders a spinner over that region.
//     Phase 2 will replace these with real reads from
//     trading_db.manthan_positions + LTP feed.
//
//   - The Summary tile aggregates over ALL returned rows. If we only
//     have real deployedCapital in Phase 1, summary.totalDeployedCapital
//     is real, but summary.NetPnL and summary.TodayPnL are also
//     pnlPending=true (same story as the per-algo tiles).
//
// A user with zero deployments returns Summary{}+empty Algos slice —
// the frontend then shows an empty-state card ("Deploy an algo to see
// it here").
func Build(strategies []*pb.Strategy, catalog algos.Catalog) Response {
	rows := make([]AlgoRow, 0, len(strategies))
	var totalDeployedCapital int64

	// Only Manthan today. Cheap lookup by iterating the catalog once
	// and putting it in a map keyed on the algo id. Kept local so we
	// re-fetch on every call — it's a static in-memory catalog with
	// ~1 entry, no need to cache further.
	all, _ := catalog.All(nil) //nolint:staticcheck  // static catalog ignores ctx
	byID := make(map[string]algos.Algo, len(all))
	for _, a := range all {
		byID[a.ID] = a
	}

	for _, s := range strategies {
		if s == nil {
			continue
		}
		// Map strategy_type → static algo id. Today the only entry is
		// MANTHAN → algo_manthan_v1. When we add more, extend here.
		algoID := algoIDFromStrategyType(s.StrategyType)
		if algoID == "" {
			continue // unknown strategy type — skip
		}
		meta, ok := byID[algoID]
		if !ok {
			continue // catalog doesn't know this algo — skip rather than render broken
		}

		row := AlgoRow{
			ID:              meta.ID,
			Name:            meta.Name,
			Type:            meta.Type,
			Style:           meta.Style,
			Logo:            meta.Logo,
			StrategyID:      s.StrategyId,
			Status:          statusFrom(s),
			DeployedCapital: deployedCapitalFrom(s),

			// Phase 1 placeholders — see file docstring.
			NetPnL:        PnL{PnLPending: true},
			TodayPnL:      PnL{PnLPending: true},
			WinRatePct:    0,
			OpenPositions: 0,
		}
		totalDeployedCapital += row.DeployedCapital
		rows = append(rows, row)
	}

	return Response{
		Summary: Summary{
			// Aggregate P&L is Phase 2 — same pending flag until real
			// numbers are wired in.
			NetPnL:               PnL{PnLPending: true},
			TodayPnL:             PnL{PnLPending: true},
			TotalDeployedCapital: totalDeployedCapital,
		},
		Algos: rows,
	}
}

// algoIDFromStrategyType maps a user-config strategy_type enum value
// to the STATIC algo id that identifies its card on the Explore screen.
//
// If a new strategy type is added on the user-config side and this
// switch doesn't know it, the row is silently dropped from the Live
// Algos response (see Build). That's intentional — better to hide a
// strategy the mobile app can't render than to show a broken card.
func algoIDFromStrategyType(t pb.StrategyType) string {
	switch t {
	case pb.StrategyType_MANTHAN:
		return "algo_manthan_v1"
	// Future:
	// case pb.StrategyType_WEEK52_BREAKOUT: return "algo_52w_v1"
	default:
		return ""
	}
}

// statusFrom derives the tri-state Status enum from user-config's
// Strategy fields. See the AlgoRow doc for the mapping.
//
// Mapping (matches the Pause/Stop mockup):
//
//	active=true,  trading_mode=LIVE   → LIVE
//	active=true,  trading_mode=PAPER  → PAUSED  (paper is "training-wheels")
//	active=false                       → PAUSED  (user hit Pause on the modal)
//
// STOPPED is NEVER returned here — all read queries in
// user-config/internal/repository filter `deleted_at IS NULL`, so a
// Stop (soft-delete) makes the row invisible to this endpoint entirely.
// If a strategy_id isn't in the response, treat as STOPPED at the UI layer.
func statusFrom(s *pb.Strategy) string {
	if !s.Active {
		return StatusPaused
	}
	if s.TradingMode == pb.TradingMode_PAPER {
		return StatusPaused
	}
	return StatusLive
}

// deployedCapitalFrom pulls total_capital out of trade_config, safely
// tolerating a nil trade_config (should never happen for Manthan but
// costs nothing to guard).
func deployedCapitalFrom(s *pb.Strategy) int64 {
	if s.TradeConfig == nil {
		return 0
	}
	return int64(s.TradeConfig.TotalCapital)
}
