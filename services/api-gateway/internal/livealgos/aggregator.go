package livealgos

import (
	pb "github.com/RohitIndira/Algo-Treading/api/proto/user_config"
	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/algos"
)

// StrategyMetrics carries the live per-strategy numbers computed by the
// handler from positions_db + LTP. `Real` is false when the handler
// couldn't produce numbers for a given strategy (positions_db unreachable
// or LTP feed unavailable); Build then falls back to pnlPending=true so
// the UI shows a spinner instead of a misleading 0.
type StrategyMetrics struct {
	Real          bool
	NetPnL        int64
	NetPct        float64
	TodayPnL      int64
	TodayPct      float64
	OpenPositions int32
	WinRatePct    float64
}

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
//   - Strategy status is derived from stopped_at + active + trading_mode:
//       stopped_at != nil                          → STOPPED (terminal)
//       active=true, trading_mode is a LIVE mode   → LIVE
//       active=true, trading_mode is PAPER         → PAUSED (paper is
//                                                    "training-wheels"
//                                                    for the strategy)
//       active=false                                → PAUSED (user hit
//                                                    Pause on the modal)
//
//   - P&L numbers come from `metricsByStrategy` keyed on strategy_id.
//     When a strategy has no entry (positions_db unreachable at request
//     time) OR entry.Real=false, the row falls back to pnlPending=true so
//     the UI spins instead of showing a misleading zero.
//
//   - Summary aggregates over strategies whose metrics were real. If ANY
//     strategy came back as pending, the summary tile also renders
//     pnlPending=true — the aggregate is only meaningful when we have a
//     number for every row.
//
// A user with zero deployments returns Summary{}+empty Algos slice —
// the frontend then shows an empty-state card ("Deploy an algo to see
// it here").
func Build(
	strategies []*pb.Strategy,
	catalog algos.Catalog,
	metricsByStrategy map[string]StrategyMetrics,
) Response {
	rows := make([]AlgoRow, 0, len(strategies))
	var totalDeployedCapital int64
	var sumNet, sumToday int64
	allReal := true
	haveAny := false

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
		}

		if m, ok := metricsByStrategy[s.StrategyId]; ok && m.Real {
			row.NetPnL = PnL{Amount: m.NetPnL, Percent: m.NetPct}
			row.TodayPnL = PnL{Amount: m.TodayPnL, Percent: m.TodayPct}
			row.WinRatePct = m.WinRatePct
			row.OpenPositions = m.OpenPositions
			sumNet += m.NetPnL
			sumToday += m.TodayPnL
			haveAny = true
		} else {
			row.NetPnL = PnL{PnLPending: true}
			row.TodayPnL = PnL{PnLPending: true}
			allReal = false
		}

		totalDeployedCapital += row.DeployedCapital
		rows = append(rows, row)
	}

	summary := Summary{TotalDeployedCapital: totalDeployedCapital}
	if haveAny && allReal {
		var netPct, todayPct float64
		if totalDeployedCapital > 0 {
			netPct = (float64(sumNet) / float64(totalDeployedCapital)) * 100
			todayPct = (float64(sumToday) / float64(totalDeployedCapital)) * 100
		}
		summary.NetPnL = PnL{Amount: sumNet, Percent: round2(netPct)}
		summary.TodayPnL = PnL{Amount: sumToday, Percent: round2(todayPct)}
	} else {
		summary.NetPnL = PnL{PnLPending: true}
		summary.TodayPnL = PnL{PnLPending: true}
	}
	return Response{Summary: summary, Algos: rows}
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
//	stopped_at != nil                  → STOPPED (terminal; user
//	                                              redeploys to run again)
//	active=true,  trading_mode=LIVE    → LIVE
//	active=true,  trading_mode=PAPER   → PAUSED  (paper is "training-wheels")
//	active=false                        → PAUSED  (user hit Pause on the modal)
//
// STOP is now a status transition, not a soft-delete — the row keeps
// showing in the list with status=STOPPED so the user can see history.
// See services/user-config/migrations/015_add_stopped_at.sql.
func statusFrom(s *pb.Strategy) string {
	if s.StoppedAt != nil {
		return StatusStopped
	}
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
