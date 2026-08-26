package main

import (
	"context"
	"database/sql"
	"log"
	"sync"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/algos"
	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/performance"
)

// algoStatsTTL bounds how stale the Explore/detail headline figures can be.
// algo_performance_daily is refreshed once a day by the evening sheet sync,
// so a few minutes of caching costs nothing in freshness and keeps the list
// endpoint off Postgres on every request.
const algoStatsTTL = 5 * time.Minute

// newAlgoStatsProvider wires the algo catalog to REAL performance data:
//
//	algo_performance_daily → performance.ComputeAlgoStats → catalog overlay
//	(primaryReturn windows, maxDrawdown, sortino)
//
// Added 2026-08-11: the catalog previously served hardcoded constants
// ("3Y Return" 28.4 / "2Y Return" 32.9 / maxDrawdown −12.6) that no data
// supported — the reference client had 1.49 years of history and a true
// drawdown of −5.68%. Returns (zero, false) whenever the figures cannot be
// computed, in which case the catalog keeps its baked-in defaults rather
// than rendering zeros.
// tradeDB (optional) adds trade-level metrics from positions_db closed lots;
// they only go live past performance.MinTradesForStats (honesty threshold).
func newAlgoStatsProvider(store performance.Store, clientMap map[string]string, ttl time.Duration, tradeDB *sql.DB) algos.StatsProvider {
	type entry struct {
		val algos.LiveStats
		ok  bool
		at  time.Time
	}
	var mu sync.Mutex
	cache := make(map[string]entry)

	return func(ctx context.Context, algoID string) (algos.LiveStats, bool) {
		// Defensive: a caller passing a nil ctx must degrade to catalog
		// defaults, never panic the whole request (2026-08-26 incident:
		// /users/me/live-algos 500'd on every call via WithTimeout(nil)).
		if ctx == nil {
			ctx = context.Background()
		}
		mu.Lock()
		if e, hit := cache[algoID]; hit && time.Since(e.at) < ttl {
			mu.Unlock()
			return e.val, e.ok
		}
		mu.Unlock()

		refID := clientMap[algoID]
		if store == nil || refID == "" {
			return algos.LiveStats{}, false
		}

		qctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		rows, err := store.FetchDaily(qctx, algoID, refID)

		var (
			val algos.LiveStats
			ok  bool
		)
		switch {
		case err != nil:
			log.Printf("algos: live stats for %s unavailable (perf query: %v) — serving catalog defaults", algoID, err)
		case len(rows) == 0:
			log.Printf("algos: live stats for %s unavailable (no rows for ref=%s) — serving catalog defaults", algoID, refID)
		default:
			st := performance.ComputeAlgoStats(rows)
			if st.Available {
				val = algos.LiveStats{
					PrimaryReturn:  st.PrimaryReturn,
					MaxDrawdownPct: st.MaxDrawdownPct,
					SortinoRatio:   st.SortinoRatio,
					SharpeRatio:    st.SharpeRatio,
					TotalReturnPct: st.TotalReturnPct,
					CAGRPct:        st.CAGRPct,
				}
				// Trade-level metrics from live closed lots (best-effort: a
				// failed query never blocks the series stats).
				if ts, terr := performance.FetchTradeStats(ctx, tradeDB); terr != nil {
					log.Printf("algos: trade stats unavailable (%v) — keeping track-record key stats", terr)
				} else if ts.Available {
					val.TradeStatsLive = true
					val.WinRatePct = ts.WinRatePct
					val.ProfitFactor = ts.ProfitFactor
					val.TotalTrades = ts.TotalTrades
					val.AvgHoldingDays = ts.AvgHoldingDays
				} else if ts.TotalTrades > 0 {
					log.Printf("algos: %d live closed lots (<%d) — key stats stay track-record until the sample is meaningful", ts.TotalTrades, performance.MinTradesForStats)
				}
				ok = true
			}
		}

		// Cache successes AND failures: a transient DB problem shouldn't turn
		// every card render into a fresh query storm.
		mu.Lock()
		cache[algoID] = entry{val: val, ok: ok, at: time.Now()}
		mu.Unlock()
		return val, ok
	}
}
