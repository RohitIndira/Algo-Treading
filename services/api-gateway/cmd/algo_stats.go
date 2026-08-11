package main

import (
	"context"
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
func newAlgoStatsProvider(store performance.Store, clientMap map[string]string, ttl time.Duration) algos.StatsProvider {
	type entry struct {
		val algos.LiveStats
		ok  bool
		at  time.Time
	}
	var mu sync.Mutex
	cache := make(map[string]entry)

	return func(ctx context.Context, algoID string) (algos.LiveStats, bool) {
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
