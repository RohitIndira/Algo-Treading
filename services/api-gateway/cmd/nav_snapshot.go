package main

// NAV snapshot job — records each live deployment's TRUE daily NAV into
// stockk_market.strategy_nav_daily using the exact math the details page
// serves. Runs at boot and every 30 minutes; per-day upsert means intraday
// runs refresh today's point and the last run after close settles it.
// See migrations/001_strategy_nav_daily.sql.

import (
	"context"
	"log"
	"time"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"

	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/livealgos"
)

const navSnapshotInterval = 30 * time.Minute

func startNAVSnapshots(ctx context.Context, store *livealgos.PostgresStore, ltp *livealgos.LTPStore, nav *livealgos.NAVStore) {
	if store == nil || nav == nil {
		log.Printf("navsnap: store/navStore unavailable — true deployment curve will not accrue")
		return
	}
	run := func() {
		// Only trading days produce settled NAV points — weekend/holiday
		// writes would add flat filler dates to the curve.
		if !indiraClient.IsTradingDay(time.Now().In(istLocMain())) {
			return
		}
		rctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()
		deps, err := store.ListActiveDeployments(rctx)
		if err != nil {
			log.Printf("navsnap: list deployments: %v", err)
			return
		}
		today := time.Now().In(istLocMain()).Truncate(24 * time.Hour)
		wrote := 0
		for _, d := range deps {
			positions, err := store.Positions(rctx, d.StrategyID, d.UserID)
			if err != nil {
				log.Printf("navsnap: positions %s/%s: %v", d.StrategyID, d.UserID, err)
				continue
			}
			var tokens []string
			seen := map[string]bool{}
			for _, p := range positions {
				if p.Status == "ACTIVE" && p.ExchangeToken.Valid && p.ExchangeToken.String != "" && !seen[p.ExchangeToken.String] {
					seen[p.ExchangeToken.String] = true
					tokens = append(tokens, p.ExchangeToken.String)
				}
			}
			var ltps map[string]livealgos.LTPQuote
			if len(tokens) > 0 && ltp != nil {
				ltps, _ = ltp.FetchByTokens(rctx, tokens)
			}
			row := livealgos.ComputeNAVRow(d, positions, ltps, today)
			if err := nav.Upsert(rctx, row); err != nil {
				log.Printf("navsnap: upsert %s: %v", d.StrategyID, err)
				continue
			}
			wrote++
		}
		if wrote > 0 {
			log.Printf("navsnap: %d deployment NAV snapshot(s) recorded for %s", wrote, today.Format("2006-01-02"))
		}
	}
	go func() {
		run()
		t := time.NewTicker(navSnapshotInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				run()
			}
		}
	}()
}

func istLocMain() *time.Location {
	loc, err := time.LoadLocation("Asia/Kolkata")
	if err != nil || loc == nil {
		return time.FixedZone("IST", 5*60*60+30*60)
	}
	return loc
}
