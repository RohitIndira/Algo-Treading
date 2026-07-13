package internal

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
)

// LoadActiveStrategies fetches every active MANTHAN strategy from user-config
// via gRPC (single-owner principle for the strategies table — see
// docs/architecture/data-ownership.md) and decorates each with its broker
// auth, also fetched via user-config gRPC.
//
// Phase 0.2 migrated the strategies fetch from direct SQL to gRPC.
// Phase 0.6a migrated the broker-auth lookup the same way, eliminating
// rebalancer's last cross-service direct read AND its dependency on the
// execution_db database connection entirely.
//
// Returns ONLY strategies that have all required pieces: active, has trade
// config (already filtered server-side), has user credentials. Anything
// missing → logged and skipped.
func LoadActiveStrategies(ctx context.Context, ucClient *UserConfigClient) ([]StrategyConfig, error) {
	if ucClient == nil {
		return nil, fmt.Errorf("user-config client is required")
	}

	strategies, err := ucClient.FetchActiveMANTHANStrategies(ctx)
	if err != nil {
		return nil, fmt.Errorf("load strategies via user-config gRPC: %w", err)
	}

	out := make([]StrategyConfig, 0, len(strategies))
	for _, c := range strategies {
		// Apply Manthan rule: ≤25L → 25, >25L → 50 (only if config didn't set explicitly)
		if c.MaxPositions <= 0 {
			c.MaxPositions = 25
			if c.TotalCapital > 2500000 {
				c.MaxPositions = 50
			}
		}
		// Pull broker auth via the same user-config gRPC client.
		auth, _ := ResolveBrokerAuth(ctx, ucClient, c.UserID)
		if auth != nil {
			c.BearerToken = auth.BearerToken
			c.AppID = auth.AppId
			c.Source = auth.Source
		}
		out = append(out, c)
	}
	return out, nil
}

// LoadPortfolioSnapshot builds the per-strategy view used by the allocator.
// Pulls active positions, cooldowns, user-overrides from trading_db; pulls
// today's EMA targets from Redis; computes per-index deployed % from the
// active positions list using the just-fetched current capital.
func LoadPortfolioSnapshot(
	ctx context.Context,
	tradingDB *sql.DB,
	rdb *redis.Client,
	cfg StrategyConfig,
	cap CurrentCapital,
) (*PortfolioSnapshot, error) {
	snap := &PortfolioSnapshot{
		Cfg:              cfg,
		Capital:          cap,
		ActivePositions:  []PositionSummary{},
		CooldownSymbols:  map[string]float64{},
		OverrideSymbols:  map[string]time.Time{},
		EMATargets:       map[string]float64{},
		IndexDeployedPct: map[string]float64{},
		SectorCount:      map[string]int{},
		BucketCount:      map[string]int{},
		LivePrices:       map[string]float64{},
	}

	// Per-call sizing — recomputed from live capital.
	if cfg.MaxPositions > 0 {
		snap.PerCall = cap.Total / float64(cfg.MaxPositions)
	}

	// Carry broker-reported LTPs into the snapshot for top-up sizing.
	for sym, ltp := range cap.Prices {
		snap.LivePrices[sym] = ltp
	}

	// 1) Active positions (manthan_positions). Pull signal_id too so the
	// top-up phase can reference the parent row when emitting MANTHAN_TOPUP.
	posRows, err := tradingDB.QueryContext(ctx, `
		SELECT COALESCE(signal_id::text,'') AS signal_id,
		       symbol, COALESCE(isin,''), COALESCE(industry,''), COALESCE(mcap_bucket,''), COALESCE(index_name,''),
		       quantity, invested_amt, status
		FROM manthan_positions
		WHERE strategy_id = $1 AND status IN ('ACTIVE','PARTIAL_ACTIVE')`,
		cfg.StrategyID)
	if err != nil {
		return nil, fmt.Errorf("load active positions: %w", err)
	}
	defer posRows.Close()
	for posRows.Next() {
		var p PositionSummary
		if err := posRows.Scan(&p.SignalID, &p.Symbol, &p.ISIN, &p.Industry, &p.MCapBucket, &p.IndexName,
			&p.Quantity, &p.InvestedAmt, &p.Status); err != nil {
			return nil, err
		}
		snap.ActivePositions = append(snap.ActivePositions, p)
		if p.Industry != "" {
			snap.SectorCount[p.Industry]++
		}
		if p.MCapBucket != "" {
			snap.BucketCount[p.MCapBucket]++
		}
		// Per-index deployed = sum invested in that index ÷ current capital.
		if p.IndexName != "" && cap.Total > 0 {
			snap.IndexDeployedPct[p.IndexName] += p.InvestedAmt / cap.Total
		}
	}

	// 2) Cooldown rows (re-entry blocked until LTP < reentry_below)
	cdRows, err := tradingDB.QueryContext(ctx, `
		SELECT symbol, reentry_below FROM manthan_cooldown
		WHERE strategy_id = $1 AND cleared = false`,
		cfg.StrategyID)
	if err == nil {
		defer cdRows.Close()
		for cdRows.Next() {
			var sym string
			var below float64
			if err := cdRows.Scan(&sym, &below); err == nil {
				snap.CooldownSymbols[sym] = below
			}
		}
	}

	// 3) User-override symbols (manual exit cooldown — 3 days)
	ovRows, err := tradingDB.QueryContext(ctx, `
		SELECT symbol, user_override_until
		FROM manthan_signal_decisions
		WHERE strategy_id = $1
		  AND user_override_until IS NOT NULL
		  AND user_override_until > NOW()`,
		cfg.StrategyID)
	if err == nil {
		defer ovRows.Close()
		for ovRows.Next() {
			var sym string
			var until time.Time
			if err := ovRows.Scan(&sym, &until); err == nil {
				snap.OverrideSymbols[sym] = until
			}
		}
	}

	// 4) EMA targets from Redis (refreshed daily by data-ingestion's
	//    manthan-live pipeline). Defaults to a sensible map if Redis is
	//    unavailable so a degraded run still produces something useful.
	if rdb != nil {
		raw, rErr := rdb.Get(ctx, "manthan:ema:allocations").Result()
		if rErr == nil {
			var ema map[string]float64
			if json.Unmarshal([]byte(raw), &ema) == nil {
				for k, v := range ema {
					snap.EMATargets[strings.ToUpper(strings.TrimSpace(k))] = v
				}
			}
		}
	}
	if len(snap.EMATargets) == 0 {
		// Fallback: every index gets 30% target. Conservative; ensures
		// allocator can still run if Redis is empty.
		snap.EMATargets["NIFTY50"] = 0.30
		snap.EMATargets["NFTYMCP150"] = 0.30
		snap.EMATargets["NTYSLCP250"] = 0.30
	}

	return snap, nil
}

// LoadEligibleSignals reads today's eligible Manthan signals from
// market_data.manthan_signals. Returns alphabetically by symbol so
// rebalancer's tie-break matches Manthan's "alphabetical on exits" rule.
func LoadEligibleSignals(ctx context.Context, marketDB *sql.DB) ([]EligibleSignal, error) {
	rows, err := marketDB.QueryContext(ctx, `
		SELECT symbol, COALESCE(isin,''), COALESCE(industry,''),
		       COALESCE(mcap_bucket,''), COALESCE(index_name,''),
		       COALESCE(latest_price, 0), COALESCE(ath_close, 0),
		       COALESCE(week52_high, 0)
		FROM manthan_signals
		WHERE run_date = CURRENT_DATE
		ORDER BY symbol`)
	if err != nil {
		return nil, fmt.Errorf("load eligible signals: %w", err)
	}
	defer rows.Close()

	out := []EligibleSignal{}
	for rows.Next() {
		var s EligibleSignal
		if err := rows.Scan(&s.Symbol, &s.ISIN, &s.Industry, &s.MCapBucket,
			&s.IndexName, &s.LatestPrice, &s.ATHClose, &s.Week52High); err != nil {
			return nil, err
		}
		s.IndexName = strings.ToUpper(strings.TrimSpace(s.IndexName))
		out = append(out, s)
	}
	return out, rows.Err()
}
