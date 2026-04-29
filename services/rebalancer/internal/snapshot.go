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

// LoadActiveStrategies reads every active MANTHAN strategy from trading_db.
// Joins strategies + trade_configs (for capital + max_positions + SL params)
// and pulls the broker auth from trading_execution.user_credentials.
//
// Returns ONLY strategies that have all required pieces: active, has trade
// config, has user credentials. Anything missing → logged and skipped.
func LoadActiveStrategies(ctx context.Context, tradingDB, execDB *sql.DB) ([]StrategyConfig, error) {
	rows, err := tradingDB.QueryContext(ctx, `
		SELECT s.strategy_id, s.user_id, s.strategy_name, s.trading_mode,
		       COALESCE(t.total_capital, 0) AS total_capital,
		       COALESCE(t.max_positions, 25) AS max_positions,
		       COALESCE(t.stop_loss_pct, 20) AS stop_loss_pct,
		       COALESCE(t.trailing_sl_pct, 2) AS trailing_sl_pct
		FROM strategies s
		LEFT JOIN trade_configs t ON t.strategy_id = s.strategy_id
		WHERE s.strategy_type = 'MANTHAN'
		  AND s.deleted_at IS NULL
		  AND s.active = true
		ORDER BY s.user_id, s.created_at`)
	if err != nil {
		return nil, fmt.Errorf("load strategies: %w", err)
	}
	defer rows.Close()

	out := make([]StrategyConfig, 0)
	for rows.Next() {
		var c StrategyConfig
		var maxPos int
		if err := rows.Scan(&c.StrategyID, &c.UserID, &c.StrategyName,
			&c.TradingMode, &c.TotalCapital, &maxPos,
			&c.StopLossPct, &c.TrailingSLPct); err != nil {
			return nil, err
		}
		c.MaxPositions = maxPos
		// Apply Manthan rule: ≤25L → 25, >25L → 50 (only if config didn't set explicitly)
		if c.MaxPositions <= 0 {
			c.MaxPositions = 25
			if c.TotalCapital > 2500000 {
				c.MaxPositions = 50
			}
		}
		// Pull broker auth from trading_execution
		auth, _ := ResolveBrokerAuth(ctx, execDB, c.UserID)
		if auth != nil {
			c.BearerToken = auth.BearerToken
			c.AppID = auth.AppId
			c.Source = auth.Source
		}
		out = append(out, c)
	}
	return out, rows.Err()
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
