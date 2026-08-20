package livealgos

// True per-deployment daily NAV — the data source for the deployed-strategy
// chart (and therefore its drawdown/Sharpe/CAGR tiles). See
// migrations/001_strategy_nav_daily.sql for why this exists and why history
// only accrues forward.

import (
	"context"
	"database/sql"
	"time"
)

// NAVPoint is one settled (or intraday-partial, for today) snapshot.
type NAVPoint struct {
	Date      time.Time
	NetPnLPct float64
}

// NAVRow is what the snapshot job writes.
type NAVRow struct {
	StrategyID       string
	UserID           string
	Date             time.Time
	DeployedCapital  int64
	NetPnLAmount     int64
	NetPnLPct        float64
	RealizedAmount   int64
	UnrealizedAmount int64
	OpenPositions    int
}

// Deployment is one active strategy the snapshot job must record.
type Deployment struct {
	StrategyID      string
	UserID          string
	DeployedCapital int64
	DeployedAt      time.Time
}

// NAVStore reads/writes strategy_nav_daily (stockk_market pool).
type NAVStore struct{ db *sql.DB }

func NewNAVStore(db *sql.DB) *NAVStore {
	if db == nil {
		return nil
	}
	return &NAVStore{db: db}
}

func (s *NAVStore) Upsert(ctx context.Context, r NAVRow) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO strategy_nav_daily
		    (strategy_id, user_id, date, deployed_capital, net_pnl_amount,
		     net_pnl_pct, realized_amount, unrealized_amount, open_positions, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9, now())
		ON CONFLICT (strategy_id, date) DO UPDATE SET
		    deployed_capital  = EXCLUDED.deployed_capital,
		    net_pnl_amount    = EXCLUDED.net_pnl_amount,
		    net_pnl_pct       = EXCLUDED.net_pnl_pct,
		    realized_amount   = EXCLUDED.realized_amount,
		    unrealized_amount = EXCLUDED.unrealized_amount,
		    open_positions    = EXCLUDED.open_positions,
		    updated_at        = now()`,
		r.StrategyID, r.UserID, r.Date, r.DeployedCapital, r.NetPnLAmount,
		r.NetPnLPct, r.RealizedAmount, r.UnrealizedAmount, r.OpenPositions)
	return err
}

// Series returns the deployment's snapshots, oldest first.
func (s *NAVStore) Series(ctx context.Context, strategyID string) ([]NAVPoint, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT date, net_pnl_pct FROM strategy_nav_daily
		WHERE strategy_id = $1 ORDER BY date`, strategyID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []NAVPoint
	for rows.Next() {
		var p NAVPoint
		if err := rows.Scan(&p.Date, &p.NetPnLPct); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// ComputeNAVRow runs the SAME per-strategy P&L math the details page serves
// (realized from exited lots + unrealized from live LTPs) and packages it as
// a snapshot row. Pure aside from the clock argument.
func ComputeNAVRow(d Deployment, positions []PositionRow, ltps map[string]LTPQuote, today time.Time) NAVRow {
	var active, exited []PositionRow
	for _, p := range positions {
		if p.Status == "ACTIVE" {
			active = append(active, p)
		} else if p.Status == "EXITED" {
			exited = append(exited, p)
		}
	}
	var realized int64
	for _, p := range exited {
		if p.RealizedPnL.Valid {
			realized += int64(p.RealizedPnL.Float64)
		}
	}
	unrealized, _ := unrealisedAndToday(active, ltps)
	net := realized + unrealized
	var pct float64
	if d.DeployedCapital > 0 {
		pct = float64(net) / float64(d.DeployedCapital) * 100
	}
	return NAVRow{
		StrategyID:       d.StrategyID,
		UserID:           d.UserID,
		Date:             today,
		DeployedCapital:  d.DeployedCapital,
		NetPnLAmount:     net,
		NetPnLPct:        round2(pct),
		RealizedAmount:   realized,
		UnrealizedAmount: unrealized,
		OpenPositions:    len(active),
	}
}

// ListActiveDeployments enumerates every live MANTHAN deployment (all users)
// for the snapshot job.
func (s *PostgresStore) ListActiveDeployments(ctx context.Context) ([]Deployment, error) {
	if s.strategiesDB == nil {
		return nil, nil
	}
	rows, err := s.strategiesDB.QueryContext(ctx, `
		SELECT s.strategy_id::text, s.user_id, COALESCE(tc.total_capital, 0), s.created_at
		FROM public.strategies s
		LEFT JOIN public.trade_configs tc ON tc.strategy_id = s.strategy_id
		WHERE s.strategy_type = 'MANTHAN' AND s.active AND s.deleted_at IS NULL`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Deployment
	for rows.Next() {
		var d Deployment
		var cap float64
		if err := rows.Scan(&d.StrategyID, &d.UserID, &cap, &d.DeployedAt); err != nil {
			return nil, err
		}
		d.DeployedCapital = int64(cap)
		out = append(out, d)
	}
	return out, rows.Err()
}
