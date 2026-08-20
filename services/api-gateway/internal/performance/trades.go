package performance

// Trade-level production metrics computed from REAL closed Manthan lots in
// positions_db (the CQRS lifecycle SSOT, fed by broker-confirmed fills).
//
// HONESTY RULE (mirrors stats.go): these replace the catalog's operator-
// supplied track-record figures ONLY when the live sample is meaningful —
// MinTradesForStats closed lots. A win rate "computed" from one stop-out is
// true and useless; below the threshold callers keep their defaults.

import (
	"context"
	"database/sql"
	"time"
)

// MinTradesForStats is the smallest closed-lot sample that may replace the
// track-record key stats on the public API.
const MinTradesForStats = 20

// TradeStats is the trade-level headline set for one algo.
type TradeStats struct {
	TotalTrades    int
	WinRatePct     float64 // wins / total × 100
	ProfitFactor   float64 // gross profit / gross loss; 0 when no losses yet
	AvgHoldingDays float64 // mean (exit − entry), calendar days
	// Available is true only when TotalTrades ≥ MinTradesForStats.
	Available bool
}

// FetchTradeStats aggregates every EXITED Manthan lot across all users.
// Strategy-level metrics: the algo behaves identically for every subscriber,
// so lots pool. Nil db → zero value (catalog defaults stay).
func FetchTradeStats(ctx context.Context, db *sql.DB) (TradeStats, error) {
	var out TradeStats
	if db == nil {
		return out, nil
	}
	qctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var wins int
	var grossProfit, grossLoss, avgDays sql.NullFloat64
	err := db.QueryRowContext(qctx, `
		SELECT count(*),
		       count(*) FILTER (WHERE realized_pnl > 0),
		       sum(realized_pnl)      FILTER (WHERE realized_pnl > 0),
		       abs(sum(realized_pnl) FILTER (WHERE realized_pnl < 0)),
		       avg(extract(epoch FROM exit_time - entry_time) / 86400)
		FROM positions
		WHERE origin = 'MANTHAN' AND status = 'EXITED'
		  AND exit_time IS NOT NULL AND realized_pnl IS NOT NULL`,
	).Scan(&out.TotalTrades, &wins, &grossProfit, &grossLoss, &avgDays)
	if err != nil {
		return TradeStats{}, err
	}
	if out.TotalTrades > 0 {
		out.WinRatePct = round2(float64(wins) / float64(out.TotalTrades) * 100)
	}
	if grossLoss.Valid && grossLoss.Float64 > 0 && grossProfit.Valid {
		out.ProfitFactor = round2(grossProfit.Float64 / grossLoss.Float64)
	}
	if avgDays.Valid {
		out.AvgHoldingDays = round2(avgDays.Float64)
	}
	out.Available = out.TotalTrades >= MinTradesForStats
	return out, nil
}
