package performance

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// ErrNoData is returned by Store methods when there are zero rows for
// the requested algo/reference/benchmark. Handler maps this to a 404.
var ErrNoData = errors.New("performance: no data for this algo")

// DailyRow is one raw row from algo_performance_daily. Nullable columns
// use sql.NullFloat64 so we can distinguish "not yet available" (early
// rows have empty daily_mtm / exposure fields) from "genuinely zero".
type DailyRow struct {
	Date             time.Time
	InvestmentAmount float64
	NumScripts       sql.NullInt64
	ReturnPct        float64
	PnLAmount        float64
	AnnualizedPct    sql.NullFloat64
	XIRRPct          sql.NullFloat64
	DailyMTMAmount   sql.NullFloat64
	DailyMTMPct      sql.NullFloat64
	ExposurePct      sql.NullFloat64
}

// BenchmarkRow is one raw row from benchmark_daily.
type BenchmarkRow struct {
	Date       time.Time
	CloseValue float64
	ReturnPct  sql.NullFloat64
}

// Store is the abstraction for fetching performance data. Today the
// only implementation is *PostgresStore; when we cache aggressively
// in front of the DB, we'll add a CachingStore that wraps this
// interface. Handlers depend on the interface, not the concrete type.
type Store interface {
	FetchDaily(ctx context.Context, algoID, referenceClientID string) ([]DailyRow, error)
	FetchBenchmark(ctx context.Context, benchmarkID string, since time.Time) ([]BenchmarkRow, error)
}

// PostgresStore reads from the stockk_market database's
// algo_performance_daily + benchmark_daily tables.
type PostgresStore struct {
	db *sql.DB
}

// NewPostgresStore wires the store to an already-open *sql.DB.
// The gateway opens ONE connection pool to stockk_market at startup
// and passes it in — see cmd/main.go.
func NewPostgresStore(db *sql.DB) *PostgresStore {
	return &PostgresStore{db: db}
}

// FetchDaily returns every performance row for (algoID, referenceClientID)
// ordered by date ascending — the natural order for chart plotting
// and cumulative-return math.
func (s *PostgresStore) FetchDaily(ctx context.Context, algoID, referenceClientID string) ([]DailyRow, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT date, investment_amount, num_scripts,
		       return_pct, pnl_amount,
		       annualized_pct, xirr_pct,
		       daily_mtm_amount, daily_mtm_pct, exposure_pct
		  FROM algo_performance_daily
		 WHERE algo_id = $1
		   AND reference_client_id = $2
		 ORDER BY date ASC`,
		algoID, referenceClientID,
	)
	if err != nil {
		return nil, fmt.Errorf("performance: fetch daily: %w", err)
	}
	defer rows.Close()

	var out []DailyRow
	for rows.Next() {
		var r DailyRow
		if err := rows.Scan(
			&r.Date, &r.InvestmentAmount, &r.NumScripts,
			&r.ReturnPct, &r.PnLAmount,
			&r.AnnualizedPct, &r.XIRRPct,
			&r.DailyMTMAmount, &r.DailyMTMPct, &r.ExposurePct,
		); err != nil {
			return nil, fmt.Errorf("performance: scan daily row: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("performance: iterate daily rows: %w", err)
	}
	if len(out) == 0 {
		return nil, ErrNoData
	}
	return out, nil
}

// FetchBenchmark returns benchmark rows on or after `since`. We filter
// server-side so we don't drag 3 years of Nifty data into memory
// when we only need to overlay it on 148 algo dates.
func (s *PostgresStore) FetchBenchmark(ctx context.Context, benchmarkID string, since time.Time) ([]BenchmarkRow, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT date, close_value, return_pct
		  FROM benchmark_daily
		 WHERE benchmark_id = $1
		   AND date >= $2
		 ORDER BY date ASC`,
		benchmarkID, since,
	)
	if err != nil {
		return nil, fmt.Errorf("performance: fetch benchmark: %w", err)
	}
	defer rows.Close()

	var out []BenchmarkRow
	for rows.Next() {
		var r BenchmarkRow
		if err := rows.Scan(&r.Date, &r.CloseValue, &r.ReturnPct); err != nil {
			return nil, fmt.Errorf("performance: scan benchmark row: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// Compile-time assertion: *PostgresStore satisfies Store.
var _ Store = (*PostgresStore)(nil)
