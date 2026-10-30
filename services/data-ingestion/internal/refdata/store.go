package refdata

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// Store writes vendor rows into the ref_* tables (migration 013, signals_db).
// Every writer is an upsert keyed on the table's primary key, so re-running a
// day or a period is idempotent — the loader is meant to be re-run freely.
type Store struct {
	db *sql.DB
}

// NewStore wraps an open signals_db handle.
func NewStore(db *sql.DB) *Store { return &Store{db: db} }

// RunRecord is one row of ref_load_runs.
type RunRecord struct {
	Kind      string
	Params    string
	Rows      int
	Err       error
	StartedAt time.Time
}

// RecordRun writes the audit row. It never fails the caller: a broken audit
// insert is logged by the caller, the data write already happened.
func (s *Store) RecordRun(ctx context.Context, r RunRecord) error {
	var errText sql.NullString
	if r.Err != nil {
		errText = sql.NullString{String: r.Err.Error(), Valid: true}
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO ref_load_runs (kind, params, rows_written, ok, error, started_at)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		r.Kind, r.Params, r.Rows, r.Err == nil, errText, r.StartedAt.UTC())
	return err
}

// UpsertEOD writes one trading day's rows. Returns rows written.
func (s *Store) UpsertEOD(ctx context.Context, rows []EODRow) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO ref_equity_eod
			(trade_date, isin, nse_symbol, bse_code, company_name, industry, sector,
			 close_price, shares_outstanding, market_cap_cr, loaded_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, now())
		ON CONFLICT (trade_date, isin) DO UPDATE SET
			nse_symbol = EXCLUDED.nse_symbol, bse_code = EXCLUDED.bse_code,
			company_name = EXCLUDED.company_name, industry = EXCLUDED.industry,
			sector = EXCLUDED.sector, close_price = EXCLUDED.close_price,
			shares_outstanding = EXCLUDED.shares_outstanding,
			market_cap_cr = EXCLUDED.market_cap_cr, loaded_at = now()`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	n := 0
	for _, r := range rows {
		if r.ISIN == "" || r.TradeDate == "" {
			continue
		}
		if _, err := stmt.ExecContext(ctx, r.TradeDate, r.ISIN, nullStr(r.NSESymbol), nullStr(r.BSECode),
			r.CompanyName, nullStr(r.Industry), nullStr(r.Sector), r.ClosePrice,
			nullF(r.SharesOutstanding), nullF(r.MarketCapCr)); err != nil {
			return n, fmt.Errorf("eod upsert %s %s: %w", r.TradeDate, r.ISIN, err)
		}
		n++
	}
	return n, tx.Commit()
}

// UpsertFundamentals writes one period under the basis that was REQUESTED
// (the vendor's pat_basis column records what it actually used).
func (s *Store) UpsertFundamentals(ctx context.Context, basis string, rows []FundamentalsRow) (int, error) {
	basis = strings.ToLower(strings.TrimSpace(basis))
	if basis == "" {
		basis = "preferred"
	}
	if len(rows) == 0 {
		return 0, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO ref_fundamentals
			(period, isin, basis, nse_symbol, bse_code, pat_cr, pat_period_type, pat_basis, eps_ttm, f_score, loaded_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, now())
		ON CONFLICT (period, isin, basis) DO UPDATE SET
			nse_symbol = EXCLUDED.nse_symbol, bse_code = EXCLUDED.bse_code,
			pat_cr = EXCLUDED.pat_cr, pat_period_type = EXCLUDED.pat_period_type,
			pat_basis = EXCLUDED.pat_basis, eps_ttm = EXCLUDED.eps_ttm,
			f_score = EXCLUDED.f_score, loaded_at = now()`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	n := 0
	for _, r := range rows {
		if r.ISIN == "" || r.Period == "" {
			continue
		}
		if _, err := stmt.ExecContext(ctx, r.Period, r.ISIN, basis, nullStr(r.NSESymbol), nullStr(r.BSECode),
			nullF(r.PATCr), nullStr(r.PATPeriodType), nullStr(r.PATBasis), nullF(r.EPSTTM), nullF(r.FScore)); err != nil {
			return n, fmt.Errorf("fundamentals upsert %s %s: %w", r.Period, r.ISIN, err)
		}
		n++
	}
	return n, tx.Commit()
}

// UpsertPriceLevels replaces the vendor snapshot. Symbols the vendor dropped
// are left in place (stale rows are harmless: this table is a cross-check,
// not the trigger basis).
func (s *Store) UpsertPriceLevels(ctx context.Context, rows []PriceLevelRow) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO ref_price_levels (symbol, co_code, company_name, week52_high, week52_low, ath_intraday, loaded_at)
		VALUES ($1, $2, $3, $4, $5, $6, now())
		ON CONFLICT (symbol) DO UPDATE SET
			co_code = EXCLUDED.co_code, company_name = EXCLUDED.company_name,
			week52_high = EXCLUDED.week52_high, week52_low = EXCLUDED.week52_low,
			ath_intraday = EXCLUDED.ath_intraday, loaded_at = now()`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	n := 0
	for _, r := range rows {
		sym := strings.ToUpper(strings.TrimSpace(r.Symbol))
		if sym == "" {
			continue
		}
		if _, err := stmt.ExecContext(ctx, sym, r.CoCode, r.CompanyName,
			nullF(r.Week52High), nullF(r.Week52Low), nullF(r.AllTimeHigh)); err != nil {
			return n, fmt.Errorf("price-levels upsert %s: %w", sym, err)
		}
		n++
	}
	return n, tx.Commit()
}

// LatestEODDate returns the newest trade_date loaded, or zero time.
func (s *Store) LatestEODDate(ctx context.Context) (time.Time, error) {
	var d sql.NullTime
	if err := s.db.QueryRowContext(ctx, `SELECT max(trade_date) FROM ref_equity_eod`).Scan(&d); err != nil {
		return time.Time{}, err
	}
	if !d.Valid {
		return time.Time{}, nil
	}
	return d.Time, nil
}

func nullStr(p *string) sql.NullString {
	if p == nil || strings.TrimSpace(*p) == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: strings.TrimSpace(*p), Valid: true}
}

func nullF(p *float64) sql.NullFloat64 {
	if p == nil {
		return sql.NullFloat64{}
	}
	return sql.NullFloat64{Float64: *p, Valid: true}
}
