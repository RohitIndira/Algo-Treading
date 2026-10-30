package refdata

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"go.uber.org/zap"
)

// Close-based all-time high (operator decision 2026-10-30: the trigger is
// "live price crosses the highest DAILY CLOSE ever", the basis Manthan has
// traded on since launch; the vendor's price-levels ATH is an intraday high
// and is used only as a cross-check).
//
// Source of truth is ref_equity_eod, which holds raw (unadjusted) closes
// since January 2021. Two things make a naive max(close) wrong:
//
//  1. Corporate actions. A 1:10 split leaves a pre-split close ten times
//     today's price in the table — an ATH no live price could ever cross.
//     The vendor adjusts shares_outstanding on the ex-date, so a jump in
//     shares_outstanding between consecutive rows IS the split/bonus ratio.
//     Closes before the jump are divided by that ratio before the max.
//  2. Pre-2021 peaks. A stock whose real ATH was in 2018 shows a lower
//     2021+ max. The vendor's intraday ATH covers full history: when it sits
//     more than vendorGapPct above our adjusted max close, the real peak
//     predates our data and the vendor's figure is used instead (an
//     intraday high is ≥ the close-based ATH, so the trigger only gets
//     stricter for those names, never looser).
//
// The same computation runs every night after the EOD load; "seed" and
// "roll forward" are the same job, which keeps it idempotent.

const (
	// A shares_outstanding change of at least this ratio between two
	// consecutive trading days is treated as a split/bonus (smaller moves —
	// QIPs, ESOPs, buybacks — are allotments, not re-denominations).
	splitMinRatio = 1.20
	// If the vendor's intraday ATH exceeds our adjusted max close by more
	// than this fraction, the true ATH predates our history.
	vendorGapPct = 0.10
	// 52-week window for the close-based high/low.
	week52Days = 365
)

// ATHResult is one symbol's computed levels.
type ATHResult struct {
	Symbol         string
	ISIN           string
	ATHClose       float64
	ATHDate        time.Time
	Week52High     float64
	Week52Low      float64
	Source         string // roll:eod | roll:eod+split | seed:vendor-intraday
	SplitsDetected int
}

type closeRow struct {
	date   time.Time
	close  float64
	shares float64 // 0 when the vendor has none
}

// ComputeATH runs the levels for every symbol in ref_equity_eod and upserts
// ref_ath_close. asOf bounds the data used (normally the latest loaded day).
func (s *Store) ComputeATH(ctx context.Context, asOf time.Time, logger *zap.Logger) (int, error) {
	vendor, err := s.vendorATH(ctx)
	if err != nil {
		return 0, err
	}
	series, isinBySym, err := s.closeSeries(ctx, asOf)
	if err != nil {
		return 0, err
	}
	results := make([]ATHResult, 0, len(series))
	splits, vendorUsed := 0, 0
	for sym, rows := range series {
		r := computeLevels(sym, rows, asOf)
		r.ISIN = isinBySym[sym]
		if r.SplitsDetected > 0 {
			splits++
		}
		if v, ok := vendor[sym]; ok && v > 0 && r.ATHClose > 0 && v > r.ATHClose*(1+vendorGapPct) {
			r.ATHClose = v
			r.ATHDate = time.Time{}
			r.Source = "seed:vendor-intraday"
			vendorUsed++
		}
		results = append(results, r)
	}
	sort.Slice(results, func(i, j int) bool { return results[i].Symbol < results[j].Symbol })
	n, err := s.upsertATH(ctx, results)
	if err != nil {
		return n, err
	}
	logger.Info("ath: levels computed", zap.Int("symbols", n), zap.Int("with_split_adjustment", splits),
		zap.Int("vendor_ath_used", vendorUsed), zap.String("as_of", asOf.Format("2006-01-02")))
	return n, nil
}

// computeLevels adjusts for splits, then takes max close (ATH) and the
// 52-week close high/low. rows must be oldest-first.
func computeLevels(sym string, rows []closeRow, asOf time.Time) ATHResult {
	res := ATHResult{Symbol: sym, Source: "roll:eod"}
	if len(rows) == 0 {
		return res
	}
	// Adjustment factor walks backwards: the factor applying to a day is the
	// product of every split ratio that happened AFTER it.
	adj := make([]float64, len(rows))
	factor := 1.0
	for i := len(rows) - 1; i >= 0; i-- {
		adj[i] = factor
		if i > 0 && rows[i].shares > 0 && rows[i-1].shares > 0 {
			ratio := rows[i].shares / rows[i-1].shares
			if ratio >= splitMinRatio {
				factor *= ratio
				res.SplitsDetected++
			}
		}
	}
	if res.SplitsDetected > 0 {
		res.Source = "roll:eod+split"
	}
	cutoff := asOf.AddDate(0, 0, -week52Days)
	for i, r := range rows {
		c := r.close / adj[i]
		if c > res.ATHClose {
			res.ATHClose, res.ATHDate = c, r.date
		}
		if !r.date.Before(cutoff) {
			if c > res.Week52High {
				res.Week52High = c
			}
			if res.Week52Low == 0 || c < res.Week52Low {
				res.Week52Low = c
			}
		}
	}
	res.ATHClose = round4(res.ATHClose)
	res.Week52High = round4(res.Week52High)
	res.Week52Low = round4(res.Week52Low)
	return res
}

func round4(v float64) float64 { return math.Round(v*10000) / 10000 }

// closeSeries loads (date, close, shares) per NSE symbol, oldest first.
// Symbols are keyed by their CURRENT nse_symbol (the vendor stamps today's
// symbol on every historical row, so a renamed company stays one series).
func (s *Store) closeSeries(ctx context.Context, asOf time.Time) (map[string][]closeRow, map[string]string, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT nse_symbol, isin, trade_date, close_price, COALESCE(shares_outstanding, 0)
		  FROM ref_equity_eod
		 WHERE nse_symbol IS NOT NULL AND nse_symbol <> '' AND trade_date <= $1
		 ORDER BY nse_symbol, trade_date`, asOf.Format("2006-01-02"))
	if err != nil {
		return nil, nil, fmt.Errorf("ath: close series: %w", err)
	}
	defer rows.Close()
	series := map[string][]closeRow{}
	isin := map[string]string{}
	for rows.Next() {
		var sym, is string
		var d time.Time
		var c, sh float64
		if err := rows.Scan(&sym, &is, &d, &c, &sh); err != nil {
			return nil, nil, err
		}
		sym = strings.ToUpper(strings.TrimSpace(sym))
		series[sym] = append(series[sym], closeRow{date: d, close: c, shares: sh})
		isin[sym] = is
	}
	return series, isin, rows.Err()
}

func (s *Store) vendorATH(ctx context.Context) (map[string]float64, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT symbol, ath_intraday FROM ref_price_levels WHERE ath_intraday IS NOT NULL`)
	if err != nil {
		return nil, fmt.Errorf("ath: vendor levels: %w", err)
	}
	defer rows.Close()
	m := map[string]float64{}
	for rows.Next() {
		var sym string
		var v float64
		if err := rows.Scan(&sym, &v); err != nil {
			return nil, err
		}
		m[strings.ToUpper(strings.TrimSpace(sym))] = v
	}
	return m, rows.Err()
}

func (s *Store) upsertATH(ctx context.Context, results []ATHResult) (int, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO ref_ath_close (symbol, isin, ath_close, ath_date, week52_high_close, week52_low_close, source, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, now())
		ON CONFLICT (symbol) DO UPDATE SET
			isin = EXCLUDED.isin, ath_close = EXCLUDED.ath_close, ath_date = EXCLUDED.ath_date,
			week52_high_close = EXCLUDED.week52_high_close, week52_low_close = EXCLUDED.week52_low_close,
			source = EXCLUDED.source, updated_at = now()`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	n := 0
	for _, r := range results {
		if r.ATHClose <= 0 {
			continue
		}
		var d sql.NullTime
		if !r.ATHDate.IsZero() {
			d = sql.NullTime{Time: r.ATHDate, Valid: true}
		}
		if _, err := stmt.ExecContext(ctx, r.Symbol, nullIfEmpty(r.ISIN), r.ATHClose, d,
			nullIfZero(r.Week52High), nullIfZero(r.Week52Low), r.Source); err != nil {
			return n, fmt.Errorf("ath upsert %s: %w", r.Symbol, err)
		}
		n++
	}
	return n, tx.Commit()
}

func nullIfEmpty(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}

func nullIfZero(v float64) sql.NullFloat64 {
	if v <= 0 {
		return sql.NullFloat64{}
	}
	return sql.NullFloat64{Float64: v, Valid: true}
}
