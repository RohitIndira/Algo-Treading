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
// Source of truth is ref_equity_eod (closes since January 2021). The
// vendor adjusts MOST histories for splits/bonuses but not all (verified
// 2026-10-30):
//
//   - RELIANCE 1:1 bonus (ex 2024-10-28), COFORGE 1:5 split (2025-06-04):
//     shares_outstanding jumps, price does NOT step — history already
//     adjusted, matches Yahoo's adjusted close to the paisa.
//   - CUB 1:3 bonus (ex 2026-06-12): shares ×1.333 AND price ×0.785 the
//     same day — history NOT adjusted; a raw max(close) gave an ATH of
//     301.75 that no post-bonus price (~227) could ever reach.
//
// So a corporate action is adjusted only when a share jump ≥ splitMinRatio
// comes with a same-day price move that cancels it (price ratio × share
// ratio within splitMatchTol of 1). A share jump with a flat price is either
// an already-adjusted history or filing-timing noise (1,817 jumps across
// 1,265 symbols, only 140 matched) and is ignored.
//
// Peaks before 2021: our history starts January 2021. The vendor's
// price-levels ATH covers full history but is an INTRADAY high and, like the
// EOD history, sometimes unadjusted (CUB: 324.1 vs an adjusted ~226; EMPIND:
// 2× Yahoo's adjusted peak) — so it is never used as the level. It is used
// only as a HINT: where it sits more than vendorGapPct above our max close,
// the real peak may predate our data, and the `ath-seed` job fetches Yahoo's
// split-adjusted daily closes for 2000–2020 into ref_ath_pre2021. The ATH is
// then max(our adjusted max close, Yahoo pre-2021 max close).
//
// A hinted symbol with no Yahoo answer yet is written with source
// "unverified:pre2021"; the DB reader leaves those out of the trigger
// universe (no early breakout on an ATH we could not confirm).
//
// The same computation runs every night after the EOD load; "seed" and
// "roll forward" are the same job, which keeps it idempotent.

const (
	// A shares_outstanding jump of at least this ratio between consecutive
	// trading days is a split/bonus CANDIDATE.
	splitMinRatio = 1.20
	// ...and it is adjusted only if price ratio × share ratio is within this
	// of 1 (the price fell by the same factor the share count rose).
	splitMatchTol = 0.15
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
	SplitsAdjusted int
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
	pre, err := s.pre2021(ctx)
	if err != nil {
		return 0, err
	}
	series, isinBySym, err := s.closeSeries(ctx, asOf)
	if err != nil {
		return 0, err
	}
	results := make([]ATHResult, 0, len(series))
	splitSyms, preUsed, unverified := 0, 0, 0
	for sym, rows := range series {
		r := computeLevels(sym, rows, asOf)
		r.ISIN = isinBySym[sym]
		if r.SplitsAdjusted > 0 {
			splitSyms++
		}
		if p, ok := pre[sym]; ok && p.days > 0 {
			if p.maxClose > r.ATHClose {
				r.ATHClose, r.ATHDate, r.Source = round4(p.maxClose), p.maxDate, "seed:yahoo-pre2021"
				preUsed++
			}
		} else if v, ok := vendor[sym]; ok && v > 0 && r.ATHClose > 0 && v > r.ATHClose*(1+vendorGapPct) {
			r.Source = "unverified:pre2021"
			unverified++
		}
		results = append(results, r)
	}
	sort.Slice(results, func(i, j int) bool { return results[i].Symbol < results[j].Symbol })
	n, err := s.upsertATH(ctx, results)
	if err != nil {
		return n, err
	}
	logger.Info("ath: levels computed", zap.Int("symbols", n), zap.Int("unadjusted_actions_fixed", splitSyms),
		zap.Int("pre2021_peak_used", preUsed), zap.Int("unverified_pre2021", unverified),
		zap.String("as_of", asOf.Format("2006-01-02")))
	if unverified > 0 {
		logger.Warn("ath: symbols whose real peak may predate 2021 and have no Yahoo seed — excluded from triggers; run -job ath-seed",
			zap.Int("symbols", unverified))
	}
	return n, nil
}

type preRow struct {
	maxClose float64
	maxDate  time.Time
	days     int
}

func (s *Store) pre2021(ctx context.Context) (map[string]preRow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT symbol, COALESCE(max_close,0), max_date, days FROM ref_ath_pre2021`)
	if err != nil {
		return nil, fmt.Errorf("ath: pre-2021 seeds: %w", err)
	}
	defer rows.Close()
	m := map[string]preRow{}
	for rows.Next() {
		var sym string
		var p preRow
		var d sql.NullTime
		if err := rows.Scan(&sym, &p.maxClose, &d, &p.days); err != nil {
			return nil, err
		}
		if d.Valid {
			p.maxDate = d.Time
		}
		m[strings.ToUpper(strings.TrimSpace(sym))] = p
	}
	return m, rows.Err()
}

// ATHSeedCandidates lists symbols whose vendor ATH hints at a pre-2021 peak
// and that have no ref_ath_pre2021 row yet (all hinted symbols when refetch).
func (s *Store) ATHSeedCandidates(ctx context.Context, refetch bool) ([]string, error) {
	q := `
		SELECT a.symbol FROM ref_ath_close a
		  JOIN ref_price_levels p ON p.symbol = a.symbol
		  LEFT JOIN ref_ath_pre2021 x ON x.symbol = a.symbol
		 WHERE p.ath_intraday > a.ath_close * $1`
	if !refetch {
		q += ` AND x.symbol IS NULL`
	}
	q += ` ORDER BY a.symbol`
	rows, err := s.db.QueryContext(ctx, q, 1+vendorGapPct)
	if err != nil {
		return nil, fmt.Errorf("ath seed candidates: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var sym string
		if err := rows.Scan(&sym); err != nil {
			return nil, err
		}
		out = append(out, sym)
	}
	return out, rows.Err()
}

// UpsertPre2021 stores one symbol's pre-2021 peak (days = 0: nothing found).
func (s *Store) UpsertPre2021(ctx context.Context, symbol string, rows []IndexClose, source string) error {
	var maxC float64
	var maxD sql.NullTime
	for _, r := range rows {
		if r.Close > maxC {
			maxC, maxD = r.Close, sql.NullTime{Time: r.Date, Valid: true}
		}
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO ref_ath_pre2021 (symbol, max_close, max_date, days, source, fetched_at)
		VALUES ($1, $2, $3, $4, $5, now())
		ON CONFLICT (symbol) DO UPDATE SET max_close = EXCLUDED.max_close, max_date = EXCLUDED.max_date,
			days = EXCLUDED.days, source = EXCLUDED.source, fetched_at = now()`,
		symbol, nullIfZero(maxC), maxD, len(rows), source)
	return err
}

// unadjustedAction reports the factor by which closes BEFORE row i must be
// divided, or 1 when the step from i-1 to i is not an unadjusted corporate
// action (see the package comment for the evidence).
func unadjustedAction(prev, cur closeRow) float64 {
	if prev.shares <= 0 || cur.shares <= 0 || prev.close <= 0 || cur.close <= 0 {
		return 1
	}
	ratio := cur.shares / prev.shares
	if ratio < splitMinRatio {
		return 1
	}
	if math.Abs(cur.close/prev.close*ratio-1) > splitMatchTol {
		return 1
	}
	return ratio
}

// computeLevels adjusts unadjusted splits/bonuses, then takes max close
// (ATH) and the 52-week close high/low. rows must be oldest-first.
func computeLevels(sym string, rows []closeRow, asOf time.Time) ATHResult {
	res := ATHResult{Symbol: sym, Source: "roll:eod"}
	if len(rows) == 0 {
		return res
	}
	// The factor for a day is the product of every unadjusted action AFTER it.
	adj := make([]float64, len(rows))
	factor := 1.0
	for i := len(rows) - 1; i >= 0; i-- {
		adj[i] = factor
		if i > 0 {
			if f := unadjustedAction(rows[i-1], rows[i]); f != 1 {
				factor *= f
				res.SplitsAdjusted++
			}
		}
	}
	if res.SplitsAdjusted > 0 {
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
