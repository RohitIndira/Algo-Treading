package manthan

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

// SourceReader is what the pipeline needs from wherever the inputs live:
// the Google Sheet today (GSheetReader) or the ref_* tables (DBReader).
// manthan-live picks one from MANTHAN_SOURCE=sheet|db.
type SourceReader interface {
	Connect(ctx context.Context) error
	ReadAll(ctx context.Context) (*ReadResult, error)
	BuySymbols(ctx context.Context) ([]string, error)
}

// DBReader assembles a ReadResult from the signals_db ref_* tables that the
// manthan-refdata loaders fill (migration 013). It produces exactly the row
// types the sheet reader produces, so Pipeline.Run does not know or care
// which source it is fed.
//
// Field provenance (operator decisions 2026-10-30):
//
//	PEAndNetProfitRow  latest ref_equity_eod day × latest ref_fundamentals
//	                   period with a PAT under the configured basis
//	                   ("preferred"); TTM PE is close ÷ eps_ttm
//	FScoreRow          latest ref_fundamentals period with an f_score
//	LifeTimeHighRow    ref_ath_close — OUR close-based ATH; 52w levels ours,
//	                   falling back to the vendor's ref_price_levels
//	BuySignalRow       ref_buy_triggers for today (IST) — the mechanical
//	                   "live LTP crossed ATH close" trigger, ATHEntry="Buy"
//	IndicesGradeRow    latest ref_index_allocation per index; a missing
//	                   index falls back to 1.0 (today's prod behaviour) loudly
//	IndustryRow        ref_equity_eod industry mapped through ref_industry_map
//	                   to the Accord names the 25% sector cap was built on
type DBReader struct {
	db     *sql.DB
	logger *zap.Logger
	basis  string
	ist    *time.Location
	now    func() time.Time
	// maxFundamentalsAge: a company whose newest PAT is older than this is
	// treated as having no PAT (it stopped reporting — the sheet would have
	// shown a blank too).
	maxFundamentalsAge time.Duration
}

// NewDBReader wraps an open signals_db handle. basis "" means "preferred".
func NewDBReader(db *sql.DB, basis string, logger *zap.Logger) *DBReader {
	basis = strings.ToLower(strings.TrimSpace(basis))
	if basis == "" {
		basis = "preferred"
	}
	ist, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		ist = time.FixedZone("IST", 5*3600+1800)
	}
	return &DBReader{db: db, logger: logger, basis: basis, ist: ist, now: time.Now,
		maxFundamentalsAge: 400 * 24 * time.Hour}
}

// Connect pings the database (the sheet reader's Connect authenticates; the
// DB equivalent is "can I reach signals_db").
func (r *DBReader) Connect(ctx context.Context) error {
	if r.db == nil {
		return fmt.Errorf("db reader: nil database handle")
	}
	return r.db.PingContext(ctx)
}

// today returns the IST calendar day as a DATE string.
func (r *DBReader) today() string {
	return r.now().In(r.ist).Format("2006-01-02")
}

// ReadAll builds the full ReadResult.
func (r *DBReader) ReadAll(ctx context.Context) (*ReadResult, error) {
	res := &ReadResult{}

	industryMap, err := r.industryMap(ctx)
	if err != nil {
		return nil, err
	}

	pe, fs, inds, eodDate, err := r.equities(ctx, industryMap)
	if err != nil {
		return nil, err
	}
	res.PEAndNetProfit, res.FScores, res.Industries = pe, fs, inds

	res.LifeTimeHighs, err = r.lifeTimeHighs(ctx)
	if err != nil {
		return nil, err
	}

	// Industry on the BuySignal row is what the pipeline reads first; feed
	// it the same mapped name the Industry rows carry.
	indBySym := make(map[string]string, len(inds))
	for _, i := range inds {
		indBySym[i.NSECode] = i.Industry
	}
	res.BuySignals, err = r.buySignals(ctx, indBySym)
	if err != nil {
		return nil, err
	}
	// The allocator sizes quantity from LatestPrice. For a stock that
	// triggered today the right price is the breakout LTP, not yesterday's
	// EOD close (a stock breaking out is by definition above it). PE stays
	// on the close, consistent with every other row.
	trig := make(map[string]float64, len(res.BuySignals))
	for _, b := range res.BuySignals {
		if b.CloseRate > 0 {
			trig[b.ScripName] = b.CloseRate
		}
	}
	for _, p := range res.PEAndNetProfit {
		if ltp, ok := trig[p.NSESymbol]; ok {
			p.LatestPrice = ltp
		}
	}

	res.IndicesGrades, err = r.indicesGrades(ctx)
	if err != nil {
		return nil, err
	}
	res.ExitStocks = nil

	r.logger.Info("db reader: ReadResult assembled",
		zap.String("eod_date", eodDate), zap.String("basis", r.basis),
		zap.Int("pe_rows", len(res.PEAndNetProfit)), zap.Int("fscore_rows", len(res.FScores)),
		zap.Int("lth_rows", len(res.LifeTimeHighs)), zap.Int("buy_signals", len(res.BuySignals)),
		zap.Int("industry_rows", len(res.Industries)), zap.Int("index_grades", len(res.IndicesGrades)))
	return res, nil
}

// BuySymbols lists today's triggered symbols (watch-loop change detection).
func (r *DBReader) BuySymbols(ctx context.Context) ([]string, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT symbol FROM ref_buy_triggers WHERE trade_date = $1 ORDER BY symbol`, r.today())
	if err != nil {
		return nil, fmt.Errorf("db reader: buy symbols: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		out = append(out, strings.ToUpper(strings.TrimSpace(s)))
	}
	return out, rows.Err()
}

func (r *DBReader) industryMap(ctx context.Context) (map[string]string, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT api_industry, manthan_industry FROM ref_industry_map`)
	if err != nil {
		return nil, fmt.Errorf("db reader: industry map: %w", err)
	}
	defer rows.Close()
	m := map[string]string{}
	for rows.Next() {
		var a, b string
		if err := rows.Scan(&a, &b); err != nil {
			return nil, err
		}
		m[strings.ToLower(strings.TrimSpace(a))] = strings.TrimSpace(b)
	}
	return m, rows.Err()
}

// equities joins the latest EOD day with the newest fundamentals per ISIN.
func (r *DBReader) equities(ctx context.Context, indMap map[string]string) ([]*PEAndNetProfitRow, []*FScoreRow, []*IndustryRow, string, error) {
	rows, err := r.db.QueryContext(ctx, `
		WITH latest AS (SELECT max(trade_date) AS d FROM ref_equity_eod),
		eod AS (
			SELECT e.* FROM ref_equity_eod e JOIN latest ON e.trade_date = latest.d
		),
		pat AS (
			SELECT DISTINCT ON (isin) isin, period, pat_cr, eps_ttm, pat_basis
			  FROM ref_fundamentals
			 WHERE basis = $1 AND pat_cr IS NOT NULL
			 ORDER BY isin, period DESC
		),
		fsc AS (
			SELECT DISTINCT ON (isin) isin, period, f_score
			  FROM ref_fundamentals
			 WHERE basis = $1 AND f_score IS NOT NULL
			 ORDER BY isin, period DESC
		)
		SELECT e.trade_date, e.isin, COALESCE(e.nse_symbol,''), COALESCE(e.bse_code,''), COALESCE(e.company_name,''),
		       COALESCE(e.industry,''), e.close_price, e.market_cap_cr,
		       p.period, p.pat_cr, p.eps_ttm, f.period, f.f_score
		  FROM eod e
		  LEFT JOIN pat p ON p.isin = e.isin
		  LEFT JOIN fsc f ON f.isin = e.isin
		 WHERE e.nse_symbol IS NOT NULL AND e.nse_symbol <> ''
		 ORDER BY e.nse_symbol`, r.basis)
	if err != nil {
		return nil, nil, nil, "", fmt.Errorf("db reader: equities: %w", err)
	}
	defer rows.Close()

	var pe []*PEAndNetProfitRow
	var fs []*FScoreRow
	var inds []*IndustryRow
	eodDate := ""
	unmapped := map[string]int{}
	stalePAT, staleFS := 0, 0
	for rows.Next() {
		var td time.Time
		var isin, sym, bse, name, apiInd string
		var close float64
		var mcap, pat, eps, fscore sql.NullFloat64
		var patPeriod, fsPeriod sql.NullString
		if err := rows.Scan(&td, &isin, &sym, &bse, &name, &apiInd, &close, &mcap, &patPeriod, &pat, &eps, &fsPeriod, &fscore); err != nil {
			return nil, nil, nil, "", err
		}
		if eodDate == "" {
			eodDate = td.Format("2006-01-02")
		}
		sym = strings.ToUpper(strings.TrimSpace(sym))

		row := &PEAndNetProfitRow{
			CompanyName: name, LatestPriceDate: td, LatestPrice: close,
			BSECode: bse, NSESymbol: sym, ISIN: isin,
		}
		if mcap.Valid {
			row.LatestMarketCap = mcap.Float64
		}
		if pat.Valid && patPeriod.Valid && !r.tooOld(patPeriod.String, td) {
			row.PAT = pat.Float64
			if eps.Valid {
				row.TTMEPS = eps.Float64
				if eps.Float64 > 0 && close > 0 {
					row.TTMPE = math.Round(close/eps.Float64*100) / 100
				}
			}
		} else if pat.Valid {
			stalePAT++
		}
		pe = append(pe, row)

		if fscore.Valid && fsPeriod.Valid {
			if r.tooOld(fsPeriod.String, td) {
				staleFS++
			} else {
				fs = append(fs, &FScoreRow{ISIN: isin, Score: fscore.Float64, CompanyName: name,
					Date: quarterEnd(fsPeriod.String)})
			}
		}

		if apiInd != "" {
			ind := apiInd
			if m, ok := indMap[strings.ToLower(apiInd)]; ok && m != "" {
				ind = m
			} else {
				unmapped[apiInd]++
			}
			inds = append(inds, &IndustryRow{NSECode: sym, ISIN: isin, Industry: ind, Name: name,
				MarketCap: row.LatestMarketCap, PE: row.TTMPE})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, nil, "", err
	}
	if len(unmapped) > 0 {
		keys := make([]string, 0, len(unmapped))
		for k := range unmapped {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		r.logger.Warn("db reader: API industries with no ref_industry_map row — API name used as-is",
			zap.Int("industries", len(keys)), zap.Strings("sample", keys[:min(len(keys), 8)]))
	}
	if stalePAT > 0 || staleFS > 0 {
		r.logger.Info("db reader: stale fundamentals ignored", zap.Int("pat", stalePAT), zap.Int("fscore", staleFS),
			zap.Duration("max_age", r.maxFundamentalsAge))
	}
	return pe, fs, inds, eodDate, nil
}

// tooOld: the quarter-end `period` (YYYY-MM) is older than maxFundamentalsAge
// relative to the EOD day.
func (r *DBReader) tooOld(period string, asOf time.Time) bool {
	qe := quarterEnd(period)
	if qe.IsZero() {
		return true
	}
	return asOf.Sub(qe) > r.maxFundamentalsAge
}

// quarterEnd turns "2026-06" into 2026-06-30.
func quarterEnd(period string) time.Time {
	t, err := time.Parse("2006-01", strings.TrimSpace(period))
	if err != nil {
		return time.Time{}
	}
	return t.AddDate(0, 1, -1)
}

func (r *DBReader) lifeTimeHighs(ctx context.Context) ([]*LifeTimeHighRow, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT a.symbol, a.ath_close, a.ath_date, a.week52_high_close, a.week52_low_close,
		       p.week52_high, p.week52_low
		  FROM ref_ath_close a
		  LEFT JOIN ref_price_levels p ON p.symbol = a.symbol
		 ORDER BY a.symbol`)
	if err != nil {
		return nil, fmt.Errorf("db reader: ath: %w", err)
	}
	defer rows.Close()
	var out []*LifeTimeHighRow
	vendor52 := 0
	for rows.Next() {
		var sym string
		var ath float64
		var athDate sql.NullTime
		var hi, lo, vhi, vlo sql.NullFloat64
		if err := rows.Scan(&sym, &ath, &athDate, &hi, &lo, &vhi, &vlo); err != nil {
			return nil, err
		}
		row := &LifeTimeHighRow{ScripName: strings.ToUpper(strings.TrimSpace(sym)), AllTimeHigh: ath, Period: "Daily"}
		if athDate.Valid {
			row.DataTime = athDate.Time.Format("2006-01-02")
		}
		switch {
		case hi.Valid && hi.Float64 > 0:
			row.Week52High = hi.Float64
		case vhi.Valid && vhi.Float64 > 0:
			row.Week52High = vhi.Float64
			vendor52++
		}
		switch {
		case lo.Valid && lo.Float64 > 0:
			row.Week52Low = lo.Float64
		case vlo.Valid && vlo.Float64 > 0:
			row.Week52Low = vlo.Float64
		}
		out = append(out, row)
	}
	if vendor52 > 0 {
		r.logger.Info("db reader: 52w high taken from vendor price-levels where ours is absent", zap.Int("symbols", vendor52))
	}
	return out, rows.Err()
}

func (r *DBReader) buySignals(ctx context.Context, indBySym map[string]string) ([]*BuySignalRow, error) {
	today := r.today()
	rows, err := r.db.QueryContext(ctx, `
		SELECT symbol, ltp, ath_close, triggered_at, source
		  FROM ref_buy_triggers WHERE trade_date = $1 ORDER BY triggered_at, symbol`, today)
	if err != nil {
		return nil, fmt.Errorf("db reader: buy triggers: %w", err)
	}
	defer rows.Close()
	var out []*BuySignalRow
	for rows.Next() {
		var sym, source string
		var ltp, ath float64
		var at time.Time
		if err := rows.Scan(&sym, &ltp, &ath, &at, &source); err != nil {
			return nil, err
		}
		sym = strings.ToUpper(strings.TrimSpace(sym))
		out = append(out, &BuySignalRow{
			ScripName: sym, DataTime: at.In(r.ist).Format("2006-01-02 15:04:05"),
			Industry: indBySym[sym], CloseRate: ltp, HighRate: ltp,
			IsAllTimeHigh: ltp > ath, ATHEntry: "Buy", Period: source,
		})
	}
	return out, rows.Err()
}

// indicesGrades returns the latest allocation per bucket index. The three
// indices the strategy sizes against must all be present; a missing one is
// filled with 1.0 (the value the sheet has fed since launch) and logged as a
// warning so the gap is visible, not silent.
func (r *DBReader) indicesGrades(ctx context.Context) ([]*IndicesGradeRow, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT DISTINCT ON (index_name) index_name, trade_date, allocation, score, close, ema21, ema50, ema100
		  FROM ref_index_allocation ORDER BY index_name, trade_date DESC`)
	if err != nil {
		return nil, fmt.Errorf("db reader: index allocation: %w", err)
	}
	defer rows.Close()
	have := map[string]*IndicesGradeRow{}
	today := r.now().In(r.ist)
	for rows.Next() {
		var name string
		var td time.Time
		var alloc, close, e21, e50, e100 float64
		var score int
		if err := rows.Scan(&name, &td, &alloc, &score, &close, &e21, &e50, &e100); err != nil {
			return nil, err
		}
		name = strings.ToUpper(strings.TrimSpace(name))
		if today.Sub(td) > 7*24*time.Hour {
			r.logger.Warn("db reader: index allocation is stale", zap.String("index", name), zap.Time("trade_date", td))
		}
		have[name] = &IndicesGradeRow{IndexName: name, TotalScore: score, Allocation: alloc, CMPClosing: close,
			EMA21: e21, EMA50: e50, EMA100: e100}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]*IndicesGradeRow, 0, 3)
	for _, name := range []string{IndexLargeCap, IndexMidCap, IndexSmallCap} {
		if g, ok := have[name]; ok {
			out = append(out, g)
			continue
		}
		r.logger.Warn("db reader: no ref_index_allocation row for index — defaulting allocation to 100%", zap.String("index", name))
		out = append(out, &IndicesGradeRow{IndexName: name, Allocation: 1.0})
	}
	return out, nil
}
