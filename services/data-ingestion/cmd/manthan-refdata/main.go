// manthan-refdata — loads Manthan reference data from the vendor APIs into
// signals_db ref_* tables (migration 013). Replaces the Google Sheet as the
// source of fundamentals, market cap, codes, industry and price levels.
//
// Jobs (-job):
//
//	eod            one trading day of /equities/eod (-date YYYY-MM-DD, default latest)
//	eod-range      every trading day in [-from, -to] (≤31 days per call; loops in 31-day chunks)
//	fundamentals   one quarter of /equities/fundamentals for all three bases (-period YYYY-MM, default latest)
//	price-levels   the vendor's intraday ATH / 52w snapshot
//	ath            recompute our close-based ATH + 52w closes for every symbol from ref_equity_eod
//	ath-seed       fetch Yahoo pre-2021 daily closes for symbols whose real peak may predate our
//	               history (-refetch to redo all hinted symbols), then rerun ath
//	index-seed     seed ref_index_close from Yahoo (-range 2y) for the three bucket indices
//	index-ema      load the NSE index closes for -date (default: latest EOD day) and recompute
//	               EMA 21/50/100 → score → allocation into ref_index_allocation
//	daily          eod + fundamentals (latest 3 quarters) + price-levels + ath + index-ema — the 08:35 IST cron
//
// Env: MANTHAN_EQ_API_KEY (required), MANTHAN_EQ_API_BASE (default vendor root),
// and the MARKET_DATA_DB_* variables data-ingestion already uses for signals_db.
//
// Every run writes a ref_load_runs row, success or failure, so the admin
// console can show "last loaded at" without reading logs.
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"go.uber.org/zap"

	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/config"
	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/internal/refdata"
)

var ist = mustLoad("Asia/Kolkata")

func mustLoad(name string) *time.Location {
	l, err := time.LoadLocation(name)
	if err != nil {
		panic(err)
	}
	return l
}

func main() {
	job := flag.String("job", "daily", "eod | eod-range | fundamentals | price-levels | daily")
	date := flag.String("date", "", "eod: trading day YYYY-MM-DD (default: vendor latest)")
	from := flag.String("from", "", "eod-range: first day YYYY-MM-DD")
	to := flag.String("to", "", "eod-range: last day YYYY-MM-DD")
	period := flag.String("period", "", "fundamentals: quarter end YYYY-MM (default: vendor latest)")
	quarters := flag.Int("quarters", 3, "daily: how many latest quarters to refresh")
	rng := flag.String("range", "2y", "index-seed: Yahoo range")
	refetch := flag.Bool("refetch", false, "ath-seed: refetch symbols that already have a pre-2021 row")
	flag.Parse()

	logger, _ := zap.NewProduction()
	defer logger.Sync()

	if err := run(*job, *date, *from, *to, *period, *quarters, *rng, *refetch, logger); err != nil {
		logger.Error("manthan-refdata failed", zap.String("job", *job), zap.Error(err))
		os.Exit(1)
	}
}

func run(job, date, from, to, period string, quarters int, rng string, refetch bool, logger *zap.Logger) error {
	apiKey := strings.TrimSpace(os.Getenv("MANTHAN_EQ_API_KEY"))
	if apiKey == "" {
		return errors.New("MANTHAN_EQ_API_KEY is not set")
	}
	client := refdata.NewClient(os.Getenv("MANTHAN_EQ_API_BASE"), apiKey)

	cfg := config.Load()
	db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		cfg.PGHost, cfg.PGPort, cfg.PGUser, cfg.PGPassword, cfg.PGDatabase, cfg.PGSSLMode))
	if err != nil {
		return fmt.Errorf("db open: %w", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		return fmt.Errorf("db ping (%s/%s): %w", cfg.PGHost, cfg.PGDatabase, err)
	}
	store := refdata.NewStore(db)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	l := &loader{client: client, store: store, index: refdata.NewIndexFetcher(), logger: logger}

	switch job {
	case "eod":
		return l.eod(ctx, date)
	case "eod-range":
		if from == "" || to == "" {
			return errors.New("eod-range needs -from and -to")
		}
		return l.eodRange(ctx, from, to)
	case "fundamentals":
		return l.fundamentals(ctx, period)
	case "price-levels":
		return l.priceLevels(ctx)
	case "ath":
		return l.ath(ctx)
	case "ath-seed":
		if err := l.athSeed(ctx, refetch); err != nil {
			return err
		}
		return l.ath(ctx)
	case "index-seed":
		return l.indexSeed(ctx, rng)
	case "index-ema":
		return l.indexEMA(ctx, date)
	case "daily":
		return l.daily(ctx, quarters)
	}
	return fmt.Errorf("unknown -job %q", job)
}

type loader struct {
	client *refdata.Client
	store  *refdata.Store
	index  *refdata.IndexFetcher
	logger *zap.Logger
}

// audited wraps one unit of work with a ref_load_runs row.
func (l *loader) audited(ctx context.Context, kind, params string, fn func() (int, error)) error {
	started := time.Now()
	n, err := fn()
	rec := refdata.RunRecord{Kind: kind, Params: params, Rows: n, Err: err, StartedAt: started}
	if aerr := l.store.RecordRun(ctx, rec); aerr != nil {
		l.logger.Warn("ref_load_runs write failed", zap.String("kind", kind), zap.Error(aerr))
	}
	if err != nil {
		l.logger.Error("load failed", zap.String("kind", kind), zap.String("params", params), zap.Int("rows", n), zap.Error(err))
		return err
	}
	l.logger.Info("load ok", zap.String("kind", kind), zap.String("params", params), zap.Int("rows", n),
		zap.Duration("took", time.Since(started)))
	return nil
}

func (l *loader) eod(ctx context.Context, date string) error {
	return l.audited(ctx, "eod", "date="+orLatest(date), func() (int, error) {
		rows, err := l.client.EOD(ctx, date)
		if err != nil {
			var nf *refdata.NotFoundError
			if errors.As(err, &nf) {
				// Not a trading day: nothing to load, not a failure.
				l.logger.Info("eod: vendor has no data for day (holiday?)", zap.String("date", date), zap.String("msg", nf.Msg))
				return 0, nil
			}
			return 0, err
		}
		return l.store.UpsertEOD(ctx, rows)
	})
}

// eodRange loads every trading day in [from, to], one vendor call per day.
// The vendor's from/to range form pages the SAME rows (1000 per page) but
// answers each page in ~9 s versus ~0.5 s for a single-day page, so per-day
// is ~15× faster for a backfill. Weekends are skipped; holidays come back
// as 404 and are logged, not failed. Days already present are skipped, so
// an interrupted backfill resumes where it stopped.
func (l *loader) eodRange(ctx context.Context, from, to string) error {
	f, err := time.ParseInLocation("2006-01-02", from, ist)
	if err != nil {
		return fmt.Errorf("-from: %w", err)
	}
	t, err := time.ParseInLocation("2006-01-02", to, ist)
	if err != nil {
		return fmt.Errorf("-to: %w", err)
	}
	loaded, err := l.store.LoadedEODDates(ctx)
	if err != nil {
		return err
	}
	days, skipped := 0, 0
	var failed []string
	for cur := f; !cur.After(t); cur = cur.AddDate(0, 0, 1) {
		if wd := cur.Weekday(); wd == time.Saturday || wd == time.Sunday {
			continue
		}
		ds := cur.Format("2006-01-02")
		if loaded[ds] {
			skipped++
			continue
		}
		// One bad day (after the client's own retries) does not stop the
		// backfill: carry on, report the gaps at the end; a rerun fills them.
		if err := l.eod(ctx, ds); err != nil {
			failed = append(failed, ds)
			if ctx.Err() != nil {
				break
			}
			continue
		}
		days++
	}
	l.logger.Info("eod-range done", zap.String("from", from), zap.String("to", to), zap.Int("days_loaded", days),
		zap.Int("days_already_present", skipped), zap.Strings("days_failed", failed))
	if len(failed) > 0 {
		return fmt.Errorf("eod-range: %d day(s) failed (rerun the same command to fill them): %s", len(failed), strings.Join(failed, ", "))
	}
	return nil
}

// fundamentals loads one quarter under all three bases so the operator's
// basis decision ("preferred", 2026-10-30) can be revisited without a reload.
func (l *loader) fundamentals(ctx context.Context, period string) error {
	for _, basis := range []string{"preferred", "consolidated", "standalone"} {
		if err := l.audited(ctx, "fundamentals", "period="+orLatest(period)+" basis="+basis, func() (int, error) {
			rows, err := l.client.Fundamentals(ctx, period, basis)
			if err != nil {
				return 0, err
			}
			return l.store.UpsertFundamentals(ctx, basis, rows)
		}); err != nil {
			return err
		}
	}
	return nil
}

func (l *loader) priceLevels(ctx context.Context) error {
	return l.audited(ctx, "price_levels", "", func() (int, error) {
		rows, err := l.client.PriceLevels(ctx)
		if err != nil {
			return 0, err
		}
		return l.store.UpsertPriceLevels(ctx, rows)
	})
}

// ath recomputes the close-based ATH and 52-week closes for every symbol
// from everything in ref_equity_eod up to the latest loaded day. Same job
// seeds and rolls forward (idempotent).
func (l *loader) ath(ctx context.Context) error {
	asOf, err := l.store.LatestEODDate(ctx)
	if err != nil {
		return err
	}
	if asOf.IsZero() {
		return errors.New("ath: ref_equity_eod is empty — run -job eod-range first")
	}
	return l.audited(ctx, "ath_roll", "as_of="+asOf.Format("2006-01-02"), func() (int, error) {
		return l.store.ComputeATH(ctx, asOf, l.logger)
	})
}

// athSeed fetches Yahoo split-adjusted daily closes 2000-01-01..2020-12-31
// for every symbol whose vendor ATH hints at a pre-2021 peak. A symbol Yahoo
// has nothing for is stored with days=0 so it is not refetched daily, and
// stays "unverified" (excluded from triggers) until -refetch finds data.
func (l *loader) athSeed(ctx context.Context, refetch bool) error {
	syms, err := l.store.ATHSeedCandidates(ctx, refetch)
	if err != nil {
		return err
	}
	from := time.Date(2000, 1, 1, 0, 0, 0, 0, ist)
	to := time.Date(2021, 1, 1, 0, 0, 0, 0, ist)
	return l.audited(ctx, "ath_seed", fmt.Sprintf("candidates=%d refetch=%v", len(syms), refetch), func() (int, error) {
		found, none, failed := 0, 0, 0
		for i, sym := range syms {
			rows, ferr := l.index.YahooDailyBetween(ctx, sym+".NS", from, to)
			if ferr != nil && !strings.Contains(ferr.Error(), "HTTP 404") && !strings.Contains(ferr.Error(), "No data found") {
				failed++
				l.logger.Warn("ath-seed: yahoo failed — will retry next run", zap.String("symbol", sym), zap.Error(ferr))
				time.Sleep(2 * time.Second)
				continue
			}
			if err := l.store.UpsertPre2021(ctx, sym, rows, "yahoo"); err != nil {
				return found, err
			}
			if len(rows) > 0 {
				found++
			} else {
				none++
			}
			if (i+1)%100 == 0 {
				l.logger.Info("ath-seed progress", zap.Int("done", i+1), zap.Int("of", len(syms)))
			}
			time.Sleep(300 * time.Millisecond) // be polite to Yahoo
		}
		l.logger.Info("ath-seed done", zap.Int("candidates", len(syms)), zap.Int("with_pre2021_history", found),
			zap.Int("no_pre2021_history", none), zap.Int("failed", failed))
		if failed > 0 {
			return found + none, fmt.Errorf("ath-seed: %d symbol(s) failed, rerun to retry", failed)
		}
		return found + none, nil
	})
}

// indexSeed loads Yahoo daily history for the three bucket indices.
func (l *loader) indexSeed(ctx context.Context, rng string) error {
	var errs []error
	for _, pair := range refdata.BucketIndexYahooSymbols() {
		name, ysym := pair[0], pair[1]
		if err := l.audited(ctx, "index_seed", "index="+name+" range="+rng, func() (int, error) {
			rows, short, err := l.index.YahooHistory(ctx, ysym, rng)
			if err != nil {
				return 0, err
			}
			for i := range rows {
				rows[i].Name = name
			}
			l.logger.Info("index seed", zap.String("index", name), zap.String("yahoo", ysym), zap.String("yahoo_name", short), zap.Int("days", len(rows)))
			return l.store.UpsertIndexCloses(ctx, rows, "yahoo")
		}); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// indexEMA loads the NSE closes for one day (default: the latest EOD day
// in ref_equity_eod, so indices and stocks line up) and rescores. When NSE
// has no file yet, Yahoo's 5-day window fills the day instead.
func (l *loader) indexEMA(ctx context.Context, date string) error {
	var day time.Time
	if date != "" {
		var err error
		if day, err = time.ParseInLocation("2006-01-02", date, ist); err != nil {
			return fmt.Errorf("-date: %w", err)
		}
	} else {
		var err error
		if day, err = l.store.LatestEODDate(ctx); err != nil {
			return err
		}
		if day.IsZero() {
			return errors.New("index-ema: no EOD day loaded and no -date given")
		}
	}
	ds := day.Format("2006-01-02")
	if err := l.audited(ctx, "index_close", "date="+ds, func() (int, error) {
		rows, err := l.index.NSEDay(ctx, day)
		if err == nil {
			return l.store.UpsertIndexCloses(ctx, rows, "nse")
		}
		var nf *refdata.NotFoundError
		if !errors.As(err, &nf) {
			l.logger.Warn("index: NSE file failed, falling back to Yahoo", zap.Error(err))
		} else {
			l.logger.Info("index: NSE file not published for day, falling back to Yahoo", zap.String("date", ds))
		}
		n := 0
		for _, pair := range refdata.BucketIndexYahooSymbols() {
			hist, _, yerr := l.index.YahooHistory(ctx, pair[1], "5d")
			if yerr != nil {
				return n, yerr
			}
			for _, h := range hist {
				if h.Date.Format("2006-01-02") == ds {
					h.Name = pair[0]
					k, uerr := l.store.UpsertIndexCloses(ctx, []refdata.IndexClose{h}, "yahoo")
					if uerr != nil {
						return n, uerr
					}
					n += k
				}
			}
		}
		if n == 0 {
			return 0, fmt.Errorf("index: neither NSE nor Yahoo has closes for %s", ds)
		}
		return n, nil
	}); err != nil {
		return err
	}
	return l.audited(ctx, "index_ema", "", func() (int, error) {
		rows, err := l.store.ComputeIndexAllocations(ctx, 150, l.logger)
		return len(rows), err
	})
}

// daily is the 08:35 IST job: yesterday's closes, the latest N quarters
// (results season fills the newest quarter gradually, so refresh several),
// and the vendor's price levels. Each step is audited on its own; a failure
// in one does not stop the others.
func (l *loader) daily(ctx context.Context, quarters int) error {
	var errs []error
	if err := l.eod(ctx, ""); err != nil {
		errs = append(errs, err)
	}
	for _, p := range latestQuarters(time.Now().In(ist), quarters) {
		if err := l.fundamentals(ctx, p); err != nil {
			errs = append(errs, err)
		}
	}
	if err := l.priceLevels(ctx); err != nil {
		errs = append(errs, err)
	}
	if err := l.ath(ctx); err != nil {
		errs = append(errs, err)
	}
	// New hints (fresh listings, a vendor level that moved) get their Yahoo
	// lookup, then the ATH is recomputed with them. No-op when none are new.
	if err := l.athSeed(ctx, false); err != nil {
		errs = append(errs, err)
	} else if err := l.ath(ctx); err != nil {
		errs = append(errs, err)
	}
	if err := l.indexEMA(ctx, ""); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// latestQuarters lists the n most recent quarter-end labels (YYYY-MM) whose
// quarter has ENDED as of now, newest first. On 2026-10-30 with n=3 that is
// 2026-09, 2026-06, 2026-03.
func latestQuarters(now time.Time, n int) []string {
	y, m := now.Year(), int(now.Month())
	// Last completed quarter end month: 3, 6, 9 or 12.
	qm := ((m - 1) / 3) * 3
	if qm == 0 {
		qm = 12
		y--
	}
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, fmt.Sprintf("%04d-%02d", y, qm))
		qm -= 3
		if qm == 0 {
			qm = 12
			y--
		}
	}
	return out
}

func orLatest(s string) string {
	if s == "" {
		return "latest"
	}
	return s
}
