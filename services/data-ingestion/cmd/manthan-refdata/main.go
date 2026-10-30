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
//	daily          eod (latest) + fundamentals (latest 3 quarters) + price-levels — the 08:35 IST cron
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
	flag.Parse()

	logger, _ := zap.NewProduction()
	defer logger.Sync()

	if err := run(*job, *date, *from, *to, *period, *quarters, logger); err != nil {
		logger.Error("manthan-refdata failed", zap.String("job", *job), zap.Error(err))
		os.Exit(1)
	}
}

func run(job, date, from, to, period string, quarters int, logger *zap.Logger) error {
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
	l := &loader{client: client, store: store, logger: logger}

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
	case "daily":
		return l.daily(ctx, quarters)
	}
	return fmt.Errorf("unknown -job %q", job)
}

type loader struct {
	client *refdata.Client
	store  *refdata.Store
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

// eodRange walks [from, to] in ≤31-day chunks (the vendor's range cap).
func (l *loader) eodRange(ctx context.Context, from, to string) error {
	f, err := time.ParseInLocation("2006-01-02", from, ist)
	if err != nil {
		return fmt.Errorf("-from: %w", err)
	}
	t, err := time.ParseInLocation("2006-01-02", to, ist)
	if err != nil {
		return fmt.Errorf("-to: %w", err)
	}
	for cur := f; !cur.After(t); cur = cur.AddDate(0, 0, 31) {
		end := cur.AddDate(0, 0, 30)
		if end.After(t) {
			end = t
		}
		a, b := cur.Format("2006-01-02"), end.Format("2006-01-02")
		if err := l.audited(ctx, "eod", "from="+a+" to="+b, func() (int, error) {
			rows, err := l.client.EODRange(ctx, a, b)
			if err != nil {
				var nf *refdata.NotFoundError
				if errors.As(err, &nf) {
					return 0, nil
				}
				return 0, err
			}
			return l.store.UpsertEOD(ctx, rows)
		}); err != nil {
			return err
		}
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
