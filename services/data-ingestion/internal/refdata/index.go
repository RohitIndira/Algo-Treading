package refdata

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/internal/ema"
)

// Index EMA allocation (operator rule, confirmed 2026-10-30): for each of the
// three bucket indices, six ±1 checks on daily closes vs EMA 21/50/100 give
// a score in −6..+6, mapped to a per-call capital fraction by
// ema.ScoreToAllocation (100/100/75/50/30/20/10%). Recomputed after the
// close, applied to the next day's entries. The scoring code already lived
// in internal/ema; this file only feeds it index closes.
//
// Close sources, both public:
//
//	NSE   https://nsearchives.nseindia.com/content/indices/ind_close_all_DDMMYYYY.csv
//	      — the daily file, authoritative, one row per index
//	Yahoo chart API, range=2y — used to SEED history (496 days, matches NSE
//	      to the paisa on 2026-10-29) and as a fallback when NSE's file is late

// Index names as the strategy knows them, with their NSE and Yahoo spellings.
var bucketIndices = []struct {
	Name  string // Manthan index_name
	NSE   string // "Index Name" column in ind_close_all
	Yahoo string
}{
	{"NIFTY50", "Nifty 50", "^NSEI"},
	{"NFTYMCP150", "Nifty Midcap 150", "NIFTYMIDCAP150.NS"},
	{"NTYSLCP250", "Nifty Smallcap 250", "NIFTYSMLCAP250.NS"},
}

// IndexFetcher pulls index closes from NSE and Yahoo.
type IndexFetcher struct {
	http  *http.Client
	sleep func(time.Duration)
	now   func() time.Time
	// nseBase / yahooBase are overridable for tests.
	nseBase   string
	yahooBase string
}

// NewIndexFetcher builds a fetcher with the public endpoints.
func NewIndexFetcher() *IndexFetcher {
	return &IndexFetcher{
		http:      &http.Client{Timeout: 30 * time.Second},
		sleep:     time.Sleep,
		now:       time.Now,
		nseBase:   "https://nsearchives.nseindia.com/content/indices",
		yahooBase: "https://query2.finance.yahoo.com/v8/finance/chart",
	}
}

// IndexClose is one index on one day.
type IndexClose struct {
	Name  string
	Date  time.Time
	Close float64
}

// NSEDay downloads ind_close_all for one trading day and returns the three
// bucket indices. A 404 (holiday / file not yet published) returns
// NotFoundError.
func (f *IndexFetcher) NSEDay(ctx context.Context, day time.Time) ([]IndexClose, error) {
	u := fmt.Sprintf("%s/ind_close_all_%s.csv", f.nseBase, day.Format("02012006"))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36")
	req.Header.Set("Referer", "https://www.nseindia.com/")
	resp, err := f.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("nse index csv: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, &NotFoundError{Msg: "NSE has no index file for " + day.Format("2006-01-02")}
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("nse index csv: HTTP %d", resp.StatusCode)
	}
	return parseNSEIndexCSV(resp.Body, day)
}

// parseNSEIndexCSV picks the bucket indices out of the NSE file. Columns:
// Index Name, Index Date (DD-MM-YYYY), Open, High, Low, Closing Index Value, …
func parseNSEIndexCSV(r io.Reader, day time.Time) ([]IndexClose, error) {
	cr := csv.NewReader(r)
	cr.FieldsPerRecord = -1
	cr.LazyQuotes = true
	rows, err := cr.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("nse index csv parse: %w", err)
	}
	if len(rows) < 2 {
		return nil, fmt.Errorf("nse index csv: no rows")
	}
	nameCol, dateCol, closeCol := -1, -1, -1
	for i, h := range rows[0] {
		switch strings.ToLower(strings.TrimSpace(h)) {
		case "index name":
			nameCol = i
		case "index date":
			dateCol = i
		case "closing index value":
			closeCol = i
		}
	}
	if nameCol < 0 || closeCol < 0 {
		return nil, fmt.Errorf("nse index csv: header missing Index Name / Closing Index Value: %v", rows[0])
	}
	want := map[string]string{}
	for _, b := range bucketIndices {
		want[strings.ToLower(b.NSE)] = b.Name
	}
	seen := map[string]bool{}
	var out []IndexClose
	for _, row := range rows[1:] {
		if len(row) <= closeCol || len(row) <= nameCol {
			continue
		}
		name, ok := want[strings.ToLower(strings.TrimSpace(row[nameCol]))]
		if !ok || seen[name] {
			continue
		}
		c, err := strconv.ParseFloat(strings.ReplaceAll(strings.TrimSpace(row[closeCol]), ",", ""), 64)
		if err != nil || c <= 0 {
			continue
		}
		d := day
		if dateCol >= 0 && len(row) > dateCol {
			if t, err := time.Parse("02-01-2006", strings.TrimSpace(row[dateCol])); err == nil {
				d = t
			}
		}
		seen[name] = true
		out = append(out, IndexClose{Name: name, Date: d, Close: c})
	}
	if len(out) != len(bucketIndices) {
		missing := []string{}
		for _, b := range bucketIndices {
			if !seen[b.Name] {
				missing = append(missing, b.NSE)
			}
		}
		return out, fmt.Errorf("nse index csv: indices missing from file: %s", strings.Join(missing, ", "))
	}
	return out, nil
}

// YahooHistory returns daily closes for one bucket index over `rng`
// (e.g. "2y"), oldest first, null days skipped. Yahoo's last point is the
// CURRENT session while the market is open (a live quote, not a close), so
// any point dated today (IST) or later is dropped: today's close comes from
// NSE's file after 15:30, never from Yahoo.
func (f *IndexFetcher) YahooHistory(ctx context.Context, yahooSymbol, rng string) ([]IndexClose, string, error) {
	return f.yahooChart(ctx, yahooSymbol, "range="+url.QueryEscape(rng)+"&interval=1d")
}

// YahooDailyBetween returns daily closes in [from, to) — used for the
// pre-2021 ATH seed. Yahoo's `close` is split/bonus-adjusted (not dividend-
// adjusted), the same basis as the vendor's adjusted EOD history.
func (f *IndexFetcher) YahooDailyBetween(ctx context.Context, yahooSymbol string, from, to time.Time) ([]IndexClose, error) {
	rows, _, err := f.yahooChart(ctx, yahooSymbol, fmt.Sprintf("period1=%d&period2=%d&interval=1d", from.Unix(), to.Unix()))
	return rows, err
}

// YahooFirstTrade returns Yahoo's firstTradeDate for a symbol (zero when
// Yahoo does not know it).
func (f *IndexFetcher) YahooFirstTrade(ctx context.Context, yahooSymbol string) (time.Time, error) {
	u := fmt.Sprintf("%s/%s?range=5d&interval=1d", f.yahooBase, url.PathEscape(yahooSymbol))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return time.Time{}, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36")
	resp, err := f.http.Do(req)
	if err != nil {
		return time.Time{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return time.Time{}, fmt.Errorf("yahoo %s: HTTP %d", yahooSymbol, resp.StatusCode)
	}
	var body struct {
		Chart struct {
			Result []struct {
				Meta struct {
					FirstTradeDate int64 `json:"firstTradeDate"`
				} `json:"meta"`
			} `json:"result"`
		} `json:"chart"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return time.Time{}, err
	}
	if len(body.Chart.Result) == 0 || body.Chart.Result[0].Meta.FirstTradeDate <= 0 {
		return time.Time{}, nil
	}
	return time.Unix(body.Chart.Result[0].Meta.FirstTradeDate, 0), nil
}

func (f *IndexFetcher) yahooChart(ctx context.Context, yahooSymbol, query string) ([]IndexClose, string, error) {
	u := fmt.Sprintf("%s/%s?%s", f.yahooBase, url.PathEscape(yahooSymbol), query)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, "", err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36")
	resp, err := f.http.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("yahoo %s: %w", yahooSymbol, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("yahoo %s: HTTP %d", yahooSymbol, resp.StatusCode)
	}
	var body struct {
		Chart struct {
			Result []struct {
				Meta struct {
					ShortName string `json:"shortName"`
				} `json:"meta"`
				Timestamp  []int64 `json:"timestamp"`
				Indicators struct {
					Quote []struct {
						Close []*float64 `json:"close"`
					} `json:"quote"`
				} `json:"indicators"`
			} `json:"result"`
			Error *struct {
				Description string `json:"description"`
			} `json:"error"`
		} `json:"chart"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, "", fmt.Errorf("yahoo %s: decode: %w", yahooSymbol, err)
	}
	if body.Chart.Error != nil {
		return nil, "", fmt.Errorf("yahoo %s: %s", yahooSymbol, body.Chart.Error.Description)
	}
	if len(body.Chart.Result) == 0 || len(body.Chart.Result[0].Indicators.Quote) == 0 {
		return nil, "", fmt.Errorf("yahoo %s: empty result", yahooSymbol)
	}
	r := body.Chart.Result[0]
	ist, _ := time.LoadLocation("Asia/Kolkata")
	n := f.now().In(ist)
	today := time.Date(n.Year(), n.Month(), n.Day(), 0, 0, 0, 0, time.UTC)
	var out []IndexClose
	dropped := 0
	for i, ts := range r.Timestamp {
		if i >= len(r.Indicators.Quote[0].Close) {
			break
		}
		c := r.Indicators.Quote[0].Close[i]
		if c == nil || *c <= 0 {
			continue
		}
		d := time.Unix(ts, 0).In(ist)
		day := time.Date(d.Year(), d.Month(), d.Day(), 0, 0, 0, 0, time.UTC)
		if !day.Before(today) {
			dropped++
			continue
		}
		out = append(out, IndexClose{Date: day, Close: *c})
	}
	_ = dropped
	return out, r.Meta.ShortName, nil
}

// UpsertIndexCloses stores closes; later loads of the same day overwrite
// (NSE's file is authoritative over Yahoo's intraday value for "today").
func (s *Store) UpsertIndexCloses(ctx context.Context, rows []IndexClose, source string) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO ref_index_close (index_name, trade_date, close, source, loaded_at)
		VALUES ($1, $2, $3, $4, now())
		ON CONFLICT (index_name, trade_date) DO UPDATE SET close = EXCLUDED.close, source = EXCLUDED.source, loaded_at = now()`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	n := 0
	for _, r := range rows {
		if r.Name == "" || r.Close <= 0 {
			continue
		}
		if _, err := stmt.ExecContext(ctx, r.Name, r.Date.Format("2006-01-02"), r.Close, source); err != nil {
			return n, fmt.Errorf("index close upsert %s %s: %w", r.Name, r.Date.Format("2006-01-02"), err)
		}
		n++
	}
	return n, tx.Commit()
}

// IndexAllocation is one computed row of ref_index_allocation.
type IndexAllocation struct {
	Name       string
	Date       time.Time
	Close      float64
	EMA21      float64
	EMA50      float64
	EMA100     float64
	Score      int
	Allocation float64
	Days       int // closes used
}

// ComputeIndexAllocations scores every bucket index on its latest stored
// close and upserts ref_index_allocation. minDays guards against scoring on
// a thin seed (EMA100 on 30 closes is meaningless): indices with fewer
// closes are skipped and reported.
func (s *Store) ComputeIndexAllocations(ctx context.Context, minDays int, logger *zap.Logger) ([]IndexAllocation, error) {
	var out []IndexAllocation
	for _, b := range bucketIndices {
		rows, err := s.db.QueryContext(ctx, `
			SELECT trade_date, close FROM ref_index_close WHERE index_name = $1 ORDER BY trade_date`, b.Name)
		if err != nil {
			return nil, fmt.Errorf("index closes %s: %w", b.Name, err)
		}
		var closes []float64
		var last time.Time
		for rows.Next() {
			var d time.Time
			var c float64
			if err := rows.Scan(&d, &c); err != nil {
				rows.Close()
				return nil, err
			}
			closes = append(closes, c)
			last = d
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
		if len(closes) < minDays {
			logger.Warn("index ema: too few closes to score — allocation NOT updated",
				zap.String("index", b.Name), zap.Int("closes", len(closes)), zap.Int("min", minDays))
			continue
		}
		e21 := ema.ComputeEMA(closes, ema.Period21)
		e50 := ema.ComputeEMA(closes, ema.Period50)
		e100 := ema.ComputeEMA(closes, ema.Period100)
		price := closes[len(closes)-1]
		score := ema.ComputeScore(price, e21, e50, e100)
		a := IndexAllocation{Name: b.Name, Date: last, Close: price, EMA21: e21, EMA50: e50, EMA100: e100,
			Score: score, Allocation: ema.ScoreToAllocation(score), Days: len(closes)}
		if _, err := s.db.ExecContext(ctx, `
			INSERT INTO ref_index_allocation (index_name, trade_date, close, ema21, ema50, ema100, score, allocation, computed_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now())
			ON CONFLICT (index_name, trade_date) DO UPDATE SET close = EXCLUDED.close, ema21 = EXCLUDED.ema21,
				ema50 = EXCLUDED.ema50, ema100 = EXCLUDED.ema100, score = EXCLUDED.score,
				allocation = EXCLUDED.allocation, computed_at = now()`,
			a.Name, a.Date.Format("2006-01-02"), a.Close, a.EMA21, a.EMA50, a.EMA100, a.Score, a.Allocation); err != nil {
			return nil, fmt.Errorf("index allocation upsert %s: %w", b.Name, err)
		}
		logger.Info("index ema: scored", zap.String("index", a.Name), zap.String("date", a.Date.Format("2006-01-02")),
			zap.Float64("close", a.Close), zap.Float64("ema21", a.EMA21), zap.Float64("ema50", a.EMA50),
			zap.Float64("ema100", a.EMA100), zap.Int("score", a.Score), zap.Float64("allocation", a.Allocation))
		out = append(out, a)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// BucketIndexYahooSymbols lists (manthan name, yahoo symbol) for seeding.
func BucketIndexYahooSymbols() [][2]string {
	out := make([][2]string, 0, len(bucketIndices))
	for _, b := range bucketIndices {
		out = append(out, [2]string{b.Name, b.Yahoo})
	}
	return out
}
