package historical

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// APIFetcher fetches 52W high/low data directly from NSE and BSE APIs.
// This is the single source of truth — no computation needed.
// Exchanges handle stock splits, adjustments, corporate actions internally.
type APIFetcher struct {
	nseClient *http.Client
	bseClient *http.Client
	redis     *redis.Client
	repo      *Repository
	logger    *zap.Logger
}

// NewAPIFetcher creates a new API fetcher with proper HTTP clients.
func NewAPIFetcher(repo *Repository, redisClient *redis.Client, logger *zap.Logger) (*APIFetcher, error) {
	jar, err := cookiejar.New(nil)
	if err != nil {
		return nil, err
	}

	return &APIFetcher{
		nseClient: &http.Client{
			Jar:     jar,
			Timeout: 15 * time.Second,
		},
		bseClient: &http.Client{
			Timeout: 15 * time.Second,
		},
		redis:  redisClient,
		repo:   repo,
		logger: logger,
	}, nil
}

// Stock52WData holds 52W data for a single stock from exchange API.
type Stock52WData struct {
	Symbol    string
	Token     string // Exchange token (NSE token or BSE scrip code)
	Exchange  string
	High52W   float64
	Low52W    float64
	HighDate  string // "06-Apr-2026"
	LowDate   string
	LTP       float64
}

// DiscoverNSEInstruments fetches the full list of NSE equity stocks and adds
// any missing ones to our instruments table. This catches new IPOs, name changes,
// and series changes that our DB doesn't know about.
func (f *APIFetcher) DiscoverNSEInstruments(ctx context.Context) error {
	f.logger.Info("Discovering new NSE instruments")

	if err := f.refreshNSECookie(); err != nil {
		return fmt.Errorf("NSE cookie: %w", err)
	}

	// NSE provides a CSV of all listed securities
	url := "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
	req.Header.Set("Referer", "https://www.nseindia.com/")

	resp, err := f.nseClient.Do(req)
	if err != nil {
		return fmt.Errorf("fetch equity list: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("equity list HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	// Parse CSV: SYMBOL, NAME OF COMPANY, SERIES, DATE OF LISTING, PAID UP VALUE, MARKET LOT, ISIN NUMBER, FACE VALUE
	lines := strings.Split(string(body), "\n")
	added := 0
	for i, line := range lines {
		if i == 0 {
			continue // Skip header
		}
		fields := strings.Split(line, ",")
		if len(fields) < 7 {
			continue
		}

		symbol := strings.TrimSpace(fields[0])
		series := strings.TrimSpace(fields[2])
		isin := strings.TrimSpace(fields[6])

		if symbol == "" {
			continue
		}

		// Check if exists
		existing, _, _ := f.repo.GetInstrumentBySymbol(ctx, symbol, "NSE")
		if existing > 0 {
			continue // Already in DB
		}

		// Add new instrument
		_, err := f.repo.db.ExecContext(ctx, `
			INSERT INTO instruments (token, symbol, series, isin, exchange, is_active)
			VALUES ($1, $2, $3, $4, 'NSE', TRUE)
			ON CONFLICT (token, exchange) DO NOTHING`,
			symbol, symbol, series, isin)
		if err == nil {
			added++
		}
	}

	f.logger.Info("NSE instrument discovery complete",
		zap.Int("new_instruments", added),
		zap.Int("total_lines", len(lines)))
	return nil
}

// SyncAll fetches 52W data from both NSE and BSE APIs and updates DB + Redis.
// Designed to run daily at 6:00 AM before market opens.
func (f *APIFetcher) SyncAll(ctx context.Context) error {
	start := time.Now()
	f.logger.Info("Starting 52W API sync (NSE + BSE)")

	// Step 0: Discover new instruments (IPOs, name changes)
	if err := f.DiscoverNSEInstruments(ctx); err != nil {
		f.logger.Warn("NSE instrument discovery failed (non-fatal)", zap.Error(err))
	}

	// NSE
	nseCount, nseErr := f.SyncNSE(ctx)
	if nseErr != nil {
		f.logger.Error("NSE API sync failed", zap.Error(nseErr))
	}

	// BSE
	bseCount, bseErr := f.SyncBSE(ctx)
	if bseErr != nil {
		f.logger.Error("BSE API sync failed", zap.Error(bseErr))
	}

	f.logger.Info("52W API sync complete",
		zap.Int("nse_updated", nseCount),
		zap.Int("bse_updated", bseCount),
		zap.Duration("duration", time.Since(start)))

	if nseErr != nil {
		return nseErr
	}
	return bseErr
}

// --- NSE API ---

// SyncNSE fetches 52W data for all NSE stocks and updates DB + Redis.
// Uses NSE India API: /api/quote-equity?symbol={SYMBOL}
// Rate limit: 1 request per second (NSE blocks faster requests).
func (f *APIFetcher) SyncNSE(ctx context.Context) (int, error) {
	// Get all NSE symbols from DB
	symbols, err := f.repo.GetAllNSESymbols(ctx)
	if err != nil {
		return 0, fmt.Errorf("get NSE symbols: %w", err)
	}

	f.logger.Info("NSE API sync starting", zap.Int("stocks", len(symbols)))

	// Get session cookie
	if err := f.refreshNSECookie(); err != nil {
		return 0, fmt.Errorf("NSE cookie: %w", err)
	}

	updated := 0
	failed := 0

	for i, symbol := range symbols {
		if ctx.Err() != nil {
			return updated, ctx.Err()
		}

		// Refresh cookie every 100 requests
		if i > 0 && i%100 == 0 {
			f.refreshNSECookie()
			time.Sleep(time.Second)
		}

		time.Sleep(time.Second) // NSE rate limit

		data, err := f.fetchNSEStock(symbol)
		if err != nil {
			failed++
			continue
		}

		// Save to DB + Redis
		f.saveToRedisAndDB(ctx, data)
		updated++

		if (i+1)%100 == 0 {
			f.logger.Info("NSE sync progress",
				zap.Int("done", i+1),
				zap.Int("total", len(symbols)),
				zap.Int("updated", updated))
		}
	}

	f.logger.Info("NSE sync complete",
		zap.Int("updated", updated),
		zap.Int("failed", failed))
	return updated, nil
}

func (f *APIFetcher) refreshNSECookie() error {
	req, _ := http.NewRequest("GET", "https://www.nseindia.com", nil)
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
	resp, err := f.nseClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.ReadAll(resp.Body)
	return nil
}

func (f *APIFetcher) fetchNSEStock(symbol string) (*Stock52WData, error) {
	url := fmt.Sprintf("https://www.nseindia.com/api/quote-equity?symbol=%s", symbol)
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
	req.Header.Set("Referer", "https://www.nseindia.com/")

	resp, err := f.nseClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	var nseResp struct {
		PriceInfo struct {
			LastPrice   float64 `json:"lastPrice"`
			WeekHighLow struct {
				Max     float64 `json:"max"`
				MaxDate string  `json:"maxDate"`
				Min     float64 `json:"min"`
				MinDate string  `json:"minDate"`
			} `json:"weekHighLow"`
		} `json:"priceInfo"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&nseResp); err != nil {
		return nil, err
	}

	if nseResp.PriceInfo.WeekHighLow.Max <= 0 {
		return nil, fmt.Errorf("no 52W data")
	}

	return &Stock52WData{
		Symbol:   symbol,
		Token:    symbol, // NSE uses symbol as token in our system
		Exchange: "NSE",
		High52W:  nseResp.PriceInfo.WeekHighLow.Max,
		Low52W:   nseResp.PriceInfo.WeekHighLow.Min,
		HighDate: nseResp.PriceInfo.WeekHighLow.MaxDate,
		LowDate:  nseResp.PriceInfo.WeekHighLow.MinDate,
		LTP:      nseResp.PriceInfo.LastPrice,
	}, nil
}

// --- BSE API ---

// SyncBSE fetches 52W data for all BSE stocks and updates DB + Redis.
// Uses BSE API: /BseIndiaAPI/api/HighLow/w?Type=EQ&flag=C&scripcode={CODE}
// Rate limit: 2 requests per second (BSE is more lenient).
func (f *APIFetcher) SyncBSE(ctx context.Context) (int, error) {
	// Get all BSE scrip codes from DB
	type bseInst struct {
		Token  string
		Symbol string
	}
	rows, err := f.repo.db.QueryContext(ctx,
		`SELECT token, symbol FROM instruments WHERE exchange='BSE' AND is_active=TRUE AND token ~ '^[0-9]+$'`)
	if err != nil {
		return 0, fmt.Errorf("get BSE instruments: %w", err)
	}
	defer rows.Close()

	var instruments []bseInst
	for rows.Next() {
		var inst bseInst
		rows.Scan(&inst.Token, &inst.Symbol)
		instruments = append(instruments, inst)
	}

	f.logger.Info("BSE API sync starting", zap.Int("stocks", len(instruments)))

	updated := 0
	failed := 0

	for i, inst := range instruments {
		if ctx.Err() != nil {
			return updated, ctx.Err()
		}

		time.Sleep(500 * time.Millisecond) // BSE rate limit (2 req/sec)

		data, err := f.fetchBSEStock(inst.Token, inst.Symbol)
		if err != nil {
			failed++
			continue
		}

		f.saveToRedisAndDB(ctx, data)
		updated++

		if (i+1)%100 == 0 {
			f.logger.Info("BSE sync progress",
				zap.Int("done", i+1),
				zap.Int("total", len(instruments)),
				zap.Int("updated", updated))
		}
	}

	f.logger.Info("BSE sync complete",
		zap.Int("updated", updated),
		zap.Int("failed", failed))
	return updated, nil
}

func (f *APIFetcher) fetchBSEStock(scripCode, symbol string) (*Stock52WData, error) {
	url := fmt.Sprintf("https://api.bseindia.com/BseIndiaAPI/api/HighLow/w?Type=EQ&flag=C&scripcode=%s", scripCode)
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
	req.Header.Set("Referer", "https://www.bseindia.com/")
	req.Header.Set("Origin", "https://www.bseindia.com")

	resp, err := f.bseClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	var bseResp struct {
		High52W string `json:"Fifty2WkHigh_adj"`
		HighDt  string `json:"Fifty2WkHigh_adjDt"`
		Low52W  string `json:"Fifty2WkLow_adj"`
		LowDt   string `json:"Fifty2WkLow_adjDt"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&bseResp); err != nil {
		return nil, err
	}

	// Clean values — BSE sometimes includes date in the value like "1611.20 (05/01/2026)"
	highStr := strings.Split(bseResp.High52W, "(")[0]
	lowStr := strings.Split(bseResp.Low52W, "(")[0]

	high, _ := strconv.ParseFloat(strings.TrimSpace(highStr), 64)
	low, _ := strconv.ParseFloat(strings.TrimSpace(lowStr), 64)

	if high <= 0 {
		return nil, fmt.Errorf("no 52W data")
	}

	return &Stock52WData{
		Symbol:   symbol,
		Token:    scripCode,
		Exchange: "BSE",
		High52W:  high,
		Low52W:   low,
		HighDate: strings.Trim(bseResp.HighDt, " ()"),
		LowDate:  strings.Trim(bseResp.LowDt, " ()"),
	}, nil
}

// --- Save to Redis + DB ---

func (f *APIFetcher) saveToRedisAndDB(ctx context.Context, data *Stock52WData) {
	if f.redis == nil {
		return
	}

	val, _ := json.Marshal(map[string]interface{}{
		"high":      data.High52W,
		"low":       data.Low52W,
		"last_close": data.LTP,
		"symbol":    data.Symbol,
		"token":     data.Token,
		"exchange":  data.Exchange,
		"high_date": data.HighDate,
		"low_date":  data.LowDate,
		"as_of":     time.Now().Format("2006-01-02"),
		"source":    data.Exchange + "_api",
	})

	ttl := 120 * time.Hour // 5 days

	pipe := f.redis.Pipeline()

	// Save by token (BSE scrip code or NSE symbol)
	pipe.Set(ctx, fmt.Sprintf("52w:token:%s", data.Token), val, ttl)

	// Also save by symbol if different from token (for NSE token numbers)
	if data.Symbol != "" && data.Symbol != data.Token {
		pipe.Set(ctx, fmt.Sprintf("52w:token:%s", data.Symbol), val, ttl)
	}

	pipe.Exec(ctx)
}
