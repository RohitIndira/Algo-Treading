package ema

import (
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
)

// BhavcopyFetcher downloads NSE sec_bhavdata_full CSV and extracts close prices.
// Used daily at 6 AM to get yesterday's official close for EMA updates.
type BhavcopyFetcher struct {
	client *http.Client
	logger *zap.Logger
}

// NewBhavcopyFetcher creates a new fetcher with NSE cookie support.
func NewBhavcopyFetcher(logger *zap.Logger) *BhavcopyFetcher {
	jar, _ := cookiejar.New(nil)
	return &BhavcopyFetcher{
		client: &http.Client{Jar: jar, Timeout: 30 * time.Second},
		logger: logger,
	}
}

// FetchCloses downloads bhavcopy for a date and returns close prices.
// Returns map[symbol]closePrice for all EQ series stocks.
func (f *BhavcopyFetcher) FetchCloses(date time.Time) (map[string]float64, error) {
	// Get NSE cookie first
	req, _ := http.NewRequest("GET", "https://www.nseindia.com", nil)
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
	resp, err := f.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("NSE cookie: %w", err)
	}
	resp.Body.Close()
	time.Sleep(500 * time.Millisecond)

	// Download sec_bhavdata_full CSV
	dateStr := date.Format("02012006") // DDMMYYYY
	url := fmt.Sprintf("https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_%s.csv", dateStr)

	req, _ = http.NewRequest("GET", url, nil)
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
	req.Header.Set("Referer", "https://www.nseindia.com/")

	resp, err = f.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("download bhavcopy: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		return nil, fmt.Errorf("no bhavcopy for %s (holiday)", date.Format("2006-01-02"))
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// Parse CSV
	// Columns: SYMBOL, SERIES, DATE1, PREV_CLOSE, OPEN_PRICE, HIGH_PRICE, LOW_PRICE,
	//          LAST_PRICE, CLOSE_PRICE, AVG_PRICE, TTL_TRD_QNTY, ...
	reader := csv.NewReader(strings.NewReader(string(body)))
	reader.TrimLeadingSpace = true
	reader.LazyQuotes = true

	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}

	// Map column names to indexes
	colIdx := make(map[string]int)
	for i, col := range header {
		colIdx[strings.TrimSpace(col)] = i
	}

	symIdx, symOK := colIdx["SYMBOL"]
	seriesIdx, seriesOK := colIdx["SERIES"]
	closeIdx, closeOK := colIdx["CLOSE_PRICE"]

	if !symOK || !seriesOK || !closeOK {
		return nil, fmt.Errorf("missing columns in CSV")
	}

	result := make(map[string]float64, 4000)

	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}

		if len(row) <= closeIdx {
			continue
		}

		series := strings.TrimSpace(row[seriesIdx])
		// Only EQ series (regular equity) — skip BE, BZ, SM, ST etc for EMA
		if series != "EQ" {
			continue
		}

		symbol := strings.TrimSpace(row[symIdx])
		closeStr := strings.TrimSpace(row[closeIdx])
		closePrice, _ := strconv.ParseFloat(closeStr, 64)

		if symbol != "" && closePrice > 0 {
			result[symbol] = closePrice
		}
	}

	f.logger.Info("Bhavcopy close prices extracted",
		zap.String("date", date.Format("2006-01-02")),
		zap.Int("stocks", len(result)))

	return result, nil
}

// FetchLatestCloses tries today, then yesterday, then day before — finds the most recent bhavcopy.
// Handles weekends and holidays by trying up to 5 days back.
func (f *BhavcopyFetcher) FetchLatestCloses() (map[string]float64, error) {
	loc, _ := time.LoadLocation("Asia/Kolkata")
	now := time.Now().In(loc)

	for daysBack := 0; daysBack < 5; daysBack++ {
		date := now.AddDate(0, 0, -daysBack)

		// Skip weekends
		if date.Weekday() == time.Saturday || date.Weekday() == time.Sunday {
			continue
		}

		closes, err := f.FetchCloses(date)
		if err != nil {
			f.logger.Debug("Bhavcopy not available, trying earlier date",
				zap.String("date", date.Format("2006-01-02")),
				zap.Error(err))
			continue
		}

		if len(closes) > 0 {
			return closes, nil
		}
	}

	return nil, fmt.Errorf("no bhavcopy available for last 5 days")
}
