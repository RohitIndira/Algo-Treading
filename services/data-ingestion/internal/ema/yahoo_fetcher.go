package ema

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"go.uber.org/zap"
)

// YahooFetcher fetches historical close prices from Yahoo Finance.
// Used one-time to seed initial EMA values (100 days of data).
type YahooFetcher struct {
	client *http.Client
	logger *zap.Logger
}

// NewYahooFetcher creates a new Yahoo Finance fetcher.
func NewYahooFetcher(logger *zap.Logger) *YahooFetcher {
	return &YahooFetcher{
		client: &http.Client{Timeout: 15 * time.Second},
		logger: logger,
	}
}

// FetchCloses fetches daily close prices for an NSE stock from Yahoo Finance.
// Returns closes ordered oldest-first (for EMA computation).
// range=6mo gives ~125 trading days — enough for EMA 100.
func (f *YahooFetcher) FetchCloses(symbol string) ([]float64, error) {
	// Yahoo uses .NS suffix for NSE stocks
	yahooSymbol := symbol + ".NS"
	url := fmt.Sprintf("https://query2.finance.yahoo.com/v8/finance/chart/%s?range=6mo&interval=1d", yahooSymbol)

	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

	resp, err := f.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// Parse Yahoo response
	var yahooResp struct {
		Chart struct {
			Result []struct {
				Indicators struct {
					Quote []struct {
						Close []interface{} `json:"close"`
					} `json:"quote"`
				} `json:"indicators"`
			} `json:"result"`
			Error interface{} `json:"error"`
		} `json:"chart"`
	}

	if err := json.Unmarshal(body, &yahooResp); err != nil {
		return nil, fmt.Errorf("parse JSON: %w", err)
	}

	if yahooResp.Chart.Error != nil {
		return nil, fmt.Errorf("Yahoo error: %v", yahooResp.Chart.Error)
	}

	if len(yahooResp.Chart.Result) == 0 || len(yahooResp.Chart.Result[0].Indicators.Quote) == 0 {
		return nil, fmt.Errorf("no data from Yahoo")
	}

	rawCloses := yahooResp.Chart.Result[0].Indicators.Quote[0].Close

	// Convert to float64, skip nulls (holidays/missing data)
	var closes []float64
	for _, v := range rawCloses {
		if v == nil {
			continue
		}
		switch c := v.(type) {
		case float64:
			if c > 0 {
				closes = append(closes, c)
			}
		case json.Number:
			f, _ := c.Float64()
			if f > 0 {
				closes = append(closes, f)
			}
		}
	}

	if len(closes) == 0 {
		return nil, fmt.Errorf("no valid close prices")
	}

	return closes, nil
}

// FetchAllNSE fetches close prices for multiple NSE symbols.
// Returns map[symbol][]float64 (closes oldest-first).
// Rate limits: ~5 requests per second (Yahoo is lenient).
func (f *YahooFetcher) FetchAllNSE(symbols []string) map[string][]float64 {
	result := make(map[string][]float64, len(symbols))
	fetched := 0
	failed := 0

	for i, sym := range symbols {
		closes, err := f.FetchCloses(sym)
		if err != nil {
			failed++
			if failed <= 5 {
				f.logger.Debug("Yahoo fetch failed", zap.String("symbol", sym), zap.Error(err))
			}
			continue
		}

		result[sym] = closes
		fetched++

		if (i+1)%100 == 0 {
			f.logger.Info("Yahoo fetch progress",
				zap.Int("done", i+1),
				zap.Int("total", len(symbols)),
				zap.Int("fetched", fetched),
				zap.Int("failed", failed))
		}

		// Rate limit: 5 per second
		time.Sleep(200 * time.Millisecond)
	}

	f.logger.Info("Yahoo fetch complete",
		zap.Int("fetched", fetched),
		zap.Int("failed", failed))

	return result
}
