package historical

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

// BhavData holds the raw CSV content downloaded from NSE/BSE for a given date.
type BhavData struct {
	Exchange  string    // "NSE" or "BSE"
	TradeDate time.Time // The trading date
	CSV       []byte    // Raw CSV bytes (unzipped if needed)
	FileName  string    // Original file name
	Source    string    // URL source identifier
}

// Downloader handles bhavcopy downloads from NSE and BSE with retry,
// cookie management, and rate limiting.
type Downloader struct {
	nseClient  *http.Client
	bseClient  *http.Client
	logger     *zap.Logger
	mu         sync.Mutex // protects NSE cookie refresh
	lastNSEReq time.Time  // rate limit NSE requests
}

// NewDownloader creates a downloader with proper HTTP clients.
func NewDownloader(logger *zap.Logger) (*Downloader, error) {
	// NSE requires cookies — use a cookie jar
	jar, err := cookiejar.New(nil)
	if err != nil {
		return nil, fmt.Errorf("create cookie jar: %w", err)
	}

	nseClient := &http.Client{
		Jar:     jar,
		Timeout: 30 * time.Second,
		// Don't follow redirects blindly
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 3 {
				return fmt.Errorf("too many redirects")
			}
			return nil
		},
	}

	bseClient := &http.Client{
		Timeout: 60 * time.Second, // BSE old archives are slow, need longer timeout
	}

	return &Downloader{
		nseClient: nseClient,
		bseClient: bseClient,
		logger:    logger,
	}, nil
}

// DownloadNSE downloads NSE bhavcopy for a given date.
// URL availability by era:
//   - 2020-present: sec_bhavdata_full (simple CSV, includes delivery data)
//   - Jun 2024-present: UDiFF zip (34-column standard format)
//   - 2006-2019: old cm{DDMONYYYY}bhav.csv.zip format
//
// Tries sources in order of preference with automatic fallback.
func (d *Downloader) DownloadNSE(date time.Time) (*BhavData, error) {
	var firstErr error

	// Try sec_bhavdata_full first (2020+, simpler, no zip, includes delivery data)
	data, err := d.downloadNSESecBhav(date)
	if err == nil {
		return data, nil
	}
	firstErr = err
	if !IsNoDataError(err) {
		d.logger.Debug("NSE sec_bhavdata_full failed, trying next",
			zap.String("date", date.Format("2006-01-02")),
			zap.Error(err))
	}

	// Try UDiFF zip format (Jun 2024+)
	data, err = d.downloadNSEUDiFF(date)
	if err == nil {
		return data, nil
	}
	if !IsNoDataError(err) {
		d.logger.Debug("NSE UDiFF failed, trying old format",
			zap.String("date", date.Format("2006-01-02")),
			zap.Error(err))
	}

	// Try old format (2006-2019): cm{DDMONYYYY}bhav.csv.zip
	data, err = d.downloadNSEOld(date)
	if err == nil {
		return data, nil
	}

	// All sources failed — if first error was NoData, propagate that
	if IsNoDataError(firstErr) {
		return nil, firstErr
	}
	return nil, fmt.Errorf("all NSE sources failed for %s: %w", date.Format("2006-01-02"), err)
}

// downloadNSESecBhav downloads the simpler sec_bhavdata_full CSV from NSE.
// Format: SYMBOL, SERIES, DATE1, PREV_CLOSE, OPEN_PRICE, HIGH_PRICE, LOW_PRICE,
//
//	LAST_PRICE, CLOSE_PRICE, AVG_PRICE, TTL_TRD_QNTY, TURNOVER_LACS,
//	NO_OF_TRADES, DELIV_QTY, DELIV_PER
func (d *Downloader) downloadNSESecBhav(date time.Time) (*BhavData, error) {
	dateStr := date.Format("02012006") // DDMMYYYY
	url := fmt.Sprintf("https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_%s.csv", dateStr)

	body, err := d.fetchNSE(url)
	if err != nil {
		return nil, err
	}

	return &BhavData{
		Exchange:  "NSE",
		TradeDate: date,
		CSV:       body,
		FileName:  fmt.Sprintf("sec_bhavdata_full_%s.csv", dateStr),
		Source:    "nse_sec_bhav",
	}, nil
}

// downloadNSEUDiFF downloads the UDiFF format zip from NSE (post July 2024).
// Format: 34 columns — TradDt, BizDt, Sgmt, Src, FinInstrmTp, FinInstrmId, ISIN,
//
//	TckrSymb, SctySrs, ... OpnPric, HghPric, LwPric, ClsPric, ...
func (d *Downloader) downloadNSEUDiFF(date time.Time) (*BhavData, error) {
	dateStr := date.Format("20060102") // YYYYMMDD
	url := fmt.Sprintf("https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_%s_F_0000.csv.zip", dateStr)

	body, err := d.fetchNSE(url)
	if err != nil {
		return nil, err
	}

	// Unzip the response
	csv, fileName, err := unzipFirstCSV(body)
	if err != nil {
		return nil, fmt.Errorf("unzip NSE UDiFF: %w", err)
	}

	return &BhavData{
		Exchange:  "NSE",
		TradeDate: date,
		CSV:       csv,
		FileName:  fileName,
		Source:    "nse_udiff",
	}, nil
}

// downloadNSEOld downloads the old NSE bhavcopy format (2006-2019).
// URL: cm{DDMONYYYY}bhav.csv.zip
// Columns: SYMBOL, SERIES, OPEN, HIGH, LOW, CLOSE, LAST, PREVCLOSE,
//
//	TOTTRDQTY, TOTTRDVAL, TIMESTAMP, TOTALTRADES, ISIN
func (d *Downloader) downloadNSEOld(date time.Time) (*BhavData, error) {
	// Month must be uppercase 3-letter: JAN, FEB, MAR, etc.
	mon := strings.ToUpper(date.Format("Jan"))
	dateStr := fmt.Sprintf("cm%02d%s%dbhav.csv.zip", date.Day(), mon, date.Year())
	url := fmt.Sprintf("https://nsearchives.nseindia.com/content/historical/EQUITIES/%d/%s/%s",
		date.Year(), mon, dateStr)

	body, err := d.fetchNSE(url)
	if err != nil {
		return nil, err
	}

	csv, fileName, err := unzipFirstCSV(body)
	if err != nil {
		return nil, fmt.Errorf("unzip NSE old: %w", err)
	}

	return &BhavData{
		Exchange:  "NSE",
		TradeDate: date,
		CSV:       csv,
		FileName:  fileName,
		Source:    "nse_old",
	}, nil
}

// fetchNSE makes an HTTP GET to NSE with proper cookies and headers.
// NSE requires a session cookie from the main website.
func (d *Downloader) fetchNSE(url string) ([]byte, error) {
	d.mu.Lock()
	// Rate limit: at least 1.5s between NSE requests
	elapsed := time.Since(d.lastNSEReq)
	if elapsed < 1500*time.Millisecond {
		time.Sleep(1500*time.Millisecond - elapsed)
	}
	d.mu.Unlock()

	// Ensure we have a valid NSE session cookie
	if err := d.refreshNSECookies(); err != nil {
		return nil, fmt.Errorf("NSE cookie refresh: %w", err)
	}

	body, err := d.httpGetWithRetry(d.nseClient, url, nseHeaders(), 3)
	if err != nil {
		return nil, err
	}

	d.mu.Lock()
	d.lastNSEReq = time.Now()
	d.mu.Unlock()

	return body, nil
}

// refreshNSECookies hits the NSE homepage to get session cookies.
// Only refreshes if cookies are likely stale (every 5 minutes).
func (d *Downloader) refreshNSECookies() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Check if we already have fresh cookies
	if d.nseClient.Jar != nil {
		// Simple heuristic: if last request was recent, cookies are probably fine
		if time.Since(d.lastNSEReq) < 5*time.Minute && !d.lastNSEReq.IsZero() {
			return nil
		}
	}

	req, err := http.NewRequest("GET", "https://www.nseindia.com", nil)
	if err != nil {
		return err
	}
	for k, v := range nseHeaders() {
		req.Header.Set(k, v)
	}

	resp, err := d.nseClient.Do(req)
	if err != nil {
		return fmt.Errorf("NSE homepage: %w", err)
	}
	defer resp.Body.Close()
	io.ReadAll(resp.Body) // drain body to reuse connection

	if resp.StatusCode != 200 {
		return fmt.Errorf("NSE homepage returned %d", resp.StatusCode)
	}

	d.logger.Debug("NSE cookies refreshed")
	return nil
}

// bseUDiFFCutover is when BSE switched from old ZIP to UDiFF CSV format.
// Old ZIP (EQ{DDMMYY}_CSV.ZIP) works: 2006 — Jul 4, 2024
// UDiFF CSV works: Jul 5, 2024 — present
var bseUDiFFCutover = time.Date(2024, 7, 5, 0, 0, 0, 0, time.UTC)

// DownloadBSE downloads BSE bhavcopy for a given date.
// Tries the correct format first based on date, with fallback to the other.
func (d *Downloader) DownloadBSE(date time.Time) (*BhavData, error) {
	if date.Before(bseUDiFFCutover) {
		// Pre July 2024: try old ZIP first (avoids wasting 30s on a guaranteed UDiFF 404)
		data, err := d.downloadBSEOld(date)
		if err == nil {
			return data, nil
		}
		if IsNoDataError(err) {
			return nil, err // Holiday — don't try UDiFF
		}
		// Timeout/server error — try UDiFF as fallback
		data, err2 := d.downloadBSEUDiFF(date)
		if err2 == nil {
			return data, nil
		}
		return nil, fmt.Errorf("all BSE sources failed for %s: %w", date.Format("2006-01-02"), err)
	}

	// Post July 2024: try UDiFF first
	data, err := d.downloadBSEUDiFF(date)
	if err == nil {
		return data, nil
	}
	if IsNoDataError(err) {
		return nil, err
	}

	// Fallback: old ZIP format (covers transition period)
	data, err = d.downloadBSEOld(date)
	if err != nil {
		return nil, fmt.Errorf("all BSE sources failed for %s: %w", date.Format("2006-01-02"), err)
	}
	return data, nil
}

// downloadBSEUDiFF downloads BSE UDiFF format (same 34 columns as NSE).
func (d *Downloader) downloadBSEUDiFF(date time.Time) (*BhavData, error) {
	dateStr := date.Format("20060102") // YYYYMMDD
	url := fmt.Sprintf("https://www.bseindia.com/download/BhavCopy/Equity/BhavCopy_BSE_CM_0_0_0_%s_F_0000.CSV", dateStr)

	body, err := d.httpGetWithRetry(d.bseClient, url, bseHeaders(), 3)
	if err != nil {
		return nil, err
	}

	return &BhavData{
		Exchange:  "BSE",
		TradeDate: date,
		CSV:       body,
		FileName:  fmt.Sprintf("BhavCopy_BSE_CM_0_0_0_%s_F_0000.CSV", dateStr),
		Source:    "bse_udiff",
	}, nil
}

// downloadBSEOld downloads the old BSE ZIP format (2006 — Jul 4, 2024).
// URL: EQ{DDMMYY}_CSV.ZIP containing a CSV with columns:
//
//	SC_CODE, SC_NAME, SC_GROUP, SC_TYPE, OPEN, HIGH, LOW, CLOSE, LAST,
//	PREVCLOSE, NO_TRADES, NO_OF_SHRS, NET_TURNOV, TDCLOINDI
func (d *Downloader) downloadBSEOld(date time.Time) (*BhavData, error) {
	dateStr := date.Format("020106") // DDMMYY
	url := fmt.Sprintf("https://www.bseindia.com/download/BhavCopy/Equity/EQ%s_CSV.ZIP", dateStr)

	body, err := d.httpGetWithRetry(d.bseClient, url, bseHeaders(), 3)
	if err != nil {
		return nil, err
	}

	csv, fileName, err := unzipFirstCSV(body)
	if err != nil {
		return nil, fmt.Errorf("unzip BSE old: %w", err)
	}

	return &BhavData{
		Exchange:  "BSE",
		TradeDate: date,
		CSV:       csv,
		FileName:  fileName,
		Source:    "bse_old",
	}, nil
}

// httpGetWithRetry performs an HTTP GET with exponential backoff retry.
// Returns ErrHoliday if server returns 404/403 (likely a holiday/no data).
func (d *Downloader) httpGetWithRetry(client *http.Client, url string, headers map[string]string, maxRetries int) ([]byte, error) {
	var lastErr error

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(1<<uint(attempt-1)) * time.Second // 1s, 2s, 4s
			d.logger.Debug("Retrying request",
				zap.String("url", url),
				zap.Int("attempt", attempt+1),
				zap.Duration("backoff", backoff))
			time.Sleep(backoff)
		}

		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			return nil, fmt.Errorf("create request: %w", err)
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}

		resp, err := client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("HTTP GET %s: %w", url, err)
			d.logger.Warn("Request failed, will retry",
				zap.String("url", url),
				zap.Error(err),
				zap.Int("attempt", attempt+1))
			continue
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()

		if err != nil {
			lastErr = fmt.Errorf("read response body: %w", err)
			continue
		}

		switch resp.StatusCode {
		case 200:
			// Validate it's not an HTML error page disguised as 200
			if isHTMLResponse(body) {
				lastErr = fmt.Errorf("received HTML instead of CSV/ZIP from %s", url)
				continue
			}
			return body, nil

		case 404, 403:
			// No data for this date (holiday or not yet published)
			return nil, &ErrNoData{
				Date:    url,
				Status:  resp.StatusCode,
				Message: fmt.Sprintf("no data available (HTTP %d)", resp.StatusCode),
			}

		case 503, 502, 500:
			// Server error — retry
			lastErr = fmt.Errorf("server error HTTP %d from %s", resp.StatusCode, url)
			d.logger.Warn("Server error, will retry",
				zap.String("url", url),
				zap.Int("status", resp.StatusCode),
				zap.Int("attempt", attempt+1))
			continue

		default:
			lastErr = fmt.Errorf("unexpected HTTP %d from %s", resp.StatusCode, url)
		}
	}

	return nil, fmt.Errorf("all %d attempts failed for %s: %w", maxRetries+1, url, lastErr)
}

// ErrNoData indicates no bhavcopy exists for a date (holiday/weekend).
type ErrNoData struct {
	Date    string
	Status  int
	Message string
}

func (e *ErrNoData) Error() string {
	return e.Message
}

// IsNoDataError checks if an error means "no data for this date" (holiday).
func IsNoDataError(err error) bool {
	_, ok := err.(*ErrNoData)
	return ok
}

// nseHeaders returns headers required for NSE requests.
func nseHeaders() map[string]string {
	return map[string]string{
		"User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
		"Referer":         "https://www.nseindia.com/",
		"Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
		"Accept-Language":  "en-US,en;q=0.9",
		"Accept-Encoding":  "identity", // Don't accept gzip — we want raw bytes
		"Connection":       "keep-alive",
	}
}

// bseHeaders returns headers required for BSE requests.
// IMPORTANT: BSE requires the Referer header from the BhavCopy page,
// otherwise it returns 404 even for valid URLs.
func bseHeaders() map[string]string {
	return map[string]string{
		"User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
		"Referer":         "https://www.bseindia.com/markets/MarketInfo/BhavCopy.aspx",
		"Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
		"Accept-Language":  "en-US,en;q=0.9",
		"Connection":       "keep-alive",
	}
}

// unzipFirstCSV extracts the first CSV file from a ZIP archive in memory.
func unzipFirstCSV(zipData []byte) ([]byte, string, error) {
	reader, err := zip.NewReader(bytes.NewReader(zipData), int64(len(zipData)))
	if err != nil {
		return nil, "", fmt.Errorf("open zip: %w", err)
	}

	for _, f := range reader.File {
		if strings.HasSuffix(strings.ToLower(f.Name), ".csv") {
			rc, err := f.Open()
			if err != nil {
				return nil, "", fmt.Errorf("open %s in zip: %w", f.Name, err)
			}
			defer rc.Close()

			data, err := io.ReadAll(rc)
			if err != nil {
				return nil, "", fmt.Errorf("read %s from zip: %w", f.Name, err)
			}
			return data, f.Name, nil
		}
	}

	return nil, "", fmt.Errorf("no CSV file found in ZIP archive")
}

// isHTMLResponse checks if the body looks like an HTML page (error page).
func isHTMLResponse(body []byte) bool {
	if len(body) < 20 {
		return false
	}
	prefix := strings.ToLower(string(body[:50]))
	return strings.Contains(prefix, "<html") || strings.Contains(prefix, "<!doctype")
}
