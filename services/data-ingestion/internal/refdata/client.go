// Package refdata loads Manthan's reference data from the vendor APIs that
// replace the Google Sheet (operator decision 2026-10-30):
//
//	equities API   GET {base}/equities/eod            — one row per company per trading day
//	               GET {base}/equities/fundamentals   — one row per company per quarter
//	price levels   GET {base}/fundamental/price-levels — vendor's intraday-high ATH / 52w
//
// The client only knows how to page through those three endpoints and honour
// the vendor's 120-requests-per-minute limit; what the rows MEAN for the
// strategy lives in the store and the pipeline reader.
package refdata

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// DefaultBase is the vendor's v2 API root. Both endpoint families hang off it.
const DefaultBase = "https://admin.stocksemoji.com/v2/api"

// Page sizes the vendor accepts. The equities endpoints take up to 1000;
// price-levels silently caps at 200 whatever you ask for.
const (
	equitiesPageSize    = 1000
	priceLevelsPageSize = 200
)

// Client talks to the vendor API.
type Client struct {
	base   string
	apiKey string
	http   *http.Client
	// sleep is swapped out in tests so a 429 does not actually wait.
	sleep func(time.Duration)
	// maxRetries bounds how many 429/5xx a single page tolerates.
	maxRetries int
}

// NewClient builds a client. base may be "" for DefaultBase. apiKey is sent
// as x-api-key on every request (price-levels does not require it today but
// sending it is harmless and future-proof).
func NewClient(base, apiKey string) *Client {
	if base == "" {
		base = DefaultBase
	}
	return &Client{
		base:       strings.TrimRight(base, "/"),
		apiKey:     apiKey,
		http:       &http.Client{Timeout: 60 * time.Second},
		sleep:      time.Sleep,
		maxRetries: 5,
	}
}

// EODRow is one company on one trading day.
type EODRow struct {
	TradeDate         string   `json:"trade_date"`
	ISIN              string   `json:"isin"`
	NSESymbol         *string  `json:"nse_symbol"`
	BSECode           *string  `json:"bse_code"`
	CompanyName       string   `json:"company_name"`
	Industry          *string  `json:"industry"`
	Sector            *string  `json:"sector"`
	ListingDate       *string  `json:"listing_date"`
	ClosePrice        float64  `json:"close_price"`
	SharesOutstanding *float64 `json:"shares_outstanding"`
	MarketCapCr       *float64 `json:"market_cap_cr"`
}

// FundamentalsRow is one company for one quarter under one basis.
type FundamentalsRow struct {
	Period        string   `json:"period"`
	ISIN          string   `json:"isin"`
	NSESymbol     *string  `json:"nse_symbol"`
	BSECode       *string  `json:"bse_code"`
	PATCr         *float64 `json:"pat_cr"`
	PATPeriodType *string  `json:"pat_period_type"`
	PATBasis      *string  `json:"pat_basis"`
	EPSTTM        *float64 `json:"eps_ttm"`
	FScore        *float64 `json:"f_score"`
}

// PriceLevelRow is the vendor's high/low snapshot for one company.
type PriceLevelRow struct {
	CoCode      int64    `json:"co_code"`
	Symbol      string   `json:"symbol"`
	CompanyName string   `json:"company_name"`
	Week52High  *float64 `json:"week_52_high"`
	Week52Low   *float64 `json:"week_52_low"`
	AllTimeHigh *float64 `json:"all_time_high"`
}

type equitiesEnvelope struct {
	Success bool            `json:"success"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
	Meta    struct {
		Page       int `json:"page"`
		Limit      int `json:"limit"`
		Total      int `json:"total"`
		TotalPages int `json:"total_pages"`
	} `json:"meta"`
}

type priceLevelsEnvelope struct {
	Success bool            `json:"success"`
	Message string          `json:"message"`
	Count   int             `json:"count"`
	Page    int             `json:"page"`
	Limit   int             `json:"limit"`
	Total   int             `json:"total"`
	Data    []PriceLevelRow `json:"data"`
}

// NotFoundError is returned when the vendor answers 404 — for /eod that
// means "not a trading day", for /fundamentals "no such period".
type NotFoundError struct{ Msg string }

func (e *NotFoundError) Error() string { return "vendor: " + e.Msg }

// EOD returns every company row for one trading day (YYYY-MM-DD). date ""
// means the vendor's latest available day.
func (c *Client) EOD(ctx context.Context, date string) ([]EODRow, error) {
	q := url.Values{}
	if date != "" {
		q.Set("date", date)
	}
	return c.eodPages(ctx, q)
}

// EODRange returns every row for the trading days in [from, to] (inclusive,
// at most 31 days apart per the vendor).
func (c *Client) EODRange(ctx context.Context, from, to string) ([]EODRow, error) {
	q := url.Values{}
	q.Set("from", from)
	q.Set("to", to)
	return c.eodPages(ctx, q)
}

func (c *Client) eodPages(ctx context.Context, q url.Values) ([]EODRow, error) {
	var out []EODRow
	err := c.pageEquities(ctx, "/equities/eod", q, func(raw json.RawMessage) error {
		var rows []EODRow
		if err := json.Unmarshal(raw, &rows); err != nil {
			return fmt.Errorf("decode eod rows: %w", err)
		}
		out = append(out, rows...)
		return nil
	})
	return out, err
}

// Fundamentals returns every company for one quarter (YYYY-MM) under one
// basis (preferred | consolidated | standalone). period "" = vendor's latest.
func (c *Client) Fundamentals(ctx context.Context, period, basis string) ([]FundamentalsRow, error) {
	q := url.Values{}
	if period != "" {
		q.Set("period", period)
	}
	if basis != "" {
		q.Set("basis", basis)
	}
	var out []FundamentalsRow
	err := c.pageEquities(ctx, "/equities/fundamentals", q, func(raw json.RawMessage) error {
		var rows []FundamentalsRow
		if err := json.Unmarshal(raw, &rows); err != nil {
			return fmt.Errorf("decode fundamentals rows: %w", err)
		}
		out = append(out, rows...)
		return nil
	})
	return out, err
}

// pageEquities walks page 1..total_pages of an equities endpoint.
func (c *Client) pageEquities(ctx context.Context, path string, q url.Values, each func(json.RawMessage) error) error {
	q.Set("limit", strconv.Itoa(equitiesPageSize))
	for page := 1; ; page++ {
		q.Set("page", strconv.Itoa(page))
		body, err := c.get(ctx, path, q)
		if err != nil {
			return err
		}
		var env equitiesEnvelope
		if err := json.Unmarshal(body, &env); err != nil {
			return fmt.Errorf("%s page %d: decode envelope: %w", path, page, err)
		}
		if !env.Success {
			return fmt.Errorf("%s page %d: vendor error: %s", path, page, env.Message)
		}
		if err := each(env.Data); err != nil {
			return err
		}
		if page >= env.Meta.TotalPages {
			return nil
		}
	}
}

// PriceLevels returns the vendor's full high/low snapshot (about 5,900 rows).
func (c *Client) PriceLevels(ctx context.Context) ([]PriceLevelRow, error) {
	var out []PriceLevelRow
	q := url.Values{}
	q.Set("limit", strconv.Itoa(priceLevelsPageSize))
	for page := 1; ; page++ {
		q.Set("page", strconv.Itoa(page))
		body, err := c.get(ctx, "/fundamental/price-levels", q)
		if err != nil {
			return nil, err
		}
		var env priceLevelsEnvelope
		if err := json.Unmarshal(body, &env); err != nil {
			return nil, fmt.Errorf("price-levels page %d: decode: %w", page, err)
		}
		if !env.Success {
			return nil, fmt.Errorf("price-levels page %d: vendor error: %s", page, env.Message)
		}
		out = append(out, env.Data...)
		// The vendor has no total_pages here; stop when a page comes back
		// short or we have everything it claims to hold.
		if len(env.Data) == 0 || len(env.Data) < env.Limit || len(out) >= env.Total {
			return out, nil
		}
	}
}

// get performs one request with the vendor's rate-limit contract: a 429
// carries Retry-After seconds; 5xx is retried with a short backoff; 404 is
// surfaced as NotFoundError; anything else is a hard error with the body.
func (c *Client) get(ctx context.Context, path string, q url.Values) ([]byte, error) {
	u := c.base + path
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	var lastErr error
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return nil, err
		}
		if c.apiKey != "" {
			req.Header.Set("x-api-key", c.apiKey)
		}
		req.Header.Set("Accept", "application/json")
		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = err
			c.sleep(time.Second * time.Duration(attempt+1))
			continue
		}
		body, rerr := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
		resp.Body.Close()
		if rerr != nil {
			return nil, fmt.Errorf("%s: read body: %w", path, rerr)
		}
		switch {
		case resp.StatusCode == http.StatusOK:
			return body, nil
		case resp.StatusCode == http.StatusNotFound:
			return nil, &NotFoundError{Msg: vendorMessage(body)}
		case resp.StatusCode == http.StatusTooManyRequests:
			wait := 5 * time.Second
			if ra, err := strconv.Atoi(strings.TrimSpace(resp.Header.Get("Retry-After"))); err == nil && ra > 0 {
				wait = time.Duration(ra) * time.Second
			}
			lastErr = fmt.Errorf("%s: 429 rate limited", path)
			c.sleep(wait)
		case resp.StatusCode >= 500:
			lastErr = fmt.Errorf("%s: HTTP %d: %s", path, resp.StatusCode, vendorMessage(body))
			c.sleep(2 * time.Second * time.Duration(attempt+1))
		default:
			return nil, fmt.Errorf("%s: HTTP %d: %s", path, resp.StatusCode, vendorMessage(body))
		}
	}
	return nil, fmt.Errorf("%s: gave up after %d retries: %w", path, c.maxRetries, lastErr)
}

func vendorMessage(body []byte) string {
	var e struct {
		Message string `json:"message"`
	}
	if json.Unmarshal(body, &e) == nil && e.Message != "" {
		return e.Message
	}
	s := strings.TrimSpace(string(body))
	if len(s) > 200 {
		s = s[:200] + "…"
	}
	return s
}
