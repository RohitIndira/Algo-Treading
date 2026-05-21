package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
)

// MarketHandler serves live market quotes read from the external Indira
// Redis (live tick feed). Read-only, public market data — no auth required.
type MarketHandler struct {
	extRedis *redis.Client // external Redis live feed; may be nil
}

func NewMarketHandler(extRedis *redis.Client) *MarketHandler {
	return &MarketHandler{extRedis: extRedis}
}

// marketQuote is the API response. Field set + json tags mirror the
// market:nse:{token} blob, trimmed to what the frontend needs. dpr_date is
// not present in NSE blobs today — kept in the contract, emitted empty.
type marketQuote struct {
	Symbol        string  `json:"symbol"`
	Token         string  `json:"token"`
	Exchange      string  `json:"exchange"`
	LTP           float64 `json:"ltp"`
	Open          float64 `json:"open"`
	High          float64 `json:"high"`
	Low           float64 `json:"low"`
	Close         float64 `json:"close"`
	PrevClose     float64 `json:"prev_close"`
	PercentChange float64 `json:"percent_change"`
	TickSize      float64 `json:"tick_size"`
	DPRLower      float64 `json:"dpr_lower"`
	DPRUpper      float64 `json:"dpr_upper"`
	DPRDate       string  `json:"dpr_date"`
	Timestamp     int64   `json:"timestamp"`
	LastUpdated   string  `json:"last_updated"`
}

// GetQuote handles GET /api/v1/market/quote?symbol=SUNDROP  (or ?token=1663)
//
// Resolution:
//   - ?token=  → direct GET market:nse:{token}
//   - ?symbol= → symbol:{SYMBOL} → isin → isin:{ISIN} → nsecode → market:nse:{nsecode}
func (h *MarketHandler) GetQuote(w http.ResponseWriter, r *http.Request) {
	if h.extRedis == nil {
		respondWithError(w, http.StatusServiceUnavailable, "market data source (ext-Redis) not configured")
		return
	}

	symbol := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("symbol")))
	token := strings.TrimSpace(r.URL.Query().Get("token"))

	// Validation — exactly one of symbol / token.
	switch {
	case symbol == "" && token == "":
		respondWithError(w, http.StatusBadRequest, "provide either ?symbol= or ?token=")
		return
	case symbol != "" && token != "":
		respondWithError(w, http.StatusBadRequest, "provide only one of ?symbol= or ?token=, not both")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	// Resolve to a numeric NSE token.
	if symbol != "" {
		if len(symbol) > 30 || !isValidSymbol(symbol) {
			respondWithError(w, http.StatusBadRequest, "invalid symbol — expected an NSE ticker (A-Z, 0-9, & -)")
			return
		}
		resolved, err := h.resolveToken(ctx, symbol)
		if err != nil {
			respondWithError(w, http.StatusNotFound, err.Error())
			return
		}
		token = resolved
	} else {
		if _, err := strconv.Atoi(token); err != nil {
			respondWithError(w, http.StatusBadRequest, "token must be numeric")
			return
		}
	}

	// Fetch the live market blob.
	raw, err := h.extRedis.Get(ctx, "market:nse:"+token).Result()
	if err == redis.Nil {
		respondWithError(w, http.StatusNotFound, fmt.Sprintf("no live market data for token %s", token))
		return
	}
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "market data lookup failed: "+err.Error())
		return
	}

	var q marketQuote
	if err := json.Unmarshal([]byte(raw), &q); err != nil {
		respondWithError(w, http.StatusInternalServerError, "market data parse failed")
		return
	}
	respondWithJSON(w, http.StatusOK, q)
}

// resolveToken maps an NSE symbol to its token via the ext-Redis chain:
//
//	symbol:{SYMBOL}  → {"isin": "..."}
//	isin:{ISIN}      → {"nsecode": "<token>"}
//
// 2 GETs. Returns a 404-friendly error at each break in the chain.
func (h *MarketHandler) resolveToken(ctx context.Context, symbol string) (string, error) {
	sraw, err := h.extRedis.Get(ctx, "symbol:"+symbol).Result()
	if err == redis.Nil {
		return "", fmt.Errorf("unknown symbol %q — not in NSE master data", symbol)
	}
	if err != nil {
		return "", fmt.Errorf("symbol lookup failed: %w", err)
	}
	var sblob struct {
		ISIN string `json:"isin"`
	}
	if err := json.Unmarshal([]byte(sraw), &sblob); err != nil || sblob.ISIN == "" {
		return "", fmt.Errorf("symbol %q has no ISIN mapping", symbol)
	}

	iraw, err := h.extRedis.Get(ctx, "isin:"+sblob.ISIN).Result()
	if err == redis.Nil {
		return "", fmt.Errorf("no token mapping for symbol %q (isin %s)", symbol, sblob.ISIN)
	}
	if err != nil {
		return "", fmt.Errorf("isin lookup failed: %w", err)
	}
	var iblob struct {
		NSECode string `json:"nsecode"`
	}
	if err := json.Unmarshal([]byte(iraw), &iblob); err != nil || iblob.NSECode == "" {
		return "", fmt.Errorf("symbol %q has no NSE token", symbol)
	}
	return iblob.NSECode, nil
}

// isValidSymbol allows the character set NSE tickers actually use.
func isValidSymbol(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		switch {
		case c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '&', c == '-':
		default:
			return false
		}
	}
	return true
}
