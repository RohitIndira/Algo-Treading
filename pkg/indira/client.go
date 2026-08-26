package indira

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

// ErrAuthExpired is returned when Indira rejects a request with one of its
// auth-related infoID codes (AU001, AU004, etc.). Callers can use errors.Is
// to short-circuit retry loops instead of papering over the failure with
// downstream JSON-parse errors.
var ErrAuthExpired = errors.New("indira: session expired (AU0xx)")

const (
	// Default timeouts
	DefaultTimeout = 30 * time.Second

	// Environment variable names
	EnvIndiraBaseURL = "INDIRA_BASE_URL"

	// Fallback base URL if environment variable is not set.
	// Matches the official "Live" environment per Indira Securities API spec.
	FallbackBaseURL = "https://livemiddleware.indiratrade.com"
)

// getDefaultBaseURL returns the base URL from environment or uses fallback
func getDefaultBaseURL() string {
	if url := os.Getenv(EnvIndiraBaseURL); url != "" {
		return url
	}
	return FallbackBaseURL
}

// Client represents an Indira Securities API client
// This client is stateless and thread-safe for concurrent use by multiple users
type Client struct {
	baseURL    string
	httpClient *http.Client
}

// AuthContext holds per-request authentication information
// This should be provided by the frontend for each request
type AuthContext struct {
	UserId      string // User ID (e.g., "ISPL19122")
	AppId       string // Application ID from frontend
	ClientId    string // Client ID (optional)
	Source      string // Source platform: IOS, AND, WEB
	BearerToken string // JWT bearer token from frontend
	SSO         bool   // Single Sign-On session flag
}

// Config holds client configuration
type Config struct {
	BaseURL string
	Timeout time.Duration
}

// NewClient creates a new Indira API client
// The client is stateless and can be shared across multiple users
func NewClient(config Config) *Client {
	if config.BaseURL == "" {
		config.BaseURL = getDefaultBaseURL()
	}

	timeout := config.Timeout
	if timeout == 0 {
		timeout = DefaultTimeout
	}

	// Optimize HTTP Transport for high-frequency trading
	// 1. connection pooling: keep more idle connections open to avoid re-establishing TCP/TLS handshakes
	// 2. per-host limits: max out the limits since we primarily talk to Indira's load balancer
	t := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     90 * time.Second,
		DisableKeepAlives:   false,
	}

	return &Client{
		baseURL: config.BaseURL,
		httpClient: &http.Client{
			Transport: t,
			Timeout:   timeout,
		},
	}
}

// NewDefaultClient creates a client with default configuration
func NewDefaultClient() *Client {
	return NewClient(Config{})
}

// ============ Helper Methods ============

// doRequest performs an HTTP request with per-request authentication headers
func (c *Client) doRequest(ctx context.Context, auth *AuthContext, method, path string, body interface{}) (*StandardResponse, error) {
	if auth == nil {
		return nil, fmt.Errorf("authentication context is required")
	}

	var reqBody io.Reader
	var jsonBody []byte
	if body != nil {
		var err error
		jsonBody, err = json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request body: %w", err)
		}
		reqBody = bytes.NewBuffer(jsonBody)
	}

	url := c.baseURL + path
	if strings.Contains(path, "place-order") || strings.Contains(path, "modify-order") {
		log.Printf("[indira] → %s %s  body=%s", method, path, string(jsonBody))
	} else {
		log.Printf("[indira] → %s %s", method, path)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Set standard headers
	req.Header.Set("Content-Type", "application/json")

	// Indira b2b api-key. Order-services endpoints (place-order / modify-order /
	// cancel-order) reject requests that lack it with 403 "unknown_client:
	// Requests without api-key must come from the app or a browser". Static
	// per-partner key, supplied via INDIRA_API_KEY. Harmless on other endpoints.
	if apiKey := os.Getenv("INDIRA_API_KEY"); apiKey != "" {
		req.Header.Set("api-key", apiKey)
	}

	// Set per-request authentication headers (from frontend)
	if auth.UserId != "" {
		req.Header.Set("userId", auth.UserId)
	}
	if auth.AppId != "" {
		req.Header.Set("appId", auth.AppId)
	}
	if auth.Source != "" {
		req.Header.Set("source", auth.Source)
	}
	if auth.BearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+auth.BearerToken)
	}
	// The `sso` header selects which Indira auth middleware validates the
	// request. Get this wrong and every strict endpoint (order-book,
	// fund-limit, holdings, createWsToken, AccountInfo) returns AU004
	// "Session expired" even for a freshly-minted JWT — because the
	// session lives in the OTHER middleware's cache. place-order (crypto-
	// only) still succeeds, which is what made this hard to spot:
	// you'd place orders you couldn't then confirm or manage.
	//
	//   sso: True   →  SSO middleware   →  validates against WEB session cache
	//                                       (JWTs where source=SSO / WEB, loginSource=SSO)
	//   sso: False  →  APP middleware   →  validates against MOBILE session cache
	//                                       (JWTs where source=IOS / AND, loginSource=APP)
	//
	// Value is case-sensitive ("True" / "False"). Verified 2026-07-15 by
	// direct comparison: same JWT + headers, only the sso value flipped,
	// turned all 5 strict-endpoint AU004s into 200 SUCCESS. Root of the
	// double-fill / orphan-position class of incident that day.
	//
	// If auth.Source is unset we default to "True" for backward compat with
	// the pre-2026-07-15 behavior (existing SSO web callers unchanged).
	switch strings.ToUpper(auth.Source) {
	case "IOS", "AND", "ANDROID", "APP":
		req.Header.Set("sso", "False")
	default:
		req.Header.Set("sso", "True")
	}

	// Execute request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	// Read response body
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	// Response logging: full bodies only where they carry signal — order-
	// mutating calls (audit trail), non-success responses (forensics: AU004s,
	// rejections), or INDIRA_LOG_BODIES=1 (debug). Success bodies on read
	// endpoints log status+size only: the order-book poller alone was writing
	// ~3 GB of repeated success dumps into the service log (2026-08-26).
	mutating := strings.Contains(path, "place-order") ||
		strings.Contains(path, "modify-order") ||
		strings.Contains(path, "cancel-order")
	head := responseBody
	if len(head) > 64 {
		head = head[:64]
	}
	successBody := resp.StatusCode == http.StatusOK && bytes.Contains(head, []byte(`"infoID":"0"`))
	if mutating || !successBody || os.Getenv("INDIRA_LOG_BODIES") == "1" {
		log.Printf("[indira] ← %s %s  status=%d  body=%s", method, path, resp.StatusCode, string(responseBody))
	} else {
		log.Printf("[indira] ← %s %s  status=%d  bytes=%d", method, path, resp.StatusCode, len(responseBody))
	}

	// Auth-expired detection runs BEFORE the HTTP-status gate because Indira
	// is inconsistent about what code it returns for AU004:
	//
	//   - Sometimes HTTP 200 with body {"infoID":"AU004","data":{}}
	//   - Sometimes HTTP 401 with body {"infoID":"AU004", ...}
	//
	// Without parsing the body first, the 401 case falls into the generic
	// "HTTP error 401: <body>" path and downstream errors.Is(err, ErrAuthExpired)
	// returns false — every auth-aware loop (reconciler, safety monitor,
	// inbox worker, entry handler) misses its short-circuit and keeps
	// hammering the broker. We hit this bug live on 2026-05-11 during
	// trade-execution end-to-end testing with a probe-token DB row.
	//
	// Parse the body first; if it's a StandardResponse with infoID=AU0**,
	// return ErrAuthExpired regardless of HTTP code.
	var stdResp StandardResponse
	bodyParsedOK := false
	if jsonErr := json.Unmarshal(responseBody, &stdResp); jsonErr == nil {
		bodyParsedOK = true
		if strings.HasPrefix(stdResp.InfoID, "AU0") {
			return nil, fmt.Errorf("%w: %s %s", ErrAuthExpired, stdResp.InfoID, stdResp.InfoMsg)
		}
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("HTTP error %d: %s", resp.StatusCode, string(responseBody))
	}

	// 200-range path: use the parsed stdResp from above, or fall through.
	if !bodyParsedOK {
		return &StandardResponse{
			Success: true,
			Data:    json.RawMessage(responseBody),
		}, nil
	}

	// If the API didn't wrap the payload in a "data" property, make the raw body
	// available in the Data field so callers can unmarshal the entire response structurally.
	if len(stdResp.Data) == 0 && len(responseBody) > 0 {
		stdResp.Data = json.RawMessage(responseBody)
	}

	return &stdResp, nil
}
