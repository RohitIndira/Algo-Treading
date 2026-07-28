package indira

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	// Default timeouts
	DefaultTimeout = 30 * time.Second

	// Environment variable names
	EnvIndiraBaseURL = "INDIRA_BASE_URL"
	EnvIndiraAPIKey  = "INDIRA_API_KEY"
	EnvIndiraAlgoID  = "INDIRA_ALGO_ID"

	// Fallback base URL if environment variable is not set
	FallbackBaseURL = "https://localhost:8000"

	// FallbackAPIKey is the shared api-key sent for every client/user if
	// INDIRA_API_KEY is not set in the environment.
	FallbackAPIKey = "b2b_ak_e25c9c6726a905dc1027a8f7bde41559"
)

// getDefaultBaseURL returns the base URL from environment or uses fallback
func getDefaultBaseURL() string {
	if url := os.Getenv(EnvIndiraBaseURL); url != "" {
		return url
	}
	return FallbackBaseURL
}

// getDefaultAPIKey returns the Indira api-key from environment or uses fallback
func getDefaultAPIKey() string {
	if key := os.Getenv(EnvIndiraAPIKey); key != "" {
		return key
	}
	return FallbackAPIKey
}

// Client represents an Indira Securities API client
// This client is stateless and thread-safe for concurrent use by multiple users
type Client struct {
	baseURL    string
	apiKey     string
	algoID     string
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
	APIKey  string
	AlgoID  string
	Timeout time.Duration
}

// NewClient creates a new Indira API client
// The client is stateless and can be shared across multiple users
func NewClient(config Config) *Client {
	if config.BaseURL == "" {
		config.BaseURL = getDefaultBaseURL()
	}
	if config.APIKey == "" {
		config.APIKey = getDefaultAPIKey()
	}
	if config.AlgoID == "" {
		config.AlgoID = os.Getenv(EnvIndiraAlgoID)
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
		apiKey:  config.APIKey,
		algoID:  config.AlgoID,
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

// indiaIST is UTC+5:30. Used to gate verbose position-book body logging to the
// EOD square-off window so full books aren't logged all day.
var indiaIST = time.FixedZone("IST", 5*3600+30*60)

// eodPositionBookDumpWindow reports whether now is inside the window during
// which the FULL position-book API response is logged per user (for diagnosing
// auto-square-off skips). Window: 15:00–15:30 IST.
func eodPositionBookDumpWindow() bool {
	now := time.Now().In(indiaIST)
	mins := now.Hour()*60 + now.Minute()
	return mins >= 15*60 && mins <= 15*60+30
}

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

	// path is normally relative to c.baseURL (the versioned /v1 REST surface),
	// but a caller may pass an already-absolute URL (e.g. the order-notify WS
	// token endpoint, which lives on a different, unversioned path than the
	// order-services/portfolio-services APIs) to bypass baseURL entirely.
	url := c.baseURL + path
	if strings.HasPrefix(path, "http://") || strings.HasPrefix(path, "https://") {
		url = path
	}
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
	// Shared api-key sent on every request for every client (same value for all users)
	req.Header.Set("api-key", c.apiKey)

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
	// Always send SSO as true by default
	req.Header.Set("sso", "true")

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

	// Position-book responses are normally too noisy to log in full, so only the
	// status is logged. EXCEPTION: during the EOD square-off window (15:00–15:30
	// IST) log the COMPLETE body per user, so the exact broker book each user's
	// square-off saw is captured for diagnosis.
	if path == "/portfolio-services/api/portfolio/v1/position-book" {
		if eodPositionBookDumpWindow() {
			log.Printf("[indira] ← %s %s  status=%d  user=%s  body=%s", method, path, resp.StatusCode, auth.UserId, string(responseBody))
		} else {
			log.Printf("[indira] ← %s %s  status=%d", method, path, resp.StatusCode)
		}
	} else {
		log.Printf("[indira] ← %s %s  status=%d  body=%s", method, path, resp.StatusCode, string(responseBody))
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("HTTP error %d: %s", resp.StatusCode, string(responseBody))
	}

	// Parse response
	var stdResp StandardResponse
	if err := json.Unmarshal(responseBody, &stdResp); err != nil {
		// Some endpoints might return different format, try to handle gracefully
		return &StandardResponse{
			Success: resp.StatusCode >= 200 && resp.StatusCode < 300,
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
