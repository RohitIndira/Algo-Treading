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
	"time"
)

const (
	// Default timeouts
	DefaultTimeout = 30 * time.Second

	// Environment variable names
	EnvIndiraBaseURL = "INDIRA_BASE_URL"

	// Fallback base URL if environment variable is not set
	FallbackBaseURL = "https://localhost:8000"
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
	log.Printf("[indira] → %s %s", method, path)
	// log.Printf("[indira] → %s %s  body=%s", method, path, string(jsonBody))
	req, err := http.NewRequestWithContext(ctx, method, url, reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Set standard headers
	req.Header.Set("Content-Type", "application/json")

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

	// Check HTTP status code — skip body for noisy position-book responses
	if path == "/portfolio-services/api/portfolio/v1/position-book" {
		log.Printf("[indira] ← %s %s  status=%d", method, path, resp.StatusCode)
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
