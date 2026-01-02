package indira

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

const (
	// Default timeouts
	DefaultTimeout = 30 * time.Second

	// Environment variable names
	EnvIndiraBaseURL = ""

	// Fallback base URL if environment variable is not set
	FallbackBaseURL = ""
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

	return &Client{
		baseURL: config.BaseURL,
		httpClient: &http.Client{
			Timeout: timeout,
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
	if body != nil {
		jsonBody, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request body: %w", err)
		}
		reqBody = bytes.NewBuffer(jsonBody)
	}

	url := c.baseURL + path
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

	// Check HTTP status code
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

	return &stdResp, nil
}
