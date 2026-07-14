// Package tradeexec is a thin HTTP client to trade-execution's
// force-exit-strategy endpoints. Used by user-config's Deactivate +
// Delete flows when the caller requests SQUARE_OFF_AT_MARKET on a
// running strategy — we call trade-execution to place reverse exit
// orders BEFORE flipping active=false / deleting the row.
//
// The endpoints trade-execution exposes today (both under paper/ws_server.go
// for historical reasons — misleading package name):
//
//	POST /ws/paper-trades/force-exit-strategy
//	    body: {"user_id":"...","strategy_id":"..."}
//	    response: {"success":true,"strategy_id":"...","exit_orders_placed":<int>}
//
//	POST /ws/live-orders/force-exit-strategy
//	    body: {"user_id":"...","strategy_id":"...","bearer_token":"...","app_id":"...","source":"WEB"}
//	    response: same shape
//
// Note: the LIVE endpoint places aggressive LIMIT orders at LTP ± 1%
// (compliance — no market orders). Effective UX is still immediate
// fill; the mockup's "at market" copy is a UX simplification.
package tradeexec

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Client wraps a base URL + http.Client. Safe for concurrent use.
type Client struct {
	baseURL string
	http    *http.Client
}

// New wires a client to the given base URL (e.g. "http://localhost:8081").
// Uses a per-call timeout of 30s — force-exit places orders serially at
// the broker; a strategy with many lots can take a few seconds.
func New(baseURL string) *Client {
	return &Client{
		baseURL: baseURL,
		http: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// forceExitResponse is the JSON shape trade-execution returns from both
// endpoints. Only exit_orders_placed is surfaced to callers via the
// method signatures below.
type forceExitResponse struct {
	Success          bool   `json:"success"`
	StrategyID       string `json:"strategy_id"`
	ExitOrdersPlaced int    `json:"exit_orders_placed"`
}

// ForceExitStrategyPaper places reverse paper exit orders for every
// ACTIVE position of (userID, strategyID). Returns the count placed.
func (c *Client) ForceExitStrategyPaper(ctx context.Context, strategyID, userID string) (int, error) {
	body := map[string]string{
		"user_id":     userID,
		"strategy_id": strategyID,
	}
	return c.post(ctx, "/ws/paper-trades/force-exit-strategy", body)
}

// ForceExitStrategyLive places reverse LIVE exit orders (aggressive
// limit at LTP ± 1%) for every ACTIVE position of (userID, strategyID).
// bearerToken + appID + source come from the request that triggered
// the Deactivate/Delete — they're used to authenticate against the
// broker when placing exits.
func (c *Client) ForceExitStrategyLive(ctx context.Context, strategyID, userID, bearerToken, appID, source string) (int, error) {
	if bearerToken == "" {
		return 0, fmt.Errorf("tradeexec: bearer_token is required for LIVE force-exit")
	}
	body := map[string]string{
		"user_id":      userID,
		"strategy_id":  strategyID,
		"bearer_token": bearerToken,
		"app_id":       appID,
		"source":       source,
	}
	return c.post(ctx, "/ws/live-orders/force-exit-strategy", body)
}

// post marshals + POSTs body, unmarshals the response, and returns the
// exit_orders_placed count. Any non-2xx status is a hard error.
func (c *Client) post(ctx context.Context, path string, body map[string]string) (int, error) {
	buf, err := json.Marshal(body)
	if err != nil {
		return 0, fmt.Errorf("tradeexec: marshal: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(buf))
	if err != nil {
		return 0, fmt.Errorf("tradeexec: new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return 0, fmt.Errorf("tradeexec: %s POST %s: %w", req.Method, path, err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return 0, fmt.Errorf("tradeexec: %s returned %d: %s", path, resp.StatusCode, string(respBody))
	}

	var out forceExitResponse
	if err := json.Unmarshal(respBody, &out); err != nil {
		return 0, fmt.Errorf("tradeexec: decode response: %w", err)
	}
	if !out.Success {
		return 0, fmt.Errorf("tradeexec: %s returned success=false", path)
	}
	return out.ExitOrdersPlaced, nil
}
