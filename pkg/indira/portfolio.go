package indira

import (
	"context"
	"encoding/json"
	"fmt"
)

// ============ Portfolio Management Methods ============

// GetHoldings retrieves delivery holdings
// auth parameter contains user-specific authentication from frontend
func (c *Client) GetHoldings(ctx context.Context, auth *AuthContext) ([]Holding, error) {
	resp, err := c.doRequest(ctx, auth, "GET", "/portfolio-services/api/portfolio/v2/holdings", nil)
	if err != nil {
		return nil, fmt.Errorf("holdings request failed: %w", err)
	}
	if resp.InfoID != "" && resp.InfoID != "0" {
		return nil, fmt.Errorf("holdings broker error: %s", resp.InfoMsg)
	}

	var holdings []Holding
	if err := json.Unmarshal(resp.Data, &holdings); err != nil {
		// Try alternative format
		var altResp map[string]interface{}
		if err2 := json.Unmarshal(resp.Data, &altResp); err2 == nil {
			if holdingsData, ok := altResp["holdings"]; ok {
				holdingsBytes, _ := json.Marshal(holdingsData)
				if err3 := json.Unmarshal(holdingsBytes, &holdings); err3 != nil {
					return nil, fmt.Errorf("failed to parse holdings: %w", err)
				}
			} else {
				return nil, fmt.Errorf("failed to parse holdings: %w", err)
			}
		} else {
			return nil, fmt.Errorf("failed to parse holdings: %w", err)
		}
	}

	return holdings, nil
}

// GetPositions retrieves trading positions
// auth parameter contains user-specific authentication from frontend
func (c *Client) GetPositions(ctx context.Context, auth *AuthContext) ([]Position, error) {
	resp, err := c.doRequest(ctx, auth, "GET", "/portfolio-services/api/portfolio/v1/position-book", nil)
	if err != nil {
		return nil, fmt.Errorf("positions request failed: %w", err)
	}
	if resp.InfoID != "" && resp.InfoID != "0" {
		return nil, fmt.Errorf("positions broker error: %s", resp.InfoMsg)
	}

	var positions []Position
	if err := json.Unmarshal(resp.Data, &positions); err == nil {
		return positions, nil
	}

	// Response is a JSON object — try common key names used by Indira API.
	var altResp map[string]interface{}
	if err2 := json.Unmarshal(resp.Data, &altResp); err2 != nil {
		return nil, fmt.Errorf("failed to parse positions: %w", err2)
	}

	for _, key := range []string{"positions", "positionBook", "data", "position"} {
		if posData, ok := altResp[key]; ok {
			posBytes, _ := json.Marshal(posData)
			var parsed []Position
			if err3 := json.Unmarshal(posBytes, &parsed); err3 == nil {
				return parsed, nil
			}
		}
	}

	// Check for an explicit API error in the response.
	if status, _ := altResp["status"].(string); status == "error" {
		if msg, _ := altResp["message"].(string); msg != "" {
			return nil, fmt.Errorf("positions API error: %s", msg)
		}
	}

	// No recognised position data found — likely no open positions today.
	return []Position{}, nil
}

// ConvertPosition converts position type (e.g., INTRADAY to DELIVERY)
// auth parameter contains user-specific authentication from frontend
func (c *Client) ConvertPosition(ctx context.Context, auth *AuthContext, req *ConvertPositionRequest) error {
	resp, err := c.doRequest(ctx, auth, "POST", "/portfolio-services/api/portfolio/v1/convert-position", req)
	if err != nil {
		return fmt.Errorf("convert position request failed: %w", err)
	}
	if resp.InfoID != "" && resp.InfoID != "0" {
		return fmt.Errorf("convert position broker error: %s", resp.InfoMsg)
	}

	// Check response
	var result map[string]interface{}
	if err := json.Unmarshal(resp.Data, &result); err == nil {
		if status, ok := result["status"].(string); ok && status == "error" {
			if msg, ok := result["message"].(string); ok {
				return fmt.Errorf("convert position failed: %s", msg)
			}
			return fmt.Errorf("convert position failed")
		}
	}

	return nil
}
