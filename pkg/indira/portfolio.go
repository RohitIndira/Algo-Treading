package indira

import (
	"context"
	"encoding/json"
	"fmt"
)

// ============ Portfolio Management Methods ============

// GetHoldings retrieves delivery holdings.
//
// Indira returns holdings under data.holdings[] (the wrapper unwraps `data`
// into resp.Data, so the array sits directly under a "holdings" key here).
// Each holding has a `symbol` *array* of exchange listings — see HoldingSymbol.
//
// Use FreeQty before submitting any SELL: the broker rejects with
// "Order entered has invalid data" when freeQty < intended sell qty even
// though the user "owns" the shares (CDSL/TPIN auth pending, auto-pledged
// for margin, T+1 not settled, etc.). holdingQty alone is insufficient.
func (c *Client) GetHoldings(ctx context.Context, auth *AuthContext) ([]Holding, error) {
	resp, err := c.doRequest(ctx, auth, "GET", "/portfolio-services/api/portfolio/v2/holdings", nil)
	if err != nil {
		return nil, fmt.Errorf("holdings request failed: %w", err)
	}

	// Standard shape: data wrapper → { invested, marketValue, holdings: [...] }
	var wrap struct {
		Holdings []Holding `json:"holdings"`
	}
	if err := json.Unmarshal(resp.Data, &wrap); err == nil && len(wrap.Holdings) > 0 {
		return wrap.Holdings, nil
	}

	// Fallback A: array directly at top level (legacy / odd brokerage modes).
	var direct []Holding
	if err := json.Unmarshal(resp.Data, &direct); err == nil {
		return direct, nil
	}

	// Fallback B: best-effort empty result if both fail. We DON'T return an
	// error because callers (the protective replayer's freeQty pre-flight,
	// the live preview panel, etc.) treat "no holdings" and "couldn't parse"
	// the same way: skip placement, alert.
	return []Holding{}, nil
}

// FreeQtyBySymbol returns a map[displaySym] → freeQty derived from current
// holdings. Convenience for SELL pre-flight: if the symbol isn't in the map
// or its value < intended sell qty, the SELL order will reject.
//
// Symbol key is `dispSym` (e.g. "KINGFA"), matching the case used elsewhere
// in our codebase (manthan_positions.symbol).
func (c *Client) FreeQtyBySymbol(ctx context.Context, auth *AuthContext) (map[string]int, error) {
	holdings, err := c.GetHoldings(ctx, auth)
	if err != nil {
		return nil, err
	}
	out := make(map[string]int, len(holdings))
	for _, h := range holdings {
		// Use the first NSE listing if available, else the first row.
		var key string
		for _, s := range h.Symbol {
			if s.Exc == "NSE" && s.DispSym != "" {
				key = s.DispSym
				break
			}
		}
		if key == "" && len(h.Symbol) > 0 {
			key = h.Symbol[0].DispSym
		}
		if key == "" {
			continue
		}
		out[key] = h.FreeQty
	}
	return out, nil
}

// GetPositions retrieves trading positions
// auth parameter contains user-specific authentication from frontend
func (c *Client) GetPositions(ctx context.Context, auth *AuthContext) ([]Position, error) {
	resp, err := c.doRequest(ctx, auth, "GET", "/portfolio-services/api/portfolio/v1/position-book", nil)
	if err != nil {
		return nil, fmt.Errorf("positions request failed: %w", err)
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
