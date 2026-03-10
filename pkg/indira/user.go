package indira

import (
	"context"
	"encoding/json"
	"fmt"
)

// ============ User & Account Methods ============

// GetAccountInfo retrieves user account information
// auth parameter contains user-specific authentication from frontend
func (c *Client) GetAccountInfo(ctx context.Context, auth *AuthContext) (*AccountInfo, error) {
	resp, err := c.doRequest(ctx, auth, "GET", "/user-services/api/user/v1/AccountInfo", nil)
	if err != nil {
		return nil, fmt.Errorf("account info request failed: %w", err)
	}

	var accountInfo AccountInfo
	if err := json.Unmarshal(resp.Data, &accountInfo); err != nil {
		return nil, fmt.Errorf("failed to parse account info: %w", err)
	}

	return &accountInfo, nil
}

// GetFundLimit retrieves fund/margin information
// auth parameter contains user-specific authentication from frontend
func (c *Client) GetFundLimit(ctx context.Context, auth *AuthContext) (*FundLimit, error) {
	resp, err := c.doRequest(ctx, auth, "GET", "/payments/api/v1/get-fund-limit", nil)
	if err != nil {
		return nil, fmt.Errorf("fund limit request failed: %w", err)
	}

	var fundLimit FundLimit
	if err := json.Unmarshal(resp.Data, &fundLimit); err != nil {
		return nil, fmt.Errorf("failed to parse fund limit: %w", err)
	}

	return &fundLimit, nil
}
