package risk

import (
	"context"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
)

// Client is a stub for risk management (currently disabled)
type Client struct {
	// Add fields if needed later
}

// RiskResponse represents risk check response
type RiskResponse struct {
	Approved   bool
	RiskScore  float64
	Violations []RiskViolation
}

// RiskViolation represents a risk rule violation
type RiskViolation struct {
	Type    ViolationType
	Message string
}

// ViolationType represents the type of risk violation
type ViolationType int

const (
	ViolationUnknown ViolationType = iota
	ViolationMaxDailyTrades
	ViolationMaxLossPerDay
	ViolationMaxPositionSize
	ViolationMaxPerTradeRisk
)

func (v ViolationType) String() string {
	switch v {
	case ViolationMaxDailyTrades:
		return "MAX_DAILY_TRADES"
	case ViolationMaxLossPerDay:
		return "MAX_LOSS_PER_DAY"
	case ViolationMaxPositionSize:
		return "MAX_POSITION_SIZE"
	case ViolationMaxPerTradeRisk:
		return "MAX_PER_TRADE_RISK"
	default:
		return "UNKNOWN"
	}
}

// NewClient creates a new risk client (stub)
func NewClient() *Client {
	return &Client{}
}

// CheckPreTradeRisk validates an order against risk limits (stub - always approves)
func (c *Client) CheckPreTradeRisk(ctx context.Context, order *models.OrderRequest, strategy *models.Strategy) (*RiskResponse, error) {
	// Stub implementation - always approve
	return &RiskResponse{
		Approved:   true,
		RiskScore:  0,
		Violations: nil,
	}, nil
}
