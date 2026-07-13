package portfolio

// Wire envelopes for the /users/me/portfolio/* endpoints. All money
// fields are rupees; percentages are dimensionless (frontend adds the %).
//
// Unrealized-P&L / current-value fields use *float64 pointers so JSON
// can distinguish `null` (LTP unavailable) from `0` (position value
// really is zero — impossible for delivery holdings, but the JSON is
// explicit either way). Matches the AG.LTP contract: never render 0
// where "unknown" was meant.

// SummaryResponse — GET /api/v1/users/me/portfolio/summary
type SummaryResponse struct {
	TotalInvested            float64 `json:"totalInvested"`
	TotalRealizedPnLLifetime float64 `json:"totalRealizedPnLLifetime"`
	TodayRealizedPnL         float64 `json:"todayRealizedPnL"`
	ActiveLotCount           int32   `json:"activeLotCount"`
	ClosedLotCount           int32   `json:"closedLotCount"`
	ManthanInvested          float64 `json:"manthanInvested"`
	UserManualInvested       float64 `json:"userManualInvested"`

	// LTP-enriched aggregates. Non-nil only when at least one ACTIVE
	// position had an available LTP; otherwise null → UI renders "—".
	CurrentValue  *float64 `json:"currentValue,omitempty"`  // SUM(qty * ltp)
	UnrealizedPnL *float64 `json:"unrealizedPnL,omitempty"` // CurrentValue - TotalInvested

	// LTPStatus surfaces the api-gateway LTP subsystem health. Values:
	//   HEALTHY     — probe up + fetch succeeded (numbers can be trusted)
	//   UNAVAILABLE — probe down OR fetch failed (UnrealizedPnL is nil)
	LTPStatus string `json:"ltpStatus"`
}

// ActivePositionRow — one row on the positions page.
type ActivePositionRow struct {
	PositionID     string   `json:"positionId"`
	Origin         string   `json:"origin"`
	Symbol         string   `json:"symbol"`
	Exchange       string   `json:"exchange"`
	StrategyID     string   `json:"strategyId,omitempty"`
	SignalID       string   `json:"signalId,omitempty"`
	EntryTimeMs    int64    `json:"entryTimeMs"`
	EntryPrice     float64  `json:"entryPrice"`
	Quantity       int32    `json:"quantity"`
	InvestedAmount float64  `json:"investedAmount"`
	CurrentSL      float64  `json:"currentSL,omitempty"`
	HighSinceEntry float64  `json:"highSinceEntry,omitempty"`

	// Enrichment — nil when LTP is unavailable for this token.
	LTP           *float64 `json:"ltp,omitempty"`
	CurrentValue  *float64 `json:"currentValue,omitempty"`  // qty * ltp
	UnrealizedPnL *float64 `json:"unrealizedPnL,omitempty"` // (ltp - entry) * qty
}

// PositionsResponse — GET /api/v1/users/me/portfolio/positions
type PositionsResponse struct {
	Positions []ActivePositionRow `json:"positions"`

	// LTPStatus — see SummaryResponse.LTPStatus.
	LTPStatus string `json:"ltpStatus"`
}

// ClosedPositionRow — one row on the history page. No LTP enrichment —
// realized_pnl is the historical truth.
type ClosedPositionRow struct {
	PositionID     string  `json:"positionId"`
	Origin         string  `json:"origin"`
	Symbol         string  `json:"symbol"`
	EntryTimeMs    int64   `json:"entryTimeMs"`
	EntryPrice     float64 `json:"entryPrice"`
	Quantity       int32   `json:"quantity"`
	ExitTimeMs     int64   `json:"exitTimeMs"`
	ExitPrice      float64 `json:"exitPrice"`
	ExitReason     string  `json:"exitReason"`
	RealizedPnL    float64 `json:"realizedPnL"`
	InvestedAmount float64 `json:"investedAmount"`
}

// HistoryResponse — GET /api/v1/users/me/portfolio/history
type HistoryResponse struct {
	Positions  []ClosedPositionRow `json:"positions"`
	Page       int32               `json:"page"`
	PageSize   int32               `json:"pageSize"`
	TotalCount int32               `json:"totalCount"`
}
