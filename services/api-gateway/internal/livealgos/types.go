// Package livealgos owns the "user's deployed algorithms dashboard"
// concept — the Live Algos screen on the mobile app.
//
// The screen is served by ONE endpoint (GET /api/v1/users/me/live-algos)
// which returns:
//
//   - A summary block (aggregated Net P&L, Today's P&L, Total Deployed
//     Capital across all the user's algos)
//   - A list of AlgoRow entries (one per deployed strategy)
//
// Data flow:
//
//   AuthMiddleware → extracts userID from JWT
//        ↓
//   Handler → calls user-config gRPC (ListUserStrategies for this user)
//           → calls algos.Catalog (for algo metadata like name/logo/style)
//           → calls Store methods for open-positions count + P&L (Phase 2)
//           → aggregator.Build produces this package's Response
//        ↓
//   Envelope wraps in {infoID, infoMsg, timestamp, data}
//
// Phase 1 shipping today:
//   - Real strategy list from user-config
//   - Real deployed capital (per strategy)
//   - Real status derived from strategy.active + trading_mode
//   - Placeholder P&L numbers (pnlPending: true), openPositions=0, winRate=0
//
// Phase 2 (follow-up PR):
//   - Real P&L math (realized from closed trades + unrealized from LTP)
//   - Real openPositions count from trading_db.manthan_positions
//   - Real winRate from closed-trade history
//   - actionRequired from an alerts subsystem + FCM push
package livealgos

// Response is the shape of the "data" field returned to the mobile app.
// The envelope (infoID, infoMsg, timestamp) is added by the handler
// via respondIndiraOK.
type Response struct {
	Summary Summary   `json:"summary"`
	Algos   []AlgoRow `json:"algos"` // empty slice, not null, when the user has no deployments
}

// Summary is the header block above the algo list — 3 tiles worth of
// data aggregated across ALL of the user's deployed algos.
type Summary struct {
	NetPnL               PnL   `json:"netPnL"`
	TodayPnL             PnL   `json:"todayPnL"`
	TotalDeployedCapital int64 `json:"totalDeployedCapital"` // rupees
}

// PnL is one profit-and-loss tile — ₹ amount + % return relative to
// the capital the P&L is computed against. `pnlPending: true` signals
// the frontend that we don't have real numbers yet (Phase 1) and to
// render a placeholder / spinner rather than "₹0 (0%)".
type PnL struct {
	Amount     int64   `json:"amount"`
	Percent    float64 `json:"percent"`
	PnLPending bool    `json:"pnlPending,omitempty"` // true = value not computed yet
}

// AlgoRow is one card on the Live Algos screen — everything needed to
// render a single deployed strategy's tile.
//
// Status semantics:
//
//	LIVE     — stopped_at IS NULL, active=true, trading_mode is a live
//	           mode (LIVE_STOCK, LIVE_HFT, etc.). Actively placing orders.
//	PAUSED   — stopped_at IS NULL, either active=false (Pause modal)
//	           or trading_mode == PAPER. Shown but no live trading.
//	           RESUME flips it back to LIVE.
//	STOPPED  — stopped_at IS NOT NULL. Terminal — cannot be resumed.
//	           Row is kept purely so the user sees history; deploy a
//	           fresh strategy to run again.
//
// StrategyID (not part of AlgoRow's public JSON but used server-side)
// identifies the specific deployed configuration. AlgoRow.ID is the
// STATIC algo id (e.g., "algo_manthan_v1") used to render the card's
// name/logo/description — same id as returned by GET /algos.
type AlgoRow struct {
	// Static metadata (from algos.Catalog)
	ID    string `json:"id"`
	Name  string `json:"name"`
	Type  string `json:"type"`
	Style string `json:"style"`
	Logo  string `json:"logo"`

	// Per-deployment fields (from user-config-service)
	StrategyID      string `json:"strategyId"`      // unique per deploy — needed for pause/stop actions
	Status          string `json:"status"`          // LIVE | PAUSED | STOPPED
	DeployedCapital int64  `json:"deployedCapital"` // rupees

	// Runtime numbers (Phase 1: placeholders with pnlPending=true)
	NetPnL         PnL     `json:"netPnL"`
	TodayPnL       PnL     `json:"todayPnL"`
	WinRatePct     float64 `json:"winRatePct"`
	OpenPositions  int32   `json:"openPositions"`

	// Optional alert card (Phase 2: from alerts subsystem)
	ActionRequired *ActionRequired `json:"actionRequired,omitempty"`
}

// ActionRequired is the coloured warning banner that shows on some
// cards ("Action Required — Stop-loss failed on RELIANCE"). When nil,
// the frontend omits the banner entirely.
type ActionRequired struct {
	Type    string `json:"type"`    // machine-readable e.g. "SL_FAILED", "MARGIN_SHORTFALL"
	Title   string `json:"title"`   // "Action Required"
	Message string `json:"message"` // user-facing detail line
}

// Status constants — return these exact strings so the frontend can
// key off them (case-sensitive).
const (
	StatusLive    = "LIVE"
	StatusPaused  = "PAUSED"
	StatusStopped = "STOPPED"
)

// ─── Details page ────────────────────────────────────────────────────
//
// Shapes returned by GET /api/v1/users/me/live-algos/{strategyId}.
// This is Screens 1 & 2 of the Details page mockup — everything except
// the drilldown lists (Holdings, Trades, Stock P&L).

// DetailsResponse is the full "data" payload for the Details endpoint.
type DetailsResponse struct {
	StrategyID      string       `json:"strategyId"`
	AlgoID          string       `json:"algoId"`
	Name            string       `json:"name"`
	Status          string       `json:"status"`
	DeployedCapital int64        `json:"deployedCapital"`
	AsOf            string       `json:"asOf"` // ISO date of latest data
	NetPnL          PnL          `json:"netPnL"`
	TodayPnL        PnL          `json:"todayPnL"`
	Chart           DetailsChart `json:"chart"`
	Metrics         Metrics      `json:"metrics"`
	TopHoldings     []Holding    `json:"topHoldings"`     // top 3-5 by invested
	AllocationSector  []AllocationEntry `json:"allocationBySector"`
	AllocationMCap    []AllocationEntry `json:"allocationByMarketCap"`
	TopPastTrades   []TradeRow   `json:"topPastTrades"`   // most recent 5 exited

	// LTPStatus surfaces the LTP subsystem's health for this response:
	// "HEALTHY" — probe up + fetch succeeded (holdings carry a real ltp)
	// "UNAVAILABLE" — probe down OR MGet failed (holdings' ltp fields are 0
	//                 but the UI should render "—" instead of "0")
	// See docs/portfolio_service_design.md §4 for the no-silent-fail
	// contract and reference_ltp_tunnel_silent_fail for the incident.
	LTPStatus       string       `json:"ltpStatus,omitempty"`
}

// DetailsChart is the P&L-over-time line on Screen 1. Points are
// indexed at 100 = deployment day. Frontend renders a growth curve.
type DetailsChart struct {
	Points []DetailsChartPoint `json:"points"`
}
type DetailsChartPoint struct {
	Date string  `json:"date"` // ISO YYYY-MM-DD
	Pct  float64 `json:"pct"`  // e.g. 116.40 = +16.40% from base
}

// Metrics is the 6-tile grid: Total Return, CAGR, Win Rate, Max DD,
// Avg Holding, Sharpe. Every field is a real number (never nil).
type Metrics struct {
	TotalReturnPct float64 `json:"totalReturnPct"`
	CAGRPct        float64 `json:"cagrPct"`
	WinRatePct     float64 `json:"winRatePct"`
	MaxDrawdownPct float64 `json:"maxDrawdownPct"` // negative — e.g. -7.4
	AvgHoldingDays float64 `json:"avgHoldingDays"`
	SharpeRatio    float64 `json:"sharpeRatio"`
}

// AllocationEntry is one row of the sector / MCap breakdown lists on
// Screen 1's expandable sections.
type AllocationEntry struct {
	Label string  `json:"label"` // "Information Technology" / "Small Cap" / ...
	Pct   float64 `json:"pct"`   // 0-100
}

// ─── Holdings drilldown ──────────────────────────────────────────────
//
// Response shape for GET /api/v1/users/me/live-algos/{strategyId}/holdings
// — the full sortable list on Screens 3 & 4.

// HoldingsResponse wraps the full active positions list.
type HoldingsResponse struct {
	Holdings  []Holding `json:"holdings"`
	// LTPStatus — see DetailsResponse.LTPStatus for semantics.
	LTPStatus string    `json:"ltpStatus,omitempty"`
}

// Holding is one card in the holdings list. Currency fields are
// rupees. Percentages are dimensionless (frontend adds the % sign).
type Holding struct {
	Symbol       string  `json:"symbol"`
	Industry     string  `json:"industry"`
	MCapBucket   string  `json:"mcapBucket"`   // LARGE / MID / SMALL / MICRO / NANO
	Quantity     int     `json:"quantity"`
	EntryPrice   float64 `json:"entryPrice"`   // avg buy per share
	Invested     int64   `json:"invested"`     // rupees
	CurrentValue int64   `json:"currentValue"` // rupees — quantity × LTP
	NetPnL       int64   `json:"netPnL"`       // rupees
	NetPnLPct    float64 `json:"netPnLPct"`    // vs Invested
	LTP          float64 `json:"ltp"`
	LTPChangePct float64 `json:"ltpChangePct"` // vs prev_close (today's move)
}

// ─── Past Trades drilldown ───────────────────────────────────────────
//
// Response shape for GET /api/v1/users/me/live-algos/{strategyId}/trades
// — the full sortable list on Screens 5 & 6.

// TradesResponse wraps every EXITED position for the strategy.
type TradesResponse struct {
	Trades []TradeRow `json:"trades"`
}

// TradeRow is one closed-position card. Realised P&L is signed
// (positive = gain, negative = loss). ProductType is fixed to
// "DELIVERY" for Manthan (positional algo, CNC on the broker side).
type TradeRow struct {
	Symbol       string  `json:"symbol"`
	Industry     string  `json:"industry"`
	ProductType  string  `json:"productType"`  // DELIVERY / INTRADAY
	EntryPrice   float64 `json:"entryPrice"`
	ExitPrice    float64 `json:"exitPrice"`
	Quantity     int     `json:"quantity"`
	RealisedPnL  int64   `json:"realisedPnL"`  // rupees, signed
	RealisedPct  float64 `json:"realisedPct"`
	ExitReason   string  `json:"exitReason"`   // SL_TRIGGERED / TARGET / MANUAL_EXIT / ...
	HoldingDays  int     `json:"holdingDays"`
	ExitDate     string  `json:"exitDate"`     // ISO YYYY-MM-DD
}

// ─── Stock P&L drilldown ─────────────────────────────────────────────
//
// Response shape for GET /api/v1/users/me/live-algos/{strategyId}/holdings/{symbol}
// — Screen 7 (individual stock overview + BUY/SELL trades table).

// StockPnLResponse is the drilldown for one symbol under one strategy.
type StockPnLResponse struct {
	Symbol      string          `json:"symbol"`
	Industry    string          `json:"industry"`
	ProductType string          `json:"productType"`
	RealisedPnL int64           `json:"realisedPnL"` // sum across all positions for this symbol
	Overview    StockPnLOverview `json:"overview"`
	Trades      []StockPnLTrade  `json:"trades"` // individual BUY/SELL fills
}

// StockPnLOverview is the "Overview" panel on Screen 7.
type StockPnLOverview struct {
	Quantity       int     `json:"quantity"`       // net position or cumulative BUY qty
	AvgBuyPrice    float64 `json:"avgBuyPrice"`
	TotalBuyValue  int64   `json:"totalBuyValue"`
	AvgSellPrice   float64 `json:"avgSellPrice"`
	TotalSellValue int64   `json:"totalSellValue"`
}

// StockPnLTrade is one row in the trades table at the bottom of
// Screen 7 — a single BUY or SELL order fill.
type StockPnLTrade struct {
	Date     string  `json:"date"`     // ISO YYYY-MM-DD
	Type     string  `json:"type"`     // BUY / SELL
	Quantity int     `json:"quantity"`
	Price    float64 `json:"price"`    // avg_fill_price
}
