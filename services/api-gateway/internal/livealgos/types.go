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
//	LIVE     — strategy.active == true AND trading_mode is a live mode
//	           (LIVE_STOCK, LIVE_HFT, etc.). Actively placing orders.
//	PAUSED   — strategy exists in the DB but a paused-by-user flag or
//	           trading_mode == PAPER is set. Shown but no live trading.
//	STOPPED  — strategy.active == false. Deactivated but the config row
//	           still exists so we can show a historical entry until the
//	           user deletes it.
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
