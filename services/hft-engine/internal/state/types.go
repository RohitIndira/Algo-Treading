// Package state holds the data structures the HFT engine works with.
// Pure data — no methods, no behaviour. Other packages (manager, strategy,
// repo, grpcserver) import from here so the whole codebase speaks the same
// vocabulary.
//
// The shapes mirror the C++ template the strategy was designed against:
//   - Config       ↔ struct StrategyConfig
//   - SideState    ↔ split out of struct StrategyState (one per BUY/SELL)
//   - ChunkState   ↔ NEW — needed because real fills are partial; the C++
//                    template assumed instant fill on place.
//   - MarketData   ↔ struct MarketData
//   - FillEvent    ↔ NEW — what trade-execution forwards to us per broker
//                    order update.
//
// Phase 1: nothing here is read or written yet — we just define the types.
// Phase 2 hangs the state machine off them.
package state

import "time"

// ─────────────────────────────────────────────────────────────────────────
// Enums
// ─────────────────────────────────────────────────────────────────────────

// Side is which book leg we're operating on. Strategies can be configured
// as BUY-only, SELL-only, or BOTH (where each side runs an independent
// state machine).
type Side string

const (
	SideBuy  Side = "BUY"
	SideSell Side = "SELL"
)

// ChunkStatus tracks the broker-side life of a single resting order.
type ChunkStatus string

const (
	ChunkOpen      ChunkStatus = "OPEN"      // placed, 0 fills
	ChunkPartial   ChunkStatus = "PARTIAL"   // 0 < filled < qty
	ChunkFilled    ChunkStatus = "FILLED"    // filled == qty (terminal-good)
	ChunkCancelled ChunkStatus = "CANCELLED" // we cancelled, broker ack'd (terminal-cancel)
)

// Mode gates whether broker calls are real or simulated.
//   PAPER — engine logs intended actions, never touches Indira.
//   LIVE  — engine places, modifies, cancels real orders.
// Defaults to PAPER everywhere; flipping to LIVE is a deliberate user choice.
type Mode string

const (
	ModePaper Mode = "PAPER"
	ModeLive  Mode = "LIVE"
)

// HaltReason explains why a side is no longer placing chunks.
// Empty string means "still running".
type HaltReason string

const (
	HaltNone           HaltReason = ""
	HaltMaxReached     HaltReason = "max_reached"     // hit max_buy_qty or max_sell_qty
	HaltPriceBand      HaltReason = "price_band"      // ask > buy_limit or bid < sell_limit
	HaltWindowClosed   HaltReason = "window_closed"   // outside trade-window
	HaltExitAPI        HaltReason = "exit"            // user pressed Stop
	HaltAuthExpired    HaltReason = "auth"            // AU004 even after JWT refresh
	HaltBrokerHardFail HaltReason = "broker"          // broker keeps rejecting place
	HaltNoData         HaltReason = "no_data"         // > N seconds since last tick
)

// ─────────────────────────────────────────────────────────────────────────
// Config — comes from strategies.trade_config JSON, never mutated at runtime
// ─────────────────────────────────────────────────────────────────────────

// Config is what the strategy's UI form persists into trade_configs.config.
// repo.LoadConfig() unmarshals one of these from the JSONB column.
type Config struct {
	StrategyID string `json:"strategy_id"` // UUID, set by repo from the row id
	UserID     string `json:"user_id"`     // set by repo from strategies.user_id

	// What we're trading
	Symbol      string `json:"symbol"`       // "IDEA"
	ISIN        string `json:"isin"`         // "INE669E01016" — used to resolve broker token
	Exchange    string `json:"exchange"`     // "NSE"
	ProductType string `json:"product_type"` // "INTRADAY" or "DELIVERY"

	Side Side `json:"side"` // BUY | SELL | BOTH ("BOTH" means run both legs)

	// Quantity caps + per-chunk size
	MaxBuyQty     int `json:"max_buy_qty"`
	MaxSellQty    int `json:"max_sell_qty"`
	SingleBuyQty  int `json:"single_buy_qty"`
	SingleSellQty int `json:"single_sell_qty"`

	// Price band — halt if ask escapes (for buy) or bid escapes (for sell)
	BuyLimitPrice  float64 `json:"buy_limit_price"`
	SellLimitPrice float64 `json:"sell_limit_price"`
	TickSize       float64 `json:"tick_size"` // rounding granularity for limit prices

	// Trade window in IST. "HH:MM" strings; "" means no window enforcement.
	WindowStart string `json:"window_start"` // e.g. "09:15"
	WindowEnd   string `json:"window_end"`   // e.g. "15:30"

	// Behaviour switches
	ModifyOnPriceChange bool `json:"modify_on_price_change"` // default true: chase the touch
	Mode                Mode `json:"mode"`                   // default PAPER
}

// ─────────────────────────────────────────────────────────────────────────
// Runtime state — owned and mutated by the strategy goroutine (Phase 2)
// ─────────────────────────────────────────────────────────────────────────

// ChunkState describes the ONE broker order currently in flight for a side.
// When nil, the side is IDLE (ready to place the next chunk on the next tick).
// When non-nil, the side is in CHUNK_OPEN or CHUNK_PARTIAL depending on Filled.
type ChunkState struct {
	Seq           int         `json:"seq"`              // 1, 2, 3, ... within this strategy run
	Qty           int         `json:"qty"`              // requested chunk size (e.g. 10)
	Filled        int         `json:"filled"`           // accumulated from FillEvents
	LimitPrice    float64     `json:"limit_price"`      // current resting limit (changes on MODIFY)
	BrokerOrderID string      `json:"broker_order_id"`  // Indira ordId
	PlacedAt      time.Time   `json:"placed_at"`
	ModifyCount   int         `json:"modify_count"`     // how many MODIFYs since PLACE
	Status        ChunkStatus `json:"status"`
}

// SideState is the per-leg view (one for BUY, one for SELL). For a BUY-only
// strategy, the SELL SideState is never touched.
type SideState struct {
	Position   int          `json:"position"`    // sum of all filled chunk qty so far
	Done       bool         `json:"done"`        // terminal — no more chunks will be placed
	HaltReason HaltReason   `json:"halt_reason"` // why Done is true (empty when Done=false)

	Current *ChunkState   `json:"current"` // chunk in flight; nil ⇒ IDLE
	History []ChunkState  `json:"history"` // closed chunks (FILLED or CANCELLED) in placement order
}

// MarketData is one bid/ask snapshot. Produced by internal/marketws and
// consumed by the strategy goroutine. The C++ template's MarketData struct
// is the same shape; we add a server-side timestamp for latency tracking.
type MarketData struct {
	Symbol string    `json:"symbol"`
	Bid    float64   `json:"bid"`
	Ask    float64   `json:"ask"`
	BidQty int       `json:"bid_qty"`
	AskQty int       `json:"ask_qty"`
	LTP    float64   `json:"ltp"`
	At     time.Time `json:"at"` // when our subscriber received this update
}

// FillEvent is one broker-observed event for one of our chunks. Pushed by
// trade-execution via the SubmitFills gRPC stream. The strategy goroutine
// applies these to its ChunkState (incrementing Filled, switching status).
type FillEvent struct {
	StrategyID     string    `json:"strategy_id"`
	BrokerOrderID  string    `json:"broker_order_id"`
	EventType      string    `json:"event_type"`       // FILL | CANCEL | REJECT
	FillQty        int       `json:"fill_qty"`         // incremental, only for FILL
	FillPrice      float64   `json:"fill_price"`       // only for FILL
	Reason         string    `json:"reason"`           // only for REJECT/CANCEL
	ExchangeTimeUS int64     `json:"exchange_time_us"` // broker timestamp
	At             time.Time `json:"at"`               // when our service received it
}

// Strategy is the full runtime state of one running HFT strategy. The
// manager keeps a map[strategyID]*Strategy; Phase 2 will add the goroutine
// that owns this struct and is the only writer to it.
type Strategy struct {
	Cfg Config // immutable after manager.Start

	Buy  SideState
	Sell SideState

	LastMD     MarketData
	StartedAt  time.Time
	LastTickAt time.Time
	Active     bool // false after Exit / halt
}
