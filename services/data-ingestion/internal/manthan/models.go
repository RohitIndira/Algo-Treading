package manthan

import "time"

// Package manthan implements the Manthan Techno-Fundamental strategy.
//
// Core concept: Select stocks making All-Time High close price, passing
// fundamental filters (MCap, PE, F-Score, Net Profit, Volume, Age).
// Allocate capital based on their index's EMA score.

// --- Raw models matching xlsx sheet columns ---

// PEAndNetProfitRow represents one row from the PEandNetProfitSheet tab.
// Primary source for: Market Cap, PE, Net Profit.
// Join key: ISIN.
type PEAndNetProfitRow struct {
	SrNo             int
	AccordCode       string    // internal company code
	CompanyName      string    // full company name
	LatestPriceDate  time.Time // date of latest price/mcap snapshot
	LatestMarketCap  float64   // ₹ Cr (for MCap filter: 500 ≤ x ≤ 150000)
	LatestPrice      float64   // current price
	TTMEPS           float64   // trailing 12-month EPS
	TTMPE            float64   // PE ratio (filter: ≤ 60)
	PAT              float64   // Profit After Tax (filter: > 0) — from FH_PAT
	BSECode          string    // BSE scrip code
	NSESymbol        string    // NSE symbol (join key to LifeTimeHigh/BuySignal)
	ISIN             string    // ISIN (primary join key)
}

// FScoreRow represents one row from the FScore tab.
// Piotroski F-Score for fundamental strength (0-100 scale in this sheet).
// Join key: ISIN.
type FScoreRow struct {
	CoCode      string
	Date        time.Time
	Score       float64 // filter: ≥ 60
	CompanyName string
	ISIN        string
}

// LifeTimeHighRow represents one row from the LifeTimeHigh tab.
// Source for: All-Time High close price (main TRIGGER), 52W high/low (for trailing SL).
// Join key: ScripName (matches NSE Symbol).
type LifeTimeHighRow struct {
	ScripName    string
	DataTime     string
	CloseRate    float64
	Rank         int
	Period       string  // usually "Daily"
	Week52High   float64 // for 20% trailing SL calculation
	AllTimeHigh  float64 // ★ MAIN TRIGGER — close > this = breakout
	Week52Low    float64
	AllTimeLow   float64
}

// BuySignalRow represents one row from the BuySignal tab.
// Pre-computed ATH signals + Industry + volume filter flag.
// Join key: ScripName.
type BuySignalRow struct {
	ScanNo            int
	ScripName         string
	DataTime          string
	PeriodNo          int
	BarNo             int    // for "first 5 bars exclusion" (new IPOs)
	Industry          string // sector (for 25% sector cap)
	OpenRate          float64
	HighRate          float64
	LowRate           float64
	CloseRate         float64
	TotalQty          int64
	ValueLks          float64  // today's traded value in lakhs
	Trades            int64
	Week52High        float64
	AvgVal20Bars      float64  // 20BarsAvgVal — average traded value over 20 bars
	AvgVal20BarsGt1Cr bool     // 20BarsAvgVal>1Cr — pre-computed volume filter
	IsAllTimeHigh     bool     // Is All Time High(CloseLine:5:L)
	Is52WkHighBO      bool     // 52WkHighBO — 52W high breakout flag
	ATHEntry          string   // "Buy" or "-"
	ATHExit           string
	Period            string
}

// IndicesGradeRow represents one row from the IndicesGradeRange tab.
// Pre-computed EMA scores and allocation % per index.
type IndicesGradeRow struct {
	IndexName     string  // NIFTY50, MIDCAP150, SML250, NIFTYNXT50, etc.
	NoOfStocks    int
	TotalPlus     int     // count of +1 indicators
	TotalMinus    int     // count of -1 indicators
	TotalScore    int     // final score (-6 to +6)
	Allocation    float64 // ★ allocation % (0.1 = 10%, 1.0 = 100%)
	CMPClosing    float64 // current/closing price of the index
	EMA21         float64
	EMA50         float64
	EMA100        float64
}

// IndustryRow represents one row from the Industry tab (live sheet only).
// Rich per-stock metadata keyed by NSE Code / ISIN.
type IndustryRow struct {
	Name              string
	BSECode           string
	NSECode           string
	ISIN              string
	Industry          string  // the sector name
	CurrentPrice      float64
	MarketCap         float64 // in ₹ Cr
	PE                float64
	ROCE              float64
	ROE               float64
	FIIHolding        float64
	DIIHolding        float64
	EVEBITDA          float64
	PAT               float64
	OPMPrecedingQtr   float64
	OPMLatestQtr      float64
	DownFromATH       float64 // % down from all-time high
	DownFrom52WHigh   float64
	Sales             float64
}

// ExitStockDetailRow represents one row from the ExitStockDetail tab.
// Tracks WHY a stock was excluded (PE too high, FScore too low, etc.).
// Used for audit trail and to skip re-including recently rejected stocks.
type ExitStockDetailRow struct {
	Date   *time.Time
	Symbol string
	Reason string  // "PE", "FSCORE", "MCAP", "FSCORE & PE", etc.
	Value  float64 // the violating value
}

// --- Merged / Derived models ---

// ManthanStock is the final merged record after joining all sheets.
// This is what gets stored in Redis + PostgreSQL as the eligible stock universe.
type ManthanStock struct {
	// Identity
	Symbol      string    `json:"symbol"`
	ISIN        string    `json:"isin"`
	CompanyName string    `json:"company_name"`
	Industry    string    `json:"industry"`       // sector
	BSECode     string    `json:"bse_code"`
	NSESymbol   string    `json:"nse_symbol"`

	// Fundamentals
	MarketCap   float64   `json:"market_cap"`     // in ₹ Cr
	PE          float64   `json:"pe"`
	PAT         float64   `json:"pat"`            // Net Profit
	FScore      float64   `json:"fscore"`
	EPS         float64   `json:"eps"`

	// Price data
	LatestPrice  float64  `json:"latest_price"`
	ATHClose     float64  `json:"ath_close"`      // ★ trigger level
	Week52High   float64  `json:"week52_high"`    // for trailing SL
	Week52Low    float64  `json:"week52_low"`

	// Trading signals (from BuySignal tab)
	BarNo             int     `json:"bar_no"`
	AvgVal20Bars      float64 `json:"avg_val_20_bars"`
	AvgVal20BarsGt1Cr bool    `json:"avg_val_20bars_gt_1cr"`
	IsAllTimeHigh     bool    `json:"is_all_time_high"`
	ATHEntry          string  `json:"ath_entry"`  // "Buy" if today's signal

	// Derived
	MCapBucket  string    `json:"mcap_bucket"`    // "LARGE", "MID", "SMALL"
	IndexName   string    `json:"index_name"`     // NIFTY50 / MIDCAP150 / SML250
	Allocation  float64   `json:"allocation"`     // 0.0-1.0 (from index EMA)

	// Filter results
	PassesFilter bool     `json:"passes_filter"`
	FilterReason string   `json:"filter_reason"`  // why it failed (if any)

	// Metadata
	UpdatedAt   time.Time `json:"updated_at"`
}

// ManthanUniverse represents the full state after processing the xlsx.
type ManthanUniverse struct {
	Stocks          []*ManthanStock       `json:"stocks"`
	IndexAllocation map[string]float64    `json:"index_allocation"` // NIFTY50 → 0.5 etc.
	EligibleCount   int                   `json:"eligible_count"`
	TotalCount      int                   `json:"total_count"`
	LoadedAt        time.Time             `json:"loaded_at"`
}

// --- Constants ---

// MCap bucket boundaries (₹ Crores)
const (
	LargeCapMinCr = 85000.0  // Large cap: ≥ 85,000 Cr
	MidCapMinCr   = 27000.0  // Mid cap:   27,000 Cr ≤ x < 85,000 Cr
	// Small cap:  < 27,000 Cr
)

// Filter thresholds (from Manthan doc)
const (
	MCapMinCr    = 500.0     // minimum market cap
	MCapMaxCr    = 150000.0  // maximum market cap (1.5 lakh Cr)
	FScoreMin    = 60.0
	PEMax        = 60.0
	NetProfitMin = 0.0       // must be > 0
	MinBarNo     = 20        // ≥ 1 month (20 trading days) since listing
)

// Index names (as they appear in IndicesGradeRange sheet)
const (
	IndexLargeCap = "NIFTY50"
	IndexMidCap   = "NFTYMCP150"   // Confirm exact name from sheet
	IndexSmallCap = "NTYSLCP250"   // Confirm exact name from sheet
)

// MapMCapToIndex returns the appropriate index name for a given market cap.
func MapMCapToIndex(mcapCr float64) string {
	switch {
	case mcapCr >= LargeCapMinCr:
		return IndexLargeCap
	case mcapCr >= MidCapMinCr:
		return IndexMidCap
	default:
		return IndexSmallCap
	}
}

// MapMCapToBucket returns "LARGE", "MID", or "SMALL".
func MapMCapToBucket(mcapCr float64) string {
	switch {
	case mcapCr >= LargeCapMinCr:
		return "LARGE"
	case mcapCr >= MidCapMinCr:
		return "MID"
	default:
		return "SMALL"
	}
}
