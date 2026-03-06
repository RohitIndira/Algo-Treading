package indira

import (
	"fmt"
	"math"
	"strings"
)

// SymbolBuilder helps build Indira-format symbols
type SymbolBuilder struct {
	Instrument string // STK, OPT, FUT, IDX
	Symbol     string // TCS, RELIANCE, etc.
	Series     string // EQ, FO, etc.
	Exchange   string // NSE, BSE
	Token      string // Exchange token
}

// BuildSymbol creates Indira-format symbol: INSTRUMENT_SYMBOL_SERIES_EXCHANGE_TOKEN
// Example: STK_TCS_EQ_NSE_11536
func (sb *SymbolBuilder) BuildSymbol() string {
	return fmt.Sprintf("%s_%s_%s_%s_%s",
		sb.Instrument,
		sb.Symbol,
		sb.Series,
		sb.Exchange,
		sb.Token,
	)
}

// NewSymbolBuilder creates a new SymbolBuilder with default values
func NewSymbolBuilder() *SymbolBuilder {
	return &SymbolBuilder{
		Instrument: "STK", // Default to stock
		Series:     "EQ",  // Default to equity
	}
}

// ParseSymbol parses an Indira-format symbol into components
// Example: "STK_TCS_EQ_NSE_11536" -> SymbolBuilder{Instrument: "STK", Symbol: "TCS", ...}
func ParseSymbol(indiraSymbol string) (*SymbolBuilder, error) {
	parts := strings.Split(indiraSymbol, "_")
	if len(parts) != 5 {
		return nil, fmt.Errorf("invalid symbol format: expected 5 parts, got %d", len(parts))
	}

	return &SymbolBuilder{
		Instrument: parts[0],
		Symbol:     parts[1],
		Series:     parts[2],
		Exchange:   parts[3],
		Token:      parts[4],
	}, nil
}

// CleanExchangeName removes "EXCHANGE_" prefix if present
func CleanExchangeName(exchange string) string {
	return strings.TrimPrefix(exchange, "EXCHANGE_")
}

// MapOrderType maps internal order types to Indira API order types
func MapOrderType(orderType string) string {
	switch strings.ToUpper(orderType) {
	case "MARKET", "MKT", "RL-MKT":
		return "Market"
	case "LIMIT", "LMT", "RL":
		return "Limit"
	case "STOPLOSS", "SL":
		return "SL"
	case "SL-MKT", "SL-M", "STOPLOSSMARKET":
		return "SL-M"
	default:
		return "Market" // Default to market order
	}
}

// MapProductType maps internal product types to Indira API product types
func MapProductType(productType string) string {
	switch strings.ToUpper(productType) {
	case "INTRADAY", "MIS", "I":
		return "INTRADAY"
	case "DELIVERY", "CNC", "D":
		return "DELIVERY"
	case "CASH", "C":
		return "CASH"
	case "MTF", "M":
		return "MTF"
	case "BRACKET", "BRACKET_ORDER", "BO":
		return "BRACKET_ORDER" // Bracket Order — Indira API prdType per spec
	default:
		return "INTRADAY" // Default to intraday
	}
}

// MapOrderSide maps internal order sides to Indira API order actions
func MapOrderSide(side string) string {
	switch strings.ToUpper(side) {
	case "BUY", "B":
		return "BUY"
	case "SELL", "S":
		return "SELL"
	default:
		return "BUY" // Default to buy
	}
}

// MapValidity maps internal validity types to Indira API validity
func MapValidity(validity string) string {
	switch strings.ToUpper(validity) {
	case "DAY", "D":
		return "DAY"
	case "IOC", "I":
		return "IOC"
	case "GTC", "GTD":
		return "DAY" // Indira might not support GTC, default to DAY
	default:
		return "DAY" // Default to day order
	}
}

// DetermineInstrument determines instrument type from exchange and symbol
func DetermineInstrument(exchange, symbol string) string {
	exchange = strings.ToUpper(exchange)

	// Check if it's an index
	if strings.Contains(symbol, "NIFTY") || strings.Contains(symbol, "SENSEX") ||
		strings.Contains(symbol, "BANKEX") || symbol == "NIFTY" || symbol == "BANKNIFTY" {
		return "IDX"
	}

	// Check if it's futures/options by exchange segment
	if strings.Contains(exchange, "FO") || strings.Contains(exchange, "NFO") {
		if strings.Contains(symbol, "FUT") {
			return "FUT"
		}
		if strings.Contains(symbol, "OPT") || strings.Contains(symbol, "CE") || strings.Contains(symbol, "PE") {
			return "OPT"
		}
	}

	// Default to stock
	return "STK"
}

// DetermineSeries determines series from exchange and instrument
func DetermineSeries(exchange, instrument string) string {
	if instrument == "OPT" || instrument == "FUT" {
		return "FO" // Futures & Options
	}
	return "EQ" // Equity
}

// RoundToTick rounds a price to the nearest valid NSE price tick.
// NSE equity tick size: 0.05 for prices >= ₹1, 0.01 for prices < ₹1.
// Floating-point result is clamped to 2 decimal places to avoid drift.
func RoundToTick(price float64) float64 {
	if price <= 0 {
		return price
	}
	tick := 0.05
	if price < 1.0 {
		tick = 0.01
	}
	rounded := math.Round(price/tick) * tick
	// Clamp to 2 decimal places to eliminate floating-point noise (e.g. 1299.5000000001)
	return math.Round(rounded*100) / 100
}
