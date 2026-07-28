package consumer

import "github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"

// rupeesPerCrore converts a raw rupee figure to crores. Trade value in rupees is
// unreadable (ACC on a typical day is ~227,000,000), so thresholds are configured
// and compared in crores throughout — matching the market-cap filter's unit.
const rupeesPerCrore = 1e7

// TradeValueCr returns trade value (turnover) in ₹ crore for the given
// day-cumulative volume and last traded price.
//
//	ACC: 167223 × 1359.1 = ₹227,272,779.3 → 22.72727793 Cr
func TradeValueCr(volume int64, ltp float64) float64 {
	return float64(volume) * ltp / rupeesPerCrore
}

// PassesTradeValue reports whether a stock's turnover satisfies the strategy's
// trade-value filter, and returns the computed trade value (₹ crore) so callers
// can log the actual figure without recomputing it.
//
// This is the ONLY place the filter's semantics live. It is called from three
// sites that each have live Redis market data — the live order path
// (Handler.processMatch), the AMN preview, and the AMN backfill — because volume
// and LTP are absent from the news event and therefore unavailable to
// matcher.Evaluator. Keeping one implementation is what stops the preview from
// disagreeing with what the engine actually trades.
//
// Semantics:
//   - mode ""      → filter off, always passes
//   - mode ABOVE   → tradeValue >= Min
//   - mode BELOW   → tradeValue <= Max
//   - mode RANGE   → Min <= tradeValue <= Max
//   - unknown mode → passes (fail OPEN)
//
// Fail-open on an unrecognised mode is deliberate: a typo or a newer producer
// writing a mode this build does not know must not silently block every trade for
// that strategy. Contrast with the market-cap range, which fails closed — there
// an out-of-order range can only be corruption, whereas here the intent is
// genuinely unknown. The caller logs a warning so it is still visible.
//
// volume == 0 (pre-open, halted, or a genuinely untraded stock) yields a trade
// value of 0, which fails ABOVE and RANGE — correct, since zero turnover cannot
// clear a liquidity floor — and passes BELOW.
func PassesTradeValue(f models.TradeValueFilter, volume int64, ltp float64) (ok bool, tradeValueCr float64) {
	tv := TradeValueCr(volume, ltp)

	switch f.Mode {
	case models.TradeValueModeOff:
		return true, tv
	case models.TradeValueModeAbove:
		return tv >= f.Min, tv
	case models.TradeValueModeBelow:
		return tv <= f.Max, tv
	case models.TradeValueModeRange:
		return tv >= f.Min && tv <= f.Max, tv
	default:
		return true, tv
	}
}

// IsKnownTradeValueMode reports whether a mode string is one this build
// understands. Call sites use it to log a warning when PassesTradeValue has
// failed open on an unrecognised mode, so bad config is noisy rather than silent.
func IsKnownTradeValueMode(mode string) bool {
	switch mode {
	case models.TradeValueModeOff,
		models.TradeValueModeAbove,
		models.TradeValueModeBelow,
		models.TradeValueModeRange:
		return true
	default:
		return false
	}
}
