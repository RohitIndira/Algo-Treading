package ema

import "math"

// EMA periods used for scoring
const (
	Period21  = 21
	Period50  = 50
	Period100 = 100
)

// StockEMA holds the 3 EMA values and computed score for a stock.
type StockEMA struct {
	Symbol  string  `json:"symbol"`
	EMA21   float64 `json:"ema_21"`
	EMA50   float64 `json:"ema_50"`
	EMA100  float64 `json:"ema_100"`
	Score   int     `json:"score"`      // -6 to +6
	AllocPct float64 `json:"alloc_pct"` // Allocation percentage based on score
}

// ComputeEMA calculates EMA from a list of close prices (oldest first).
// Returns the final EMA value.
// Formula: EMA = α × price + (1-α) × prev_EMA
// where α = 2 / (period + 1)
func ComputeEMA(closes []float64, period int) float64 {
	if len(closes) == 0 {
		return 0
	}
	if len(closes) < period {
		// Not enough data — use SMA of available data as approximation
		sum := 0.0
		for _, c := range closes {
			sum += c
		}
		return sum / float64(len(closes))
	}

	// Start with SMA of first N prices as seed
	sum := 0.0
	for i := 0; i < period; i++ {
		sum += closes[i]
	}
	ema := sum / float64(period)

	// Apply EMA formula for remaining prices
	alpha := 2.0 / float64(period+1)
	for i := period; i < len(closes); i++ {
		ema = alpha*closes[i] + (1-alpha)*ema
	}

	return math.Round(ema*100) / 100 // Round to 2 decimal places
}

// UpdateEMA computes the new EMA given today's close and yesterday's EMA.
// This is O(1) — called once per stock per day.
func UpdateEMA(todayClose, prevEMA float64, period int) float64 {
	alpha := 2.0 / float64(period+1)
	ema := alpha*todayClose + (1-alpha)*prevEMA
	return math.Round(ema*100) / 100
}

// ComputeScore calculates the EMA score (-6 to +6) for a stock.
// 6 checks: 3 price-vs-EMA + 3 EMA-alignment.
func ComputeScore(price, ema21, ema50, ema100 float64) int {
	score := 0

	// Price vs EMA (3 checks)
	if price > ema21 {
		score++
	} else {
		score--
	}
	if price > ema50 {
		score++
	} else {
		score--
	}
	if price > ema100 {
		score++
	} else {
		score--
	}

	// EMA alignment (3 checks)
	if ema21 > ema50 {
		score++
	} else {
		score--
	}
	if ema50 > ema100 {
		score++
	} else {
		score--
	}
	if ema21 > ema100 {
		score++
	} else {
		score--
	}

	return score
}

// ScoreToAllocation maps EMA score to capital allocation percentage.
// Based on Manthan strategy allocation table.
func ScoreToAllocation(score int) float64 {
	switch {
	case score >= 6:
		return 1.00 // 100%
	case score >= 4:
		return 1.00 // 100%
	case score >= 2:
		return 0.75 // 75%
	case score >= 0:
		return 0.50 // 50%
	case score >= -2:
		return 0.30 // 30%
	case score >= -4:
		return 0.20 // 20%
	default:
		return 0.10 // 10%
	}
}

// ComputeAll computes EMA 21/50/100, score, and allocation from close prices.
// closes should be ordered oldest-first.
// currentPrice is the latest LTP (may differ from last close).
func ComputeAll(closes []float64, currentPrice float64) StockEMA {
	ema21 := ComputeEMA(closes, Period21)
	ema50 := ComputeEMA(closes, Period50)
	ema100 := ComputeEMA(closes, Period100)

	price := currentPrice
	if price <= 0 && len(closes) > 0 {
		price = closes[len(closes)-1] // Use last close if no LTP
	}

	score := ComputeScore(price, ema21, ema50, ema100)
	alloc := ScoreToAllocation(score)

	return StockEMA{
		EMA21:    ema21,
		EMA50:    ema50,
		EMA100:   ema100,
		Score:    score,
		AllocPct: alloc,
	}
}
