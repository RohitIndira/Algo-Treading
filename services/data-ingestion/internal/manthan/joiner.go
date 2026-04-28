package manthan

import (
	"strings"
	"time"

	"go.uber.org/zap"
)

// Joiner merges the 6 raw sheet rows into a single ManthanStock per stock.
//
// Join strategy:
//   1. Master universe = PEandNetProfit (has ISIN + NSESymbol)
//   2. FScore joined by ISIN — stocks without FScore are DROPPED (hard filter)
//   3. LifeTimeHigh joined by NSESymbol — missing ATH is allowed (kept for record)
//   4. BuySignal joined by NSESymbol — missing signals mean no trigger today
//   5. IndicesGradeRange used to tag each stock's index allocation
//
// Filter decisions (from strategy spec + user confirmation 2026-04-15):
//   - Reject missing FScore (hard)
//   - Ignore ExitStockDetail sheet entirely (we compute exits ourselves)
type Joiner struct {
	logger *zap.Logger
}

// NewJoiner creates a new joiner.
func NewJoiner(logger *zap.Logger) *Joiner {
	return &Joiner{logger: logger}
}

// JoinStats holds counts of drops at each join step, for observability.
type JoinStats struct {
	PEInput           int // rows in PEandNetProfit
	DroppedMissingFS  int // dropped because FScore missing for this ISIN
	JoinedTotal       int // final ManthanStock count
	MatchedLTH        int // how many got ATH data from LifeTimeHigh
	MatchedBuySignal  int // how many got BuySignal row (Industry, BarNo, etc.)
	MissingLTH        int
	MissingBuySignal  int
	LargeCap          int
	MidCap            int
	SmallCap          int
}

// Join merges the raw ReadResult into a unified ManthanUniverse.
// Stocks missing FScore are dropped. All others kept for downstream filtering.
func (j *Joiner) Join(raw *ReadResult) (*ManthanUniverse, *JoinStats) {
	stats := &JoinStats{PEInput: len(raw.PEAndNetProfit)}

	// --- Build lookup indexes ---
	fsByISIN := make(map[string]*FScoreRow, len(raw.FScores))
	for _, f := range raw.FScores {
		fsByISIN[f.ISIN] = f
	}

	lthBySym := make(map[string]*LifeTimeHighRow, len(raw.LifeTimeHighs))
	for _, l := range raw.LifeTimeHighs {
		lthBySym[strings.TrimSpace(l.ScripName)] = l
	}

	bsBySym := make(map[string]*BuySignalRow, len(raw.BuySignals))
	for _, b := range raw.BuySignals {
		bsBySym[strings.TrimSpace(b.ScripName)] = b
	}

	indexAllocation := make(map[string]float64, len(raw.IndicesGrades))
	for _, ig := range raw.IndicesGrades {
		indexAllocation[ig.IndexName] = ig.Allocation
	}

	// --- Merge ---
	now := time.Now()
	stocks := make([]*ManthanStock, 0, len(raw.PEAndNetProfit))

	for _, pe := range raw.PEAndNetProfit {
		fs, ok := fsByISIN[pe.ISIN]
		if !ok {
			stats.DroppedMissingFS++
			continue
		}

		bucket := MapMCapToBucket(pe.LatestMarketCap)
		indexName := MapMCapToIndex(pe.LatestMarketCap)

		stock := &ManthanStock{
			Symbol:      pe.NSESymbol,
			ISIN:        pe.ISIN,
			CompanyName: pe.CompanyName,
			BSECode:     pe.BSECode,
			NSESymbol:   pe.NSESymbol,

			MarketCap: pe.LatestMarketCap,
			PE:        pe.TTMPE,
			PAT:       pe.PAT,
			FScore:    fs.Score,
			EPS:       pe.TTMEPS,

			LatestPrice: pe.LatestPrice,

			MCapBucket: bucket,
			IndexName:  indexName,
			Allocation: indexAllocation[indexName],

			UpdatedAt: now,
		}

		// LifeTimeHigh enrichment (ATH + 52W bounds)
		if lth, ok := lthBySym[pe.NSESymbol]; ok {
			stock.ATHClose = lth.AllTimeHigh
			stock.Week52High = lth.Week52High
			stock.Week52Low = lth.Week52Low
			stats.MatchedLTH++
		} else {
			stats.MissingLTH++
		}

		// BuySignal enrichment (Industry, BarNo, volume, today's signal)
		if bs, ok := bsBySym[pe.NSESymbol]; ok {
			stock.Industry = bs.Industry
			stock.BarNo = bs.BarNo
			stock.AvgVal20Bars = bs.AvgVal20Bars
			stock.AvgVal20BarsGt1Cr = bs.AvgVal20BarsGt1Cr
			stock.IsAllTimeHigh = bs.IsAllTimeHigh
			stock.ATHEntry = bs.ATHEntry
			// Prefer LTH's 52W high; fall back to BuySignal's if LTH missing
			if stock.Week52High == 0 {
				stock.Week52High = bs.Week52High
			}
			stats.MatchedBuySignal++
		} else {
			stats.MissingBuySignal++
		}

		switch bucket {
		case "LARGE":
			stats.LargeCap++
		case "MID":
			stats.MidCap++
		default:
			stats.SmallCap++
		}

		stocks = append(stocks, stock)
	}

	stats.JoinedTotal = len(stocks)

	j.logger.Info("Manthan join complete",
		zap.Int("pe_input", stats.PEInput),
		zap.Int("dropped_missing_fscore", stats.DroppedMissingFS),
		zap.Int("joined", stats.JoinedTotal),
		zap.Int("matched_lth", stats.MatchedLTH),
		zap.Int("missing_lth", stats.MissingLTH),
		zap.Int("matched_buy_signal", stats.MatchedBuySignal),
		zap.Int("missing_buy_signal", stats.MissingBuySignal),
		zap.Int("large_cap", stats.LargeCap),
		zap.Int("mid_cap", stats.MidCap),
		zap.Int("small_cap", stats.SmallCap),
	)

	return &ManthanUniverse{
		Stocks:          stocks,
		IndexAllocation: indexAllocation,
		TotalCount:      stats.JoinedTotal,
		LoadedAt:        now,
	}, stats
}
