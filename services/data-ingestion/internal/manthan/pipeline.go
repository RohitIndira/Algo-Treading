package manthan

import (
	"strings"
	"time"

	"go.uber.org/zap"
)

// Pipeline processes the Manthan xlsx into a list of eligible stocks.
//
// Flow (confirmed 2026-04-15):
//   1. Universe = every ScripName in BuySignal sheet
//   2. For each stock, enrich from PE / FScore / LifeTimeHigh
//   3. If ANY required field is missing → DROP (record reason)
//   4. Apply all filters (MCap, PE, FScore, PAT, BarNo, Volume, ATH trigger)
//   5. Apply portfolio caps (25% sector, 50% MCap bucket) — FCFS tie-break
//   6. Return eligible stocks for publishing to Kafka + Postgres + Redis
type Pipeline struct {
	logger *zap.Logger
}

// NewPipeline creates a new pipeline.
func NewPipeline(logger *zap.Logger) *Pipeline {
	return &Pipeline{logger: logger}
}

// DropReason records why a stock was excluded.
type DropReason struct {
	Symbol string
	Stage  string // "missing_data", "filter", "cap"
	Reason string
}

// PipelineResult holds the output of a full pipeline run.
type PipelineResult struct {
	Eligible        []*ManthanStock          // passed all filters + caps
	FilteredOut     []*ManthanStock          // passed data check but failed a filter
	Drops           []DropReason             // missing-data drops
	IndexAllocation map[string]float64       // from IndicesGradeRange
	Stats           PipelineStats
}

// PipelineStats is a counter bundle for observability.
type PipelineStats struct {
	BuySignalTotal     int
	MissingData        int
	FilterFailed       int
	CapRejected        int
	Eligible           int
	BySectorCap        int
	ByMCapBucketCap    int

	// Per-filter reject counts
	FailedMCap       int
	FailedPE         int
	FailedPAT        int
	FailedFScore     int
	FailedBarNo      int
	FailedVolume     int
	FailedATHEntry   int
	FailedIndustry   int
}

// Run executes the full pipeline on the raw xlsx data.
func (p *Pipeline) Run(raw *ReadResult) *PipelineResult {
	now := time.Now()

	// --- Build lookup indexes keyed for fast join ---
	// PE by NSESymbol (primary identity)
	peBySym := make(map[string]*PEAndNetProfitRow, len(raw.PEAndNetProfit))
	// PE by ISIN (fallback)
	peByISIN := make(map[string]*PEAndNetProfitRow, len(raw.PEAndNetProfit))
	for _, pe := range raw.PEAndNetProfit {
		if pe.NSESymbol != "" {
			peBySym[pe.NSESymbol] = pe
		}
		if pe.ISIN != "" {
			peByISIN[pe.ISIN] = pe
		}
	}

	fsByISIN := make(map[string]*FScoreRow, len(raw.FScores))
	for _, f := range raw.FScores {
		fsByISIN[f.ISIN] = f
	}

	lthBySym := make(map[string]*LifeTimeHighRow, len(raw.LifeTimeHighs))
	for _, l := range raw.LifeTimeHighs {
		lthBySym[strings.TrimSpace(l.ScripName)] = l
	}

	indexAllocation := make(map[string]float64, len(raw.IndicesGrades))
	for _, ig := range raw.IndicesGrades {
		indexAllocation[strings.ToUpper(ig.IndexName)] = ig.Allocation
	}

	// Build ScripName → Industry lookup. Priority order:
	//   1. Industry tab (live sheet) — richest source, keyed by NSECode and ISIN
	//   2. BuySignal rows with non-empty Industry — fallback for xlsx export
	industryBySym := make(map[string]string)
	industryByISIN := make(map[string]string)
	for _, ind := range raw.Industries {
		if ind.NSECode != "" && ind.Industry != "" {
			industryBySym[ind.NSECode] = ind.Industry
		}
		if ind.ISIN != "" && ind.Industry != "" {
			industryByISIN[ind.ISIN] = ind.Industry
		}
	}
	for _, bs := range raw.BuySignals {
		sym := strings.TrimSpace(bs.ScripName)
		ind := strings.TrimSpace(bs.Industry)
		if sym != "" && ind != "" {
			if _, seen := industryBySym[sym]; !seen {
				industryBySym[sym] = ind
			}
		}
	}

	res := &PipelineResult{
		IndexAllocation: indexAllocation,
	}
	stats := &res.Stats

	// --- Universe: only BuySignal rows where ATH_Entry = "Buy" ---
	//
	// Dedup by symbol at intake — the source sheet occasionally lists the
	// same ScripName on multiple rows (e.g. UNIPARTS × 2, IOLCP × 2 on
	// 2026-07-17). Without dedup we'd publish two Kafka `manthan.signals`
	// messages per symbol, downstream would emit two BUY orders, and the
	// portfolio caps math would over-count that symbol against the sector
	// bucket. Keep the FIRST row (FCFS tie-break, matches the cap rule).
	seenSym := make(map[string]struct{})
	var buyCandidates []*BuySignalRow
	dupSkipped := 0
	for _, bs := range raw.BuySignals {
		if strings.TrimSpace(bs.ATHEntry) != "Buy" {
			continue
		}
		sym := strings.TrimSpace(bs.ScripName)
		if sym != "" {
			if _, ok := seenSym[sym]; ok {
				dupSkipped++
				continue
			}
			seenSym[sym] = struct{}{}
		}
		buyCandidates = append(buyCandidates, bs)
	}
	if dupSkipped > 0 {
		p.logger.Warn("BuySignal dedup: dropped duplicate ScripName rows",
			zap.Int("skipped", dupSkipped),
			zap.Int("kept", len(buyCandidates)))
	}
	stats.BuySignalTotal = len(buyCandidates)

	for _, bs := range buyCandidates {
		sym := strings.TrimSpace(bs.ScripName)
		if sym == "" {
			stats.MissingData++
			res.Drops = append(res.Drops, DropReason{
				Symbol: "(blank)", Stage: "missing_data", Reason: "empty ScripName",
			})
			continue
		}

		// --- Industry lookup ---
		// Try BuySignal row, then Industry tab by symbol. ISIN fallback happens
		// after we enrich from PE sheet.
		industry := strings.TrimSpace(bs.Industry)
		if industry == "" {
			industry = industryBySym[sym]
		}

		// --- Enrich from PE sheet (MCap, PE, PAT, ISIN) ---
		pe, ok := peBySym[sym]
		if !ok {
			stats.MissingData++
			res.Drops = append(res.Drops, DropReason{
				Symbol: sym, Stage: "missing_data", Reason: "not in PEandNetProfitSheet",
			})
			continue
		}
		if pe.LatestMarketCap <= 0 {
			stats.MissingData++
			res.Drops = append(res.Drops, DropReason{
				Symbol: sym, Stage: "missing_data", Reason: "MarketCap missing",
			})
			continue
		}
		if pe.ISIN == "" {
			stats.MissingData++
			res.Drops = append(res.Drops, DropReason{
				Symbol: sym, Stage: "missing_data", Reason: "ISIN missing",
			})
			continue
		}
		// PE allowed to be 0 only if PAT<=0 (loss-maker) — but loss-makers fail PAT filter anyway
		if pe.TTMPE <= 0 && pe.PAT > 0 && pe.TTMEPS > 0 {
			stats.MissingData++
			res.Drops = append(res.Drops, DropReason{
				Symbol: sym, Stage: "missing_data", Reason: "PE=0 despite PAT>0 & EPS>0 (sheet bug)",
			})
			continue
		}

		// ISIN fallback for Industry (Industry tab is keyed by both NSECode and ISIN)
		if industry == "" {
			industry = industryByISIN[pe.ISIN]
		}
		if industry == "" {
			stats.MissingData++
			res.Drops = append(res.Drops, DropReason{
				Symbol: sym, Stage: "missing_data", Reason: "Industry blank (not in Industry tab)",
			})
			continue
		}

		// --- Enrich from FScore (by ISIN) ---
		fs, ok := fsByISIN[pe.ISIN]
		if !ok || fs.Score <= 0 {
			stats.MissingData++
			res.Drops = append(res.Drops, DropReason{
				Symbol: sym, Stage: "missing_data", Reason: "FScore missing",
			})
			continue
		}

		// --- Enrich from LifeTimeHigh (by symbol) ---
		lth, ok := lthBySym[sym]
		if !ok || lth.AllTimeHigh <= 0 {
			stats.MissingData++
			res.Drops = append(res.Drops, DropReason{
				Symbol: sym, Stage: "missing_data", Reason: "AllTimeHigh missing",
			})
			continue
		}
		if lth.Week52High <= 0 {
			stats.MissingData++
			res.Drops = append(res.Drops, DropReason{
				Symbol: sym, Stage: "missing_data", Reason: "52WkHigh missing",
			})
			continue
		}

		// All required data present — build the merged record.
		bucket := MapMCapToBucket(pe.LatestMarketCap)
		idxName := MapMCapToIndex(pe.LatestMarketCap)

		stock := &ManthanStock{
			Symbol:            sym,
			ISIN:              pe.ISIN,
			CompanyName:       pe.CompanyName,
			Industry:          industry,
			BSECode:           pe.BSECode,
			NSESymbol:         pe.NSESymbol,
			MarketCap:         pe.LatestMarketCap,
			PE:                pe.TTMPE,
			PAT:               pe.PAT,
			FScore:            fs.Score,
			EPS:               pe.TTMEPS,
			LatestPrice:       pe.LatestPrice,
			ATHClose:          lth.AllTimeHigh,
			Week52High:        lth.Week52High,
			Week52Low:         lth.Week52Low,
			BarNo:             bs.BarNo,
			AvgVal20Bars:      bs.AvgVal20Bars,
			AvgVal20BarsGt1Cr: bs.AvgVal20BarsGt1Cr,
			IsAllTimeHigh:     bs.IsAllTimeHigh,
			ATHEntry:          bs.ATHEntry,
			MCapBucket:        bucket,
			IndexName:         idxName,
			Allocation:        indexAllocation[strings.ToUpper(idxName)],
			UpdatedAt:         now,
		}

		// --- Apply filters ---
		if !p.applyFilters(stock, stats) {
			stats.FilterFailed++
			res.FilteredOut = append(res.FilteredOut, stock)
			continue
		}

		// Mark as passing
		stock.PassesFilter = true
		res.Eligible = append(res.Eligible, stock)
	}

	// NOTE: Sector (25%) and MCap bucket (50%) caps are NOT applied here.
	// Those are portfolio-allocation rules, applied when assigning stocks to a
	// user's capital. This phase is pure stock filtering — it produces the full
	// eligible universe, and downstream allocation logic enforces caps per user.
	stats.Eligible = len(res.Eligible)

	p.logger.Info("Manthan pipeline complete",
		zap.Int("buy_signal_total", stats.BuySignalTotal),
		zap.Int("missing_data_drops", stats.MissingData),
		zap.Int("filter_failed", stats.FilterFailed),
		zap.Int("cap_rejected", stats.CapRejected),
		zap.Int("eligible_final", stats.Eligible),
	)

	return res
}

// applyFilters returns true if stock passes all Manthan filters.
// Records per-filter failure counts in stats.
func (p *Pipeline) applyFilters(s *ManthanStock, stats *PipelineStats) bool {
	ok := true
	if s.MarketCap < MCapMinCr || s.MarketCap > MCapMaxCr {
		stats.FailedMCap++
		s.FilterReason = "MCap out of range"
		ok = false
	}
	if s.PE <= 0 || s.PE > PEMax {
		stats.FailedPE++
		if s.FilterReason == "" {
			s.FilterReason = "PE out of range"
		}
		ok = false
	}
	if s.PAT <= NetProfitMin {
		stats.FailedPAT++
		if s.FilterReason == "" {
			s.FilterReason = "PAT <= 0"
		}
		ok = false
	}
	if s.FScore < FScoreMin {
		stats.FailedFScore++
		if s.FilterReason == "" {
			s.FilterReason = "FScore < 60"
		}
		ok = false
	}
	// NOTE: BarNo (IPO age) and 20BarsAvgVal (volume) filters disabled —
	// these fields are not present in the live Google Sheet. Will be enabled
	// once the maintainer adds them or we compute them from tick data.
	return ok
}

// applyCaps enforces portfolio caps: 25% per sector, 50% per MCap bucket.
// FCFS tie-break: BuySignal arrival order wins. Later signals breaching cap
// are rejected and counted.
func (p *Pipeline) applyCaps(candidates []*ManthanStock, stats *PipelineStats) []*ManthanStock {
	const sectorCap = 0.25
	const bucketCap = 0.50

	total := len(candidates)
	if total == 0 {
		return candidates
	}

	// Compute integer caps (ceil so small portfolios aren't starved).
	// e.g., total=4 → sector cap=1 stock (25%), bucket cap=2 stocks (50%)
	sectorMax := int(float64(total)*sectorCap + 0.9999)
	bucketMax := int(float64(total)*bucketCap + 0.9999)
	if sectorMax < 1 {
		sectorMax = 1
	}
	if bucketMax < 1 {
		bucketMax = 1
	}

	sectorCount := make(map[string]int)
	bucketCount := make(map[string]int)
	out := make([]*ManthanStock, 0, total)

	for _, s := range candidates {
		if sectorCount[s.Industry] >= sectorMax {
			stats.CapRejected++
			stats.BySectorCap++
			s.FilterReason = "sector cap reached (25%)"
			s.PassesFilter = false
			continue
		}
		if bucketCount[s.MCapBucket] >= bucketMax {
			stats.CapRejected++
			stats.ByMCapBucketCap++
			s.FilterReason = "mcap bucket cap reached (50%)"
			s.PassesFilter = false
			continue
		}
		sectorCount[s.Industry]++
		bucketCount[s.MCapBucket]++
		out = append(out, s)
	}
	return out
}
