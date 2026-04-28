// Test: Run full Manthan pipeline and show final eligible universe.
// Usage: go run ./services/data-ingestion/cmd/manthan-pipeline/
package main

import (
	"fmt"
	"sort"

	"go.uber.org/zap"

	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/internal/manthan"
)

const xlsxPath = "/home/rohitt/Algo-Treading/manthan-sheet-reader/Algo TTM PE EPS.xlsx"

func main() {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	reader := manthan.NewXLSXReader(xlsxPath, logger)
	raw, err := reader.ReadAll()
	if err != nil {
		panic(err)
	}

	pipeline := manthan.NewPipeline(logger)
	result := pipeline.Run(raw)

	fmt.Printf("\n=== MANTHAN PIPELINE RESULT ===\n")
	fmt.Printf("ATH_Entry=Buy stocks:  %d\n", result.Stats.BuySignalTotal)
	fmt.Printf("Dropped (data):        %d\n", result.Stats.MissingData)
	fmt.Printf("Failed filters:        %d\n", result.Stats.FilterFailed)
	fmt.Printf("Rejected (caps):       %d\n", result.Stats.CapRejected)
	fmt.Printf("  by sector cap:       %d\n", result.Stats.BySectorCap)
	fmt.Printf("  by mcap bucket cap:  %d\n", result.Stats.ByMCapBucketCap)
	fmt.Printf("FINAL ELIGIBLE:        %d\n", result.Stats.Eligible)

	fmt.Printf("\n=== PER-FILTER REJECTS ===\n")
	fmt.Printf("MCap range:       %d\n", result.Stats.FailedMCap)
	fmt.Printf("PE > 60 / <=0:    %d\n", result.Stats.FailedPE)
	fmt.Printf("PAT <= 0:         %d\n", result.Stats.FailedPAT)
	fmt.Printf("FScore < 60:      %d\n", result.Stats.FailedFScore)
	fmt.Printf("BarNo < 20:       %d\n", result.Stats.FailedBarNo)
	fmt.Printf("20BarVol <= 1Cr:  %d\n", result.Stats.FailedVolume)

	// Per-symbol drops (explicit audit)
	fmt.Printf("\n=== DROPPED STOCKS (missing data) ===\n")
	for _, d := range result.Drops {
		fmt.Printf("  %-14s %s\n", d.Symbol, d.Reason)
	}

	// Drop-reason summary
	fmt.Printf("\n=== MISSING-DATA DROP REASONS ===\n")
	reasonCount := make(map[string]int)
	for _, d := range result.Drops {
		reasonCount[d.Reason]++
	}
	type rc struct {
		R string
		N int
	}
	rl := make([]rc, 0, len(reasonCount))
	for k, v := range reasonCount {
		rl = append(rl, rc{k, v})
	}
	sort.Slice(rl, func(i, j int) bool { return rl[i].N > rl[j].N })
	for _, item := range rl {
		fmt.Printf("  %-50s %d\n", item.R, item.N)
	}

	// Final eligible stocks
	fmt.Printf("\n=== FINAL ELIGIBLE STOCKS ===\n")
	if len(result.Eligible) == 0 {
		fmt.Println("  (none today)")
	} else {
		for _, s := range result.Eligible {
			fmt.Printf("\n  %s — %s\n", s.Symbol, s.CompanyName)
			fmt.Printf("    Industry=%q  Bucket=%s  Index=%s  Alloc=%.1f%%\n",
				s.Industry, s.MCapBucket, s.IndexName, s.Allocation*100)
			fmt.Printf("    MCap=%.0f Cr  PE=%.2f  FScore=%.0f  PAT=%.2f Cr\n",
				s.MarketCap, s.PE, s.FScore, s.PAT)
			fmt.Printf("    ATH=%.2f  Price=%.2f  52WHi=%.2f\n",
				s.ATHClose, s.LatestPrice, s.Week52High)
			fmt.Printf("    BarNo=%d  20BarVal=%.0f Lks  >1Cr=%v  ATH_Entry=%s\n",
				s.BarNo, s.AvgVal20Bars, s.AvgVal20BarsGt1Cr, s.ATHEntry)
		}
	}

	// Show every stock that passed data check but failed a filter
	fmt.Printf("\n=== FILTER-REJECTED STOCKS ===\n")
	for _, s := range result.FilteredOut {
		fmt.Printf("  %-14s  reason=%-35s  MCap=%7.0f PE=%6.2f FS=%3.0f PAT=%8.2f BarNo=%d\n",
			s.Symbol, s.FilterReason, s.MarketCap, s.PE, s.FScore, s.PAT, s.BarNo)
	}
}
