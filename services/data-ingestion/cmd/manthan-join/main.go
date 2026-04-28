// Test: Run joiner and verify output integrity.
// Usage: go run ./services/data-ingestion/cmd/manthan-join/
package main

import (
	"fmt"
	"sort"
	"strings"

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

	joiner := manthan.NewJoiner(logger)
	universe, stats := joiner.Join(raw)

	fmt.Printf("\n=== JOIN STATS ===\n")
	fmt.Printf("PE input rows:           %d\n", stats.PEInput)
	fmt.Printf("Dropped (no FScore):     %d\n", stats.DroppedMissingFS)
	fmt.Printf("Joined (final):          %d\n", stats.JoinedTotal)
	fmt.Printf("  With ATH data:         %d\n", stats.MatchedLTH)
	fmt.Printf("  Missing ATH data:      %d\n", stats.MissingLTH)
	fmt.Printf("  With BuySignal data:   %d\n", stats.MatchedBuySignal)
	fmt.Printf("  Missing BuySignal:     %d\n", stats.MissingBuySignal)
	fmt.Printf("\n=== MCap Bucket Distribution ===\n")
	fmt.Printf("  LargeCap (≥85K Cr):   %d\n", stats.LargeCap)
	fmt.Printf("  MidCap (27K-85K Cr):   %d\n", stats.MidCap)
	fmt.Printf("  SmallCap (<27K Cr):    %d\n", stats.SmallCap)

	fmt.Printf("\n=== Index Allocation ===\n")
	type kv struct {
		K string
		V float64
	}
	idxList := make([]kv, 0, len(universe.IndexAllocation))
	for k, v := range universe.IndexAllocation {
		idxList = append(idxList, kv{k, v})
	}
	sort.Slice(idxList, func(i, j int) bool { return idxList[i].V > idxList[j].V })
	for _, item := range idxList {
		fmt.Printf("  %-15s %.2f%%\n", item.K, item.V*100)
	}

	// --- Audit: today's Buy candidates after join ---
	fmt.Printf("\n=== TODAY'S ATH_Entry=Buy CANDIDATES (post-join) ===\n")
	buyCount := 0
	for _, s := range universe.Stocks {
		if s.ATHEntry == "Buy" {
			buyCount++
			fmt.Printf("\n  %s (%s)\n", s.Symbol, s.CompanyName)
			fmt.Printf("    ISIN=%s  Industry=%q\n", s.ISIN, s.Industry)
			fmt.Printf("    MCap=%.0f Cr  Bucket=%s  Index=%s  Alloc=%.2f%%\n",
				s.MarketCap, s.MCapBucket, s.IndexName, s.Allocation*100)
			fmt.Printf("    PE=%.2f  FScore=%.0f  PAT=%.2f Cr\n", s.PE, s.FScore, s.PAT)
			fmt.Printf("    ATHClose=%.2f  LatestPrice=%.2f  52WHi=%.2f\n",
				s.ATHClose, s.LatestPrice, s.Week52High)
			fmt.Printf("    BarNo=%d  20BarAvgVal=%.0f Lks  >1Cr=%v\n",
				s.BarNo, s.AvgVal20Bars, s.AvgVal20BarsGt1Cr)
		}
	}
	fmt.Printf("\nTotal Buy candidates (after FScore join): %d\n", buyCount)

	// --- Integrity check: duplicate NSE symbols? ---
	fmt.Printf("\n=== INTEGRITY CHECKS ===\n")
	seen := make(map[string]int)
	emptySym := 0
	for _, s := range universe.Stocks {
		if strings.TrimSpace(s.Symbol) == "" {
			emptySym++
			continue
		}
		seen[s.Symbol]++
	}
	dupes := 0
	for _, c := range seen {
		if c > 1 {
			dupes++
		}
	}
	fmt.Printf("Empty NSESymbol:   %d\n", emptySym)
	fmt.Printf("Duplicate symbols: %d\n", dupes)

	// --- Sanity: Industry coverage after join ---
	withInd := 0
	withoutInd := 0
	for _, s := range universe.Stocks {
		if strings.TrimSpace(s.Industry) == "" {
			withoutInd++
		} else {
			withInd++
		}
	}
	fmt.Printf("\n=== INDUSTRY COVERAGE (post-join) ===\n")
	fmt.Printf("With Industry:    %d\n", withInd)
	fmt.Printf("Without Industry: %d\n", withoutInd)

	// --- Sanity: ATH coverage after join ---
	withATH := 0
	withoutATH := 0
	for _, s := range universe.Stocks {
		if s.ATHClose > 0 {
			withATH++
		} else {
			withoutATH++
		}
	}
	fmt.Printf("\n=== ATH COVERAGE (post-join) ===\n")
	fmt.Printf("With ATH data:    %d\n", withATH)
	fmt.Printf("Without ATH data: %d\n", withoutATH)

	// --- Sample 3 stocks per bucket (smoke test) ---
	fmt.Printf("\n=== SAMPLES PER BUCKET ===\n")
	for _, b := range []string{"LARGE", "MID", "SMALL"} {
		fmt.Printf("\n-- %s --\n", b)
		count := 0
		for _, s := range universe.Stocks {
			if s.MCapBucket != b {
				continue
			}
			if count >= 3 {
				break
			}
			fmt.Printf("  %-12s MCap=%8.0f Cr PE=%6.2f FS=%3.0f ATH=%8.2f Price=%8.2f\n",
				s.Symbol, s.MarketCap, s.PE, s.FScore, s.ATHClose, s.LatestPrice)
			count++
		}
	}
}
