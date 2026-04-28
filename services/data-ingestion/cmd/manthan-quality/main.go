// Test: Data quality checks — null values, anomalies, duplicates.
// Usage: go run ./services/data-ingestion/cmd/manthan-quality/
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
	result, err := reader.ReadAll()
	if err != nil {
		panic(err)
	}

	// ---- PEandNetProfit quality ----
	fmt.Printf("\n=== PEandNetProfit Quality ===\n")
	peZeroMcap := 0
	peZeroPE := 0
	peNegPE := 0
	peZeroPAT := 0
	peNegPAT := 0
	peMcapAbove1_5L := 0  // beyond 1.5L Cr
	peMcapBelow500 := 0   // below 500 Cr
	for _, p := range result.PEAndNetProfit {
		if p.LatestMarketCap == 0 {
			peZeroMcap++
		}
		if p.TTMPE == 0 {
			peZeroPE++
		}
		if p.TTMPE < 0 {
			peNegPE++
		}
		if p.PAT == 0 {
			peZeroPAT++
		}
		if p.PAT < 0 {
			peNegPAT++
		}
		if p.LatestMarketCap > manthan.MCapMaxCr {
			peMcapAbove1_5L++
		}
		if p.LatestMarketCap > 0 && p.LatestMarketCap < manthan.MCapMinCr {
			peMcapBelow500++
		}
	}
	fmt.Printf("Total rows:           %d\n", len(result.PEAndNetProfit))
	fmt.Printf("MCap = 0 (missing):   %d\n", peZeroMcap)
	fmt.Printf("MCap > 1.5L Cr:       %d (above max)\n", peMcapAbove1_5L)
	fmt.Printf("MCap < 500 Cr:        %d (below min)\n", peMcapBelow500)
	fmt.Printf("PE = 0:               %d\n", peZeroPE)
	fmt.Printf("PE < 0 (negative):    %d (loss-making)\n", peNegPE)
	fmt.Printf("PAT = 0:              %d\n", peZeroPAT)
	fmt.Printf("PAT < 0 (losses):     %d\n", peNegPAT)

	// ---- FScore quality ----
	fmt.Printf("\n=== FScore Quality ===\n")
	fsZero := 0
	fsBelow60 := 0
	fsAbove60 := 0
	fsRanges := map[string]int{"0": 0, "1-30": 0, "31-59": 0, "60-79": 0, "80-100": 0}
	for _, f := range result.FScores {
		if f.Score == 0 {
			fsZero++
		}
		if f.Score < 60 {
			fsBelow60++
		}
		if f.Score >= 60 {
			fsAbove60++
		}
		switch {
		case f.Score == 0:
			fsRanges["0"]++
		case f.Score < 30:
			fsRanges["1-30"]++
		case f.Score < 60:
			fsRanges["31-59"]++
		case f.Score < 80:
			fsRanges["60-79"]++
		default:
			fsRanges["80-100"]++
		}
	}
	fmt.Printf("Total rows:       %d\n", len(result.FScores))
	fmt.Printf("Score = 0:        %d\n", fsZero)
	fmt.Printf("Score < 60:       %d\n", fsBelow60)
	fmt.Printf("Score ≥ 60:       %d\n", fsAbove60)
	fmt.Printf("Distribution:\n")
	order := []string{"0", "1-30", "31-59", "60-79", "80-100"}
	for _, k := range order {
		fmt.Printf("  %-10s %d\n", k, fsRanges[k])
	}

	// ---- LifeTimeHigh quality ----
	fmt.Printf("\n=== LifeTimeHigh Quality ===\n")
	lthZero := 0
	lthBelow52W := 0 // ATH should be >= 52W high
	lthDupes := make(map[string]int)
	for _, l := range result.LifeTimeHighs {
		if l.AllTimeHigh == 0 {
			lthZero++
		}
		if l.AllTimeHigh > 0 && l.Week52High > 0 && l.AllTimeHigh < l.Week52High {
			lthBelow52W++
		}
		lthDupes[l.ScripName]++
	}
	dupeCount := 0
	for _, c := range lthDupes {
		if c > 1 {
			dupeCount++
		}
	}
	fmt.Printf("Total rows:              %d\n", len(result.LifeTimeHighs))
	fmt.Printf("ATH = 0 (missing):       %d ⚠️\n", lthZero)
	fmt.Printf("ATH < 52W high:          %d (impossible — data bug?)\n", lthBelow52W)
	fmt.Printf("Duplicate ScripNames:    %d\n", dupeCount)

	// ---- BuySignal quality ----
	fmt.Printf("\n=== BuySignal Quality ===\n")
	bsBarNoZero := 0
	bsBarNoLessThan5 := 0
	bsBarNoLessThan20 := 0
	bsIsATHTrue := 0
	bsATHBuy := 0
	bsATH_HoldExit := make(map[string]int)

	for _, b := range result.BuySignals {
		if b.BarNo == 0 {
			bsBarNoZero++
		}
		if b.BarNo > 0 && b.BarNo < 5 {
			bsBarNoLessThan5++
		}
		if b.BarNo > 0 && b.BarNo < 20 {
			bsBarNoLessThan20++
		}
		if b.IsAllTimeHigh {
			bsIsATHTrue++
		}
		if b.ATHEntry == "Buy" {
			bsATHBuy++
		}
		bsATH_HoldExit[b.ATHEntry]++
	}
	fmt.Printf("Total rows:               %d\n", len(result.BuySignals))
	fmt.Printf("BarNo = 0:                %d\n", bsBarNoZero)
	fmt.Printf("BarNo < 5 (fresh IPO):    %d\n", bsBarNoLessThan5)
	fmt.Printf("BarNo < 20 (< 1 month):   %d\n", bsBarNoLessThan20)
	fmt.Printf("IsAllTimeHigh = true:     %d\n", bsIsATHTrue)
	fmt.Printf("ATH_Entry = 'Buy':        %d\n", bsATHBuy)
	fmt.Println("ATH_Entry value distribution:")
	type kv struct {
		K string
		V int
	}
	list := make([]kv, 0, len(bsATH_HoldExit))
	for k, v := range bsATH_HoldExit {
		list = append(list, kv{k, v})
	}
	sort.Slice(list, func(i, j int) bool { return list[i].V > list[j].V })
	for _, item := range list {
		fmt.Printf("  %q → %d\n", item.K, item.V)
	}

	// ---- Data freshness check ----
	fmt.Printf("\n=== DATA FRESHNESS ===\n")
	dateMap := make(map[string]int)
	for _, p := range result.PEAndNetProfit {
		if !p.LatestPriceDate.IsZero() {
			dateMap[p.LatestPriceDate.Format("2006-01-02")]++
		}
	}
	// Get top 3 latest dates
	dates := make([]string, 0, len(dateMap))
	for d := range dateMap {
		dates = append(dates, d)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(dates)))
	fmt.Printf("PEandNetProfit price dates (top 5):\n")
	for i, d := range dates {
		if i >= 5 {
			break
		}
		fmt.Printf("  %s: %d rows\n", d, dateMap[d])
	}

	// ---- Symbol format consistency ----
	fmt.Printf("\n=== SYMBOL FORMAT CHECK ===\n")
	symPatterns := make(map[string]int) // pattern: "lowercase", "hyphen", "dot" etc.
	for _, p := range result.PEAndNetProfit {
		sym := p.NSESymbol
		if strings.ContainsAny(sym, "abcdefghijklmnopqrstuvwxyz") {
			symPatterns["has_lowercase"]++
		}
		if strings.Contains(sym, "-") {
			symPatterns["has_hyphen"]++
		}
		if strings.Contains(sym, ".") {
			symPatterns["has_dot"]++
		}
		if strings.Contains(sym, " ") {
			symPatterns["has_space"]++
		}
	}
	for k, v := range symPatterns {
		fmt.Printf("  %s: %d\n", k, v)
	}
}
