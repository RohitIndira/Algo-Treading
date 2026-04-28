// Test: Analyze ExitStockDetail — who got excluded today and why.
// Usage: go run ./services/data-ingestion/cmd/manthan-exits/
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

	fmt.Printf("\n=== EXIT STOCK DETAIL ANALYSIS ===\n")
	fmt.Printf("Total rows: %d\n\n", len(result.ExitStocks))

	// Group by reason
	reasonCount := make(map[string]int)
	reasonExamples := make(map[string][]string)
	for _, e := range result.ExitStocks {
		r := e.Reason
		if r == "" {
			r = "(empty)"
		}
		reasonCount[r]++
		if len(reasonExamples[r]) < 3 {
			reasonExamples[r] = append(reasonExamples[r], fmt.Sprintf("%s=%.2f", e.Symbol, e.Value))
		}
	}

	// Sort by count
	type rc struct {
		Reason string
		Count  int
	}
	list := make([]rc, 0, len(reasonCount))
	for k, v := range reasonCount {
		list = append(list, rc{k, v})
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Count > list[j].Count })

	fmt.Printf("%-40s %6s  %s\n", "REASON", "COUNT", "EXAMPLES")
	fmt.Println(strings.Repeat("─", 100))
	for _, item := range list {
		fmt.Printf("%-40s %6d  %v\n", item.Reason, item.Count, reasonExamples[item.Reason])
	}

	// Dates coverage
	fmt.Printf("\n=== DATE COVERAGE ===\n")
	dateCount := make(map[string]int)
	withDate := 0
	withoutDate := 0
	for _, e := range result.ExitStocks {
		if e.Date != nil {
			dateCount[e.Date.Format("2006-01-02")]++
			withDate++
		} else {
			withoutDate++
		}
	}
	fmt.Printf("With date:    %d\n", withDate)
	fmt.Printf("Without date: %d ⚠️\n", withoutDate)
	if len(dateCount) > 0 {
		fmt.Printf("\nDates:\n")
		type dc struct {
			D string
			N int
		}
		dl := make([]dc, 0, len(dateCount))
		for k, v := range dateCount {
			dl = append(dl, dc{k, v})
		}
		sort.Slice(dl, func(i, j int) bool { return dl[i].D < dl[j].D })
		for _, d := range dl {
			fmt.Printf("  %s: %d exits\n", d.D, d.N)
		}
	}

	// Cross-check: is SGFIN both in BuySignal (Buy) AND ExitStockDetail?
	fmt.Printf("\n=== CONFLICTING SIGNALS CHECK ===\n")
	buyStocks := make(map[string]bool)
	for _, b := range result.BuySignals {
		if b.ATHEntry == "Buy" {
			buyStocks[b.ScripName] = true
		}
	}
	fmt.Printf("ATH_Entry=Buy stocks: %d\n", len(buyStocks))

	exitStocks := make(map[string][]string)
	for _, e := range result.ExitStocks {
		exitStocks[e.Symbol] = append(exitStocks[e.Symbol], e.Reason)
	}

	conflictCount := 0
	for sym := range buyStocks {
		if reasons, ok := exitStocks[sym]; ok {
			fmt.Printf("  ⚠️  %s: signals BUY but also in exit list — reasons: %v\n", sym, reasons)
			conflictCount++
		}
	}
	if conflictCount == 0 {
		fmt.Println("  No conflicts")
	}

	// Check: ALL exit reasons unique values
	fmt.Printf("\n=== UNIQUE EXIT REASONS (exact strings) ===\n")
	for _, item := range list {
		fmt.Printf("  %q  → %d stocks\n", item.Reason, item.Count)
	}
}
