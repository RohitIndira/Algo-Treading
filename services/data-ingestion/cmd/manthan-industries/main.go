// Test: Analyze all unique industries for sector cap logic.
// Usage: go run ./services/data-ingestion/cmd/manthan-industries/
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

	industries := make(map[string]int)
	emptyCount := 0
	nonEmptyCount := 0
	samplesEmpty := []string{}

	for _, b := range result.BuySignals {
		if strings.TrimSpace(b.Industry) == "" {
			emptyCount++
			if len(samplesEmpty) < 20 {
				samplesEmpty = append(samplesEmpty, b.ScripName)
			}
		} else {
			industries[b.Industry]++
			nonEmptyCount++
		}
	}

	fmt.Printf("\n=== INDUSTRY ANALYSIS ===\n")
	fmt.Printf("Total BuySignal rows: %d\n", len(result.BuySignals))
	fmt.Printf("With Industry:        %d (%.1f%%)\n", nonEmptyCount, float64(nonEmptyCount)*100/float64(len(result.BuySignals)))
	fmt.Printf("Without Industry:     %d (%.1f%%)\n", emptyCount, float64(emptyCount)*100/float64(len(result.BuySignals)))
	fmt.Printf("Unique Industries:    %d\n\n", len(industries))

	type ind struct {
		Name  string
		Count int
	}
	list := make([]ind, 0, len(industries))
	for k, v := range industries {
		list = append(list, ind{k, v})
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Count > list[j].Count })

	fmt.Println("=== ALL INDUSTRIES (by count) ===")
	fmt.Printf("%-55s %6s\n", "INDUSTRY", "COUNT")
	fmt.Println(strings.Repeat("─", 62))
	for _, i := range list {
		fmt.Printf("%-55s %6d\n", i.Name, i.Count)
	}

	fmt.Printf("\n=== SAMPLE STOCKS MISSING INDUSTRY (first 20) ===\n")
	for _, s := range samplesEmpty {
		fmt.Printf("  %s\n", s)
	}

	// Check: for ATH_Entry=Buy stocks, do they have Industry?
	fmt.Printf("\n=== INDUSTRY FOR ATH_ENTRY=BUY STOCKS ===\n")
	for _, b := range result.BuySignals {
		if b.ATHEntry == "Buy" {
			industry := b.Industry
			if industry == "" {
				industry = "(empty)"
			}
			fmt.Printf("  %s → Industry: %s\n", b.ScripName, industry)
		}
	}

	// Industry normalization check — see if same industry has different spellings
	fmt.Printf("\n=== POTENTIAL DUPLICATES (case/space variations) ===\n")
	normalized := make(map[string][]string)
	for name := range industries {
		key := strings.ToLower(strings.TrimSpace(name))
		normalized[key] = append(normalized[key], name)
	}
	dupeFound := false
	for k, variants := range normalized {
		if len(variants) > 1 {
			fmt.Printf("  %q → %v\n", k, variants)
			dupeFound = true
		}
	}
	if !dupeFound {
		fmt.Println("  (none — all unique)")
	}
}
