// Re-verify: which BuySignal rows have empty Industry? Are they ETFs?
package main

import (
	"fmt"
	"strings"

	"go.uber.org/zap"

	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/internal/manthan"
)

const xlsxPath = "/home/rohitt/Algo-Treading/manthan-sheet-reader/Algo TTM PE EPS.xlsx"

func main() {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	reader := manthan.NewXLSXReader(xlsxPath, logger)
	raw, _ := reader.ReadAll()

	empty := 0
	filled := 0
	var emptyExamples []string

	for _, b := range raw.BuySignals {
		if strings.TrimSpace(b.Industry) == "" {
			empty++
			if len(emptyExamples) < 30 {
				emptyExamples = append(emptyExamples,
					fmt.Sprintf("%s (BarNo=%d CloseRate=%.2f Val=%.0f Lks)",
						b.ScripName, b.BarNo, b.CloseRate, b.ValueLks))
			}
		} else {
			filled++
		}
	}

	fmt.Printf("\n=== BUYSIGNAL INDUSTRY RE-CHECK ===\n")
	fmt.Printf("Total BuySignal rows: %d\n", len(raw.BuySignals))
	fmt.Printf("With Industry:    %d (%.1f%%)\n", filled, float64(filled)*100/float64(len(raw.BuySignals)))
	fmt.Printf("Empty Industry:   %d (%.1f%%)\n", empty, float64(empty)*100/float64(len(raw.BuySignals)))

	fmt.Printf("\nFirst 30 empty-Industry stocks:\n")
	for _, s := range emptyExamples {
		fmt.Printf("  %s\n", s)
	}
}
