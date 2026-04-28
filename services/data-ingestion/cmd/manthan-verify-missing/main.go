// Test: Verify that missing FScore stocks are loss-making (negative PAT).
// Usage: go run ./services/data-ingestion/cmd/manthan-verify-missing/
package main

import (
	"fmt"

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

	// Build FScore ISIN set
	fsISINs := make(map[string]bool, len(raw.FScores))
	for _, f := range raw.FScores {
		fsISINs[f.ISIN] = true
	}

	// Cross-check: for stocks WITHOUT FScore, what's their PAT distribution?
	missingFS_PATneg := 0
	missingFS_PATzero := 0
	missingFS_PATpos := 0
	var profitableMissingFS []string

	hasFS_PATneg := 0
	hasFS_PATzero := 0
	hasFS_PATpos := 0

	for _, p := range raw.PEAndNetProfit {
		hasFS := fsISINs[p.ISIN]
		if hasFS {
			switch {
			case p.PAT < 0:
				hasFS_PATneg++
			case p.PAT == 0:
				hasFS_PATzero++
			default:
				hasFS_PATpos++
			}
		} else {
			switch {
			case p.PAT < 0:
				missingFS_PATneg++
			case p.PAT == 0:
				missingFS_PATzero++
			default:
				missingFS_PATpos++
				if len(profitableMissingFS) < 20 {
					profitableMissingFS = append(profitableMissingFS,
						fmt.Sprintf("%s (PAT=%.1f Cr, MCap=%.0f Cr, PE=%.1f)",
							p.NSESymbol, p.PAT, p.LatestMarketCap, p.TTMPE))
				}
			}
		}
	}

	fmt.Printf("\n=== MISSING FSCORE HYPOTHESIS CHECK ===\n")
	fmt.Printf("Theory: stocks missing FScore are all loss-makers (PAT < 0)\n\n")

	fmt.Printf("Stocks WITH FScore:\n")
	fmt.Printf("  PAT < 0 (losses):     %d\n", hasFS_PATneg)
	fmt.Printf("  PAT = 0:              %d\n", hasFS_PATzero)
	fmt.Printf("  PAT > 0 (profitable): %d\n", hasFS_PATpos)

	fmt.Printf("\nStocks WITHOUT FScore:\n")
	fmt.Printf("  PAT < 0 (losses):     %d  <-- expected to be all\n", missingFS_PATneg)
	fmt.Printf("  PAT = 0:              %d\n", missingFS_PATzero)
	fmt.Printf("  PAT > 0 (profitable): %d  <-- UNEXPECTED\n", missingFS_PATpos)

	if missingFS_PATpos > 0 {
		fmt.Printf("\n⚠️  %d PROFITABLE stocks are missing FScore — these are lost opportunities!\n", missingFS_PATpos)
		fmt.Printf("Examples (first 20):\n")
		for _, s := range profitableMissingFS {
			fmt.Printf("  %s\n", s)
		}
	} else {
		fmt.Printf("\n✅ Hypothesis CONFIRMED: all missing-FScore stocks are loss-makers. No data fix needed.\n")
	}

	// Also: PE=0 check
	fmt.Printf("\n=== PE=0 HYPOTHESIS CHECK ===\n")
	fmt.Printf("Theory: stocks with PE=0 are all loss-makers\n\n")
	pe0_neg := 0
	pe0_zero := 0
	pe0_pos := 0
	var profitablePE0 []string
	for _, p := range raw.PEAndNetProfit {
		if p.TTMPE != 0 {
			continue
		}
		switch {
		case p.PAT < 0:
			pe0_neg++
		case p.PAT == 0:
			pe0_zero++
		default:
			pe0_pos++
			if len(profitablePE0) < 10 {
				profitablePE0 = append(profitablePE0,
					fmt.Sprintf("%s (PAT=%.1f Cr)", p.NSESymbol, p.PAT))
			}
		}
	}
	fmt.Printf("Stocks with PE=0:\n")
	fmt.Printf("  PAT < 0:  %d\n", pe0_neg)
	fmt.Printf("  PAT = 0:  %d\n", pe0_zero)
	fmt.Printf("  PAT > 0:  %d  <-- UNEXPECTED\n", pe0_pos)
	if pe0_pos > 0 {
		fmt.Printf("Examples:\n")
		for _, s := range profitablePE0 {
			fmt.Printf("  %s\n", s)
		}
	}
}
