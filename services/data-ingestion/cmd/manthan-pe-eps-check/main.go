// Verify: for PE=0 + PAT>0 stocks, what is their TTMEPS?
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
	raw, _ := reader.ReadAll()

	epsNeg := 0
	epsZero := 0
	epsPos := 0
	var samples []string

	for _, p := range raw.PEAndNetProfit {
		// Focus on PE=0 AND PAT>0 (the 501 "suspicious" rows)
		if p.TTMPE != 0 || p.PAT <= 0 {
			continue
		}
		switch {
		case p.TTMEPS < 0:
			epsNeg++
			if len(samples) < 15 {
				samples = append(samples,
					fmt.Sprintf("  %-14s PAT=%8.2f Cr  EPS=%7.2f  PE=%.2f  <- NEG EPS",
						p.NSESymbol, p.PAT, p.TTMEPS, p.TTMPE))
			}
		case p.TTMEPS == 0:
			epsZero++
			if len(samples) < 15 {
				samples = append(samples,
					fmt.Sprintf("  %-14s PAT=%8.2f Cr  EPS=%7.2f  PE=%.2f  <- ZERO EPS",
						p.NSESymbol, p.PAT, p.TTMEPS, p.TTMPE))
			}
		default:
			epsPos++
			if len(samples) < 15 {
				samples = append(samples,
					fmt.Sprintf("  %-14s PAT=%8.2f Cr  EPS=%7.2f  PE=%.2f  <- POS EPS (real bug)",
						p.NSESymbol, p.PAT, p.TTMEPS, p.TTMPE))
			}
		}
	}

	fmt.Printf("\n=== PE=0 & PAT>0 → What is TTMEPS? ===\n")
	fmt.Printf("Total stocks in this group: %d\n\n", epsNeg+epsZero+epsPos)
	fmt.Printf("TTMEPS < 0 (negative EPS):  %d  <-- user's hypothesis\n", epsNeg)
	fmt.Printf("TTMEPS = 0:                 %d\n", epsZero)
	fmt.Printf("TTMEPS > 0 (real bug):      %d\n", epsPos)

	fmt.Printf("\nSamples:\n")
	for _, s := range samples {
		fmt.Println(s)
	}
}
