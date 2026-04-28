package main

import (
	"fmt"
	"go.uber.org/zap"
	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/internal/manthan"
)

func main() {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	reader := manthan.NewXLSXReader("/home/rohitt/Algo-Treading/manthan-sheet-reader/Algo TTM PE EPS.xlsx", logger)
	raw, _ := reader.ReadAll()

	fsISINs := make(map[string]bool)
	for _, f := range raw.FScores {
		fsISINs[f.ISIN] = true
	}

	// Profitable missing FScore that WOULD pass MCap + PE filters
	wouldPass := 0
	var samples []string
	for _, p := range raw.PEAndNetProfit {
		if fsISINs[p.ISIN] {
			continue
		}
		if p.PAT <= 0 {
			continue
		}
		if p.LatestMarketCap < 500 || p.LatestMarketCap > 150000 {
			continue
		}
		if p.TTMPE == 0 || p.TTMPE > 60 {
			continue
		}
		if p.NSESymbol == "" {
			continue
		}
		wouldPass++
		if len(samples) < 15 {
			samples = append(samples, fmt.Sprintf("  %-14s MCap=%7.0f Cr  PE=%6.2f  PAT=%7.2f Cr",
				p.NSESymbol, p.LatestMarketCap, p.TTMPE, p.PAT))
		}
	}
	fmt.Printf("\nProfitable stocks missing FScore that pass (MCap 500-150K + PE 0<x<=60 + has NSESymbol):\n")
	fmt.Printf("Count: %d\n\n", wouldPass)
	for _, s := range samples {
		fmt.Println(s)
	}
}
