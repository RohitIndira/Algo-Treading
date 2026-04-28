// Test: Verify join keys across sheets (ISIN, Symbol/ScripName).
// Usage: go run ./services/data-ingestion/cmd/manthan-joins/
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
	result, err := reader.ReadAll()
	if err != nil {
		panic(err)
	}

	// ---- PEandNetProfit ISIN coverage ----
	peISINs := make(map[string]string) // ISIN → NSE Symbol
	peEmpty := 0
	for _, p := range result.PEAndNetProfit {
		if p.ISIN == "" {
			peEmpty++
			continue
		}
		peISINs[p.ISIN] = p.NSESymbol
	}
	fmt.Printf("\n=== PEandNetProfit ISIN Coverage ===\n")
	fmt.Printf("Total rows:      %d\n", len(result.PEAndNetProfit))
	fmt.Printf("Unique ISINs:    %d\n", len(peISINs))
	fmt.Printf("Empty ISIN:      %d\n", peEmpty)

	// Duplicate ISINs?
	peDupeCount := 0
	peISINsMulti := make(map[string][]string)
	for _, p := range result.PEAndNetProfit {
		if p.ISIN != "" {
			peISINsMulti[p.ISIN] = append(peISINsMulti[p.ISIN], p.NSESymbol)
		}
	}
	for isin, syms := range peISINsMulti {
		if len(syms) > 1 {
			peDupeCount++
			if peDupeCount <= 5 {
				fmt.Printf("  DUPE ISIN %s → symbols: %v\n", isin, syms)
			}
		}
	}
	fmt.Printf("Duplicate ISINs: %d\n", peDupeCount)

	// ---- FScore ISIN coverage ----
	fsISINs := make(map[string]float64)
	fsEmpty := 0
	for _, f := range result.FScores {
		if f.ISIN == "" {
			fsEmpty++
			continue
		}
		fsISINs[f.ISIN] = f.Score
	}
	fmt.Printf("\n=== FScore ISIN Coverage ===\n")
	fmt.Printf("Total rows:      %d\n", len(result.FScores))
	fmt.Printf("Unique ISINs:    %d\n", len(fsISINs))
	fmt.Printf("Empty ISIN:      %d ⚠️\n", fsEmpty)

	// ---- ISIN Join: PEandNetProfit ∩ FScore ----
	matched := 0
	onlyPE := 0
	onlyFS := 0

	for isin := range peISINs {
		if _, ok := fsISINs[isin]; ok {
			matched++
		} else {
			onlyPE++
		}
	}
	for isin := range fsISINs {
		if _, ok := peISINs[isin]; !ok {
			onlyFS++
		}
	}
	fmt.Printf("\n=== JOIN: PEandNetProfit ∩ FScore (by ISIN) ===\n")
	fmt.Printf("Matched (both):       %d\n", matched)
	fmt.Printf("Only in PE:           %d\n", onlyPE)
	fmt.Printf("Only in FScore:       %d\n", onlyFS)

	// ---- ScripName Join: BuySignal → PEandNetProfit (Symbol) ----
	symbolsFromPE := make(map[string]bool)
	for sym := range peISINsMulti {
		_ = sym
	}
	for _, p := range result.PEAndNetProfit {
		if p.NSESymbol != "" {
			symbolsFromPE[p.NSESymbol] = true
		}
	}

	bsMatched := 0
	bsOnly := 0
	bsOnlyExamples := []string{}
	for _, b := range result.BuySignals {
		sym := strings.TrimSpace(b.ScripName)
		if _, ok := symbolsFromPE[sym]; ok {
			bsMatched++
		} else {
			bsOnly++
			if len(bsOnlyExamples) < 10 {
				bsOnlyExamples = append(bsOnlyExamples, sym)
			}
		}
	}
	fmt.Printf("\n=== JOIN: BuySignal.ScripName → PEandNetProfit.NSESymbol ===\n")
	fmt.Printf("Matched:          %d\n", bsMatched)
	fmt.Printf("Unmatched:        %d\n", bsOnly)
	fmt.Printf("Unmatched examples: %v\n", bsOnlyExamples)

	// ---- LifeTimeHigh Join ----
	lthSymbols := make(map[string]float64)
	for _, l := range result.LifeTimeHighs {
		lthSymbols[strings.TrimSpace(l.ScripName)] = l.AllTimeHigh
	}
	lthMatched := 0
	lthOnly := 0
	lthOnlyExamples := []string{}
	for sym := range symbolsFromPE {
		if _, ok := lthSymbols[sym]; ok {
			lthMatched++
		} else {
			lthOnly++
			if len(lthOnlyExamples) < 10 {
				lthOnlyExamples = append(lthOnlyExamples, sym)
			}
		}
	}
	fmt.Printf("\n=== JOIN: LifeTimeHigh.ScripName → PEandNetProfit.NSESymbol ===\n")
	fmt.Printf("Total PE symbols:  %d\n", len(symbolsFromPE))
	fmt.Printf("Has ATH data:      %d\n", lthMatched)
	fmt.Printf("Missing ATH:       %d\n", lthOnly)
	fmt.Printf("Missing examples:  %v\n", lthOnlyExamples)

	// ---- Full pipeline test: ATH_Entry=Buy → do we have ALL data? ----
	fmt.Printf("\n=== FULL JOIN TEST: ATH_Entry=Buy stocks ===\n")
	peBySymbol := make(map[string]*manthan.PEAndNetProfitRow)
	for _, p := range result.PEAndNetProfit {
		if p.NSESymbol != "" {
			peBySymbol[p.NSESymbol] = p
		}
	}

	for _, b := range result.BuySignals {
		if b.ATHEntry != "Buy" {
			continue
		}
		sym := b.ScripName
		fmt.Printf("\n%s:\n", sym)
		fmt.Printf("  BuySignal   : Industry=%q BarNo=%d AvgVal=%.0f Gt1Cr=%v\n",
			b.Industry, b.BarNo, b.AvgVal20Bars, b.AvgVal20BarsGt1Cr)

		pe, ok := peBySymbol[sym]
		if ok {
			fmt.Printf("  PE/NetProfit: ISIN=%s MCap=%.0f Cr PE=%.2f PAT=%.2f\n",
				pe.ISIN, pe.LatestMarketCap, pe.TTMPE, pe.PAT)
			if score, ok := fsISINs[pe.ISIN]; ok {
				fmt.Printf("  FScore      : %.0f\n", score)
			} else {
				fmt.Printf("  FScore      : MISSING ⚠️\n")
			}
		} else {
			fmt.Printf("  PE/NetProfit: NOT FOUND ⚠️\n")
		}

		if ath, ok := lthSymbols[sym]; ok {
			fmt.Printf("  ATH Close   : %.2f\n", ath)
		} else {
			fmt.Printf("  ATH Close   : MISSING ⚠️\n")
		}
	}
}
