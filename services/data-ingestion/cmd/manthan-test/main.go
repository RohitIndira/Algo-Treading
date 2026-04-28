package main

import (
	"fmt"
	"os"

	"go.uber.org/zap"

	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/internal/manthan"
)

func main() {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	reader := manthan.NewXLSXReader("/home/rohitt/Algo-Treading/manthan-sheet-reader/Algo TTM PE EPS.xlsx", logger)
	result, err := reader.ReadAll()
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}

	fmt.Println("\n===== READ RESULTS =====")
	fmt.Printf("PEAndNetProfit: %d rows\n", len(result.PEAndNetProfit))
	fmt.Printf("FScores:        %d rows\n", len(result.FScores))
	fmt.Printf("LifeTimeHighs:  %d rows\n", len(result.LifeTimeHighs))
	fmt.Printf("BuySignals:     %d rows\n", len(result.BuySignals))
	fmt.Printf("IndicesGrades:  %d rows\n", len(result.IndicesGrades))
	fmt.Printf("ExitStocks:     %d rows\n", len(result.ExitStocks))

	fmt.Println("\n===== SAMPLE DATA =====")

	if len(result.PEAndNetProfit) > 0 {
		r := result.PEAndNetProfit[0]
		fmt.Printf("\nPEAndNetProfit[0]: %s (%s) MCap=%.0f Cr, PE=%.2f, PAT=%.2f, ISIN=%s\n",
			r.NSESymbol, r.CompanyName, r.LatestMarketCap, r.TTMPE, r.PAT, r.ISIN)
	}

	if len(result.FScores) > 0 {
		r := result.FScores[0]
		fmt.Printf("FScore[0]:         %s score=%.0f ISIN=%s\n", r.CompanyName, r.Score, r.ISIN)
	}

	if len(result.LifeTimeHighs) > 0 {
		r := result.LifeTimeHighs[0]
		fmt.Printf("LifeTimeHigh[0]:   %s ATH=%.2f, 52WH=%.2f\n", r.ScripName, r.AllTimeHigh, r.Week52High)
	}

	if len(result.BuySignals) > 0 {
		buyCount := 0
		for _, r := range result.BuySignals {
			if r.ATHEntry == "Buy" {
				buyCount++
				if buyCount <= 3 {
					fmt.Printf("BuySignal (Buy):   %s Industry=%s BarNo=%d IsATH=%v Avg>1Cr=%v AvgVal=%.0f\n",
						r.ScripName, r.Industry, r.BarNo, r.IsAllTimeHigh, r.AvgVal20BarsGt1Cr, r.AvgVal20Bars)
				}
			}
		}
		fmt.Printf("Total ATH_Entry=Buy signals: %d\n", buyCount)
	}

	fmt.Println("\nIndices Allocation:")
	for _, r := range result.IndicesGrades {
		fmt.Printf("  %-15s Score=%+d  Allocation=%.0f%%  CMP=%.2f  EMA21=%.2f\n",
			r.IndexName, r.TotalScore, r.Allocation*100, r.CMPClosing, r.EMA21)
	}

	if len(result.ExitStocks) > 0 {
		fmt.Printf("\nExitStocks (first 5):\n")
		for i := 0; i < 5 && i < len(result.ExitStocks); i++ {
			r := result.ExitStocks[i]
			fmt.Printf("  %s — reason: %s, value: %.2f\n", r.Symbol, r.Reason, r.Value)
		}
	}
}
