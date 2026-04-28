// Show complete data for every ATH_Entry=Buy stock — from all 4 sheets.
// Proves where exactly each piece of data is missing.
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

	// Build all lookup maps from this Excel alone (truth source)
	peBySym := make(map[string]*manthan.PEAndNetProfitRow)
	for _, p := range raw.PEAndNetProfit {
		if p.NSESymbol != "" {
			peBySym[p.NSESymbol] = p
		}
	}
	fsByISIN := make(map[string]*manthan.FScoreRow)
	for _, f := range raw.FScores {
		fsByISIN[f.ISIN] = f
	}
	lthBySym := make(map[string]*manthan.LifeTimeHighRow)
	for _, l := range raw.LifeTimeHighs {
		lthBySym[strings.TrimSpace(l.ScripName)] = l
	}

	// Build ScripName -> Industry map from ALL BuySignal rows that have it populated.
	// This is the "intelligent industry join" — if stock X's row has Industry blank,
	// we might find it in another context. (Note: in practice each ScripName is unique,
	// but this safeguards against the rare duplicate.)
	industryBySym := make(map[string]string)
	for _, bs := range raw.BuySignals {
		sym := strings.TrimSpace(bs.ScripName)
		ind := strings.TrimSpace(bs.Industry)
		if sym != "" && ind != "" {
			industryBySym[sym] = ind
		}
	}
	fmt.Printf("Industry lookup map built from %d BuySignal rows\n", len(industryBySym))

	// Now walk Buy candidates
	fmt.Printf("\n=== ATH_Entry=Buy CANDIDATES — FULL DATA AUDIT ===\n")
	count := 0
	for _, bs := range raw.BuySignals {
		if strings.TrimSpace(bs.ATHEntry) != "Buy" {
			continue
		}
		count++
		sym := strings.TrimSpace(bs.ScripName)

		// Try industry from this row; if blank, fall back to lookup map
		industry := strings.TrimSpace(bs.Industry)
		industrySource := "BuySignal row"
		if industry == "" {
			if alt, ok := industryBySym[sym]; ok {
				industry = alt
				industrySource = "BuySignal lookup"
			} else {
				industrySource = "NOT FOUND in xlsx"
			}
		}

		fmt.Printf("\n%d. %s\n", count, sym)
		fmt.Printf("   BuySignal   : BarNo=%d Industry=%q(%s) 20BarVal=%.0f >1Cr=%v ATH_Entry=%s\n",
			bs.BarNo, industry, industrySource, bs.AvgVal20Bars, bs.AvgVal20BarsGt1Cr, bs.ATHEntry)

		if pe, ok := peBySym[sym]; ok {
			fmt.Printf("   PE Sheet    : ISIN=%s MCap=%.0f Cr PE=%.2f PAT=%.2f Cr EPS=%.2f Price=%.2f\n",
				pe.ISIN, pe.LatestMarketCap, pe.TTMPE, pe.PAT, pe.TTMEPS, pe.LatestPrice)

			if fs, ok := fsByISIN[pe.ISIN]; ok {
				fmt.Printf("   FScore      : %.0f\n", fs.Score)
			} else {
				fmt.Printf("   FScore      : MISSING\n")
			}
		} else {
			fmt.Printf("   PE Sheet    : NOT FOUND\n")
			fmt.Printf("   FScore      : cannot check (no ISIN)\n")
		}

		if lth, ok := lthBySym[sym]; ok {
			fmt.Printf("   LifeTimeHigh: ATH=%.2f 52WHi=%.2f 52WLo=%.2f\n",
				lth.AllTimeHigh, lth.Week52High, lth.Week52Low)
		} else {
			fmt.Printf("   LifeTimeHigh: MISSING\n")
		}
	}
	fmt.Printf("\nTotal Buy candidates: %d\n", count)
}
