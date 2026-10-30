// manthan-trigger-probe — READ-ONLY dry run of the mechanical Buy trigger.
//
// For each SYMBOL:ISIN:ATH given, it resolves the exchange token and reads
// the live price from the external price Redis exactly as the trigger
// watcher does, and prints whether the stock WOULD trigger now. It writes
// nothing (no DB, no Redis SET). Use it to check the feed path for a stock,
// or after the flip to see why something did or did not trigger.
//
//	EXT_REDIS_ADDR=host:6379 EXT_REDIS_PASSWORD=... \
//	  manthan-trigger-probe -pairs SGFIN:INE618R01015:734.95,KEI:INE878B01027:5899
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/internal/manthan"
)

func main() {
	pairs := flag.String("pairs", "", "comma-separated SYMBOL:ISIN:ATH_CLOSE")
	flag.Parse()
	addr := strings.TrimSpace(os.Getenv("EXT_REDIS_ADDR"))
	if addr == "" || *pairs == "" {
		fmt.Fprintln(os.Stderr, "need EXT_REDIS_ADDR and -pairs SYMBOL:ISIN:ATH[,...]")
		os.Exit(2)
	}
	var cands []manthan.TriggerCandidate
	for _, p := range strings.Split(*pairs, ",") {
		f := strings.Split(strings.TrimSpace(p), ":")
		if len(f) != 3 {
			fmt.Fprintf(os.Stderr, "bad pair %q\n", p)
			os.Exit(2)
		}
		ath, err := strconv.ParseFloat(f[2], 64)
		if err != nil {
			fmt.Fprintf(os.Stderr, "bad ATH in %q\n", p)
			os.Exit(2)
		}
		cands = append(cands, manthan.TriggerCandidate{Symbol: strings.ToUpper(f[0]), ISIN: strings.ToUpper(f[1]), ATHClose: ath})
	}
	w := manthan.NewTriggerWatcher(nil, nil, addr, os.Getenv("EXT_REDIS_PASSWORD"), zap.NewNop())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ist, _ := time.LoadLocation("Asia/Kolkata")
	fmt.Printf("probe at %s IST, trigger window open now: %v\n", time.Now().In(ist).Format("2006-01-02 15:04:05"), w.InWindow(time.Now()))
	for _, c := range cands {
		r := w.Probe(ctx, c)
		fmt.Printf("%-12s isin=%s token=%-8s ltp=%-10s ath=%-10.2f price_age=%-10s would_trigger=%v %s\n",
			c.Symbol, c.ISIN, r.Token, fmtF(r.LTP), c.ATHClose, r.Age, r.WouldTrigger, r.Note)
	}
}

func fmtF(v float64) string {
	if v <= 0 {
		return "-"
	}
	return strconv.FormatFloat(v, 'f', 2, 64)
}
