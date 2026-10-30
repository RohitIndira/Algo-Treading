// manthan-shadow — compares the Google Sheet source with the signals_db
// ref_* source for the SAME universe, so the two-week shadow period before
// the flip (operator decision 2026-10-30) produces a verdict instead of a
// feeling.
//
// For every symbol on the sheet's Buy list it reports, field by field, what
// the sheet says and what the database says (market cap, PE, PAT, F-score,
// ATH, 52w high, industry, allocation), then runs the real pipeline on both
// inputs — the DB input with the sheet's Buy list substituted so the
// trigger difference does not drown the data difference — and diffs the
// ELIGIBLE / FILTERED / DROPPED verdicts. Exit code 0 always; the summary
// line and the per-symbol table are the output. -json writes the full
// report for the console or a cron archive.
//
// Env: the sheet variables manthan-live uses (MANTHAN_SHEET_ID,
// MANTHAN_CREDS) and the MARKET_DATA_DB_* variables for signals_db.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"go.uber.org/zap"

	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/config"
	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/internal/manthan"
)

const (
	defaultSheetID = "1E_MzQNQFNvnmR8wMZMCyzPKc-SjOei4wwQp4QAey5sc"
	defaultCreds   = "/home/rohitt/Algo-Treading/services/data-ingestion/credentials/manthan-sheet.json"
)

// FieldDiff is one compared number or string.
type FieldDiff struct {
	Field string  `json:"field"`
	Sheet string  `json:"sheet"`
	DB    string  `json:"db"`
	Pct   float64 `json:"pct,omitempty"` // relative difference for numbers
	Flag  bool    `json:"flag"`          // beyond tolerance
}

// SymbolReport is one Buy-list symbol.
type SymbolReport struct {
	Symbol        string      `json:"symbol"`
	SheetVerdict  string      `json:"sheet_verdict"`
	DBVerdict     string      `json:"db_verdict"`
	VerdictAgrees bool        `json:"verdict_agrees"`
	SheetReason   string      `json:"sheet_reason,omitempty"`
	DBReason      string      `json:"db_reason,omitempty"`
	Fields        []FieldDiff `json:"fields"`
	Flags         int         `json:"flags"`
}

// Report is the whole run.
type Report struct {
	RunAt           time.Time             `json:"run_at"`
	BuyList         int                   `json:"buy_list"`
	VerdictAgree    int                   `json:"verdict_agree"`
	VerdictDisagree int                   `json:"verdict_disagree"`
	FieldFlags      int                   `json:"field_flags"`
	Allocation      map[string][2]float64 `json:"allocation_sheet_db"`
	Symbols         []SymbolReport        `json:"symbols"`
	DBTriggersToday []string              `json:"db_triggers_today"`
}

func main() {
	jsonOut := flag.String("json", "", "write the full report to this file")
	jsonDir := flag.String("json-dir", "", "write the report to <dir>/<IST date>.json (for a daily cron archive)")
	tol := flag.Float64("tol", 0.05, "relative tolerance for numeric fields (0.05 = 5%)")
	flag.Parse()
	if *jsonDir != "" && *jsonOut == "" {
		ist, _ := time.LoadLocation("Asia/Kolkata")
		if err := os.MkdirAll(*jsonDir, 0o755); err == nil {
			*jsonOut = *jsonDir + "/" + time.Now().In(ist).Format("2006-01-02") + ".json"
		}
	}
	logger, _ := zap.NewProduction()
	defer logger.Sync()
	if err := run(*jsonOut, *tol, logger); err != nil {
		logger.Error("manthan-shadow failed", zap.Error(err))
		os.Exit(1)
	}
}

func run(jsonOut string, tol float64, logger *zap.Logger) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	sheetID := os.Getenv("MANTHAN_SHEET_ID")
	if sheetID == "" {
		sheetID = defaultSheetID
	}
	creds := os.Getenv("MANTHAN_CREDS")
	if creds == "" {
		creds = defaultCreds
	}
	sheet := manthan.NewGSheetReader(sheetID, creds, logger)
	if err := sheet.Connect(ctx); err != nil {
		return fmt.Errorf("sheet connect: %w", err)
	}
	sheetRaw, err := sheet.ReadAll(ctx)
	if err != nil {
		return fmt.Errorf("sheet read: %w", err)
	}

	cfg := config.Load()
	db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		cfg.PGHost, cfg.PGPort, cfg.PGUser, cfg.PGPassword, cfg.PGDatabase, cfg.PGSSLMode))
	if err != nil {
		return err
	}
	defer db.Close()
	dbr := manthan.NewDBReader(db, os.Getenv("MANTHAN_PAT_BASIS"), logger)
	if err := dbr.Connect(ctx); err != nil {
		return fmt.Errorf("db connect: %w", err)
	}
	dbRaw, err := dbr.ReadAll(ctx)
	if err != nil {
		return fmt.Errorf("db read: %w", err)
	}

	rep := compare(sheetRaw, dbRaw, tol, logger)
	printReport(rep)
	if jsonOut != "" {
		b, _ := json.MarshalIndent(rep, "", "  ")
		if err := os.WriteFile(jsonOut, b, 0o644); err != nil {
			return err
		}
	}
	return nil
}

// compare does the work; it is pure so it can be unit-tested with fixtures.
func compare(sheetRaw, dbRaw *manthan.ReadResult, tol float64, logger *zap.Logger) *Report {
	rep := &Report{RunAt: time.Now().UTC(), Allocation: map[string][2]float64{}}

	// Universe: the sheet's Buy list (deduped, first row wins like the pipeline).
	var buy []*manthan.BuySignalRow
	seen := map[string]bool{}
	for _, b := range sheetRaw.BuySignals {
		sym := strings.ToUpper(strings.TrimSpace(b.ScripName))
		if strings.TrimSpace(b.ATHEntry) != "Buy" || sym == "" || seen[sym] {
			continue
		}
		seen[sym] = true
		buy = append(buy, b)
	}
	rep.BuyList = len(buy)
	for _, b := range dbRaw.BuySignals {
		rep.DBTriggersToday = append(rep.DBTriggersToday, b.ScripName)
	}

	// DB input with the sheet's Buy list substituted (industry from the DB side).
	dbInd := map[string]string{}
	for _, i := range dbRaw.Industries {
		dbInd[i.NSECode] = i.Industry
	}
	dbForPipe := *dbRaw
	dbForPipe.BuySignals = nil
	for _, b := range buy {
		c := *b
		c.Industry = dbInd[strings.ToUpper(strings.TrimSpace(b.ScripName))]
		dbForPipe.BuySignals = append(dbForPipe.BuySignals, &c)
	}

	sheetRes := manthan.NewPipeline(logger).Run(sheetRaw)
	dbRes := manthan.NewPipeline(logger).Run(&dbForPipe)
	for k, v := range sheetRes.IndexAllocation {
		rep.Allocation[k] = [2]float64{v, dbRes.IndexAllocation[k]}
	}
	for k, v := range dbRes.IndexAllocation {
		if _, ok := rep.Allocation[k]; !ok {
			rep.Allocation[k] = [2]float64{0, v}
		}
	}

	sv, sr, sStock := verdicts(sheetRes)
	dv, dr, dStock := verdicts(dbRes)

	sheetPE := index(sheetRaw.PEAndNetProfit, func(p *manthan.PEAndNetProfitRow) string { return p.NSESymbol })
	dbPE := index(dbRaw.PEAndNetProfit, func(p *manthan.PEAndNetProfitRow) string { return p.NSESymbol })
	sheetLTH := index(sheetRaw.LifeTimeHighs, func(l *manthan.LifeTimeHighRow) string { return strings.TrimSpace(l.ScripName) })
	dbLTH := index(dbRaw.LifeTimeHighs, func(l *manthan.LifeTimeHighRow) string { return strings.TrimSpace(l.ScripName) })
	sheetFS := map[string]float64{}
	for _, f := range sheetRaw.FScores {
		sheetFS[f.ISIN] = f.Score
	}
	dbFS := map[string]float64{}
	for _, f := range dbRaw.FScores {
		dbFS[f.ISIN] = f.Score
	}
	sheetInd := map[string]string{}
	for _, i := range sheetRaw.Industries {
		sheetInd[i.NSECode] = i.Industry
	}

	for _, b := range buy {
		sym := strings.ToUpper(strings.TrimSpace(b.ScripName))
		r := SymbolReport{Symbol: sym, SheetVerdict: sv[sym], DBVerdict: dv[sym], SheetReason: sr[sym], DBReason: dr[sym]}
		if r.SheetVerdict == "" {
			r.SheetVerdict = "ABSENT"
		}
		if r.DBVerdict == "" {
			r.DBVerdict = "ABSENT"
		}
		r.VerdictAgrees = r.SheetVerdict == r.DBVerdict
		sp, dp := sheetPE[sym], dbPE[sym]
		num := func(field string, a, b float64) {
			fd := FieldDiff{Field: field, Sheet: fmtNum(a), DB: fmtNum(b)}
			if a > 0 && b > 0 {
				fd.Pct = math.Round(math.Abs(a-b)/a*1000) / 10
				fd.Flag = math.Abs(a-b)/a > tol
			} else if a != b {
				fd.Flag = true
			}
			if fd.Flag {
				r.Flags++
			}
			r.Fields = append(r.Fields, fd)
		}
		str := func(field, a, b string) {
			fd := FieldDiff{Field: field, Sheet: a, DB: b, Flag: !strings.EqualFold(strings.TrimSpace(a), strings.TrimSpace(b))}
			if fd.Flag {
				r.Flags++
			}
			r.Fields = append(r.Fields, fd)
		}
		if sp != nil && dp != nil {
			num("market_cap", sp.LatestMarketCap, dp.LatestMarketCap)
			num("pe", sp.TTMPE, dp.TTMPE)
			num("pat", sp.PAT, dp.PAT)
			num("eps", sp.TTMEPS, dp.TTMEPS)
			num("price", sp.LatestPrice, dp.LatestPrice)
			num("fscore", sheetFS[sp.ISIN], dbFS[dp.ISIN])
			str("isin", sp.ISIN, dp.ISIN)
		} else {
			str("pe_row", present(sp != nil), present(dp != nil))
		}
		sl, dl := sheetLTH[sym], dbLTH[sym]
		if sl != nil && dl != nil {
			num("ath", sl.AllTimeHigh, dl.AllTimeHigh)
			num("week52_high", sl.Week52High, dl.Week52High)
		} else {
			str("ath_row", present(sl != nil), present(dl != nil))
		}
		si := strings.TrimSpace(b.Industry)
		if si == "" {
			si = sheetInd[sym]
		}
		str("industry", si, dbInd[sym])
		if ss, ds := sStock[sym], dStock[sym]; ss != nil && ds != nil {
			num("allocation", ss.Allocation, ds.Allocation)
			str("bucket", ss.MCapBucket, ds.MCapBucket)
		}
		if r.VerdictAgrees {
			rep.VerdictAgree++
		} else {
			rep.VerdictDisagree++
		}
		rep.FieldFlags += r.Flags
		rep.Symbols = append(rep.Symbols, r)
	}
	sort.Slice(rep.Symbols, func(i, j int) bool {
		if rep.Symbols[i].VerdictAgrees != rep.Symbols[j].VerdictAgrees {
			return !rep.Symbols[i].VerdictAgrees
		}
		if rep.Symbols[i].Flags != rep.Symbols[j].Flags {
			return rep.Symbols[i].Flags > rep.Symbols[j].Flags
		}
		return rep.Symbols[i].Symbol < rep.Symbols[j].Symbol
	})
	return rep
}

// verdicts flattens a pipeline result into symbol → verdict/reason/stock.
func verdicts(res *manthan.PipelineResult) (map[string]string, map[string]string, map[string]*manthan.ManthanStock) {
	v, r, s := map[string]string{}, map[string]string{}, map[string]*manthan.ManthanStock{}
	for _, st := range res.Eligible {
		v[st.Symbol], s[st.Symbol] = "ELIGIBLE", st
	}
	for _, st := range res.FilteredOut {
		v[st.Symbol], r[st.Symbol], s[st.Symbol] = "FILTERED", st.FilterReason, st
	}
	for _, d := range res.Drops {
		v[d.Symbol], r[d.Symbol] = "DROPPED", d.Reason
	}
	return v, r, s
}

func index[T any](rows []T, key func(T) string) map[string]T {
	m := make(map[string]T, len(rows))
	for _, r := range rows {
		k := strings.ToUpper(key(r))
		if k == "" {
			continue
		}
		if _, ok := m[k]; !ok {
			m[k] = r
		}
	}
	return m
}

func present(b bool) string {
	if b {
		return "present"
	}
	return "missing"
}

func fmtNum(v float64) string {
	if v == 0 {
		return "0"
	}
	if math.Abs(v-math.Round(v)) < 1e-9 {
		return fmt.Sprintf("%.0f", v)
	}
	return fmt.Sprintf("%.2f", v)
}

func printReport(rep *Report) {
	fmt.Printf("manthan-shadow %s — sheet Buy list %d | verdicts agree %d, disagree %d | field flags %d | db triggers today %d\n",
		rep.RunAt.Format(time.RFC3339), rep.BuyList, rep.VerdictAgree, rep.VerdictDisagree, rep.FieldFlags, len(rep.DBTriggersToday))
	keys := make([]string, 0, len(rep.Allocation))
	for k := range rep.Allocation {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		a := rep.Allocation[k]
		fmt.Printf("  allocation %-11s sheet %.2f  db %.2f\n", k, a[0], a[1])
	}
	for _, s := range rep.Symbols {
		mark := "  "
		if !s.VerdictAgrees {
			mark = "!!"
		}
		fmt.Printf("%s %-12s sheet=%-9s db=%-9s flags=%d", mark, s.Symbol, s.SheetVerdict, s.DBVerdict, s.Flags)
		if s.SheetReason != "" || s.DBReason != "" {
			fmt.Printf("  [sheet: %s | db: %s]", s.SheetReason, s.DBReason)
		}
		fmt.Println()
		for _, f := range s.Fields {
			if f.Flag {
				if f.Pct > 0 {
					fmt.Printf("       %-12s sheet=%-14s db=%-14s (%.1f%%)\n", f.Field, f.Sheet, f.DB, f.Pct)
				} else {
					fmt.Printf("       %-12s sheet=%-14s db=%s\n", f.Field, f.Sheet, f.DB)
				}
			}
		}
	}
}
