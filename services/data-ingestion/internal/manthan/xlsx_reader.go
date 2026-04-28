package manthan

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/xuri/excelize/v2"
	"go.uber.org/zap"
)

// XLSXReader reads the Manthan strategy xlsx file (7 sheets) and parses
// each sheet into typed structs. Column order is fixed per sheet structure.
type XLSXReader struct {
	filePath string
	logger   *zap.Logger
}

// NewXLSXReader creates a new reader for the given xlsx file path.
func NewXLSXReader(filePath string, logger *zap.Logger) *XLSXReader {
	return &XLSXReader{filePath: filePath, logger: logger}
}

// Sheet names (must match xlsx tabs exactly)
const (
	SheetPEAndNetProfit    = "PEandNetProfitSheet"
	SheetFScore            = "FScore"
	SheetLifeTimeHigh      = "LifeTimeHigh"
	SheetBuySignal         = "BuySignal"
	SheetIndicesGradeRange = "IndicesGradeRange"
	SheetIndustry          = "Industry"
	SheetExitStockDetail   = "ExitStockDetail"
)

// ReadResult holds all parsed rows from the xlsx/gsheet source.
type ReadResult struct {
	PEAndNetProfit []*PEAndNetProfitRow
	FScores        []*FScoreRow
	LifeTimeHighs  []*LifeTimeHighRow
	BuySignals     []*BuySignalRow
	IndicesGrades  []*IndicesGradeRow
	Industries     []*IndustryRow // from the "Industry" tab (live gsheet only)
	ExitStocks     []*ExitStockDetailRow
}

// ReadAll opens the xlsx file and parses all 6 active sheets (LTH_OLD skipped).
func (r *XLSXReader) ReadAll() (*ReadResult, error) {
	f, err := excelize.OpenFile(r.filePath)
	if err != nil {
		return nil, fmt.Errorf("open xlsx: %w", err)
	}
	defer f.Close()

	result := &ReadResult{}

	// Read each sheet — any single sheet failure is logged but doesn't abort.
	// This lets us still process data even if one tab has issues.
	var firstErr error
	tryRead := func(name string, fn func(*excelize.File) error) {
		if err := fn(f); err != nil {
			r.logger.Error("Failed to read sheet",
				zap.String("sheet", name), zap.Error(err))
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	tryRead(SheetPEAndNetProfit, func(f *excelize.File) error {
		rows, err := r.readPEAndNetProfit(f)
		if err == nil {
			result.PEAndNetProfit = rows
			r.logger.Info("Read sheet", zap.String("sheet", SheetPEAndNetProfit), zap.Int("rows", len(rows)))
		}
		return err
	})

	tryRead(SheetFScore, func(f *excelize.File) error {
		rows, err := r.readFScore(f)
		if err == nil {
			result.FScores = rows
			r.logger.Info("Read sheet", zap.String("sheet", SheetFScore), zap.Int("rows", len(rows)))
		}
		return err
	})

	tryRead(SheetLifeTimeHigh, func(f *excelize.File) error {
		rows, err := r.readLifeTimeHigh(f)
		if err == nil {
			result.LifeTimeHighs = rows
			r.logger.Info("Read sheet", zap.String("sheet", SheetLifeTimeHigh), zap.Int("rows", len(rows)))
		}
		return err
	})

	tryRead(SheetBuySignal, func(f *excelize.File) error {
		rows, err := r.readBuySignal(f)
		if err == nil {
			result.BuySignals = rows
			r.logger.Info("Read sheet", zap.String("sheet", SheetBuySignal), zap.Int("rows", len(rows)))
		}
		return err
	})

	tryRead(SheetIndicesGradeRange, func(f *excelize.File) error {
		rows, err := r.readIndicesGrade(f)
		if err == nil {
			result.IndicesGrades = rows
			r.logger.Info("Read sheet", zap.String("sheet", SheetIndicesGradeRange), zap.Int("rows", len(rows)))
		}
		return err
	})

	tryRead(SheetExitStockDetail, func(f *excelize.File) error {
		rows, err := r.readExitStocks(f)
		if err == nil {
			result.ExitStocks = rows
			r.logger.Info("Read sheet", zap.String("sheet", SheetExitStockDetail), zap.Int("rows", len(rows)))
		}
		return err
	})

	return result, firstErr
}

// --- Per-sheet parsers ---
// Column positions match the xlsx structure. Parsers work on [][]string
// (header row + data rows) so both xlsx and Google Sheets sources can reuse them.

// col returns the i-th cell of a row, or empty string if index out of range.
// Used throughout because rows may be short (trailing empty cells are trimmed).
func col(row []string, i int) string {
	if i >= len(row) {
		return ""
	}
	return row[i]
}

// parsePEAndNetProfit parses PEandNetProfitSheet rows (header in row 0).
func parsePEAndNetProfit(rows [][]string) ([]*PEAndNetProfitRow, error) {
	if len(rows) < 2 {
		return nil, fmt.Errorf("no data rows")
	}
	out := make([]*PEAndNetProfitRow, 0, len(rows)-1)
	for _, row := range rows[1:] {
		if len(row) < 12 {
			continue
		}
		rec := &PEAndNetProfitRow{
			SrNo:            parseInt(col(row, 0)),
			AccordCode:      strings.TrimSpace(col(row, 1)),
			CompanyName:     strings.TrimSpace(col(row, 2)),
			LatestPriceDate: parseDate(col(row, 3)),
			LatestMarketCap: parseFloat(col(row, 4)),
			LatestPrice:     parseFloat(col(row, 5)),
			TTMEPS:          parseFloat(col(row, 6)),
			TTMPE:           parseFloat(col(row, 7)),
			PAT:             parseFloat(col(row, 8)),
			BSECode:         strings.TrimSpace(col(row, 9)),
			NSESymbol:       strings.TrimSpace(col(row, 10)),
			ISIN:            strings.TrimSpace(col(row, 11)),
		}
		if rec.ISIN == "" {
			continue
		}
		out = append(out, rec)
	}
	return out, nil
}

// parseFScore parses FScore tab rows.
func parseFScore(rows [][]string) ([]*FScoreRow, error) {
	if len(rows) < 2 {
		return nil, fmt.Errorf("no data rows")
	}
	out := make([]*FScoreRow, 0, len(rows)-1)
	for _, row := range rows[1:] {
		if len(row) < 5 {
			continue
		}
		rec := &FScoreRow{
			CoCode:      strings.TrimSpace(col(row, 0)),
			Date:        parseDate(col(row, 1)),
			Score:       parseFloat(col(row, 2)),
			CompanyName: strings.TrimSpace(col(row, 3)),
			ISIN:        strings.TrimSpace(col(row, 4)),
		}
		if rec.ISIN == "" {
			continue
		}
		out = append(out, rec)
	}
	return out, nil
}

// parseLifeTimeHigh parses LifeTimeHigh tab rows.
func parseLifeTimeHigh(rows [][]string) ([]*LifeTimeHighRow, error) {
	if len(rows) < 2 {
		return nil, fmt.Errorf("no data rows")
	}
	out := make([]*LifeTimeHighRow, 0, len(rows)-1)
	for _, row := range rows[1:] {
		if len(row) < 9 {
			continue
		}
		rec := &LifeTimeHighRow{
			ScripName:   strings.TrimSpace(col(row, 0)),
			DataTime:    strings.TrimSpace(col(row, 1)),
			CloseRate:   parseFloat(col(row, 2)),
			Rank:        parseInt(col(row, 3)),
			Period:      strings.TrimSpace(col(row, 4)),
			Week52High:  parseFloat(col(row, 5)),
			AllTimeHigh: parseFloat(col(row, 6)),
			Week52Low:   parseFloat(col(row, 7)),
			AllTimeLow:  parseFloat(col(row, 8)),
		}
		if rec.ScripName == "" {
			continue
		}
		out = append(out, rec)
	}
	return out, nil
}

// parseBuySignal parses BuySignal tab rows.
//
// Multiple layouts are supported; detection is by header names (not column
// count) so the parser is resilient to columns being added/removed:
//   - "Wide" (xlsx export): 42+ cols — includes Industry, BarNo, volume, etc.
//   - "Narrow-2 col" (live GSheet historic): ScripName, ATH_Entry
//   - "Narrow-3 col" (live GSheet historic): Date, ScripName, ATH_Entry
//   - "Narrow-4 col" (live GSheet current):  ScanNo, ScripName, DataTime, ATH_Entry
//
// When using a narrow layout, fields other than the ones identified default
// to zero — enrichment must come from other tabs (PE, FScore, LifeTimeHigh).
func parseBuySignal(rows [][]string) ([]*BuySignalRow, error) {
	if len(rows) < 2 {
		return nil, fmt.Errorf("no data rows")
	}

	header := rows[0]
	colCount := len(header)

	// Header-name lookup — tolerates reordering.
	hdrIdx := func(names ...string) int {
		for i, h := range header {
			clean := strings.ToLower(strings.TrimSpace(h))
			for _, want := range names {
				if clean == want {
					return i
				}
			}
		}
		return -1
	}

	scripIdx, athIdx, dateIdx := 0, 1, -1
	narrow := colCount <= 5

	if narrow {
		// Locate columns by header name; fall back to positional defaults
		// for the very old 2-col export that had no proper header.
		if i := hdrIdx("scripname", "symbol", "scrip"); i >= 0 {
			scripIdx = i
		}
		if i := hdrIdx("ath_entry", "athentry", "ath entry", "buy signal", "buy"); i >= 0 {
			athIdx = i
		}
		if i := hdrIdx("datatime", "date", "data time", "datetime"); i >= 0 {
			dateIdx = i
		}
	}

	out := make([]*BuySignalRow, 0, len(rows)-1)
	for _, row := range rows[1:] {
		if len(row) < 2 {
			continue
		}
		var rec *BuySignalRow
		if narrow {
			rec = &BuySignalRow{
				ScripName: strings.TrimSpace(col(row, scripIdx)),
				ATHEntry:  strings.TrimSpace(col(row, athIdx)),
			}
			if dateIdx >= 0 {
				rec.DataTime = strings.TrimSpace(col(row, dateIdx))
			}
		} else {
			rec = &BuySignalRow{
				ScanNo:            parseInt(col(row, 0)),
				ScripName:         strings.TrimSpace(col(row, 1)),
				DataTime:          strings.TrimSpace(col(row, 2)),
				PeriodNo:          parseInt(col(row, 3)),
				BarNo:             parseInt(col(row, 4)),
				Industry:          strings.TrimSpace(col(row, 5)),
				OpenRate:          parseFloat(col(row, 6)),
				HighRate:          parseFloat(col(row, 7)),
				LowRate:           parseFloat(col(row, 8)),
				CloseRate:         parseFloat(col(row, 11)),
				TotalQty:          parseInt64(col(row, 12)),
				ValueLks:          parseFloat(col(row, 13)),
				Trades:            parseInt64(col(row, 14)),
				Week52High:        parseFloat(col(row, 19)),
				AvgVal20Bars:      parseFloat(col(row, 30)),
				IsAllTimeHigh:     parseBool(col(row, 31)),
				Is52WkHighBO:      parseBool(col(row, 32)),
				ATHEntry:          strings.TrimSpace(col(row, 35)),
				ATHExit:           strings.TrimSpace(col(row, 38)),
				AvgVal20BarsGt1Cr: parseBool(col(row, 39)),
				Period:            strings.TrimSpace(col(row, 40)),
			}
		}
		if rec.ScripName == "" {
			continue
		}
		out = append(out, rec)
	}
	return out, nil
}

// parseIndicesGrade parses IndicesGradeRange tab rows.
func parseIndicesGrade(rows [][]string) ([]*IndicesGradeRow, error) {
	if len(rows) < 2 {
		return nil, fmt.Errorf("no data rows")
	}
	out := make([]*IndicesGradeRow, 0, len(rows)-1)
	for _, row := range rows[1:] {
		if len(row) < 10 {
			continue
		}
		rec := &IndicesGradeRow{
			IndexName:  strings.TrimSpace(col(row, 0)),
			NoOfStocks: parseInt(col(row, 1)),
			TotalPlus:  parseInt(col(row, 2)),
			TotalMinus: parseInt(col(row, 3)),
			TotalScore: parseInt(col(row, 4)),
			Allocation: parseFloat(col(row, 5)),
			CMPClosing: parseFloat(col(row, 6)),
			EMA21:      parseFloat(col(row, 7)),
			EMA50:      parseFloat(col(row, 8)),
			EMA100:     parseFloat(col(row, 9)),
		}
		if rec.IndexName == "" {
			continue
		}
		out = append(out, rec)
	}
	return out, nil
}

// parseIndustry parses the Industry tab (live sheet only — not in xlsx export).
// Header: Name, BSE Code, NSE Code, ISIN, Industry, Current Price, Market Cap,
// PE, ROCE, ROE, FII, DII, EVEBITDA, PAT, OPM q-1, OPM latest, Down ATH, Down 52W, Sales
func parseIndustry(rows [][]string) ([]*IndustryRow, error) {
	if len(rows) < 2 {
		return nil, nil
	}
	out := make([]*IndustryRow, 0, len(rows)-1)
	for _, row := range rows[1:] {
		if len(row) < 5 {
			continue
		}
		rec := &IndustryRow{
			Name:            strings.TrimSpace(col(row, 0)),
			BSECode:         strings.TrimSpace(col(row, 1)),
			NSECode:         strings.TrimSpace(col(row, 2)),
			ISIN:            strings.TrimSpace(col(row, 3)),
			Industry:        strings.TrimSpace(col(row, 4)),
			CurrentPrice:    parseFloat(col(row, 5)),
			MarketCap:       parseFloat(col(row, 6)),
			PE:              parseFloat(col(row, 7)),
			ROCE:            parseFloat(col(row, 8)),
			ROE:             parseFloat(col(row, 9)),
			FIIHolding:      parseFloat(col(row, 10)),
			DIIHolding:      parseFloat(col(row, 11)),
			EVEBITDA:        parseFloat(col(row, 12)),
			PAT:             parseFloat(col(row, 13)),
			OPMPrecedingQtr: parseFloat(col(row, 14)),
			OPMLatestQtr:    parseFloat(col(row, 15)),
			DownFromATH:     parseFloat(col(row, 16)),
			DownFrom52WHigh: parseFloat(col(row, 17)),
			Sales:           parseFloat(col(row, 18)),
		}
		if rec.NSECode == "" && rec.ISIN == "" {
			continue
		}
		out = append(out, rec)
	}
	return out, nil
}

// parseExitStocks parses ExitStockDetail tab rows.
func parseExitStocks(rows [][]string) ([]*ExitStockDetailRow, error) {
	if len(rows) < 2 {
		return nil, nil
	}
	out := make([]*ExitStockDetailRow, 0, len(rows)-1)
	for _, row := range rows[1:] {
		if len(row) < 3 {
			continue
		}
		rec := &ExitStockDetailRow{
			Symbol: strings.TrimSpace(col(row, 1)),
			Reason: strings.TrimSpace(col(row, 2)),
		}
		if len(row) >= 4 {
			rec.Value = parseFloat(col(row, 3))
		}
		if d := parseDate(col(row, 0)); !d.IsZero() {
			rec.Date = &d
		}
		if rec.Symbol == "" {
			continue
		}
		out = append(out, rec)
	}
	return out, nil
}

// --- XLSXReader methods delegate to the pure parsers above ---

func (r *XLSXReader) readPEAndNetProfit(f *excelize.File) ([]*PEAndNetProfitRow, error) {
	rows, err := f.GetRows(SheetPEAndNetProfit)
	if err != nil {
		return nil, err
	}
	return parsePEAndNetProfit(rows)
}

func (r *XLSXReader) readFScore(f *excelize.File) ([]*FScoreRow, error) {
	rows, err := f.GetRows(SheetFScore)
	if err != nil {
		return nil, err
	}
	return parseFScore(rows)
}

func (r *XLSXReader) readLifeTimeHigh(f *excelize.File) ([]*LifeTimeHighRow, error) {
	rows, err := f.GetRows(SheetLifeTimeHigh)
	if err != nil {
		return nil, err
	}
	return parseLifeTimeHigh(rows)
}

func (r *XLSXReader) readBuySignal(f *excelize.File) ([]*BuySignalRow, error) {
	rows, err := f.GetRows(SheetBuySignal)
	if err != nil {
		return nil, err
	}
	return parseBuySignal(rows)
}

func (r *XLSXReader) readIndicesGrade(f *excelize.File) ([]*IndicesGradeRow, error) {
	rows, err := f.GetRows(SheetIndicesGradeRange)
	if err != nil {
		return nil, err
	}
	return parseIndicesGrade(rows)
}

func (r *XLSXReader) readExitStocks(f *excelize.File) ([]*ExitStockDetailRow, error) {
	rows, err := f.GetRows(SheetExitStockDetail)
	if err != nil {
		return nil, err
	}
	return parseExitStocks(rows)
}

// --- Parser helpers ---

// parseFloat parses a string to float64. Returns 0 on failure or empty.
// Handles:  "1,234.56"  "  50%  "  "-"  "None"  "" → correctly parses values.
// Percent values like "50%" are converted to 0.50.
func parseFloat(s string) float64 {
	s = strings.TrimSpace(s)
	if s == "" || s == "-" || s == "None" || s == "N/A" {
		return 0
	}

	// Handle percent formatted cells: "50%" → 0.50
	isPercent := strings.HasSuffix(s, "%")
	s = strings.TrimSuffix(s, "%")
	s = strings.TrimSpace(s)

	// Remove thousands separator
	s = strings.ReplaceAll(s, ",", "")

	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}

	if isPercent {
		return v / 100.0
	}
	return v
}

// parseInt parses a string to int. Returns 0 on failure.
func parseInt(s string) int {
	s = strings.TrimSpace(s)
	if s == "" || s == "-" {
		return 0
	}
	s = strings.ReplaceAll(s, ",", "")
	// Handle float strings like "1.0"
	if strings.Contains(s, ".") {
		f, _ := strconv.ParseFloat(s, 64)
		return int(f)
	}
	v, _ := strconv.Atoi(s)
	return v
}

// parseInt64 parses a string to int64.
func parseInt64(s string) int64 {
	s = strings.TrimSpace(s)
	if s == "" || s == "-" {
		return 0
	}
	s = strings.ReplaceAll(s, ",", "")
	if strings.Contains(s, ".") {
		f, _ := strconv.ParseFloat(s, 64)
		return int64(f)
	}
	v, _ := strconv.ParseInt(s, 10, 64)
	return v
}

// parseBool parses common boolean representations.
func parseBool(s string) bool {
	s = strings.ToLower(strings.TrimSpace(s))
	switch s {
	case "true", "1", "yes", "y":
		return true
	}
	return false
}

// parseDate attempts multiple date formats commonly found in Excel.
func parseDate(s string) time.Time {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}
	}
	formats := []string{
		"2006-01-02 15:04:05",
		"2006-01-02",
		"02/01/2006",       // DD/MM/YYYY
		"2-Jan-2006",
		"02-Jan-2006",
		time.RFC3339,
	}
	for _, f := range formats {
		if t, err := time.Parse(f, s); err == nil {
			return t
		}
	}
	return time.Time{}
}
