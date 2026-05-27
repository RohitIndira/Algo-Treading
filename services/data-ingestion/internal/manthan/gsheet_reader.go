package manthan

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"
	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
)

// GSheetReader reads the live Manthan strategy Google Sheet via the Sheets API.
// Uses a GCP Service Account (JSON key file) — the service account email must
// have Viewer access on the sheet.
//
// The sheet structure is identical to the exported xlsx (same tab names and
// column orders). All parsers from xlsx_reader.go are reused — we just swap
// the source from excelize rows to Sheets API values.
type GSheetReader struct {
	sheetID         string
	credentialsFile string
	logger          *zap.Logger
	svc             *sheets.Service
}

// NewGSheetReader creates a new live-sheet reader. Call Connect() before ReadAll().
func NewGSheetReader(sheetID, credentialsFile string, logger *zap.Logger) *GSheetReader {
	return &GSheetReader{
		sheetID:         sheetID,
		credentialsFile: credentialsFile,
		logger:          logger,
	}
}

// Connect authenticates with Sheets API using the service account credentials.
func (r *GSheetReader) Connect(ctx context.Context) error {
	svc, err := sheets.NewService(ctx,
		option.WithCredentialsFile(r.credentialsFile),
		option.WithScopes(sheets.SpreadsheetsReadonlyScope),
	)
	if err != nil {
		return fmt.Errorf("sheets.NewService: %w", err)
	}
	r.svc = svc
	return nil
}

// ReadAll fetches all 6 active tabs from the Google Sheet and parses them
// into the same typed structs as the xlsx reader.
func (r *GSheetReader) ReadAll(ctx context.Context) (*ReadResult, error) {
	if r.svc == nil {
		return nil, fmt.Errorf("reader not connected — call Connect() first")
	}

	// First learn which tabs actually exist in this spreadsheet — the
	// Sheets `BatchGet` is all-or-nothing: a single missing range makes
	// the entire call return 404, which used to crash the pipeline if
	// (for example) the operator removed the ExitStockDetail tab. By
	// probing the metadata first we can filter our wanted ranges down
	// to only the tabs that exist; the per-tab `record()` calls below
	// already log a clear warning for tabs that didn't come back, so
	// missing-tab handling stays uniform.
	meta, err := r.svc.Spreadsheets.Get(r.sheetID).
		Fields("sheets.properties.title").
		Context(ctx).
		Do()
	if err != nil {
		return nil, fmt.Errorf("sheets metadata Get: %w", err)
	}
	existing := make(map[string]bool, len(meta.Sheets))
	for _, s := range meta.Sheets {
		if s != nil && s.Properties != nil {
			existing[s.Properties.Title] = true
		}
	}

	// The full set of tabs the pipeline KNOWS HOW TO PARSE, paired with
	// the read-range. Tabs not present in `existing` are skipped here.
	wanted := []struct {
		tab    string
		rng    string
	}{
		{SheetPEAndNetProfit, SheetPEAndNetProfit + "!A:Z"},
		{SheetFScore, SheetFScore + "!A:Z"},
		{SheetLifeTimeHigh, SheetLifeTimeHigh + "!A:Z"},
		{SheetBuySignal, SheetBuySignal + "!A:AZ"},
		{SheetIndicesGradeRange, SheetIndicesGradeRange + "!A:AZ"},
		{SheetIndustry, SheetIndustry + "!A:Z"},
		{SheetExitStockDetail, SheetExitStockDetail + "!A:D"},
	}
	ranges := make([]string, 0, len(wanted))
	skipped := make([]string, 0)
	for _, w := range wanted {
		if existing[w.tab] {
			ranges = append(ranges, w.rng)
		} else {
			skipped = append(skipped, w.tab)
		}
	}
	if len(skipped) > 0 {
		r.logger.Warn("Sheet tabs missing from spreadsheet — skipping",
			zap.Strings("missing_tabs", skipped),
			zap.Int("present_tabs", len(ranges)))
	}
	if len(ranges) == 0 {
		return nil, fmt.Errorf("sheets: none of the expected tabs %v are present in spreadsheet %s",
			func() []string { out := make([]string, len(wanted)); for i, w := range wanted { out[i] = w.tab }; return out }(),
			r.sheetID)
	}

	start := time.Now()
	resp, err := r.svc.Spreadsheets.Values.BatchGet(r.sheetID).
		Ranges(ranges...).
		ValueRenderOption("FORMATTED_VALUE").
		DateTimeRenderOption("FORMATTED_STRING").
		Context(ctx).
		Do()
	if err != nil {
		return nil, fmt.Errorf("sheets BatchGet: %w", err)
	}
	r.logger.Info("Fetched live sheet",
		zap.Int("tabs", len(resp.ValueRanges)),
		zap.Duration("elapsed", time.Since(start)),
	)

	// Build tab→rows index so we can match by Range prefix.
	rowsByTab := make(map[string][][]string, len(resp.ValueRanges))
	for _, vr := range resp.ValueRanges {
		tab := strings.TrimSpace(strings.Split(vr.Range, "!")[0])
		tab = strings.Trim(tab, "'") // strip quotes if present
		rowsByTab[tab] = interfaceGridToStrings(vr.Values)
	}

	result := &ReadResult{}
	var firstErr error
	record := func(name string, fn func([][]string) error) {
		rows, ok := rowsByTab[name]
		if !ok {
			r.logger.Warn("Sheet tab missing", zap.String("tab", name))
			return
		}
		if err := fn(rows); err != nil {
			r.logger.Error("Failed to parse tab", zap.String("tab", name), zap.Error(err))
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	record(SheetPEAndNetProfit, func(rows [][]string) error {
		parsed, err := parsePEAndNetProfit(rows)
		if err == nil {
			result.PEAndNetProfit = parsed
			r.logger.Info("Parsed tab", zap.String("tab", SheetPEAndNetProfit), zap.Int("rows", len(parsed)))
		}
		return err
	})
	record(SheetFScore, func(rows [][]string) error {
		parsed, err := parseFScore(rows)
		if err == nil {
			result.FScores = parsed
			r.logger.Info("Parsed tab", zap.String("tab", SheetFScore), zap.Int("rows", len(parsed)))
		}
		return err
	})
	record(SheetLifeTimeHigh, func(rows [][]string) error {
		parsed, err := parseLifeTimeHigh(rows)
		if err == nil {
			result.LifeTimeHighs = parsed
			r.logger.Info("Parsed tab", zap.String("tab", SheetLifeTimeHigh), zap.Int("rows", len(parsed)))
		}
		return err
	})
	record(SheetBuySignal, func(rows [][]string) error {
		parsed, err := parseBuySignal(rows)
		if err == nil {
			result.BuySignals = parsed
			r.logger.Info("Parsed tab", zap.String("tab", SheetBuySignal), zap.Int("rows", len(parsed)))
		}
		return err
	})
	record(SheetIndicesGradeRange, func(rows [][]string) error {
		parsed, err := parseIndicesGrade(rows)
		if err == nil {
			result.IndicesGrades = parsed
			r.logger.Info("Parsed tab", zap.String("tab", SheetIndicesGradeRange), zap.Int("rows", len(parsed)))
		}
		return err
	})
	record(SheetIndustry, func(rows [][]string) error {
		parsed, err := parseIndustry(rows)
		if err == nil {
			result.Industries = parsed
			r.logger.Info("Parsed tab", zap.String("tab", SheetIndustry), zap.Int("rows", len(parsed)))
		}
		return err
	})
	record(SheetExitStockDetail, func(rows [][]string) error {
		parsed, err := parseExitStocks(rows)
		if err == nil {
			result.ExitStocks = parsed
			r.logger.Info("Parsed tab", zap.String("tab", SheetExitStockDetail), zap.Int("rows", len(parsed)))
		}
		return err
	})
	return result, firstErr
}

// interfaceGridToStrings converts Sheets API [][]interface{} to [][]string.
func interfaceGridToStrings(grid [][]interface{}) [][]string {
	out := make([][]string, 0, len(grid))
	for _, row := range grid {
		r := make([]string, len(row))
		for i, v := range row {
			if v == nil {
				continue
			}
			r[i] = fmt.Sprintf("%v", v)
		}
		out = append(out, r)
	}
	return out
}
