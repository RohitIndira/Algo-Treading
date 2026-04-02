package historical

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
)

// OHLCVRecord is the normalized output from any bhavcopy format.
type OHLCVRecord struct {
	Symbol         string
	Series         string
	ISIN           string
	Token          string    // Exchange-specific code (BSE scrip code or NSE token)
	Exchange       string    // "NSE" or "BSE"
	TradeDate      time.Time
	Open           float64
	High           float64
	Low            float64
	Close          float64
	PrevClose      float64
	Volume         int64
	DeliveryVolume int64
	DeliveryPct    float64
	Turnover       float64
	NumTrades      int64
}

// Parser converts raw bhavcopy CSV into normalized OHLCV records.
type Parser struct {
	logger *zap.Logger
}

// NewParser creates a new CSV parser.
func NewParser(logger *zap.Logger) *Parser {
	return &Parser{logger: logger}
}

// Parse auto-detects the CSV format and parses accordingly.
func (p *Parser) Parse(data *BhavData) ([]OHLCVRecord, error) {
	switch data.Source {
	case "nse_sec_bhav":
		return p.parseNSESecBhav(data)
	case "nse_udiff":
		return p.parseUDiFF(data, "NSE")
	case "nse_old":
		return p.parseNSEOld(data)
	case "bse_udiff":
		return p.parseUDiFF(data, "BSE")
	case "bse_old":
		return p.parseBSEOld(data)
	default:
		return nil, fmt.Errorf("unknown source format: %s", data.Source)
	}
}

// parseNSESecBhav parses NSE sec_bhavdata_full CSV.
// Columns: SYMBOL, SERIES, DATE1, PREV_CLOSE, OPEN_PRICE, HIGH_PRICE, LOW_PRICE,
//
//	LAST_PRICE, CLOSE_PRICE, AVG_PRICE, TTL_TRD_QNTY, TURNOVER_LACS,
//	NO_OF_TRADES, DELIV_QTY, DELIV_PER
func (p *Parser) parseNSESecBhav(data *BhavData) ([]OHLCVRecord, error) {
	reader := csv.NewReader(strings.NewReader(string(data.CSV)))
	reader.TrimLeadingSpace = true
	reader.LazyQuotes = true

	// Read header
	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	colIdx := mapColumns(header)

	var records []OHLCVRecord
	lineNum := 1

	for {
		lineNum++
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			p.logger.Warn("Skip malformed row", zap.Int("line", lineNum), zap.Error(err))
			continue
		}

		symbol := getCol(row, colIdx, "SYMBOL")
		series := getCol(row, colIdx, "SERIES")

		// Only keep equity series (EQ, BE, BZ, SM, ST)
		if !isEquitySeries(series) {
			continue
		}

		rec := OHLCVRecord{
			Symbol:         strings.TrimSpace(symbol),
			Series:         strings.TrimSpace(series),
			Exchange:       "NSE",
			TradeDate:      data.TradeDate,
			Open:           parseFloat(getCol(row, colIdx, "OPEN_PRICE")),
			High:           parseFloat(getCol(row, colIdx, "HIGH_PRICE")),
			Low:            parseFloat(getCol(row, colIdx, "LOW_PRICE")),
			Close:          parseFloat(getCol(row, colIdx, "CLOSE_PRICE")),
			PrevClose:      parseFloat(getCol(row, colIdx, "PREV_CLOSE")),
			Volume:         parseInt(getCol(row, colIdx, "TTL_TRD_QNTY")),
			Turnover:       parseFloat(getCol(row, colIdx, "TURNOVER_LACS")) * 100000, // Convert lacs to actual
			NumTrades:      parseInt(getCol(row, colIdx, "NO_OF_TRADES")),
			DeliveryVolume: parseInt(getCol(row, colIdx, "DELIV_QTY")),
			DeliveryPct:    parseFloat(getCol(row, colIdx, "DELIV_PER")),
		}

		if rec.Open == 0 && rec.High == 0 && rec.Low == 0 && rec.Close == 0 {
			continue // skip empty rows
		}

		records = append(records, rec)
	}

	return records, nil
}

// parseNSEOld parses the old NSE cm*bhav.csv.zip format (2006-2019).
// Columns: SYMBOL, SERIES, OPEN, HIGH, LOW, CLOSE, LAST, PREVCLOSE,
//
//	TOTTRDQTY, TOTTRDVAL, TIMESTAMP, TOTALTRADES, ISIN
func (p *Parser) parseNSEOld(data *BhavData) ([]OHLCVRecord, error) {
	reader := csv.NewReader(strings.NewReader(string(data.CSV)))
	reader.TrimLeadingSpace = true
	reader.LazyQuotes = true

	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	colIdx := mapColumns(header)

	var records []OHLCVRecord
	lineNum := 1

	for {
		lineNum++
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			p.logger.Warn("Skip malformed row", zap.Int("line", lineNum), zap.Error(err))
			continue
		}

		series := strings.TrimSpace(getCol(row, colIdx, "SERIES"))
		if !isEquitySeries(series) {
			continue
		}

		rec := OHLCVRecord{
			Symbol:    strings.TrimSpace(getCol(row, colIdx, "SYMBOL")),
			Series:    series,
			ISIN:      strings.TrimSpace(getCol(row, colIdx, "ISIN")),
			Exchange:  "NSE",
			TradeDate: data.TradeDate,
			Open:      parseFloat(getCol(row, colIdx, "OPEN")),
			High:      parseFloat(getCol(row, colIdx, "HIGH")),
			Low:       parseFloat(getCol(row, colIdx, "LOW")),
			Close:     parseFloat(getCol(row, colIdx, "CLOSE")),
			PrevClose: parseFloat(getCol(row, colIdx, "PREVCLOSE")),
			Volume:    parseInt(getCol(row, colIdx, "TOTTRDQTY")),
			Turnover:  parseFloat(getCol(row, colIdx, "TOTTRDVAL")),
			NumTrades: parseInt(getCol(row, colIdx, "TOTALTRADES")),
		}

		if rec.Open == 0 && rec.High == 0 && rec.Low == 0 && rec.Close == 0 {
			continue
		}

		records = append(records, rec)
	}

	return records, nil
}

// parseUDiFF parses the new UDiFF format used by both NSE and BSE (post July 2024).
// 34 columns — we extract the OHLCV fields we need.
// Filter: FinInstrmTp=STK, Sgmt=CM (or CASH), SsnId starts with F (final session)
func (p *Parser) parseUDiFF(data *BhavData, exchange string) ([]OHLCVRecord, error) {
	reader := csv.NewReader(strings.NewReader(string(data.CSV)))
	reader.TrimLeadingSpace = true
	reader.LazyQuotes = true

	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	colIdx := mapColumns(header)

	var records []OHLCVRecord
	lineNum := 1

	for {
		lineNum++
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			p.logger.Warn("Skip malformed row", zap.Int("line", lineNum), zap.Error(err))
			continue
		}

		// Filter: only stocks (not derivatives), capital market segment, final session
		instType := strings.TrimSpace(getCol(row, colIdx, "FinInstrmTp"))
		segment := strings.TrimSpace(getCol(row, colIdx, "Sgmt"))
		sessionID := strings.TrimSpace(getCol(row, colIdx, "SsnId"))
		series := strings.TrimSpace(getCol(row, colIdx, "SctySrs"))

		if instType != "STK" {
			continue
		}
		if segment != "CM" && segment != "CASH" {
			continue
		}
		// Only final session data (F1, F2) — skip interim/pre-open (I1, I2)
		if !strings.HasPrefix(sessionID, "F") {
			continue
		}
		if !isEquitySeries(series) {
			continue
		}

		rec := OHLCVRecord{
			Symbol:    strings.TrimSpace(getCol(row, colIdx, "TckrSymb")),
			Series:    series,
			ISIN:      strings.TrimSpace(getCol(row, colIdx, "ISIN")),
			Token:     strings.TrimSpace(getCol(row, colIdx, "FinInstrmId")),
			Exchange:  exchange,
			TradeDate: data.TradeDate,
			Open:      parseFloat(getCol(row, colIdx, "OpnPric")),
			High:      parseFloat(getCol(row, colIdx, "HghPric")),
			Low:       parseFloat(getCol(row, colIdx, "LwPric")),
			Close:     parseFloat(getCol(row, colIdx, "ClsPric")),
			PrevClose: parseFloat(getCol(row, colIdx, "PrvsClsgPric")),
			Volume:    parseInt(getCol(row, colIdx, "TtlTradgVol")),
			Turnover:  parseFloat(getCol(row, colIdx, "TtlTrfVal")),
			NumTrades: parseInt(getCol(row, colIdx, "TtlNbOfTxsExctd")),
		}

		if rec.Open == 0 && rec.High == 0 && rec.Low == 0 && rec.Close == 0 {
			continue
		}

		records = append(records, rec)
	}

	return records, nil
}

// parseBSEOld parses the old BSE EQ_ISINCODE format (pre-July 2024).
// Columns: SC_CODE, SC_NAME, SC_GROUP, SC_TYPE, OPEN, HIGH, LOW, CLOSE, LAST,
//
//	PREVCLOSE, NO_TRADES, NO_OF_SHRS, NET_TURNOV, TDCLOINDI, ISIN_CODE,
//	TRADING_DATE, FILLER2, FILLER3
func (p *Parser) parseBSEOld(data *BhavData) ([]OHLCVRecord, error) {
	reader := csv.NewReader(strings.NewReader(string(data.CSV)))
	reader.TrimLeadingSpace = true
	reader.LazyQuotes = true

	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	colIdx := mapColumns(header)

	var records []OHLCVRecord
	lineNum := 1

	for {
		lineNum++
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			p.logger.Warn("Skip malformed row", zap.Int("line", lineNum), zap.Error(err))
			continue
		}

		scGroup := strings.TrimSpace(getCol(row, colIdx, "SC_GROUP"))
		// Filter for equity groups (A, B, T, Z, etc.) — skip derivatives
		if scGroup == "" {
			continue
		}

		rec := OHLCVRecord{
			Symbol:    strings.TrimSpace(getCol(row, colIdx, "SC_NAME")),
			Token:     strings.TrimSpace(getCol(row, colIdx, "SC_CODE")),
			ISIN:      strings.TrimSpace(getCol(row, colIdx, "ISIN_CODE")),
			Series:    scGroup,
			Exchange:  "BSE",
			TradeDate: data.TradeDate,
			Open:      parseFloat(getCol(row, colIdx, "OPEN")),
			High:      parseFloat(getCol(row, colIdx, "HIGH")),
			Low:       parseFloat(getCol(row, colIdx, "LOW")),
			Close:     parseFloat(getCol(row, colIdx, "CLOSE")),
			PrevClose: parseFloat(getCol(row, colIdx, "PREVCLOSE")),
			Volume:    parseInt(getCol(row, colIdx, "NO_OF_SHRS")),
			Turnover:  parseFloat(getCol(row, colIdx, "NET_TURNOV")),
			NumTrades: parseInt(getCol(row, colIdx, "NO_TRADES")),
		}

		if rec.Open == 0 && rec.High == 0 && rec.Low == 0 && rec.Close == 0 {
			continue
		}

		records = append(records, rec)
	}

	return records, nil
}

// --- Helper functions ---

// mapColumns builds a case-insensitive column name -> index map from the header row.
func mapColumns(header []string) map[string]int {
	m := make(map[string]int, len(header))
	for i, col := range header {
		key := strings.TrimSpace(col)
		// Store both original and uppercase for case-insensitive lookup
		m[key] = i
		m[strings.ToUpper(key)] = i
	}
	return m
}

// getCol safely retrieves a column value by name.
func getCol(row []string, colIdx map[string]int, name string) string {
	idx, ok := colIdx[name]
	if !ok {
		idx, ok = colIdx[strings.ToUpper(name)]
		if !ok {
			return ""
		}
	}
	if idx >= len(row) {
		return ""
	}
	return strings.TrimSpace(row[idx])
}

// parseFloat converts a string to float64, returning 0 on failure.
func parseFloat(s string) float64 {
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, ",", "") // Remove thousands separator
	if s == "" || s == "-" {
		return 0
	}
	v, _ := strconv.ParseFloat(s, 64)
	return v
}

// parseInt converts a string to int64, returning 0 on failure.
func parseInt(s string) int64 {
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, ",", "")
	if s == "" || s == "-" {
		return 0
	}
	v, _ := strconv.ParseInt(s, 10, 64)
	return v
}

// isEquitySeries returns true if the series code represents equity shares.
func isEquitySeries(series string) bool {
	series = strings.TrimSpace(strings.ToUpper(series))
	switch series {
	case "EQ", "BE", "BZ", "SM", "ST", "A", "B", "T", "Z", "X", "XT", "P", "":
		return true
	}
	return false
}
