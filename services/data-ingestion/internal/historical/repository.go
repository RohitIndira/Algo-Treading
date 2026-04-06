package historical

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"
)

// Repository handles PostgreSQL operations for the market_data database.
type Repository struct {
	db     *sql.DB
	logger *zap.Logger
}

// NewRepository creates a new repository backed by the given DB connection.
func NewRepository(db *sql.DB, logger *zap.Logger) *Repository {
	return &Repository{db: db, logger: logger}
}

// --- Instruments ---

// UpsertInstrument inserts or updates an instrument. Returns the instrument_id.
func (r *Repository) UpsertInstrument(ctx context.Context, rec OHLCVRecord) (int, error) {
	var id int
	err := r.db.QueryRowContext(ctx, `
		INSERT INTO instruments (token, symbol, series, isin, exchange, is_active, updated_at)
		VALUES ($1, $2, $3, $4, $5, TRUE, NOW())
		ON CONFLICT (token, exchange) DO UPDATE SET
			symbol = EXCLUDED.symbol,
			series = EXCLUDED.series,
			isin = COALESCE(NULLIF(EXCLUDED.isin, ''), instruments.isin),
			is_active = TRUE,
			updated_at = NOW()
		RETURNING instrument_id`,
		rec.Token, rec.Symbol, rec.Series, rec.ISIN, rec.Exchange,
	).Scan(&id)
	return id, err
}

// GetInstrumentID looks up instrument_id by token+exchange. Returns 0 if not found.
func (r *Repository) GetInstrumentID(ctx context.Context, token, exchange string) (int, error) {
	var id int
	err := r.db.QueryRowContext(ctx,
		`SELECT instrument_id FROM instruments WHERE token = $1 AND exchange = $2`,
		token, exchange,
	).Scan(&id)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return id, err
}

// instrumentCache is an in-memory cache to avoid repeated DB lookups during bulk inserts.
type instrumentCache struct {
	repo  *Repository
	cache map[string]int // key: "token:exchange" -> instrument_id
}

func newInstrumentCache(repo *Repository) *instrumentCache {
	return &instrumentCache{repo: repo, cache: make(map[string]int, 8000)}
}

func (ic *instrumentCache) getOrCreate(ctx context.Context, rec OHLCVRecord) (int, error) {
	key := rec.Token + ":" + rec.Exchange
	if id, ok := ic.cache[key]; ok {
		return id, nil
	}

	// Try symbol:exchange if token is empty
	if rec.Token == "" {
		key = rec.Symbol + ":" + rec.Exchange
		if id, ok := ic.cache[key]; ok {
			return id, nil
		}
		// Use symbol as token for NSE records that don't have token
		rec.Token = rec.Symbol
	}

	id, err := ic.repo.UpsertInstrument(ctx, rec)
	if err != nil {
		return 0, err
	}
	ic.cache[key] = id
	return id, nil
}

// --- Daily OHLCV ---

// BulkUpsertOHLCV inserts OHLCV records in batches using multi-row INSERT ... ON CONFLICT.
// Returns (rows inserted, rows skipped/updated, error).
func (r *Repository) BulkUpsertOHLCV(ctx context.Context, records []OHLCVRecord, source string) (int, int, error) {
	if len(records) == 0 {
		return 0, 0, nil
	}

	ic := newInstrumentCache(r)
	inserted := 0
	skipped := 0
	batchSize := 500

	for i := 0; i < len(records); i += batchSize {
		end := i + batchSize
		if end > len(records) {
			end = len(records)
		}
		batch := records[i:end]

		ins, skip, err := r.upsertBatch(ctx, ic, batch, source)
		if err != nil {
			return inserted, skipped, fmt.Errorf("batch %d-%d: %w", i, end, err)
		}
		inserted += ins
		skipped += skip
	}

	return inserted, skipped, nil
}

func (r *Repository) upsertBatch(ctx context.Context, ic *instrumentCache, batch []OHLCVRecord, source string) (int, int, error) {
	if len(batch) == 0 {
		return 0, 0, nil
	}

	// Build multi-row VALUES clause
	var sb strings.Builder
	sb.WriteString(`INSERT INTO daily_ohlcv
		(instrument_id, trade_date, open, high, low, close, volume,
		 delivery_volume, delivery_pct, turnover, prev_close, pct_change, source)
		VALUES `)

	args := make([]interface{}, 0, len(batch)*13)
	paramIdx := 1

	for j, rec := range batch {
		instID, err := ic.getOrCreate(ctx, rec)
		if err != nil {
			r.logger.Warn("Skip record: instrument lookup failed",
				zap.String("symbol", rec.Symbol),
				zap.Error(err))
			continue
		}

		if j > 0 {
			sb.WriteString(",")
		}

		var pctChange float64
		if rec.PrevClose > 0 {
			pctChange = ((rec.Close - rec.PrevClose) / rec.PrevClose) * 100
		}

		sb.WriteString(fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
			paramIdx, paramIdx+1, paramIdx+2, paramIdx+3, paramIdx+4, paramIdx+5,
			paramIdx+6, paramIdx+7, paramIdx+8, paramIdx+9, paramIdx+10, paramIdx+11, paramIdx+12))
		paramIdx += 13

		args = append(args,
			instID, rec.TradeDate, rec.Open, rec.High, rec.Low, rec.Close,
			rec.Volume, rec.DeliveryVolume, rec.DeliveryPct, rec.Turnover,
			rec.PrevClose, pctChange, source,
		)
	}

	sb.WriteString(` ON CONFLICT (instrument_id, trade_date) DO UPDATE SET
		open = EXCLUDED.open,
		high = EXCLUDED.high,
		low = EXCLUDED.low,
		close = EXCLUDED.close,
		volume = EXCLUDED.volume,
		delivery_volume = EXCLUDED.delivery_volume,
		delivery_pct = EXCLUDED.delivery_pct,
		turnover = EXCLUDED.turnover,
		prev_close = EXCLUDED.prev_close,
		pct_change = EXCLUDED.pct_change,
		source = EXCLUDED.source`)

	if len(args) == 0 {
		return 0, len(batch), nil
	}

	result, err := r.db.ExecContext(ctx, sb.String(), args...)
	if err != nil {
		return 0, 0, fmt.Errorf("bulk upsert: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	return int(rowsAffected), len(batch) - int(rowsAffected), nil
}

// --- Materialized View ---

// RefreshMaterializedView refreshes mv_52w_high_low concurrently (no read locks).
func (r *Repository) RefreshMaterializedView(ctx context.Context) error {
	_, err := r.db.ExecContext(ctx, "REFRESH MATERIALIZED VIEW CONCURRENTLY mv_52w_high_low")
	return err
}

// --- 52W Data for Redis Sync ---

// Week52Data holds pre-computed 52W data for a single instrument.
type Week52Data struct {
	InstrumentID    int
	Token           string
	Symbol          string
	ISIN            string
	Exchange        string
	Week52High      float64
	Week52Low       float64
	LastClose       float64
	LastTradeDate   time.Time
	PositionInRange float64 // 0-100%
	DataDays        int     // Fix #4: number of trading days of data (< 252 = partial/IPO)
}

// GetAll52WData reads the entire materialized view for Redis sync.
func (r *Repository) GetAll52WData(ctx context.Context) ([]Week52Data, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT instrument_id, token, symbol, COALESCE(isin,''), exchange,
		       week_52_high, week_52_low, COALESCE(last_close, 0),
		       last_trade_date, COALESCE(position_in_52w_range, 0),
		       COALESCE(data_days, 0)
		FROM mv_52w_high_low`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []Week52Data
	for rows.Next() {
		var d Week52Data
		if err := rows.Scan(&d.InstrumentID, &d.Token, &d.Symbol, &d.ISIN, &d.Exchange,
			&d.Week52High, &d.Week52Low, &d.LastClose,
			&d.LastTradeDate, &d.PositionInRange, &d.DataDays); err != nil {
			return nil, err
		}
		result = append(result, d)
	}
	return result, rows.Err()
}

// --- Data Load Log ---

// StartLoadLog creates a new entry in data_load_log and returns the load_id.
func (r *Repository) StartLoadLog(ctx context.Context, source, exchange string, tradeDate time.Time, fileName string, fileSize int64) (int, error) {
	var id int
	err := r.db.QueryRowContext(ctx, `
		INSERT INTO data_load_log (source, trade_date, exchange, file_name, file_size_bytes, status)
		VALUES ($1, $2, $3, $4, $5, 'RUNNING')
		RETURNING load_id`,
		source, tradeDate, exchange, fileName, fileSize,
	).Scan(&id)
	return id, err
}

// CompleteLoadLog marks a load as successful.
func (r *Repository) CompleteLoadLog(ctx context.Context, loadID, rowsLoaded, rowsSkipped int, durationMs int) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE data_load_log SET
			status = 'SUCCESS',
			rows_loaded = $1,
			rows_skipped = $2,
			duration_ms = $3,
			completed_at = NOW()
		WHERE load_id = $4`,
		rowsLoaded, rowsSkipped, durationMs, loadID)
	return err
}

// FailLoadLog marks a load as failed.
func (r *Repository) FailLoadLog(ctx context.Context, loadID int, errMsg string) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE data_load_log SET
			status = 'FAILED',
			error_message = $1,
			completed_at = NOW()
		WHERE load_id = $2`,
		errMsg, loadID)
	return err
}

// IsDateLoaded checks if a date+exchange has already been successfully loaded.
func (r *Repository) IsDateLoaded(ctx context.Context, tradeDate time.Time, exchange string) (bool, error) {
	var exists bool
	err := r.db.QueryRowContext(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM data_load_log
			WHERE trade_date = $1 AND exchange = $2 AND status = 'SUCCESS'
		)`, tradeDate, exchange).Scan(&exists)
	return exists, err
}

// --- Cleanup ---

// CleanupOldData deletes OHLCV rows older than 370 calendar days.
// Uses calendar days (not trading days) so holidays don't cause over-deletion.
// Also cleans up old load log entries.
func (r *Repository) CleanupOldData(ctx context.Context) (int64, error) {
	result, err := r.db.ExecContext(ctx,
		`DELETE FROM daily_ohlcv WHERE trade_date < CURRENT_DATE - INTERVAL '370 days'`)
	if err != nil {
		return 0, fmt.Errorf("cleanup ohlcv: %w", err)
	}
	deleted, _ := result.RowsAffected()

	// Also cleanup old load log (best effort, non-critical)
	_, _ = r.db.ExecContext(ctx,
		`DELETE FROM data_load_log WHERE trade_date < CURRENT_DATE - INTERVAL '370 days'`)

	return deleted, nil
}

// --- Stock Split / Corporate Action Detection ---

// SplitDetection holds info about a detected stock split or corporate action.
type SplitDetection struct {
	InstrumentID int
	Symbol       string
	Exchange     string
	PrevClose    float64
	TodayOpen    float64
	GapPct       float64
}

// DetectSplits finds stocks where today's open price has a >30% gap from
// the previous close. This indicates a stock split, bonus, or corporate action.
// Returns the list of affected instruments so their history can be reset.
func (r *Repository) DetectSplits(ctx context.Context, tradeDate time.Time) ([]SplitDetection, error) {
	rows, err := r.db.QueryContext(ctx, `
		WITH today AS (
			SELECT instrument_id, open, trade_date
			FROM daily_ohlcv
			WHERE trade_date = $1
		),
		prev AS (
			SELECT DISTINCT ON (instrument_id)
				instrument_id, close
			FROM daily_ohlcv
			WHERE trade_date < $1
			ORDER BY instrument_id, trade_date DESC
		)
		SELECT t.instrument_id, i.symbol, i.exchange, p.close AS prev_close, t.open AS today_open,
			   ABS((t.open - p.close) / NULLIF(p.close, 0) * 100) AS gap_pct
		FROM today t
		JOIN prev p ON p.instrument_id = t.instrument_id
		JOIN instruments i ON i.instrument_id = t.instrument_id
		WHERE p.close > 0
		  AND ABS((t.open - p.close) / p.close * 100) > 30`, tradeDate)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []SplitDetection
	for rows.Next() {
		var d SplitDetection
		if err := rows.Scan(&d.InstrumentID, &d.Symbol, &d.Exchange,
			&d.PrevClose, &d.TodayOpen, &d.GapPct); err != nil {
			return nil, err
		}
		result = append(result, d)
	}
	return result, rows.Err()
}

// ResetInstrumentHistory deletes all OHLCV history for an instrument EXCEPT today's data.
// Used after detecting a stock split so stale pre-split data doesn't pollute 52W calculations.
func (r *Repository) ResetInstrumentHistory(ctx context.Context, instrumentID int, keepDate time.Time) error {
	_, err := r.db.ExecContext(ctx,
		`DELETE FROM daily_ohlcv WHERE instrument_id = $1 AND trade_date < $2`,
		instrumentID, keepDate)
	return err
}

// --- Redis Recovery ---

// Get52WForSymbol returns 52W data for a specific symbol+exchange from the materialized view.
func (r *Repository) Get52WForSymbol(ctx context.Context, symbol, exchange string) (*Week52Data, error) {
	var d Week52Data
	err := r.db.QueryRowContext(ctx, `
		SELECT instrument_id, token, symbol, COALESCE(isin,''), exchange,
		       week_52_high, week_52_low, COALESCE(last_close, 0),
		       last_trade_date, COALESCE(position_in_52w_range, 0), COALESCE(data_days, 0)
		FROM mv_52w_high_low
		WHERE symbol = $1 AND exchange = $2
		LIMIT 1`, symbol, exchange,
	).Scan(&d.InstrumentID, &d.Token, &d.Symbol, &d.ISIN, &d.Exchange,
		&d.Week52High, &d.Week52Low, &d.LastClose,
		&d.LastTradeDate, &d.PositionInRange, &d.DataDays)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &d, nil
}

// SplitSuspect represents a stock where 52W high is >2x last close (likely stock split).
type SplitSuspect struct {
	InstrumentID int
	Symbol       string
	Exchange     string
	Week52High   float64
	LastClose    float64
}

// FindSplitSuspects finds stocks in the matview where 52W high is more than
// 2x the last close price. This is a strong signal of an unhandled stock split.
func (r *Repository) FindSplitSuspects(ctx context.Context) ([]SplitSuspect, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT instrument_id, symbol, exchange, week_52_high, last_close
		FROM mv_52w_high_low
		WHERE last_close > 0 AND week_52_high > last_close * 2`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []SplitSuspect
	for rows.Next() {
		var s SplitSuspect
		if err := rows.Scan(&s.InstrumentID, &s.Symbol, &s.Exchange,
			&s.Week52High, &s.LastClose); err != nil {
			return nil, err
		}
		result = append(result, s)
	}
	return result, rows.Err()
}

// Has52WData checks if the materialized view has any data.
func (r *Repository) Has52WData(ctx context.Context) (bool, error) {
	var count int
	err := r.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM mv_52w_high_low`).Scan(&count)
	return count > 0, err
}

// --- Breakout Events ---

// BreakoutEvent represents a 52W breakout detection.
type BreakoutEvent struct {
	InstrumentID    int
	Token           string
	Symbol          string
	Exchange        string
	BreakoutType    string  // "52W_HIGH" or "52W_LOW"
	BreakoutPrice   float64
	Prev52WH        float64
	Prev52WL        float64
	Volume          int64
	PctChange       float64
	Source          string    // "websocket" or "bhavcopy"
	BreakoutAt      time.Time // Actual time of breakout from broker (not our detection time)
}

// InsertBreakoutEvent stores a breakout event. Uses ON CONFLICT to ignore
// duplicate breakouts for the same stock on the same day (only first detection counts).
// Returns true if inserted (new breakout), false if already existed.
func (r *Repository) InsertBreakoutEvent(ctx context.Context, evt BreakoutEvent) (bool, error) {
	// Use broker's breakout timestamp if available, otherwise use current time
	breakoutAt := evt.BreakoutAt
	if breakoutAt.IsZero() {
		breakoutAt = time.Now()
	}
	result, err := r.db.ExecContext(ctx, `
		INSERT INTO breakout_events
			(instrument_id, token, symbol, exchange, breakout_type,
			 breakout_price, prev_52w_high, prev_52w_low, volume, pct_change, source, detected_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		ON CONFLICT (instrument_id, breakout_type, trade_date) DO NOTHING`,
		evt.InstrumentID, evt.Token, evt.Symbol, evt.Exchange, evt.BreakoutType,
		evt.BreakoutPrice, evt.Prev52WH, evt.Prev52WL, evt.Volume, evt.PctChange, evt.Source, breakoutAt)
	if err != nil {
		return false, err
	}
	rows, _ := result.RowsAffected()
	return rows > 0, nil
}

// GetTodayBreakouts returns all breakout events for today.
func (r *Repository) GetTodayBreakouts(ctx context.Context) ([]BreakoutEvent, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT instrument_id, token, symbol, exchange, breakout_type,
		       breakout_price, COALESCE(prev_52w_high,0), COALESCE(prev_52w_low,0),
		       COALESCE(volume,0), COALESCE(pct_change,0), source
		FROM breakout_events
		WHERE trade_date = CURRENT_DATE
		ORDER BY detected_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []BreakoutEvent
	for rows.Next() {
		var e BreakoutEvent
		if err := rows.Scan(&e.InstrumentID, &e.Token, &e.Symbol, &e.Exchange,
			&e.BreakoutType, &e.BreakoutPrice, &e.Prev52WH, &e.Prev52WL,
			&e.Volume, &e.PctChange, &e.Source); err != nil {
			return nil, err
		}
		result = append(result, e)
	}
	return result, rows.Err()
}

// DeleteFalseBreakout removes a today's breakout that turned out to be false
// (WebSocket provided more accurate 52W values). Returns true if a row was deleted.
func (r *Repository) DeleteFalseBreakout(ctx context.Context, symbol, breakoutType string) (bool, error) {
	result, err := r.db.ExecContext(ctx, `
		DELETE FROM breakout_events
		WHERE symbol = $1 AND breakout_type = $2 AND trade_date = CURRENT_DATE`,
		symbol, breakoutType)
	if err != nil {
		return false, err
	}
	rows, _ := result.RowsAffected()
	return rows > 0, nil
}

// GetInstrumentBySymbol looks up instrument by symbol+exchange.
func (r *Repository) GetInstrumentBySymbol(ctx context.Context, symbol, exchange string) (int, string, error) {
	var id int
	var token string
	err := r.db.QueryRowContext(ctx,
		`SELECT instrument_id, token FROM instruments WHERE symbol = $1 AND exchange = $2 LIMIT 1`,
		symbol, exchange).Scan(&id, &token)
	if err == sql.ErrNoRows {
		return 0, "", nil
	}
	return id, token, err
}

// GetAllNSESymbols returns all active NSE instrument symbols for WebSocket subscription.
func (r *Repository) GetAllNSESymbols(ctx context.Context) ([]string, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT DISTINCT symbol FROM instruments WHERE exchange = 'NSE' AND is_active = TRUE AND symbol != ''`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var symbols []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		symbols = append(symbols, s)
	}
	return symbols, rows.Err()
}
