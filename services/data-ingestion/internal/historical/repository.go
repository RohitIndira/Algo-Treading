package historical

import (
	"context"
	"database/sql"
	"time"

	"go.uber.org/zap"
)

// Repository handles PostgreSQL operations for the signals_db database.
type Repository struct {
	db     *sql.DB
	logger *zap.Logger
}

// NewRepository creates a new repository backed by the given DB connection.
func NewRepository(db *sql.DB, logger *zap.Logger) *Repository {
	return &Repository{db: db, logger: logger}
}

// --- Instruments ---

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

// GetAllPrevClose returns the most recent close price for every NSE instrument.
// Used for O(1) stock split detection: ratio = ltp / prev_close.
func (r *Repository) GetAllPrevClose(ctx context.Context) (map[string]float64, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT DISTINCT ON (i.symbol) i.symbol, d.close
		FROM daily_ohlcv d
		JOIN instruments i ON i.instrument_id = d.instrument_id
		WHERE i.exchange = 'NSE' AND i.is_active = TRUE
		ORDER BY i.symbol, d.trade_date DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]float64, 4000)
	for rows.Next() {
		var symbol string
		var closePrice float64
		if err := rows.Scan(&symbol, &closePrice); err != nil {
			return nil, err
		}
		result[symbol] = closePrice
	}
	return result, rows.Err()
}

// --- Breakout Events ---

// BreakoutEvent represents a 52W breakout detection.
type BreakoutEvent struct {
	InstrumentID  int
	Token         string
	Symbol        string
	Exchange      string
	BreakoutType  string  // "52W_HIGH" or "52W_LOW"
	BreakoutPrice float64
	Prev52WH      float64
	Prev52WL      float64
	Volume        int64
	PctChange     float64
	Source        string    // "websocket"
	BreakoutAt    time.Time // Actual time of breakout from broker
}

// InsertBreakoutEvent stores a breakout event. On conflict, updates to higher high / lower low.
func (r *Repository) InsertBreakoutEvent(ctx context.Context, evt BreakoutEvent) (bool, error) {
	breakoutAt := evt.BreakoutAt
	if breakoutAt.IsZero() {
		breakoutAt = time.Now()
	}
	result, err := r.db.ExecContext(ctx, `
		INSERT INTO breakout_events
			(instrument_id, token, symbol, exchange, breakout_type,
			 breakout_price, prev_52w_high, prev_52w_low, volume, pct_change, source, detected_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		ON CONFLICT (instrument_id, breakout_type, trade_date) DO UPDATE SET
			breakout_price = CASE
				WHEN breakout_events.breakout_type = '52W_HIGH'
					AND EXCLUDED.breakout_price > breakout_events.breakout_price
				THEN EXCLUDED.breakout_price
				WHEN breakout_events.breakout_type = '52W_LOW'
					AND EXCLUDED.breakout_price < breakout_events.breakout_price
				THEN EXCLUDED.breakout_price
				ELSE breakout_events.breakout_price
			END,
			volume = EXCLUDED.volume,
			detected_at = CASE
				WHEN (breakout_events.breakout_type = '52W_HIGH' AND EXCLUDED.breakout_price > breakout_events.breakout_price)
				  OR (breakout_events.breakout_type = '52W_LOW' AND EXCLUDED.breakout_price < breakout_events.breakout_price)
				THEN EXCLUDED.detected_at
				ELSE breakout_events.detected_at
			END`,
		evt.InstrumentID, evt.Token, evt.Symbol, evt.Exchange, evt.BreakoutType,
		evt.BreakoutPrice, evt.Prev52WH, evt.Prev52WL, evt.Volume, evt.PctChange, evt.Source, breakoutAt)
	if err != nil {
		return false, err
	}
	rows, _ := result.RowsAffected()
	return rows > 0, nil
}

// ResetInstrumentHistory deletes all OHLCV history for an instrument EXCEPT today's data.
// Used after detecting a stock split so stale pre-split data doesn't pollute.
func (r *Repository) ResetInstrumentHistory(ctx context.Context, instrumentID int, keepDate time.Time) error {
	_, err := r.db.ExecContext(ctx,
		`DELETE FROM daily_ohlcv WHERE instrument_id = $1 AND trade_date < $2`,
		instrumentID, keepDate)
	return err
}

// DeleteFalseBreakout removes today's breakout for a stock. Returns true if deleted.
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
